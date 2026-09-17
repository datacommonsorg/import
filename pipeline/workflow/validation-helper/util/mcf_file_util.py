# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Utility functions to read, write, normalize, and fingerprint MCF nodes."""

from collections import OrderedDict
import csv
import hashlib
import os
import re
import sys
from typing import Union
from absl import logging

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
sys.path.append(_SCRIPT_DIR)
sys.path.append(os.path.dirname(_SCRIPT_DIR))

import file_util
from counters import Counters

_DEFAULT_NODE_PVS = OrderedDict({
    'Node': '',
    'typeOf': '',
    'subClassOf': '',
    'name': '',
    'description': '',
    'populationType': '',
    'measuredProperty': '',
    'measurementQualifier': '',
    'statType': '',
    'measurementDenominator': '',
})

_STATVAR_DCID_IGNORE_PROPS = {
    'name', 'description', 'descriptionUrl', 'alternateName',
    'nameWithLanguage', 'constraintProperties', 'memberOf', 'provenance'
}


def add_namespace(value: str, namespace: str = 'dcid') -> str:
    """Returns the value with a namespace prefix for references."""
    if isinstance(value, list):
        value_list = [add_namespace(v) for v in value]
        return ','.join(value_list)
    if value and isinstance(value, str):
        if value[0].isalpha() or value[0].isdigit():
            if ',' in value:
                value_list = get_value_list(value)
                return ','.join([add_namespace(v) for v in value_list])
            has_alpha = False
            for c in value:
                if c.isalpha() or c == '_' or c == '/':
                    has_alpha = True
                    break
            if has_alpha and value.find(':') < 0:
                return f'{namespace}:{value}'
    return value


def strip_namespace(value: str) -> str:
    """Returns the value without the namespace prefix."""
    if value and isinstance(value, str):
        if '"' in value:
            # Do not modify quoted strings.
            return value
        pos = 0
        len_value = len(value)
        while pos < len_value:
            if not value[pos].isalpha():
                break
            pos += 1
        if pos < len_value and value[pos] == ':':
            return value[pos + 1:].strip()
    return value


def strip_value(value: str) -> str:
    """Returns the string value with leading/trailing space stripped."""
    if value and isinstance(value, str):
        value = value.strip()
        if value and value[0] == '"' and value[-1] == '"':
            value_str = value[1:-1]
            value_str.strip()
            value = '"' + value_str + '"'
    return value


def get_pv_from_line(line: str) -> tuple[str, str]:
    """Returns a tuple of (property, value) from the line."""
    pos = line.find(':')
    if pos < 0:
        return ('', line)
    prop = line[:pos].strip()
    value = line[pos + 1:].strip()
    return (prop, value)


def add_pv_to_node(
    prop: str,
    value: str,
    node: dict,
    append_value: bool = True,
    strip_namespaces: bool = False,
    normalize: bool = True,
) -> dict:
    """Add a property:value to the node dictionary."""
    if node is None:
        node = {}
    if value is None:
        return node
    if isinstance(value, int) or isinstance(value, float):
        value = str(value)
    if value and isinstance(value, str):
        if strip_namespaces:
            value = strip_namespace(value)
    if isinstance(value, list):
        if strip_namespaces:
            value = [strip_namespace(v) for v in value]
        value = ",".join(value)

    if normalize:
        if value and isinstance(value, str):
            value = strip_value(value)
            if value and ',' in value:
                # Split the comma separated value into a list.
                value = normalize_list(value, False)

    existing_value = node.get(prop)
    if existing_value is not None and prop != 'Node' and prop != 'dcid':
        # Property already exists. Add value to a list if not present.
        if value is not None and value != existing_value:
            if append_value:
                # If new value or existing value is a list, need to dedup and merge
                if (isinstance(existing_value, list) or ',' in existing_value or
                        isinstance(value, list) or ',' in value):
                    # Merge the lists
                    unique_values = set()
                    unique_values.update(get_value_list(existing_value))
                    unique_values.update(get_value_list(value))
                    if '"' in existing_value or '"' in value:
                        unique_values = [
                            get_quoted_value(v, is_quoted=True)
                            for v in unique_values
                        ]
                    if strip_namespaces:
                        unique_values = [
                            strip_namespace(v) for v in unique_values
                        ]
                    node[prop] = ",".join(sorted(unique_values))
                else:
                    # Existing and new values are not list, so can be appended.
                    node[prop] = f'{node[prop]},{value}'
            else:
                # Replace with new value
                node[prop] = value
    else:
        # Add a new property:value
        node[prop] = value
    return node


def add_comment_to_node(comment: str, node: dict) -> dict:
    """Add a comment to the node. The comments are preserved in the order read."""
    num_comments = 0
    for c, v in node.items():
        if not c or c[0] != '#':
            continue
        if v == comment:
            return node
        num_comments += 1
    next_comment_index = num_comments + 1
    node[f'# comment{next_comment_index}'] = comment
    return node


def get_node_dcid(pvs: dict) -> str:
    """Returns the dcid of the node without the namespace prefix."""
    if not pvs:
        return ''
    dcid = pvs.get('Node', '')
    dcid = pvs.get('dcid', dcid)
    dcid = dcid.strip(' "')
    return strip_namespace(dcid)


def get_non_name_props(pvs: dict,
                       ignore_props: set = _STATVAR_DCID_IGNORE_PROPS) -> set:
    """Returns the properties of the node ignoring name/descriptions."""
    props = set()
    if not pvs:
        return props
    for prop in pvs.keys():
        if prop and prop not in ignore_props and prop[0] != '#':
            props.add(prop)
    return props


def check_nodes_can_merge(node1: dict, node2: dict) -> bool:
    """Returns True if two nodes can be merged."""
    dcid1 = get_node_dcid(node1)
    dcid2 = get_node_dcid(node2)
    if dcid1 and dcid2 and dcid1 != dcid2:
        logging.error(
            f'Cannot merge nodes with different dcids: {node1}, {node2}')
        return False

    typeof1 = strip_namespace(node1.get('typeOf', ''))
    typeof2 = strip_namespace(node2.get('typeOf', ''))

    if typeof1 == 'StatisticalVariable' or typeof2 == 'StatisticalVariable':
        if typeof1 and typeof2 and typeof1 != typeof2:
            logging.error(f'Cannot merge {dcid1} of type: {typeof1}, {typeof2}')
            return False

        cprops1 = get_non_name_props(node1)
        cprops2 = get_non_name_props(node2)
        if cprops1 != cprops2:
            logging.error(
                f'Conflict in merging statvar props for {dcid1}: {cprops1}, {cprops2}'
            )
            return False
        for prop in cprops1:
            val1 = normalize_value(node1.get(prop, ''))
            val2 = normalize_value(node2.get(prop, ''))
            if ',' in val1 or ',' in val2:
                logging.error(
                    f'Statvar {dcid1} has multiple values for {prop}: {node1}, {node2}'
                )
                return False
            if val1 and val2 and val1 != val2:
                logging.error(
                    f'Statvar {dcid1} has conflicting values for {prop}: {node1}, {node2}'
                )
                return False

    return True


def add_mcf_node(
    pvs: dict,
    nodes: dict,
    strip_namespaces: bool = False,
    append_values: bool = True,
    normalize: bool = True,
    counters: Counters = None,
) -> bool:
    """Add a node with property values into the nodes dict."""
    if pvs is None or len(pvs) == 0:
        return False
    dcid = get_node_dcid(pvs)
    if dcid == '':
        logging.warning(f'Ignoring node without a dcid: {pvs}')
    if strip_namespaces:
        dcid = strip_namespace(dcid)
    else:
        dcid = add_namespace(dcid)
    if dcid not in nodes:
        nodes[dcid] = {}
    else:
        node = nodes[dcid]
        can_merge = check_nodes_can_merge(node, pvs)
        if not can_merge:
            logging.error(f'Cannot merge {dcid}: {node} with {pvs}')
            if counters is not None:
                counters.add_counter('error-mcf-node-merge', 1, dcid)
            return False
    node = nodes[dcid]
    for prop, value in pvs.items():
        add_pv_to_node(prop, value, node, append_values, strip_namespaces,
                       normalize)
    logging.level_debug() and logging.log(
        2, f'Added node {dcid} with properties: {pvs.keys()}')
    return True


def load_mcf_nodes(
    filenames: Union[str, list],
    nodes: dict = None,
    strip_namespaces: bool = False,
    append_values: bool = True,
    normalize: bool = True,
    counters: Counters = None,
) -> dict:
    """Return a dict of nodes from the MCF file keyed by dcid."""
    if nodes is None:
        nodes = _get_new_node(normalize)

    if not filenames:
        return nodes

    if counters is None:
        counters = Counters()

    files = []
    if isinstance(filenames, str):
        filenames = filenames.split(',')
    for file in filenames:
        files.extend(file_util.file_get_matching(file))
    for file in files:
        if not file:
            continue
        counters.add_counter('mcf-files-loaded', 1)
        num_nodes = 0
        num_props = 0
        if file.endswith('.csv'):
            file_nodes = file_util.file_load_csv_dict(file)
            for key, pvs in file_nodes.items():
                if 'Node' not in pvs:
                    pvs['Node'] = key
                num_props += len(pvs)
                if not add_mcf_node(pvs, nodes, strip_namespaces, append_values,
                                    normalize, counters):
                    logging.error(f'Unable to add node from {file}: {pvs}')
            num_nodes = len(file_nodes)
        else:
            line_number = 0
            with file_util.FileIO(file, 'r', errors='ignore') as input_f:
                pvs = _get_new_node(normalize)
                for line in input_f:
                    line_number += 1
                    line = re.sub(r'\s+$', '', re.sub(r'^\s+', '', line))
                    if line and line[0] == '"' and line[-1] == '"':
                        line = line[1:-1]
                    if line == '""':
                        line = ''
                    if line.count('""') > 1:
                        line = line.replace('""', '"')
                    if line == '':
                        if pvs:
                            if not add_mcf_node(pvs, nodes, strip_namespaces,
                                                append_values, normalize,
                                                counters):
                                logging.error(
                                    f'Unable to add node from {file}:{line_number}: {pvs}'
                                )
                            else:
                                num_nodes += 1
                            pvs = _get_new_node(normalize)
                    elif line[0] == '#':
                        add_comment_to_node(line, pvs)
                    else:
                        prop, value = get_pv_from_line(line)
                        if strip_namespaces:
                            value = strip_namespace(value)
                        add_pv_to_node(prop, value, pvs, append_values,
                                       strip_namespaces, normalize)
                        num_props += 1
                if pvs:
                    if not add_mcf_node(pvs, nodes, strip_namespaces,
                                        append_values, normalize, counters):
                        logging.error(
                            f'Unable to add node from {file}:{line_number}: {pvs}'
                        )
                    num_nodes += 1
        logging.info(
            f'Loaded {num_nodes} nodes with {num_props} properties from file {file}'
        )
        counters.add_counter('mcf-nodes-loaded', num_nodes)
    return nodes


def get_numeric_value(value: str,
                      decimal_char: str = '.',
                      separator_chars: str = ' ,$%') -> Union[int, float, None]:
    """Returns the float/int value from string or None."""
    if isinstance(value, int) or isinstance(value, float):
        return value
    if value and isinstance(value, str):
        try:
            normalized_value = value.strip()
            if (normalized_value[0].isdigit() or normalized_value[0] == '.' or
                    normalized_value[0] == '-' or normalized_value[0] == '+'):
                normalized_value = ''.join(
                    [c for c in normalized_value if c not in separator_chars])
                if decimal_char != '.':
                    normalized_value = '.'.join(
                        normalized_value.split(decimal_char))
                if value.count('.') > 1:
                    normalized_value = normalized_value.replace('.', '')
            if normalized_value.count('.') == 1:
                float_val = float(normalized_value)
                int_val = int(float_val)
                if int_val == float_val:
                    return int_val
                return float_val
            return int(normalized_value)
        except ValueError:
            return None
    return None


def get_quoted_value(value: str, is_quoted: bool = None) -> str:
    """Returns a quoted string if there are spaces and special characters."""
    if not value or not isinstance(value, str):
        return value

    value = value.strip('"')
    value = value.strip()
    if value.startswith('[') and value.endswith(']'):
        return normalize_range(value)
    if value and (' ' in value or ',' in value or is_quoted):
        if value and value[0] != '"':
            return '"' + value + '"'
    return value


def get_value_list(value: str) -> list:
    """Returns the value as a list."""
    if isinstance(value, list):
        return value
    value_list = []
    if not isinstance(value, str):
        value = str(value)
    is_quoted = '"' in value
    try:
        if is_quoted and "," in value:
            row = list(
                csv.reader([value],
                           delimiter=',',
                           quotechar='"',
                           skipinitialspace=True))[0]
        else:
            row = value.split(',')
        for v in row:
            val_normalized = get_quoted_value(v, is_quoted=is_quoted)
            value_list.append(val_normalized)
    except csv.Error:
        logging.error(
            f'Too large value {len(value)}, failed to convert to list')
        value_list = [value]
    return value_list


def normalize_list(value: str, sort: bool = True) -> str:
    """Normalize a comma separated list of strings."""
    if ',' in value:
        has_quotes = False
        if '"' in value:
            if value[0] == '"' and value[-1] == '"':
                if '{' in value or '[' in value:
                    return value
            value_list = get_value_list(value)
            has_quotes = True
        else:
            value_list = value.split(',')
        values = []
        for v in value_list:
            if v not in values:
                normalized_v = normalize_value(
                    v,
                    quantity_range_to_dcid=False,
                    maybe_list=False,
                    is_quoted=has_quotes,
                )
                normalized_v = str(normalized_v)
                values.append(normalized_v)
        if sort:
            values = sorted(values)
        return ','.join(values)
    else:
        return value


def normalize_range(value: str, quantity_range_to_dcid: bool = False) -> str:
    """Normalize a quantity range into [<N> <M> Unit]."""
    quantity_pat = (
        r'\[ *(?P<unit1>[A-Z][A-Za-z0-9_/]*)? *(?P<start>[0-9\.]+|-)?'
        r' *(?P<end>[0-9\.]+|-)? *(?P<unit2>[A-Z][A-Za-z0-9_]*)? *\]')
    matches = re.search(quantity_pat, value)
    if not matches:
        return value

    match_dict = matches.groupdict()
    if not match_dict:
        return value

    logging.log(2, f'Matched range: {match_dict}')

    start = match_dict.get('start', '')
    end = match_dict.get('end', '')
    unit = match_dict.get('unit1', '')
    unit2 = match_dict.get('unit2', unit)
    if unit2:
        unit = unit2
    normalized_range = ''
    if quantity_range_to_dcid:
        if unit:
            normalized_range += unit
        if start and start != '-':
            if end:
                if end != '-':
                    normalized_range += f'{start}To{end}'
                else:
                    normalized_range += f'{start}Onwards'
            else:
                normalized_range += f'{start}'
        else:
            normalized_range += f'Upto{end}'
        return add_namespace(normalized_range)
    normalized_range = '['
    if start:
        normalized_range += start + ' '
    if end:
        normalized_range += end + ' '
    if unit:
        normalized_range += unit
    normalized_range += ']'
    return normalized_range


def normalize_value(
    value,
    quantity_range_to_dcid: bool = False,
    maybe_list: bool = True,
    is_quoted: bool = False,
) -> str:
    """Normalize a property value adding a standard namespace prefix 'dcid:'."""
    if value:
        if isinstance(value, str):
            value = value.strip()
            if not value:
                return ''
            if value[0] == '"' and value[-1] == '"' and len(value) > 100:
                return value
            if value.startswith('[') and value.endswith(']') and ',' in value:
                inner_list = value[1:-1].strip()
                normalized_list = normalize_list(inner_list)
                return f'[{normalized_list}]'
            if ',' in value and maybe_list:
                return normalize_list(value)
            if value[0] == '[':
                return normalize_range(value, quantity_range_to_dcid)
            number = get_numeric_value(value)
            if number:
                return normalize_value(number)
            if ' ' in value or ',' in value or is_quoted:
                return get_quoted_value(value, is_quoted)
            if '__' in value:
                values = strip_namespace(value).split('__')
                value = '__'.join(sorted(values))
            return add_namespace(strip_namespace(value))
        elif isinstance(value, float):
            return f'{value}'
        elif isinstance(value, list):
            values = sorted([
                normalize_value(x, quantity_range_to_dcid, is_quoted=is_quoted)
                for x in value
            ])
            return ','.join(values)
    return value


def normalize_mcf_node(
    node: dict,
    ignore_comments: bool = True,
    quantity_range_to_dcid: bool = False,
) -> dict:
    """Returns a normalized MCF node with all PVs in alphabetical order."""
    normal_node = OrderedDict({})
    props = list(node.keys())
    dcid = get_node_dcid(node)
    if dcid:
        normal_node['Node'] = add_namespace(dcid)
    for p in ['Node', 'dcid']:
        if p in props:
            props.remove(p)

    for p in sorted(props):
        if p and p[0] == '#' and ignore_comments:
            continue
        value = node[p]
        if not value:
            continue
        normal_node[p] = normalize_value(value, quantity_range_to_dcid)
    logging.log(3, f'Normalized {node} to {normal_node}')
    return normal_node


def fingerprint_node(pvs: dict,
                     ignore_props: set = {},
                     compare_props: set = {}) -> str:
    """Returns a fingerprint of all property:values in the pvs dict."""
    fp = []
    normalized_pvs = normalize_mcf_node(pvs, quantity_range_to_dcid=True)
    for p in sorted(normalized_pvs.keys()):
        if p not in ignore_props:
            if not compare_props or p in compare_props:
                fp.append(f'{p}={normalized_pvs[p]}')
    return ';'.join(fp)


def node_dict_to_text(node: dict, default_pvs: dict = _DEFAULT_NODE_PVS) -> str:
    """Convert a dictionary node of PVs into text."""
    props = list(node.keys())
    pvs = []
    for prop in node.keys():
        if prop and prop[0] != '#':
            break
        pvs.append(node[prop])
        props.remove(prop)

    for prop, default_value in default_pvs.items():
        value = node.get(prop, default_value)
        if value != '':
            pvs.append(_get_prop_value_line(prop, value))
        if prop in props:
            props.remove(prop)
    for prop in props:
        if prop and prop[0] == '#':
            if prop.startswith('# comment'):
                pvs.append(f'{node[prop]}')
            else:
                pvs.append(f'{prop}{node[prop]}')
            continue
        value = node.get(prop, '')
        if value != '':
            pvs.append(_get_prop_value_line(prop, value))
    return '\n'.join(pvs)


def write_mcf_nodes(
    node_dicts: list,
    filename: str,
    mode: str = 'w',
    default_pvs: dict = _DEFAULT_NODE_PVS,
    header: str = None,
    ignore_comments: bool = True,
    sort: bool = False,
):
    """Write the nodes to an MCF file."""
    if not node_dicts:
        return
    if isinstance(node_dicts, dict):
        node_dicts = [node_dicts]
    if filename.endswith('.csv'):
        node_dict = node_dicts[0]
        for d in node_dicts[1:]:
            node_dict.update(d)
        file_util.file_write_csv_dict(node_dict, filename)
        return
    filename_base = os.path.basename(filename)
    with file_util.FileIO(filename, mode) as output_f:
        if header is not None:
            output_f.write(header)
            output_f.write('\n')
        for nodes in node_dicts:
            node_keys = list(nodes.keys())
            if sort:
                node_keys = sorted(node_keys)
            for dcid in node_keys:
                node = nodes[dcid]
                if 'dcid' not in node and 'Node' not in node:
                    node = dict(node)
                    node['Node'] = f'l:{filename_base}/' + hashlib.md5(
                        str(dcid).encode('utf-8')).hexdigest()
                if sort:
                    node = normalize_mcf_node(node, ignore_comments)
                pvs = node_dict_to_text(node, default_pvs)
                if len(pvs) > 0:
                    output_f.write(pvs)
                    output_f.write('\n\n')


def _get_prop_value_line(prop, value) -> str:
    """Return a text line for a property and value."""
    if isinstance(value, list):
        value = ','.join([add_namespace(x) for x in value])
    else:
        value = add_namespace(value)
    return f'{prop}: {value}'


def _get_new_node(normalize: bool = True) -> dict:
    """Returns OrderedDict if normalize is true, else a dict."""
    if normalize:
        return OrderedDict()
    return dict()
