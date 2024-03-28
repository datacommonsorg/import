# Copyright 2020 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Library to parse an MCF file into triples."""

from collections.abc import Iterator
from csv import reader
import dataclasses
import io
import sys

from stats.data import Triple

_VALUE = 'VALUE'
_ID = 'ID'
_node = 'Node'
_context = 'Context'
_dcid = 'dcid'
_namespace = 'namespace'
_dcid_prefix = 'dcid:'
_dcs_prefix = 'dcs:'
_schema_prefix = 'schema:'
_local_prefix = 'l:'
_delim = ':'

#
# Internal Functions
#


def _strip_quotes(s: str) -> str:
  if s.startswith('"') and s.endswith('"'):
    return s[1:-1]
  return s


def _strip_ns(v: str) -> str:
  return v[v.find(_delim) + 1:]


def _is_schema_ref_property(prop: str) -> bool:
  return prop in ('typeOf', 'subClassOf', 'subPropertyOf', 'rangeIncludes',
                  'domainIncludes', 'specializationOf')


def _is_common_ref_property(prop: str) -> bool:
  return (_is_schema_ref_property(prop) or
          prop in ('location', 'observedNode', 'containedInPlace',
                   'containedIn', 'populationType', 'measuredProperty',
                   'measurementDenominator', 'populationGroup',
                   'constraintProperties', 'measurementMethod', 'comparedNode'))


def _is_global_ref(value: str) -> bool:
  return (value.startswith(_dcid_prefix) or value.startswith(_dcs_prefix) or
          value.startswith(_schema_prefix))


def _is_local_ref(value: str) -> bool:
  return value.startswith(_local_prefix)


@dataclasses.dataclass
class _ParseContext:
  """Context used for parser"""
  # MCF file line number.
  lno: int = 0
  # Indicates we're within Context block.
  in_context: bool = False
  # Namespace map.
  ns_map: dict = dataclasses.field(default_factory=dict)
  # Current Node block name.
  node: str = ''
  node_without_ns: str = ''
  # Indicates whether the current Node block includes dcid property.
  has_dcid: bool = False


def _as_err(msg: str, pc: _ParseContext) -> str:
  return 'Line ' + str(pc.lno) + ': ' + msg


def _parse_value(prop: str, value: str, pc: _ParseContext) -> tuple[str, str]:
  """Parses an MCF value string into a pair of value and type.

    Args:
        prop: Property name
        value: Value string
        pc: ParseContext instance

    Returns:
        Pair of value and type.
    """
  expect_ref = _is_common_ref_property(prop)

  if value.startswith('"'):
    assert len(value) > 2 and value.endswith('"'),\
        _as_err('malformed string', pc)
    value = _strip_quotes(value)
    if not expect_ref:
      return (value, _VALUE)

  if _is_global_ref(value):
    return (_strip_ns(value), _ID)

  if _is_local_ref(value):
    # For local ref, we retain the prefix.
    return (value, _ID)

  ns_prefix = value.split(':', 1)[0] + _delim
  if ns_prefix != value and ns_prefix in pc.ns_map:
    value = value.replace(ns_prefix, pc.ns_map[ns_prefix], 1)
    return (value, _ID)

  if expect_ref:
    # If we're here, user likely forgot to add a "dcid:", "dcs:" or
    # "schema:" prefix.  We cannot tell apart if they failed to add an local
    # ref ('l:'), but we err on the side of user being careful about adding
    # local refs and accept the MCF without failing.
    return (value, _ID)

  return (value, _VALUE)


def _parse_values(prop: str, values_str: str,
                  pc: _ParseContext) -> list[tuple[str, str]]:
  value_pairs = []
  for values in reader([values_str]):
    for value in values:
      value_pairs.append(_parse_value(prop, value.strip(), pc))
    break
  return value_pairs


def _update_ns_map(values_str: str, pc: _ParseContext):
  """Updates the namespace map with namespace values.

    Args:
        values_str: String of values for namespace prop from MCF.
        pc: ParseContext instance.
    """
  for values in reader([values_str]):
    for value in values:
      value = _strip_quotes(value.strip())
      parts = value.split('=', 1)
      assert len(parts) == 2, _as_err('malformed namespace value', pc)
      k = parts[0] + _delim
      assert k not in pc.ns_map,\
          _as_err('duplicate values for namespace prefix ' + k, pc)
      pc.ns_map[k] = parts[1].rstrip('/') + '/'
    break


def _to_triple(subject_id: str, predicate: str, value: str,
               value_type: str) -> Triple:
  if value_type == _ID:
    return Triple(subject_id, predicate, object_id=value)
  else:
    return Triple(subject_id, predicate, object_value=value)


def _mcf_to_triples(mcf_file: io.StringIO) -> Iterator[Triple]:
  """ Parses the file containing a Node MCF graph into triples.

  Args:
    mcf_file: Node MCF file object opened for read.

  Returns:
    An Iterable of triples.

  On parse failures, throws AssertionError with a reference to offending line
  number in the MCF file.
  """

  pc = _ParseContext()
  for line in mcf_file:
    pc.lno += 1

    line = line.strip()
    if not line or line.startswith('//') or line.startswith('#'):
      continue

    parts = line.split(_delim, 1)
    assert len(parts) == 2,\
        _as_err('Malformed line without a colon delimiter', pc)
    assert parts[0], _as_err('Malformed empty property', pc)
    prop, val_str = parts[0].strip(), parts[1].strip()

    if prop == _context:
      # New Context block.
      assert not pc.node, _as_err('found Context block after Node', pc)
      pc.in_context = True

    elif prop == _node:
      # New Node block.
      assert val_str, _as_err('Node with no name', pc)
      assert ',' not in val_str, _as_err('Malformed Node name with ","', pc)
      assert not val_str.startswith('"'),\
          _as_err('Malformed Node name starting with "', pc)

      # Finalize current node.
      if (pc.node and not pc.has_dcid and _is_global_ref(pc.node)):
        yield _to_triple(pc.node, _dcid, _strip_ns(pc.node), _VALUE)

      # Update to new node.
      pc.node = val_str
      pc.node_without_ns = _strip_ns(pc.node)
      pc.in_context = False
      pc.has_dcid = False

    elif pc.in_context:
      # Processing inside Context block.
      assert prop == _namespace,\
          _as_err('Context block only supports "namespace" property', pc)
      _update_ns_map(val_str, pc)

    else:
      # Processing inside Node block.
      assert pc.node, _as_err('Prop-Values before Node or Context block', pc)

      for vp in _parse_values(prop, val_str, pc):
        yield _to_triple(pc.node_without_ns, prop, vp[0], vp[1])

      if prop == _dcid:
        pc.has_dcid = True

  # Finalize current node.
  if (pc.node and not pc.has_dcid and _is_global_ref(pc.node)):
    yield _to_triple(pc.node, _dcid, _strip_ns(pc.node), _VALUE)


def mcf_to_triples(mcf_file: io.StringIO) -> list[Triple]:
  return list(_mcf_to_triples(mcf_file))


if __name__ == '__main__':
  with open(sys.argv[1], 'r') as f:
    for t in mcf_to_triples(f):
      print(t)
