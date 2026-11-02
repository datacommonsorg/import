# Copyright 2024 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging

from kg_util.mcf_parser import mcf_to_triples
from stats.data import strip_namespace
from stats.data import Triple
from stats.graph_writer import GraphWriter
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import is_uri_or_namespace
from util.filesystem import File

_ID = 'ID'
_DCID = 'dcid'
_TYPE_OF = 'typeOf'
_PROVENANCE = 'Provenance'
_SOURCE = 'Source'

# Metadata node predicates that map onto Provenance / Source fields (matched
# without namespace). Only a predicate's first value fills its field.
_PROVENANCE_FIELDS = frozenset({'name', 'url', 'source'})
_SOURCE_FIELDS = frozenset({'name', 'url'})

# The max length of a node value.
# We limit it to the max size of a mysql text field.
_MAX_CHARS = 2**16 - 1


class McfImporter(Importer):
  """Imports a MCF file.

  The MCF nodes are parsed and inserted as triples via the GraphWriter.
    """

  def __init__(self, input_file: File, graph_writer: GraphWriter,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_file = input_file
    self.graph_writer = graph_writer
    self.reporter = reporter
    self.nodes = nodes

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      # Pass 1: Resolve local ID mappings and identify metadata subject IDs
      local2dcid = {}
      metadata_subject_ids = set()
      triples_list = list(mcf_to_triples(self.input_file.read_string_io()))
      for subject_id, predicate, value, _ in triples_list:
        if predicate == _DCID and value:
          local2dcid[subject_id] = value
        elif predicate == _TYPE_OF and strip_namespace(value) in [
            _PROVENANCE, _SOURCE
        ]:
          metadata_subject_ids.add(subject_id)

      # Pass 2: Map and stream triples to the graph writer in chunks
      logging.info("Streaming MCF triple parsing and writes for %s...",
                   self.input_file.full_path())
      chunk = []
      all_metadata_triples = []

      for parser_triple in triples_list:
        subject_id, predicate, value, value_type = parser_triple
        if predicate == _DCID:
          continue

        triple = _to_triple(parser_triple, local2dcid)

        # Metadata nodes are registered below and written once by Nodes, which
        # also adds derived properties (e.g. a Source's domain).
        resolved_subject = local2dcid.get(subject_id, subject_id)
        if subject_id in metadata_subject_ids or resolved_subject in metadata_subject_ids:
          all_metadata_triples.append(triple)
          continue

        # Only flush at subject boundaries to prevent splitting a subject's triples
        if chunk and triple.subject_id != chunk[-1].subject_id and len(
            chunk) >= 10000:
          self.graph_writer.write_triples(chunk, self.input_file)
          chunk = []
        chunk.append(triple)

      if chunk:
        self.graph_writer.write_triples(chunk, self.input_file)

      # Register all collected metadata nodes at the end
      if all_metadata_triples:
        _register_metadata_nodes(all_metadata_triples,
                                 self.nodes,
                                 import_folder=self.nodes.config.import_name(
                                     self.input_file))

      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e


def _to_triple(parser_triple: list[str], local2dcid: dict[str, str]) -> Triple:
  [subject_id, predicate, value, value_type] = parser_triple
  if len(value) > _MAX_CHARS:
    raise ValueError(
        f"Value of property {predicate} in node {subject_id} too long (got: {len(value)}, max: {_MAX_CHARS})"
    )

  # Resolve the subject ID using the map, fallback to original if not mapped
  resolved_subject = local2dcid.get(subject_id, subject_id)

  # If it was not mapped AND it doesn't start with a valid namespace or URI, it's an error!
  if resolved_subject == subject_id and not is_uri_or_namespace(
      resolved_subject):
    raise ValueError(f"dcid not specified for node: {subject_id}")

  if value_type == _ID:
    return Triple(resolved_subject, predicate, object_id=value)

  return Triple(resolved_subject, predicate, object_value=value)


def _register_metadata_nodes(triples: list[Triple], nodes: Nodes,
                             import_folder: str) -> None:
  """Registers the Provenance and Source nodes defined by `triples` in `nodes`.

  Sources are attached to `import_folder`, the folder of the MCF file that
  defines them. A node typed both Provenance and Source is registered as a
  Provenance. Every triple that doesn't fill a field (see _split_fields) is
  kept, as is, as one of the node's custom triples, so nothing is lost.

  Field matching is robust against:
  - Namespaced predicates (e.g., 'dcs:url', 'schema:name').
  - Namespaced typeOf values (e.g., 'dcs:Provenance', 'dcs:Source').
  - String literal quote escaping, in field values.
  """
  for sub_id, node_triples in _group_by_subject(triples).items():
    node_types = _node_types(node_triples)
    if _PROVENANCE in node_types:
      fields, custom = _split_fields(node_triples, _PROVENANCE,
                                     _PROVENANCE_FIELDS)
      nodes.register_provenance(id=sub_id,
                                name=fields.get('name', ''),
                                url=fields.get('url', ''),
                                source_id=fields.get('source', ''),
                                custom_triples=custom)
    elif _SOURCE in node_types:
      fields, custom = _split_fields(node_triples, _SOURCE, _SOURCE_FIELDS)
      nodes.register_source(id=sub_id,
                            name=fields.get('name', ''),
                            url=fields.get('url', ''),
                            custom_triples=custom,
                            import_folder=import_folder)


def _group_by_subject(triples: list[Triple]) -> dict[str, list[Triple]]:
  """Groups `triples` by subject, keeping first-seen subject order."""
  by_subject: dict[str, list[Triple]] = {}
  for triple in triples:
    by_subject.setdefault(triple.subject_id, []).append(triple)
  return by_subject


def _node_types(node_triples: list[Triple]) -> set[str]:
  """Returns the namespace-stripped typeOf values of a node."""
  return {
      strip_namespace(t.object_id or '')
      for t in node_triples
      if strip_namespace(t.predicate) == _TYPE_OF
  }


def _split_fields(
    node_triples: list[Triple], node_type: str,
    field_names: frozenset[str]) -> tuple[dict[str, str], list[Triple]]:
  """Splits a node's triples into fields and custom triples.

  Returns (fields, custom). `fields` maps each of `field_names` found (matched
  without namespace) to the cleaned literal of its first value. `custom` holds
  every other triple: other predicates, other types, and extra field values.
  The `node_type` typeOf triple is dropped: the registered node writes it.
  """
  fields: dict[str, str] = {}
  custom: list[Triple] = []
  for triple in node_triples:
    pred = strip_namespace(triple.predicate)
    if pred == _TYPE_OF and strip_namespace(triple.object_id or
                                            '') == node_type:
      continue
    if pred in field_names and pred not in fields:
      fields[pred] = _clean_literal(triple.object_id or triple.object_value)
      continue
    custom.append(triple)
  return fields, custom


def _clean_literal(val: str) -> str:
  """Cleans leading/trailing quotes and whitespace from literal strings safely."""
  if not val:
    return ""
  return val.strip().strip('"').strip("'")
