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
import pandas as pd
from stats import constants
from stats.data import RowEntity
from stats.data import strip_namespace
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import is_uri_or_namespace
from util.filesystem import File

_ID = 'ID'
_DCID = 'dcid'

# The max length of a node value.
# We limit it to the max size of a mysql text field.
_MAX_CHARS = 2**16 - 1


class McfImporter(Importer):
  """Imports a MCF file.

  For main DC, the file is simply copied to the output directory.
  For custom DC, the MCF nodes are inserted as triples in the DB.
    """

  def __init__(self,
               input_file: File,
               output_file: File,
               db: Db,
               reporter: FileImportReporter,
               is_main_dc: bool,
               nodes: Nodes = None) -> None:
    self.input_file = input_file
    self.output_file = output_file
    self.db = db
    self.reporter = reporter
    self.is_main_dc = is_main_dc
    self.nodes = nodes

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      # For main DC, simply copy the file over.
      if self.is_main_dc:
        self.output_file.write(self.input_file.read())
      else:
        triples = self._mcf_to_triples()

        # Extract and register Provenance / Source nodes in nodes registry
        _register_metadata_nodes(triples, self.nodes)

        logging.info("Inserting %s triples from %s", len(triples),
                     self.input_file.full_path())
        self.db.insert_triples(triples, self.input_file)

      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def _mcf_to_triples(self) -> list[Triple]:
    parser_triples: list[list[str]] = []
    # DCID references
    local2dcid: dict[str, str] = {}

    for parser_triple in mcf_to_triples(self.input_file.read_string_io()):
      [subject_id, predicate, value, _] = parser_triple

      # If it's not a DCID property, save it for later processing and move on
      if predicate != _DCID:
        parser_triples.append(parser_triple)
        continue

      # Ignore empty DCID values
      if not value:
        continue

      # If we have seen it, warn if there is a conflict, but maintain legacy behavior (overwrite)
      if subject_id in local2dcid and local2dcid[subject_id] != value:
        logging.warning(
            "Conflicting DCID for subject %s: '%s' vs '%s'. Overwriting as per legacy behavior.",
            subject_id, local2dcid[subject_id], value)

      local2dcid[subject_id] = value

    return list(map(lambda x: _to_triple(x, local2dcid), parser_triples))


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


def _register_metadata_nodes(triples: list[Triple], nodes: Nodes) -> None:
  """Extracts and registers Provenance and Source nodes from parsed MCF triples.

  This helper is robust against:
  - Namespaced predicates (e.g., 'dcs:url', 'schema:name', 'dcs:sourceLink').
  - Namespaced typeOf values (e.g., 'dcs:Provenance', 'dcs:Source').
  - Dual property names for source links ('sourceLink' vs 'source').
  - String literal quote escaping.
  """
  if not nodes:
    return

  subject_properties = {}
  for triple in triples:
    sub_id = triple.subject_id
    if sub_id not in subject_properties:
      subject_properties[sub_id] = {}

  # Strip namespace from predicate for robust matching
    pred = strip_namespace(triple.predicate)

    if pred == "typeOf":
      subject_properties[sub_id]["typeOf"] = strip_namespace(triple.object_id or
                                                             "")
    elif pred == "url":
      val = triple.object_value or triple.object_id or ""
      subject_properties[sub_id]["url"] = _clean_literal(val)
    elif pred == "name":
      val = triple.object_value or triple.object_id or ""
      subject_properties[sub_id]["name"] = _clean_literal(val)
    elif pred == "sourceLink":
      # Map sourceLink to the source ID
      subject_properties[sub_id]["sourceLink"] = triple.object_id

  for sub_id, props in subject_properties.items():
    node_type = props.get("typeOf")
    if node_type == "Provenance":
      nodes.register_provenance(id=sub_id,
                                name=props.get("name", ""),
                                url=props.get("url", ""),
                                source_id=props.get("sourceLink", ""))
    elif node_type == "Source":
      nodes.register_source(id=sub_id,
                            name=props.get("name", ""),
                            url=props.get("url", ""))


def _clean_literal(val: str) -> str:
  """Cleans leading/trailing quotes and whitespace from literal strings safely."""
  if not val:
    return ""
  return val.strip().strip('"').strip("'")
