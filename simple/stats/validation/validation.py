# Copyright 2026 Google Inc.
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

from stats.config.config import Config
from stats.graph.data import PreprocessorError
from stats.graph.nodes import Nodes
from stats.graph.util import has_namespace_prefix
from stats.report.model.summary import ValidationErrorType


class MetadataValidator:
  """Validates the semantic metadata integrity of an ingestion run.

  Verifies that:
  1. Any provenance referenced in config.json is defined in the MCF files.
  2. Any defined provenance in the MCF files points to a Source.
  """

  def __init__(self, config: Config, nodes: Nodes) -> None:
    self.config = config
    self.nodes = nodes

  def validate(self) -> None:
    """Performs all metadata validation checks.

    Raises:
      PreprocessorError: If any validation check fails.
    """
    referenced_provenances = self._collect_referenced_provenances()
    if not referenced_provenances:
      return

    defined_provenances, provenance_to_source = self._collect_defined_nodes()

    self._validate_provenance_definitions(referenced_provenances,
                                          defined_provenances)
    self._validate_source_links(defined_provenances, provenance_to_source)

    logging.info(
        "Metadata validation completed successfully. All provenances and sources are valid."
    )

  def _collect_referenced_provenances(self) -> set[str]:
    """Extracts all referenced provenance DCIDs from config.json.

    The config model has checked that every entry names a valid one.
    """
    return {
        self._clean_dcid(entry.provenance)
        for entry in self.config.model.input_files
    }

  def _collect_defined_nodes(self) -> tuple[set[str], dict[str, str]]:
    """Gathers the Provenances that MCF files defined, and their Sources."""
    defined_provenances = set()
    provenance_to_source = {}
    for prov in self.nodes.provenances.values():
      clean_prov_id = self._clean_dcid(prov.id)
      defined_provenances.add(clean_prov_id)
      if prov.source_id:
        provenance_to_source[clean_prov_id] = self._clean_dcid(prov.source_id)
    return defined_provenances, provenance_to_source

  def _validate_provenance_definitions(self, referenced: set[str],
                                       defined: set[str]) -> None:
    """Verifies all referenced provenances exist in the defined set."""
    missing = sorted(list(referenced - defined))
    if missing:
      raise PreprocessorError(
          f"Metadata Validation Failed: The following referenced provenances "
          f"are not defined in your MCF files: {missing}. "
          f"Please define them in an MCF file (e.g., Node: dcid:YourProvenance).",
          ValidationErrorType.MISSING_PROVENANCE)

  def _validate_source_links(self, defined_provs: set[str],
                             links: dict[str, str]) -> None:
    """Verifies all defined provenances link to a Source."""
    missing_sources = []
    for prov in sorted(defined_provs):
      source = links.get(prov)
      if not source:
        missing_sources.append(prov)

    if missing_sources:
      details = [
          f"  - Provenance '{p}' has no linked Source (source property is missing or empty)"
          for p in missing_sources
      ]
      raise PreprocessorError(
          "Metadata Validation Failed: Linked sources are missing for "
          "defined provenances:\n" + "\n".join(details) +
          "\nPlease specify a source property on these Provenance nodes.",
          ValidationErrorType.MISSING_SOURCE)

  def _clean_dcid(self, val: str) -> str:
    """Normalizes a DCID value by ensuring it starts with a namespace prefix."""
    if not val:
      return ""
    if has_namespace_prefix(val):
      return val
    return f"dcid:{val}"
