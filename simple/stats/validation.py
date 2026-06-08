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
from stats.config import Config
from stats.db import Db
from stats.util import is_uri_or_namespace


class MetadataValidator:
  """Validates the semantic metadata integrity of an ingestion run.

  Verifies that:
  1. Any provenance referenced in config.json is defined in the MCF files.
  2. Any defined provenance in the MCF files points to a defined Source.
  """

  def __init__(self, config: Config, db: Db) -> None:
    self.config = config
    self.db = db

  def validate(self) -> None:
    """Performs all metadata validation checks.

    Raises:
      ValueError: If any validation check fails.
    """
    referenced_provenances = self._collect_referenced_provenances()
    if not referenced_provenances:
      return

    defined_provenances, defined_sources, provenance_to_source = self._collect_defined_nodes()

    self._validate_provenance_definitions(referenced_provenances,
                                          defined_provenances)
    self._validate_source_links(defined_provenances, defined_sources,
                                provenance_to_source)

    logging.info(
        "Metadata validation completed successfully. All provenances and sources are valid."
    )

  def _collect_referenced_provenances(self) -> set[str]:
    """Extracts all referenced provenance DCIDs from config.json.

    Every input file is strictly required to have a valid provenance DCID
    starting with 'dcid:'.
    """
    referenced = set()
    entries = self.config.data.get("inputFiles", [])

    for entry in entries:
      if not isinstance(entry, dict):
        continue
      prov = entry.get("provenance")
      if not prov:
        raise ValueError(
            f"Metadata Validation Failed: Every input file in config.json "
            f"must have a 'provenance' property. "
            f"Found entry missing provenance: {entry}"
        )
      if not is_uri_or_namespace(prov):
        raise ValueError(
            f"Metadata Validation Failed: The 'provenance' property must be "
            f"a valid DCID or URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL). "
            f"Found invalid provenance: '{prov}'"
        )
      referenced.add(self._clean_dcid(prov))
    return referenced

  def _collect_defined_nodes(self) -> tuple[set[str], set[str], dict[str, str]]:
    """Gathers all defined Provenances, Sources, and their links from the DB triples."""
    defined_provenances = set()
    defined_sources = set()
    provenance_to_source = {}

    all_triples = []
    db_triples = getattr(self.db, "_triples", {})
    if isinstance(db_triples, dict):
      for triples_list in db_triples.values():
        all_triples.extend(triples_list)

    for triple in all_triples:
      sub = self._clean_dcid(triple.subject_id)
      pred = triple.predicate

      if pred == "typeOf":
        obj = triple.object_id or ""
        if "Provenance" in obj:
          defined_provenances.add(sub)
        elif "Source" in obj:
          defined_sources.add(sub)

      elif pred in ["sourceLink", "source"]:
        obj = triple.object_id or ""
        if obj:
          provenance_to_source[sub] = self._clean_dcid(obj)

    return defined_provenances, defined_sources, provenance_to_source

  def _validate_provenance_definitions(self, referenced: set[str],
                                       defined: set[str]) -> None:
    """Verifies all referenced provenances exist in the defined set."""
    missing = sorted(list(referenced - defined))
    if missing:
      raise ValueError(
          f"Metadata Validation Failed: The following referenced provenances "
          f"are not defined in your MCF files: {missing}. "
          f"Please define them in an MCF file (e.g., Node: dcid:YourProvenance)."
      )

  def _validate_source_links(self, defined_provs: set[str],
                             defined_sources: set[str],
                             links: dict[str, str]) -> None:
    """Verifies all defined provenances link to a valid defined Source."""
    missing_sources = {}
    for prov in defined_provs:
      source = links.get(prov)
      if not source or source not in defined_sources:
        missing_sources[prov] = source or "None"

    if missing_sources:
      details = [
          f"  - Provenance '{p}' points to missing/empty Source '{s}'"
          for p, s in missing_sources.items()
      ]
      raise ValueError(
          f"Metadata Validation Failed: Linked sources are missing for "
          f"defined provenances:\n" + "\n".join(details) +
          f"\nPlease define the missing Source nodes in your MCF files."
      )

  def _clean_dcid(self, val: str) -> str:
    """Normalizes a DCID value by ensuring it starts with 'dcid:' and has no prefix namespaces."""
    if val.startswith(("http://", "https://")):
      return val
    if val.startswith("dcid:"):
      return val
    if ":" in val:
      return f"dcid:{val.split(':', 1)[1]}"
    return f"dcid:{val}"
