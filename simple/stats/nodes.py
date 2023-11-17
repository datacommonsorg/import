# Copyright 2023 Google Inc.
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

from dataclasses import dataclass
import logging
import re

import pandas as pd
from stats.config import Config
from stats.data import Entity
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVar
from stats.data import StatVarGroup
from stats.data import Triple
from util.filehandler import FileHandler

_CUSTOM_SV_ID_PREFIX = "custom/statvar_"
_CUSTOM_GROUP_ID_PREFIX = "custom/g/group_"
_ROOT_GROUP_ID = "dc/g/Root"
# Pattern to check if a string conforms to that of a valid SV ID.
# Note that slashes ("/") are intentionally not considered here
# since it can be confusing for custom DCs.
_SV_ID_PATTERN = r"^[A-Za-z0-9_]+$"
# If group path for a variable is empty, we'll put it under a default custom group.
_DEFAULT_CUSTOM_GROUP_PATH = "__DEFAULT__"
_DEFAULT_CUSTOM_GROUP = StatVarGroup("custom/g/Root", "Custom Variables",
                                     _ROOT_GROUP_ID)

_CUSTOM_PROVENANCE_ID_PREFIX = "c/p/"
_CUSTOM_SOURCE_ID_PREFIX = "c/s/"
_DEFAULT_SOURCE = Source(f"{_CUSTOM_SOURCE_ID_PREFIX}default",
                         "Custom Data Commons")


class Nodes:

  def __init__(self, config: Config) -> None:
    self.config = config
    # Dictionary of SVs from column name to SV
    self.variables: dict[str, StatVar] = {}
    # Dictionary of SVGs from SVG path to SVG
    self.groups: dict[str, StatVarGroup] = {}
    # Dictionary of SVGs from SVG id to SVG
    self.ids_to_groups: dict[str, StatVarGroup] = {}
    # Dictionary of entities from entity DCID to Entity
    self.entities: dict[str, Entity] = {}
    # dict from provenance name to Provenance
    self.provenances: dict[str, Provenance] = {}
    # dict from source name to Source
    self.sources: dict[str, Source] = {}
    self._load_provenances_and_sources()
    # Used to generate SV IDs
    self._sv_generated_id_count = 0

  def _load_provenances_and_sources(self):
    # Load default Source
    self.sources[_DEFAULT_SOURCE.id] = _DEFAULT_SOURCE
    # Load from config
    for prov_cfg in self.config.provenances.values():
      source_cfg = self.config.provenance_sources.get(prov_cfg.name)
      source_id = self._source_id(source_cfg)
      self._provenance(prov_name=prov_cfg.name,
                       prov_url=prov_cfg.url,
                       source_id=source_id)

  def _provenance(self,
                  prov_name: str,
                  prov_url: str = "",
                  source_id: str = _DEFAULT_SOURCE.id) -> Provenance:
    provenance = self.provenances.get(prov_name)
    if provenance:
      return provenance

    provenance = Provenance(
        id=f"{_CUSTOM_PROVENANCE_ID_PREFIX}{len(self.provenances) + 1}",
        source_id=source_id,
        name=prov_name,
        url=prov_url or prov_name)
    self.provenances[provenance.name] = provenance

    return provenance

  def _source_id(self, source_cfg: Source | None) -> str:
    if not source_cfg:
      return _DEFAULT_SOURCE.id

    source = self.sources.get(source_cfg.name)
    if not source:
      source = Source(id=f"{_CUSTOM_SOURCE_ID_PREFIX}{len(self.sources)}",
                      name=source_cfg.name,
                      url=source_cfg.url)
      self.sources[source.name] = source

    return source.id

  def provenance(self, input_file_name: str) -> Provenance:
    return self._provenance(self.config.provenance_name(input_file_name))

  def variable(self, sv_column_name: str, input_file_name: str) -> StatVar:
    if not sv_column_name in self.variables:
      var_cfg = self.config.variable(sv_column_name)
      group = self.group(var_cfg.group_path)
      group_id = group.id if group else _ROOT_GROUP_ID
      self.variables[sv_column_name] = StatVar(
          self._sv_id(sv_column_name),
          var_cfg.name,
          description=var_cfg.description,
          nl_sentences=var_cfg.nl_sentences,
          group_id=group_id)

    return self._add_provenance(self.variables[sv_column_name],
                                self.provenance(input_file_name))

  def _add_provenance(self, sv: StatVar, provenance: Provenance) -> StatVar:
    sv.add_provenance(provenance)
    svg = self.ids_to_groups.get(sv.group_id)
    while svg:
      svg.add_provenance(provenance)
      svg = self.ids_to_groups.get(svg.parent_id)
    return sv

  def _sv_id(self, sv_column_name: str) -> str:
    if re.fullmatch(_SV_ID_PATTERN, sv_column_name):
      return sv_column_name
    self._sv_generated_id_count += 1
    return f"{_CUSTOM_SV_ID_PREFIX}{self._sv_generated_id_count}"

  def group(self, group_path: str) -> StatVarGroup | None:
    if not group_path:
      return self._default_custom_group()
    if group_path in self.groups:
      return self.groups[group_path]

    tokens = group_path.split("/")
    for index in range(len(tokens)):
      path = "/".join(tokens[:index + 1])
      if path not in self.groups:
        parent_path = "" if "/" not in path else path[:path.rindex("/")]
        parent_id = (self.groups[parent_path].id
                     if parent_path in self.groups else _ROOT_GROUP_ID)
        svg = StatVarGroup(f"{_CUSTOM_GROUP_ID_PREFIX}{len(self.groups) + 1}",
                           tokens[index], parent_id)
        self.groups[path] = svg
        self.ids_to_groups[svg.id] = svg

    return self.groups[group_path]

  def _default_custom_group(self) -> StatVarGroup:
    if _DEFAULT_CUSTOM_GROUP_PATH not in self.groups:
      self.groups[_DEFAULT_CUSTOM_GROUP_PATH] = _DEFAULT_CUSTOM_GROUP
    return self.groups[_DEFAULT_CUSTOM_GROUP_PATH]

  def entities_with_type(self, entity_dcids: list[str], entity_type: str):
    for entity_dcid in entity_dcids:
      if entity_dcid not in self.entities:
        self.entities[entity_dcid] = Entity(entity_dcid, entity_type)

  def triples(self, triples_fh: FileHandler | None = None) -> list[Triple]:
    triples: list[Triple] = []
    for source in self.sources.values():
      triples.extend(source.triples())
    for provenance in self.provenances.values():
      triples.extend(provenance.triples())
    for group in self.groups.values():
      triples.extend(group.triples())
    for variable in self.variables.values():
      triples.extend(variable.triples())
    for entities in self.entities.values():
      triples.extend(entities.triples())

    if triples_fh:
      logging.info("Writing %s triples to: %s", len(triples), str(triples_fh))
      triples_fh.write_string(pd.DataFrame(triples).to_csv(index=False))

    return triples
