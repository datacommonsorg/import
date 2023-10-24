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


class Nodes:

  def __init__(self, config: Config) -> None:
    self.config = config
    # Dictionary of SVs from column name to SV
    self.variables: dict[str, StatVar] = {}
    # Dictionary of SVGs from SVG path to SVG
    self.groups: dict[str, StatVarGroup] = {}
    # Used to generate SV IDs
    self._sv_generated_id_count = 0

  def variable(self, sv_column_name: str) -> StatVar:
    if sv_column_name in self.variables:
      return self.variables[sv_column_name]

    var_cfg = self.config.variable(sv_column_name)
    group = self.group(var_cfg.group_path)
    group_id = group.id if group else _ROOT_GROUP_ID
    self.variables[sv_column_name] = StatVar(
        self._sv_id(sv_column_name),
        var_cfg.name,
        description=var_cfg.description,
        nl_sentences=var_cfg.nl_sentences,
        group_id=group_id,
    )
    return self.variables[sv_column_name]

  def _sv_id(self, sv_column_name: str) -> str:
    if re.fullmatch(_SV_ID_PATTERN, sv_column_name):
      return sv_column_name
    self._sv_generated_id_count += 1
    return f"{_CUSTOM_SV_ID_PREFIX}{self._sv_generated_id_count}"

  def group(self, group_path: str) -> StatVarGroup | None:
    if not group_path:
      return None
    if group_path in self.groups:
      return self.groups[group_path]

    tokens = group_path.split("/")
    for index in range(len(tokens)):
      path = "/".join(tokens[:index + 1])
      if path not in self.groups:
        parent_path = "" if "/" not in path else path[:path.rindex("/")]
        parent_id = (self.groups[parent_path].id
                     if parent_path in self.groups else _ROOT_GROUP_ID)
        self.groups[path] = StatVarGroup(
            f"{_CUSTOM_GROUP_ID_PREFIX}{len(self.groups) + 1}", tokens[index],
            parent_id)

    return self.groups[group_path]

  def triples(self, triples_fh: FileHandler | None = None) -> list[Triple]:
    triples: list[Triple] = []
    for group in self.groups.values():
      triples.extend(group.triples())
    for variable in self.variables.values():
      triples.extend(variable.triples())

    if triples_fh:
      logging.info("Writing %s triples to: %s", len(triples), str(triples_fh))
      triples_fh.write_string(pd.DataFrame(triples).to_csv(index=False))

    return triples
