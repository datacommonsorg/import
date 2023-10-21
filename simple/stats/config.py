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

from stats.data import StatVar
from stats.data import StatVarGroup

_INPUT_FILES_FIELD = "inputFiles"
_ENTITY_TYPE_FIELD = "entityType"
_IGNORE_COLUMNS_FIELD = "ignoreColumns"
_VARIABLES_FIELD = "variables"
_NAME_FIELD = "name"
_DESCRIPTION_FIELD = "description"
_NL_SENTENCES_FIELD = "nlSentences"
_GROUP_FIELD = "group"

_CUSTOM_SV_ID_PREFIX = "custom/statvar_"
_CUSTOM_GROUP_ID_PREFIX = "custom/g/group_"
_ROOT_GROUP_ID = "dc/g/Root"


class Config:
  """A wrapper around the import config specified via config.json.

    It provides convenience methods for accessing parameters from the config.
    """

  def __init__(self, data: dict) -> None:
    self.data = data

  def variables_and_groups(
      self, sv_names: list[str]) -> (list[StatVar], list[StatVarGroup]):
    groups = self._groups(sv_names)
    variables = self._variables(sv_names, groups=groups)
    return list(variables.values()), list(groups.values())

  def _groups(self, sv_names: list[str]):
    # Group Path to SVGroup
    groups: dict[str, StatVarGroup] = {}

    for sv_name in sv_names:
      group_path = self._variable_group(sv_name)
      if group_path:
        tokens = group_path.split("/")
        for index in range(len(tokens)):
          path = "/".join(tokens[:index + 1])
          if path not in groups:
            id = f"{_CUSTOM_GROUP_ID_PREFIX}{len(groups) + 1}"
            name = tokens[index]
            parent_path = "" if "/" not in path else path[:path.rindex("/")]
            parent_id = groups[
                parent_path].id if parent_path in groups else _ROOT_GROUP_ID
            groups[path] = StatVarGroup(id, name, parent_id)

    return groups

  def _variables(self,
                 sv_names: list[str],
                 groups: dict[str, StatVarGroup] = None):
    if not groups:
      groups = self._groups(sv_names)

    # Variable column name to SV
    svs: dict[str, StatVar] = {}

    for sv_name in sv_names:
      if sv_name in svs:
        continue

      var = self._variable(sv_name)
      id = f"{_CUSTOM_SV_ID_PREFIX}{len(svs) + 1}"
      name = var.get(_NAME_FIELD, sv_name)
      group_path = self._variable_group(sv_name)
      group_id = groups[
          group_path].id if group_path and group_path in groups else ""
      svs[sv_name] = StatVar(id,
                             name,
                             description=var.get(_DESCRIPTION_FIELD, ""),
                             nl_sentences=var.get(_NL_SENTENCES_FIELD, []),
                             group_id=group_id)

    return svs

  def entity_type(self, input_file_name: str) -> str:
    return self.data.get(_INPUT_FILES_FIELD,
                         {}).get(input_file_name,
                                 {}).get(_ENTITY_TYPE_FIELD, "")

  def ignore_columns(self, input_file_name: str) -> str:
    return self.data.get(_INPUT_FILES_FIELD,
                         {}).get(input_file_name,
                                 {}).get(_IGNORE_COLUMNS_FIELD, [])

  def _variable(self, variable_name: str) -> dict:
    return self.data.get(_VARIABLES_FIELD, {}).get(variable_name, {})

  def _variable_group(self, variable_name: str) -> str:
    return self._variable(variable_name).get(_GROUP_FIELD, "")
