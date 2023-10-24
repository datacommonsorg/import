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

  def variable(self, variable_name: str) -> StatVar:
    var_cfg = self.data.get(_VARIABLES_FIELD, {}).get(variable_name, {})
    return StatVar(
        "",
        var_cfg.get(_NAME_FIELD, variable_name),
        description=var_cfg.get(_DESCRIPTION_FIELD, ""),
        nl_sentences=var_cfg.get(_NL_SENTENCES_FIELD, []),
        group_path=var_cfg.get(_GROUP_FIELD, ""),
    )

  def entity_type(self, input_file_name: str) -> str:
    return (self.data.get(_INPUT_FILES_FIELD,
                          {}).get(input_file_name,
                                  {}).get(_ENTITY_TYPE_FIELD, ""))

  def ignore_columns(self, input_file_name: str) -> str:
    return (self.data.get(_INPUT_FILES_FIELD,
                          {}).get(input_file_name,
                                  {}).get(_IGNORE_COLUMNS_FIELD, []))
