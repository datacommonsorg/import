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

_INPUT_FILES_FIELD = "inputFiles"
_ENTITY_TYPE_FIELD = "entityType"
_IGNORE_COLUMNS_FIELD = "ignoreColumns"


class Config:
    """A wrapper around the import config specified via config.json.

    It provides convenience methods for accessing parameters from the config.
    """

    def __init__(self, data: dict) -> None:
        self.data = data

    def get_entity_type(self, input_file_name: str) -> str:
        return self.data.get(_INPUT_FILES_FIELD,
                             {}).get(input_file_name,
                                     {}).get(_ENTITY_TYPE_FIELD, "")

    def get_ignore_columns(self, input_file_name: str) -> str:
        return self.data.get(_INPUT_FILES_FIELD,
                             {}).get(input_file_name,
                                     {}).get(_IGNORE_COLUMNS_FIELD, [])
