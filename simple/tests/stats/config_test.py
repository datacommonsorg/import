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

import unittest

from stats.config import Config
from stats.data import StatVar
from stats.data import StatVarGroup

CONFIG_DATA = {
    "inputFiles": {
        "a.csv": {
            "entityType": "Country"
        },
        "b.csv": {
            "entityType": "",
            "ignoreColumns": ["ignore1", "ignore2"]
        },
    },
    "variables": {
        "Variable 1": {
            "group": "Parent Group/Child Group 1"
        },
        "Variable 2": {
            "group": "Parent Group/Child Group 1"
        },
        "var3": {
            "name": "Var 3 Name",
            "description": "Var 3 Description",
            "nlSentences": ["Sentence 1", "Sentence 2"],
            "group": "Parent Group/Child Group 2",
        },
    },
}

TEST_SV_COLUMN_NAMES = [
    "Variable 1", "Variable 2", "var3", "Variable with no config"
]

EXPECTED_GROUPS = [
    StatVarGroup("custom/g/group_1", "Parent Group", "dc/g/Root"),
    StatVarGroup("custom/g/group_2", "Child Group 1", "custom/g/group_1"),
    StatVarGroup("custom/g/group_3", "Child Group 2", "custom/g/group_1"),
]

EXPECTED_VARIABLES = [
    StatVar(
        "custom/statvar_1",
        "Variable 1",
        group_id="custom/g/group_2",
    ),
    StatVar(
        "custom/statvar_2",
        "Variable 2",
        group_id="custom/g/group_2",
    ),
    StatVar(
        "custom/statvar_3",
        "Var 3 Name",
        description="Var 3 Description",
        nl_sentences=["Sentence 1", "Sentence 2"],
        group_id="custom/g/group_3",
    ),
    StatVar(
        "custom/statvar_4",
        "Variable with no config",
        group_id="dc/g/Root",
    ),
]


class TestConfig(unittest.TestCase):

  def test_variables_and_groups(self):
    self.maxDiff = None

    config = Config(CONFIG_DATA)
    variables, groups = config.variables_and_groups(TEST_SV_COLUMN_NAMES)
    self.assertListEqual(variables, EXPECTED_VARIABLES)
    self.assertListEqual(groups, EXPECTED_GROUPS)
