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


class TestConfig(unittest.TestCase):

  def test_variable(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(
        config.variable("Variable 1"),
        StatVar("", "Variable 1", group_path="Parent Group/Child Group 1"),
    )
    self.assertEqual(
        config.variable("Variable 2"),
        StatVar("", "Variable 2", group_path="Parent Group/Child Group 1"),
    )
    self.assertEqual(
        config.variable("var3"),
        StatVar(
            "",
            "Var 3 Name",
            description="Var 3 Description",
            nl_sentences=["Sentence 1", "Sentence 2"],
            group_path="Parent Group/Child Group 2",
        ),
    )
    self.assertEqual(
        config.variable("Variable with no config"),
        StatVar("", "Variable with no config"),
    )

  def test_entity_type(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.entity_type("a.csv"), "Country")
    self.assertEqual(config.entity_type("b.csv"), "")
    self.assertEqual(config.entity_type("not-in-config.csv"), "")

  def test_ignore_columns(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.ignore_columns("a.csv"), [])
    self.assertEqual(config.ignore_columns("b.csv"), ["ignore1", "ignore2"])
    self.assertEqual(config.ignore_columns("not-in-config.csv"), [])
