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
from stats.data import Triple
from stats.nodes import Nodes

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

CONFIG = Config(CONFIG_DATA)

TEST_SV_COLUMN_NAMES = [
    "Variable 1",
    "Variable 2",
    "var3",
    "Variable with no config",
]

TEST_ENTITY_DCIDS_1 = ["country/AFG", "country/USA"]
TEST_ENTITY_TYPE_1 = "Country"

TEST_ENTITY_DCIDS_2 = ["dc/1234"]
TEST_ENTITY_TYPE_2 = "PowerPlant"

EXPECTED_TRIPLES = [
    Triple(
        "custom/g/group_1",
        "typeOf",
        object_id="StatVarGroup",
    ),
    Triple(
        "custom/g/group_1",
        "name",
        object_value="Parent Group",
    ),
    Triple(
        "custom/g/group_1",
        "specializationOf",
        object_id="dc/g/Root",
    ),
    Triple(
        "custom/g/group_2",
        "typeOf",
        object_id="StatVarGroup",
    ),
    Triple(
        "custom/g/group_2",
        "name",
        object_value="Child Group 1",
    ),
    Triple(
        "custom/g/group_2",
        "specializationOf",
        object_id="custom/g/group_1",
    ),
    Triple(
        "custom/g/group_3",
        "typeOf",
        object_id="StatVarGroup",
    ),
    Triple(
        "custom/g/group_3",
        "name",
        object_value="Child Group 2",
    ),
    Triple(
        "custom/g/group_3",
        "specializationOf",
        object_id="custom/g/group_1",
    ),
    Triple(
        "custom/g/Root",
        "typeOf",
        object_id="StatVarGroup",
    ),
    Triple(
        "custom/g/Root",
        "name",
        object_value="Custom Variables",
    ),
    Triple(
        "custom/g/Root",
        "specializationOf",
        object_id="dc/g/Root",
    ),
    Triple(
        "custom/statvar_1",
        "typeOf",
        object_id="StatisticalVariable",
    ),
    Triple(
        "custom/statvar_1",
        "name",
        object_value="Variable 1",
    ),
    Triple(
        "custom/statvar_1",
        "memberOf",
        object_id="custom/g/group_2",
    ),
    Triple(
        "custom/statvar_2",
        "typeOf",
        object_id="StatisticalVariable",
    ),
    Triple(
        "custom/statvar_2",
        "name",
        object_value="Variable 2",
    ),
    Triple(
        "custom/statvar_2",
        "memberOf",
        object_id="custom/g/group_2",
    ),
    Triple(
        "var3",
        "typeOf",
        object_id="StatisticalVariable",
    ),
    Triple(
        "var3",
        "name",
        object_value="Var 3 Name",
    ),
    Triple(
        "var3",
        "description",
        object_value="Var 3 Description",
    ),
    Triple(
        "var3",
        "memberOf",
        object_id="custom/g/group_3",
    ),
    Triple(
        "custom/statvar_3",
        "typeOf",
        object_id="StatisticalVariable",
    ),
    Triple(
        "custom/statvar_3",
        "name",
        object_value="Variable with no config",
    ),
    Triple(
        "custom/statvar_3",
        "memberOf",
        object_id="custom/g/Root",
    ),
    Triple(
        "country/AFG",
        "typeOf",
        object_id="Country",
    ),
    Triple(
        "country/USA",
        "typeOf",
        object_id="Country",
    ),
    Triple(
        "dc/1234",
        "typeOf",
        object_id="PowerPlant",
    ),
]


class TestNodes(unittest.TestCase):

  def test_variable_with_no_config(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable with no config")
    self.assertEqual(
        sv,
        StatVar("custom/statvar_1",
                "Variable with no config",
                group_id="custom/g/Root"),
    )

  def test_variable_with_config(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("var3")
    self.assertEqual(
        sv,
        StatVar(
            "var3",
            "Var 3 Name",
            description="Var 3 Description",
            nl_sentences=["Sentence 1", "Sentence 2"],
            group_id="custom/g/group_2",
        ),
    )

  def test_variable_with_group(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable 1")
    self.assertEqual(
        sv,
        StatVar("custom/statvar_1", "Variable 1", group_id="custom/g/group_2"),
    )
    self.assertListEqual(
        list(nodes.groups.values()),
        [
            StatVarGroup("custom/g/group_1", "Parent Group", "dc/g/Root"),
            StatVarGroup("custom/g/group_2", "Child Group 1",
                         "custom/g/group_1"),
        ],
    )

  def test_multiple_variables_in_same_group(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable 1")
    self.assertEqual(
        sv,
        StatVar("custom/statvar_1", "Variable 1", group_id="custom/g/group_2"),
    )
    sv = nodes.variable("Variable 2")
    self.assertEqual(
        sv,
        StatVar("custom/statvar_2", "Variable 2", group_id="custom/g/group_2"),
    )
    self.assertListEqual(
        list(nodes.groups.values()),
        [
            StatVarGroup("custom/g/group_1", "Parent Group", "dc/g/Root"),
            StatVarGroup("custom/g/group_2", "Child Group 1",
                         "custom/g/group_1"),
        ],
    )

  def test_triples(self):
    nodes = Nodes(CONFIG)
    for sv_column_name in TEST_SV_COLUMN_NAMES:
      nodes.variable(sv_column_name)

    nodes.entities_with_type(TEST_ENTITY_DCIDS_1, TEST_ENTITY_TYPE_1)
    nodes.entities_with_type(TEST_ENTITY_DCIDS_2, TEST_ENTITY_TYPE_2)

    triples = nodes.triples()

    self.assertListEqual(triples, EXPECTED_TRIPLES)

  def test_multiple_parent_groups(self):
    """This is to test a bug fix related to groups.
    
    The bug was that if there are multiple custom parent groups and
    if a variable is inserted inbetween,
    the second parent is put under custom/g/Root instead of dc/g/Root.

    The fix checks that both parents are under dc/g/Root
    """
    nodes = Nodes(Config({}))
    nodes.group("Parent 1/Child 1")
    nodes.variable("foo")
    nodes.group("Parent 2/Child 1")

    self.assertEqual(nodes.groups["Parent 1"].parent_id, "dc/g/Root")
    self.assertEqual(nodes.groups["Parent 2"].parent_id, "dc/g/Root")
