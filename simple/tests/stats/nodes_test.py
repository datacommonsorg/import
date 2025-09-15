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

import os
import shutil
import tempfile
import unittest

from stats.config import Config
from stats.data import Property
from stats.data import Provenance
from stats.data import StatVar
from stats.data import StatVarGroup
from stats.nodes import Nodes
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from util.filesystem import create_store
from util.filesystem import File

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "nodes")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")

CONFIG_DATA = {
    "inputFiles": {
        "a.csv": {
            "entityType": "Country",
            "provenance": "Provenance1"
        },
        "b.csv": {
            "entityType": "",
            "ignoreColumns": ["ignore1", "ignore2"]
        },
        "events.csv": {
            "entityType": "Country",
            "provenance": "Provenance1",
            "eventType": "CrimeEvent"
        },
        "entities.csv": {
            "rowEntityType": "FooEntity",
            "provenance": "Provenance1"
        }
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
            "searchDescriptions": ["Sentence 1", "Sentence 2"],
            "group": "Parent Group/Child Group 2",
        },
    },
    "events": {
        "CrimeEvent": {
            "name": "Crime Event",
            "description": "Crime Event description"
        }
    },
    "entities": {
        "FooEntity": {
            "name": "Foo Entity",
            "description": "Foo Entity description"
        }
    },
    "sources": {
        "Source1": {
            "url": "http://source1.com",
            "provenances": {
                "Provenance1": "http://source1.com/provenance1"
            }
        }
    },
}

CONFIG = Config(CONFIG_DATA)

TEST_SV_COLUMN_AND_INPUT_FILE_NAMES = [
    ("Variable 1", "a.csv"),
    ("Variable 2", "a.csv"),
    ("var3", "b.csv"),
    ("Variable with no config", "x.csv"),
]

TEST_ENTITY_DCIDS_1 = ["country/AFG", "country/USA"]
TEST_ENTITY_TYPE_1 = "Country"

TEST_ENTITY_DCIDS_2 = ["dc/1234"]
TEST_ENTITY_TYPE_2 = "PowerPlant"

TEST_EVENT_TYPE_INPUT_FILE_NAMES = [("CrimeEvent", "events.csv"),
                                    ("Event no config", "x.csv")]

TEST_ENTITY_TYPE_INPUT_FILE_NAMES = [("FooEntity", "entities.csv"),
                                     ("Entity no config", "x.csv")]

TEST_PROPERTY_COLUMNS = ["foo", "foo bar"]


class TestNodes(unittest.TestCase):

  def make_file(self, path: str) -> File:
    return self.store.as_dir().open_file(path)

  def setUp(self):
    self.store = create_store("mem://")
    self.a = self.make_file("a.csv")
    self.b = self.make_file("b.csv")
    self.events = self.make_file("events.csv")
    self.entities = self.make_file("entities.csv")
    self.x = self.make_file("x.csv")
    self.map = {
        "a.csv": self.a,
        "b.csv": self.b,
        "events.csv": self.events,
        "entities.csv": self.entities,
        "x.csv": self.x,
    }

  def tearDown(self):
    self.store.close()

  def test_triples(self):
    nodes = Nodes(CONFIG)
    for sv_column_name, input_file_name in TEST_SV_COLUMN_AND_INPUT_FILE_NAMES:
      nodes.variable(sv_column_name, self.map[input_file_name])

    nodes.entities_with_type(TEST_ENTITY_DCIDS_1, TEST_ENTITY_TYPE_1)
    nodes.entities_with_type(TEST_ENTITY_DCIDS_2, TEST_ENTITY_TYPE_2)

    for event_type_name, input_file_name in TEST_EVENT_TYPE_INPUT_FILE_NAMES:
      nodes.event_type(event_type_name, self.map[input_file_name])

    for entity_type_name, input_file_name in TEST_ENTITY_TYPE_INPUT_FILE_NAMES:
      nodes.entity_type(entity_type_name, self.map[input_file_name])

    for property_column_name in TEST_PROPERTY_COLUMNS:
      nodes.property(property_column_name)

    with tempfile.TemporaryDirectory() as temp_dir:
      output_path = os.path.join(temp_dir, f"triples.csv")
      expected_path = os.path.join(_EXPECTED_DIR, f"triples.csv")
      nodes.triples(
          create_store(output_path, create_if_missing=True,
                       treat_as_file=True).as_file())

      if is_write_mode():
        shutil.copy(output_path, expected_path)
        return

      compare_files(self, output_path, expected_path)

  def test_variable_with_no_config(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable with no config", self.a)
    self.assertEqual(
        sv,
        StatVar(
            "Variable_with_no_config",
            "Variable with no config",
            group_id="c/g/Root",
            provenance_ids=["c/p/1"],
            source_ids=["c/s/1"],
        ),
    )

  def test_variable_with_config(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("var3", self.a)
    self.assertEqual(
        sv,
        StatVar(
            "var3",
            "Var 3 Name",
            description="Var 3 Description",
            search_descriptions=["Sentence 1", "Sentence 2"],
            group_id="custom/g/group_2",
            provenance_ids=["c/p/1"],
            source_ids=["c/s/1"],
        ),
    )

  def test_variable_with_group(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable 1", self.a)
    self.assertEqual(
        sv,
        StatVar(
            "Variable_1",
            "Variable 1",
            group_id="custom/g/group_2",
            provenance_ids=["c/p/1"],
            source_ids=["c/s/1"],
        ),
    )
    self.assertListEqual(
        list(nodes.groups.values()),
        [
            StatVarGroup(
                "custom/g/group_1",
                "Parent Group",
                "dc/g/Root",
                provenance_ids=["c/p/1"],
                source_ids=["c/s/1"],
            ),
            StatVarGroup(
                "custom/g/group_2",
                "Child Group 1",
                "custom/g/group_1",
                provenance_ids=["c/p/1"],
                source_ids=["c/s/1"],
            ),
        ],
    )

  def test_multiple_variables_in_same_group(self):
    nodes = Nodes(CONFIG)
    sv = nodes.variable("Variable 1", self.a)
    self.assertEqual(
        sv,
        StatVar(
            "Variable_1",
            "Variable 1",
            group_id="custom/g/group_2",
            provenance_ids=["c/p/1"],
            source_ids=["c/s/1"],
        ),
    )
    sv = nodes.variable("Variable 2", self.a)
    self.assertEqual(
        sv,
        StatVar(
            "Variable_2",
            "Variable 2",
            group_id="custom/g/group_2",
            provenance_ids=["c/p/1"],
            source_ids=["c/s/1"],
        ),
    )
    self.assertListEqual(
        list(nodes.groups.values()),
        [
            StatVarGroup(
                "custom/g/group_1",
                "Parent Group",
                "dc/g/Root",
                provenance_ids=["c/p/1"],
                source_ids=["c/s/1"],
            ),
            StatVarGroup(
                "custom/g/group_2",
                "Child Group 1",
                "custom/g/group_1",
                provenance_ids=["c/p/1"],
                source_ids=["c/s/1"],
            ),
        ],
    )

  def test_provenance(self):
    nodes = Nodes(CONFIG)
    nodes.variable("Variable 1", self.a)
    nodes.variable("Variable X", self.x)

    self.assertEqual(
        nodes.provenance(self.a),
        Provenance(id="c/p/1",
                   source_id="c/s/1",
                   name="Provenance1",
                   url="http://source1.com/provenance1"))
    self.assertEqual(
        nodes.provenance(self.x),
        Provenance(id="c/p/default",
                   source_id="c/s/default",
                   name="Custom Import",
                   url="custom-import"))

  def test_multiple_parent_groups(self):
    """This is to test a bug fix related to groups.

    The bug was that if there are multiple custom parent groups and
    if a variable is inserted inbetween,
    the second parent is put under custom/g/Root instead of dc/g/Root.

    The fix checks that both parents are under dc/g/Root
    """
    nodes = Nodes(Config({}))
    nodes.group("Parent 1/Child 1")
    nodes.variable("foo", self.x)
    nodes.group("Parent 2/Child 1")

    self.assertEqual(nodes.groups["Parent 1"].parent_id, "dc/g/Root")
    self.assertEqual(nodes.groups["Parent 2"].parent_id, "dc/g/Root")

  def test_properties(self):
    nodes = Nodes(CONFIG)
    property = nodes.property("foo")
    self.assertEqual(
        property,
        Property(
            "foo",
            "foo",
        ),
    )
    property = nodes.property("foo bar")
    self.assertEqual(
        property,
        Property(
            "foo_bar",
            "foo bar",
        ),
    )
    property = nodes.property("bar-foo")
    self.assertEqual(
        property,
        Property(
            "bar_foo",
            "bar-foo",
        ),
    )
    property = nodes.property("fooहिंदीbar")
    self.assertEqual(
        property,
        Property(
            "c/prop/1",
            "fooहिंदीbar",
        ),
    )
    property = nodes.property("barहिंदीfoo")
    self.assertEqual(
        property,
        Property(
            "c/prop/2",
            "barहिंदीfoo",
        ),
    )

  def test_custom_namespace_affects_generated_ids(self):
    # Use a variable name that cannot be converted to a valid DCID to force ID generation.
    cfg = Config({"customIdNamespace": "ONE"})
    nodes = Nodes(cfg)
    sv = nodes.variable("Var?1", self.a)
    # Generated stat var id should use the custom namespace
    self.assertEqual(sv.id, "ONE/statvar_1")

    # Group ids should use the custom namespace as well
    nodes = Nodes(cfg)
    nodes.group("Parent/Child")
    self.assertListEqual(
        list(nodes.groups.values()),
        [
            StatVarGroup(
                "ONE/g/group_1",
                "Parent",
                "dc/g/Root",
            ),
            StatVarGroup(
                "ONE/g/group_2",
                "Child",
                "ONE/g/group_1",
            ),
        ],
    )

  def test_default_custom_root_group_name_override(self):
    cfg = Config({"defaultCustomRootStatVarGroupName": "ONE Data"})
    nodes = Nodes(cfg)
    # Inserting a var with no group should create the default custom group
    nodes.variable("Any Var", self.a)
    # Find the default custom root svg among groups and verify its name
    from stats import schema_constants as sc
    default_groups = [
        g for g in nodes.groups.values()
        if g.id == sc.DEFAULT_CUSTOM_ROOT_SVG_ID
    ]
    self.assertTrue(default_groups, "Default custom root SVG should exist")
    self.assertEqual(default_groups[0].name, "ONE Data")
