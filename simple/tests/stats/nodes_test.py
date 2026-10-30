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

from stats import schema_constants as sc
from stats.config import Config
from stats.data import Property
from stats.data import Provenance
from stats.data import Triple
from stats.nodes import Nodes
from util.filesystem import create_store
from util.filesystem import File

CONFIG_DATA = {
    "inputFiles": [{
        "pattern": "a.csv",
        "provenance": "Provenance1"
    }, {
        "pattern": "b.csv",
        "ignoreColumns": ["ignore1", "ignore2"]
    }, {
        "pattern": "entities.csv",
        "rowEntityType": "FooEntity",
        "provenance": "Provenance1"
    }, {
        "pattern": "x.csv",
        "provenance": "Provenance1"
    }],
    "entities": {
        "FooEntity": {
            "name": "Foo Entity",
            "description": "Foo Entity description"
        }
    },
}

CONFIG = Config(CONFIG_DATA)


class TestNodes(unittest.TestCase):

  def make_file(self, path: str) -> File:
    return self.store.as_dir().open_file(path)

  def setUp(self):
    self.store = create_store("mem://")
    self.a = self.make_file("a.csv")
    self.b = self.make_file("b.csv")
    self.entities = self.make_file("entities.csv")
    self.x = self.make_file("x.csv")

  def tearDown(self):
    self.store.close()

  def test_provenance(self):
    nodes = Nodes(CONFIG)
    self.assertEqual(
        nodes.provenance(self.a),
        Provenance(id="Provenance1",
                   source_id="",
                   name="Provenance1",
                   url="",
                   provenance_id="Provenance1"))
    self.assertEqual(
        nodes.provenance(self.x),
        Provenance(id="Provenance1",
                   source_id="",
                   name="Provenance1",
                   url="",
                   provenance_id="Provenance1"))

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

  def test_root_stat_var_group_created(self):
    nodes = Nodes(CONFIG)
    self.assertIn(sc.ROOT_SVG_ID, nodes.groups)
    self.assertNotIn(sc.DEFAULT_CUSTOM_ROOT_SVG_ID, nodes.groups)
    root_svg = nodes.groups[sc.ROOT_SVG_ID]
    self.assertEqual(root_svg.id, "dc/g/Root")
    self.assertEqual(root_svg.name, "Data Commons Variables")
    self.assertEqual(root_svg.parent_id, "")
    root_triples = root_svg.triples()
    self.assertIn(Triple("dc/g/Root", "typeOf", object_id="StatVarGroup"),
                  root_triples)
    self.assertIn(
        Triple("dc/g/Root", "name", object_value="Data Commons Variables"),
        root_triples)
    for t in root_triples:
      self.assertNotEqual(t.predicate, "specializationOf")
      self.assertNotEqual(t.predicate, "includedIn")

  def test_triples_by_provenance_dir_and_deduplication(self):
    nodes = Nodes(Config(CONFIG_DATA))
    entity_type = nodes.entity_type("FooEntity", self.entities)
    entity_type.provenance_ids = ["prov/1", "prov/1"]
    shards = nodes.triples_by_provenance_dir()
    self.assertIn("prov/1", shards)
    triples = shards["prov/1"]
    normalized_tuples = [t.normalized_tuple() for t in triples]
    self.assertEqual(len(normalized_tuples), len(set(normalized_tuples)))
