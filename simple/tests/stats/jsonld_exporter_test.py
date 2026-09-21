# Copyright 2026 Google Inc.
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

import json
import os
import tempfile
import unittest

from rdflib import Graph
from rdflib import Literal
from rdflib import URIRef
from stats.jsonld_exporter import DCID_URL
from stats.jsonld_exporter import expand_id
from stats.jsonld_exporter import write_shard
from util.filesystem import create_store

_NS_MAP = {"dcid": DCID_URL}


class TestExpandId(unittest.TestCase):
  """expand_id turns the short ids used in triples into absolute URIs."""

  def test_empty_input_has_no_uri(self):
    for empty in [None, ""]:
      with self.subTest(item=empty):
        self.assertIsNone(expand_id(empty))

  def test_absolute_urls_are_left_alone(self):
    for url in ["http://example.com/a", "https://example.com/b"]:
      with self.subTest(url=url):
        self.assertEqual(expand_id(url), URIRef(url))

  def test_dcid_prefix_is_replaced_with_the_browser_url(self):
    self.assertEqual(expand_id("dcid:country/USA"),
                     URIRef(f"{DCID_URL}country/USA"))

  def test_bare_ids_are_treated_as_dcids(self):
    self.assertEqual(expand_id("country/USA"), URIRef(f"{DCID_URL}country/USA"))

  def test_leading_slash_does_not_double_up(self):
    self.assertEqual(expand_id("/country/USA"),
                     URIRef(f"{DCID_URL}country/USA"))


class TestWriteShard(unittest.TestCase):
  """write_shard serializes an RDF graph as a compacted JSON-LD shard."""

  def _write_and_read(self, graph: Graph, index: int, prefix: str) -> dict:
    with tempfile.TemporaryDirectory() as temp_dir:
      with create_store(temp_dir) as store:
        write_shard(graph, index, store.as_dir(), _NS_MAP, prefix=prefix)
        shard_path = os.path.join(temp_dir, f"{prefix}-{index:05d}.jsonld")
        self.assertTrue(os.path.exists(shard_path), shard_path)
        with open(shard_path, "r") as f:
          return json.load(f)

  def _graph_with(self, *triples) -> Graph:
    graph = Graph()
    for triple in triples:
      graph.add(triple)
    return graph

  def test_ids_are_compacted_against_the_namespace_map(self):
    subject = expand_id("dcid:country/USA")
    doc = self._write_and_read(
        self._graph_with(
            (subject, expand_id("typeOf"), expand_id("Country")),
            (subject, expand_id("name"), Literal("United States"))), 0, "node")

    self.assertEqual(doc["@context"], _NS_MAP)
    by_id = {node["@id"]: node for node in doc["@graph"]}
    self.assertIn("dcid:country/USA", by_id)
    node = by_id["dcid:country/USA"]
    self.assertEqual(node["dcid:typeOf"], {"@id": "dcid:Country"})
    self.assertEqual(node["dcid:name"], "United States")

  def test_every_subject_gets_its_own_graph_entry(self):
    doc = self._write_and_read(
        self._graph_with(
            (expand_id("dcid:a"), expand_id("name"), Literal("A")),
            (expand_id("dcid:b"), expand_id("name"), Literal("B"))), 1, "node")

    self.assertCountEqual([node["@id"] for node in doc["@graph"]],
                          ["dcid:a", "dcid:b"])

  def test_single_subject_is_still_wrapped_in_a_graph(self):
    # pyld compacts a lone subject to a top-level node rather than a @graph.
    # write_shard re-wraps it so every shard has the same shape.
    doc = self._write_and_read(
        self._graph_with(
            (expand_id("dcid:only"), expand_id("name"), Literal("Only"))), 2,
        "node")

    self.assertEqual(doc["@context"], _NS_MAP)
    self.assertEqual(len(doc["@graph"]), 1)
    self.assertEqual(doc["@graph"][0]["@id"], "dcid:only")

  def test_shard_name_uses_the_prefix_and_zero_padded_index(self):
    graph = self._graph_with(
        (expand_id("dcid:x"), expand_id("name"), Literal("X")))
    with tempfile.TemporaryDirectory() as temp_dir:
      with create_store(temp_dir) as store:
        write_shard(graph, 42, store.as_dir(), _NS_MAP, prefix="observation")
      self.assertEqual(os.listdir(temp_dir), ["observation-00042.jsonld"])


if __name__ == "__main__":
  unittest.main()
