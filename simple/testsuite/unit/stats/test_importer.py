# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Tests for stats.importer."""

import unittest

from stats.data import Triple
from stats.importer import count_nodes_from_triples


def _type_of(node_id: str, node_type: str) -> Triple:
  return Triple(node_id, "typeOf", object_id=node_type)


class TestCountNodesFromTriples(unittest.TestCase):
  """Tests for count_nodes_from_triples."""

  def test_dedupes_nodes_across_chunks(self):
    """A node in two chunks that share `seen` counts once."""
    seen = set()
    first = count_nodes_from_triples(
        [_type_of("dcid:Count_X", "StatisticalVariable")], seen)
    after_first = set(seen)
    counts = first.add(
        count_nodes_from_triples(
            [_type_of("dcid:Count_X", "StatisticalVariable")], seen))

    self.assertEqual(counts.nodes, 1)
    self.assertEqual(counts.nodes_by_type, {"StatisticalVariable": 1})
    # `seen` only grows: nothing counted by the first chunk is dropped.
    self.assertLessEqual(after_first, seen)

  def test_counts_each_type_of_a_node_separately(self):
    """One node with two types counts twice, once under each type."""
    counts = count_nodes_from_triples([
        _type_of("dcid:region1", "City"),
        _type_of("dcid:region1", "dcs:AdministrativeArea2"),
    ])

    self.assertEqual(counts.nodes, 2)
    self.assertEqual(counts.nodes_by_type, {
        "AdministrativeArea2": 1,
        "City": 1
    })


if __name__ == "__main__":
  unittest.main()
