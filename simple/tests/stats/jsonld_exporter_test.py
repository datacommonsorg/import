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
from unittest import mock

import pandas as pd
from stats.db import create_and_update_db
from stats.db import create_sqlite_config
from stats.db import Triple
from stats.jsonld_exporter import export_to_jsonld
from util.filesystem import create_store


class TestJsonLdExporter(unittest.TestCase):

  def test_export(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      # Create a test DB
      db_file = output_dir.open_file("test.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      # Insert some triples using keyword arguments as in db_test.py
      db.insert_triples([
          Triple(subject_id="sub1",
                 predicate="typeOf",
                 object_id="StatisticalVariable"),
          Triple(subject_id="sub1", predicate="name", object_value="Name1"),
          Triple(subject_id="p1",
                 predicate="url",
                 object_value="http://example.com/p1")
      ])

      # Insert some observations
      df = pd.DataFrame([("e1", "v1", "2026", "100", "p1", "", "", "", "", "")],
                        columns=[
                            "entity", "variable", "date", "value", "provenance",
                            "unit", "scaling_factor", "measurement_method",
                            "observation_period", "properties"
                        ])

      # Mock input file as it's required by insert_observations but not used for DB operations in SqlDb
      mock_file = mock.Mock()
      mock_file.path = "dummy.csv"

      db.insert_observations(df, mock_file)
      db.commit()

      # Export with small chunk size to force multiple shards
      export_to_jsonld(db, output_dir, chunk_size=1)

      # Verify files exist
      # 3 triples with chunk_size=1 -> 3 shards (0, 1 and 2)
      # 1 observation with chunk_size=1 -> 1 shard (3)
      shard_paths = [
          os.path.join(temp_dir, "node-00000.jsonld"),
          os.path.join(temp_dir, "node-00001.jsonld"),
          os.path.join(temp_dir, "node-00002.jsonld"),
          os.path.join(temp_dir, "observation-00000.jsonld")
      ]
      for path in shard_paths:
        self.assertTrue(os.path.exists(path), f"File {path} does not exist")

      # Verify content of shard 0 (should have first triple)
      with open(shard_paths[0], 'r') as f:
        shard0 = json.load(f)
        self.assertIn('@graph', shard0)
        nodes = {node['@id']: node for node in shard0['@graph']}
        self.assertIn('dcid:sub1', nodes)
        self.assertEqual(nodes['dcid:sub1']['@type'],
                         'dcid:StatisticalVariable')

      # Verify content of observation shard (should have observations)
      with open(shard_paths[3], 'r') as f:
        shard3 = json.load(f)
        self.assertIn('@graph', shard3)
        nodes = {node['@id']: node for node in shard3['@graph']}
        obs_nodes = [node for node in nodes if node.startswith('dcid:obs_')]
        self.assertTrue(
            len(obs_nodes) > 0, "No observation node found in shard")
        obs_node_id = obs_nodes[0]
        self.assertEqual(nodes[obs_node_id]['dcid:value'], 100.0)
        self.assertEqual(nodes[obs_node_id]['dcid:provenanceUrl'],
                         "http://example.com/p1")

  def test_empty_db(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      db_file = output_dir.open_file("test_empty.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      export_to_jsonld(db, output_dir, chunk_size=1)

      # Verify no shards created
      shard_paths = [
          os.path.join(temp_dir, "node-00000.jsonld"),
          os.path.join(temp_dir, "observation-00000.jsonld")
      ]
      for path in shard_paths:
        self.assertFalse(os.path.exists(path), f"File {path} should not exist")

  def test_double_prefixing(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      db_file = output_dir.open_file("test_prefix.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      db.insert_triples([
          Triple(subject_id="https://example.com/sub1",
                 predicate="typeOf",
                 object_id="StatisticalVariable"),
          Triple(subject_id="dcid:sub2", predicate="name", object_value="Name2")
      ])
      db.commit()

      export_to_jsonld(db, output_dir, chunk_size=10)

      shard_path = os.path.join(temp_dir, "node-00000.jsonld")
      self.assertTrue(os.path.exists(shard_path))

      with open(shard_path, 'r') as f:
        shard = json.load(f)
        nodes = {node['@id']: node for node in shard['@graph']}
        self.assertIn('dcid:example.com/sub1', nodes)
        self.assertIn('dcid:sub2', nodes)

  def test_malformed_json(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      db_file = output_dir.open_file("test_json.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      df = pd.DataFrame(
          [("e1", "v1", "2026", "100", "p1", "", "", "", "", "{invalid_json}")],
          columns=[
              "entity", "variable", "date", "value", "provenance", "unit",
              "scaling_factor", "measurement_method", "observation_period",
              "properties"
          ])

      mock_file = mock.Mock()
      mock_file.path = "dummy.csv"
      db.insert_observations(df, mock_file)
      db.commit()

      # Should log a warning but not fail
      export_to_jsonld(db, output_dir, chunk_size=10)

      shard_path = os.path.join(temp_dir, "observation-00000.jsonld")
      self.assertTrue(os.path.exists(shard_path))

  def test_custom_context(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      db_file = output_dir.open_file("test_context.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      db.insert_triples([
          Triple(subject_id="sub1",
                 predicate="https://example.com/ns/prop",
                 object_value="Value1")
      ])
      db.commit()

      # Pass a custom context
      custom_context = {"ex": "https://example.com/ns/"}
      export_to_jsonld(db, output_dir, chunk_size=10, context=custom_context)

      shard_path = os.path.join(temp_dir, "node-00000.jsonld")
      self.assertTrue(os.path.exists(shard_path))

      with open(shard_path, 'r') as f:
        shard = json.load(f)
        self.assertIn('@context', shard)
        self.assertIn('ex', shard['@context'])
        self.assertEqual(shard['@context']['ex'], "https://example.com/ns/")

        nodes = {node['@id']: node for node in shard['@graph']}
        self.assertIn('dcid:sub1', nodes)
        self.assertIn('ex:prop', nodes['dcid:sub1'])
        self.assertEqual(nodes['dcid:sub1']['ex:prop'], "Value1")

  def test_non_numeric_value(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      output_dir = temp_store.as_dir()

      db_file = output_dir.open_file("test_non_numeric.db")
      db = create_and_update_db(create_sqlite_config(db_file))

      df = pd.DataFrame(
          [("e1", "v1", "2026", "Unavailable", "p1", "", "", "", "", "")],
          columns=[
              "entity", "variable", "date", "value", "provenance", "unit",
              "scaling_factor", "measurement_method", "observation_period",
              "properties"
          ])

      mock_file = mock.Mock()
      mock_file.path = "dummy.csv"
      with self.assertRaises(ValueError) as ctx:
        db.insert_observations(df, mock_file)
      self.assertIn("Invalid non-numeric value(s)", str(ctx.exception))


if __name__ == "__main__":
  unittest.main()
