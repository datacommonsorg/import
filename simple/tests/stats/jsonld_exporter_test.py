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
from stats.jsonld_exporter import export_to_jsonld
from stats.db import create_and_update_db, create_sqlite_config, Triple
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
          Triple(subject_id="sub1", predicate="typeOf", object_id="StatisticalVariable"),
          Triple(subject_id="sub1", predicate="name", object_value="Name1")
      ])
      
      # Insert some observations
      df = pd.DataFrame(
          [("e1", "v1", "2026", "100", "p1", "", "", "", "", "")],
          columns=[
              "entity", "variable", "date", "value", "provenance", "unit",
              "scaling_factor", "measurement_method", "observation_period",
              "properties"
          ])
      
      # Mock input file as it's required by insert_observations but not used for DB operations in SqlDb
      mock_file = mock.Mock()
      mock_file.path = "dummy.csv"
      
      db.insert_observations(df, mock_file)
      db.commit()
      
      # Export with small chunk size to force multiple shards
      export_to_jsonld(db, output_dir, chunk_size=1)
      
      # Verify files exist
      # 2 triples with chunk_size=1 -> 2 shards (0 and 1)
      # 1 observation with chunk_size=1 -> 1 shard (2)
      shard_paths = [os.path.join(temp_dir, f"output-{i:05d}.jsonld") for i in range(3)]
      for path in shard_paths:
        self.assertTrue(os.path.exists(path), f"File {path} does not exist")
      
      # Verify content of shard 0 (should have first triple)
      with open(shard_paths[0], 'r') as f:
        shard0 = json.load(f)
        self.assertIn('@graph', shard0)
        nodes = {node['@id']: node for node in shard0['@graph']}
        self.assertIn('dcid:sub1', nodes)
        self.assertEqual(nodes['dcid:sub1']['@type'], 'dcid:StatisticalVariable')
          
      # Verify content of shard 2 (should have observations)
      with open(shard_paths[2], 'r') as f:
        shard2 = json.load(f)
        self.assertIn('@graph', shard2)
        nodes = {node['@id']: node for node in shard2['@graph']}
        self.assertIn('dcid:obs_0', nodes)
        self.assertEqual(nodes['dcid:obs_0']['dcid:value'], 100.0)

if __name__ == "__main__":
  unittest.main()

