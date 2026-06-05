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

import json
import os
import tempfile
import unittest
from unittest import mock

import pandas as pd
from stats.data import Triple
from stats.jsonld_stream_db import JsonLdStreamDb
from util.filesystem import create_store


class TestJsonLdStreamDb(unittest.TestCase):

  def setUp(self):
    self.mock_config = mock.MagicMock()
    self.mock_config.custom_id_namespace.return_value = "custom"
    self.mock_config.data = {"importName": "test_import"}
    self.mock_config.import_name = lambda f: f.path.split("/")[0] if (f and "/" in f.path) else "test_import"

    self.mock_nodes = mock.MagicMock()
    self.mock_nodes.config = self.mock_config
    self.mock_nodes.provenances = {}

  def test_directory_creation(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)

      db = JsonLdStreamDb(output_dir=temp_store.as_dir(),
                          import_names=["test_import"],
                          nodes=self.mock_nodes)

      # The jsonld folder should be created under the output dir
      self.assertTrue(os.path.isdir(os.path.join(temp_dir, "jsonld")))

      # The db path should contain test_import
      self.assertEqual(db.import_name, "test_import")
      self.assertTrue(db.jsonld_dir.full_path().startswith(
          os.path.join(temp_dir, "jsonld", "test_import_")))

  def test_insert_observations_and_triples(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db = JsonLdStreamDb(output_dir=temp_store.as_dir(),
                          import_names=["test_import"],
                          nodes=self.mock_nodes)

      # Insert observations
      df = pd.DataFrame([("e1", "v1", "2026", "100", "p1", "", "", "", "", "")],
                        columns=[
                            "entity", "variable", "date", "value", "provenance",
                            "unit", "scaling_factor", "measurement_method",
                            "observation_period", "properties"
                        ])
      mock_file = mock.Mock(path="test_import/data.csv")
      db.insert_observations(df, mock_file)
      self.assertEqual(len(db._obs_records["test_import"]), 1)
      self.assertEqual(db._obs_records["test_import"][0][0], "e1")

      # Insert triples
      triples = [Triple("sub1", "pred1", object_value="val1")]
      db.insert_triples(triples, mock_file)
      self.assertEqual(len(db._triples["test_import"]), 1)

  def test_commit_and_close_local(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db = JsonLdStreamDb(output_dir=temp_store.as_dir(),
                          import_names=["test_import"],
                          nodes=self.mock_nodes)

      # Insert observations
      df = pd.DataFrame([("e1", "v1", "2026", "100", "p1", "", "", "", "", "")],
                        columns=[
                            "entity", "variable", "date", "value", "provenance",
                            "unit", "scaling_factor", "measurement_method",
                            "observation_period", "properties"
                        ])
      mock_file = mock.Mock(path="test_import/data.csv")
      db.insert_observations(df, mock_file)

      # Insert triples
      triples = [Triple("sub1", "typeOf", object_id="StatisticalVariable")]
      db.insert_triples(triples, mock_file)

      db.commit_and_close()

      # Shards should be written directly to the target unique directory
      target_dir_path = db.jsonld_dir.full_path()
      obs_shard = os.path.join(target_dir_path, "test_import", "observation-00000.jsonld")
      node_shard = os.path.join(target_dir_path, "test_import", "node-00000.jsonld")

      self.assertTrue(os.path.exists(obs_shard))
      self.assertTrue(os.path.exists(node_shard))

      # Validate observation shard content
      with open(obs_shard, "r") as f:
        data = json.load(f)
        self.assertIn("@graph", data)
        graph = data["@graph"]
        self.assertEqual(len(graph), 1)
        self.assertEqual(graph[0]["dcid:observationAbout"]["@id"], "dcid:e1")
        self.assertEqual(graph[0]["dcid:value"], 100)

      # Validate node shard content
      with open(node_shard, "r") as f:
        data = json.load(f)
        self.assertIn("@graph", data)
        graph = data["@graph"]
        self.assertEqual(len(graph), 1)
        self.assertEqual(graph[0]["@id"], "dcid:sub1")
        self.assertEqual(graph[0]["@type"], "dcid:StatisticalVariable")

  @mock.patch("google.cloud.storage.Client")
  def test_commit_and_close_gcs(self, mock_storage_client):
    # Setup GCS mock
    mock_client_instance = mock_storage_client.return_value
    mock_bucket = mock_client_instance.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)

      # Mock the output dir as a GCS path
      mock_output_dir = mock.MagicMock()
      mock_output_dir.open_dir.return_value.open_dir.return_value.full_path.return_value = "gs://my-bucket/ingestion/test"
      mock_output_dir.open_dir.return_value.open_dir.return_value.isdir.return_value = False

      db = JsonLdStreamDb(output_dir=mock_output_dir,
                          import_names=["test_import"],
                          nodes=self.mock_nodes)

      # Insert observation
      df = pd.DataFrame([("e1", "v1", "2026", "100", "p1", "", "", "", "", "")],
                        columns=[
                            "entity", "variable", "date", "value", "provenance",
                            "unit", "scaling_factor", "measurement_method",
                            "observation_period", "properties"
                        ])
      mock_file = mock.Mock(path="test_import/data.csv")
      db.insert_observations(df, mock_file)

      db.commit_and_close()

      # Verify storage bucket call was made
      mock_storage_client.assert_called_once()
      mock_client_instance.bucket.assert_called_with("my-bucket")

      # Verify upload blob calls
      mock_bucket.blob.assert_called_with(
          "ingestion/test/test_import/observation-00000.jsonld")
      mock_blob.upload_from_filename.assert_called_once()

  def test_node_fast_vs_rdflib_parity(self):
    """Rigorous parity test: Compares fast path output with rdflib path output."""
    from stats.jsonld_stream_db import _write_node_shard_fast
    from stats.jsonld_stream_db import _write_node_shard_rdflib

    complex_triples = [
        Triple(subject_id="sub1",
               predicate="typeOf",
               object_id="StatisticalVariable"),
        Triple(subject_id="sub1", predicate="typeOf", object_id="Thing"),
        Triple(subject_id="sub1", predicate="name", object_value="Test Node"),
        # Multi-valued properties
        Triple(subject_id="sub1",
               predicate="alternateName",
               object_value="Alias A"),
        Triple(subject_id="sub1",
               predicate="alternateName",
               object_value="Alias B"),
        # Duplicate values for testing deduplication
        Triple(subject_id="sub1",
               predicate="alternateName",
               object_value="Alias A"),
        Triple(subject_id="sub1", predicate="intValue", object_value=99),
        # References vs Values
        Triple(subject_id="sub1", predicate="memberOf", object_id="groupA"),
        # Number types
        Triple(subject_id="sub1", predicate="countValue", object_value=15.8),
        Triple(subject_id="sub1", predicate="intValue", object_value=99),
        # External URL predicate/object
        Triple(subject_id="sub1",
               predicate="http://schema.org/url",
               object_id="https://example.org"),
    ]
    from stats.jsonld_exporter import DCID_URL
    ns_map = {"dcid": DCID_URL}

    with tempfile.TemporaryDirectory() as temp_dir_fast, \
         tempfile.TemporaryDirectory() as temp_dir_rdflib:

      _write_node_shard_fast((complex_triples, 0, temp_dir_fast, ns_map))
      _write_node_shard_rdflib((complex_triples, 0, temp_dir_rdflib, ns_map))

      fast_file = os.path.join(temp_dir_fast, "node-00000.jsonld")
      rdflib_file = os.path.join(temp_dir_rdflib, "node-00000.jsonld")

      self.assertTrue(os.path.exists(fast_file))
      self.assertTrue(os.path.exists(rdflib_file))

      with open(fast_file, "r") as f:
        fast_json = json.load(f)
      with open(rdflib_file, "r") as f:
        rdflib_json = json.load(f)

      # Helper to normalize a JSON-LD graph for strict comparison
      def normalize_graph(graph):
        normalized = {}
        for item in graph["@graph"]:
          item_id = item["@id"]
          normalized_item = {}
          for k, v in item.items():
            if k == "@id":
              continue
            # If value is list, sort it to ensure order-independence
            if isinstance(v, list):
              sorted_v = sorted(
                  v,
                  key=lambda x: x["@id"]
                  if isinstance(x, dict) and "@id" in x else str(x))
              normalized_item[k] = sorted_v
            else:
              normalized_item[k] = v
          normalized[item_id] = normalized_item
        return normalized

      self.assertEqual(normalize_graph(fast_json), normalize_graph(rdflib_json))

  def test_observation_parsing_edge_cases(self):
    """Rigorous data type & properties parsing check to ensure zero property loss."""
    from stats.jsonld_stream_db import _write_observation_shard

    # Custom properties as nested JSON
    custom_props = json.dumps({
        "customIntProp": 42,
        "dcid:customStrProp": "customVal",
        "http://schema.org/url": "https://test-prop.org"
    })

    # Rows with edge-case numbers and strings
    chunk = [
        # entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props
        ("country/ALB", "v1", "2026", "99", "p1", "unit1", "100", "m1", "P1Y",
         custom_props),
        ("country/USA", "v1", "2026.5", "123.45", "p1", None, "10.5", None,
         None, None),
        ("country/IND", "v1", "2026-06", "Unavailable", "p1", None, None, None,
         None, None),
    ]

    ns_map = {"dcid": "https://datacommons.org/ontology/"}
    prov_urls = {"p1": "http://my-provenance.org/url"}

    with tempfile.TemporaryDirectory() as temp_dir:
      _write_observation_shard((chunk, 0, temp_dir, ns_map, prov_urls))

      shard_file = os.path.join(temp_dir, "observation-00000.jsonld")
      self.assertTrue(os.path.exists(shard_file))

      with open(shard_file, "r") as f:
        data = json.load(f)

      self.assertIn("@graph", data)
      graph = data["@graph"]
      self.assertEqual(len(graph), 3)

      # 1. Verify first observation (Int types, Custom Props)
      obs1 = [
          o for o in graph
          if o["dcid:observationAbout"]["@id"] == "dcid:country/ALB"
      ][0]
      self.assertEqual(obs1["dcid:value"], 99)
      self.assertEqual(obs1["dcid:observationDate"], 2026)
      self.assertEqual(obs1["dcid:scalingFactor"], 100)
      self.assertEqual(obs1["dcid:provenanceUrl"],
                       "http://my-provenance.org/url")
      self.assertEqual(obs1["dcid:observationPeriod"], "P1Y")

      # Verify custom properties from JSON string
      self.assertEqual(obs1["dcid:customIntProp"], 42)
      self.assertEqual(obs1["dcid:customStrProp"], "customVal")
      self.assertEqual(obs1["http://schema.org/url"], "https://test-prop.org")

      # 2. Verify second observation (Float types)
      obs2 = [
          o for o in graph
          if o["dcid:observationAbout"]["@id"] == "dcid:country/USA"
      ][0]
      self.assertEqual(obs2["dcid:value"], 123.45)
      self.assertEqual(obs2["dcid:observationDate"], 2026.5)
      self.assertEqual(obs2["dcid:scalingFactor"], 10.5)

      # 3. Verify third observation (Non-numeric value & Date string)
      obs3 = [
          o for o in graph
          if o["dcid:observationAbout"]["@id"] == "dcid:country/IND"
      ][0]
      self.assertEqual(obs3["dcid:value"], "Unavailable")
      self.assertEqual(obs3["dcid:observationDate"], "2026-06")


if __name__ == "__main__":
  unittest.main()
