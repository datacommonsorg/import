# Copyright 2024 Google Inc.
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
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from fakeredis import FakeRedis
from freezegun import freeze_time
from stats import constants
from stats.db_cache import ENV_REDIS_HOST
from stats.runner import RunMode
from stats.runner import Runner
from tests.stats.test_util import compare_csv_files
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import read_full_db_from_file
from tests.stats.test_util import use_fake_gzip_time
from tests.stats.test_util import write_full_db_to_file

from util import dc_client

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "runner")
_CONFIG_DIR = os.path.join(_TEST_DATA_DIR, "config")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


@freeze_time("2025-01-23")
def _test_runner(test: unittest.TestCase,
                 test_name: str,
                 config_path: str = None,
                 output_dir_name: str = None,
                 run_mode: RunMode = RunMode.CUSTOM_DC,
                 input_db_file_name: str = None):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    if config_path:
      input_dir = None
      remote_entity_types_path = None
    else:
      input_dir = os.path.join(_INPUT_DIR, test_name)
      remote_entity_types_path = os.path.join(input_dir,
                                              "remote_entity_types.json")

    db_path = os.path.join(temp_dir, "datacommons.db")
    if (input_db_file_name):
      input_db_file = os.path.join(input_dir, input_db_file_name)
      read_full_db_from_file(db_path, input_db_file)

    output_dir_name = output_dir_name if output_dir_name else test_name
    expected_dir = os.path.join(_EXPECTED_DIR, output_dir_name)
    expected_nl_dir = os.path.join(expected_dir, constants.NL_DIR_NAME)
    Path(expected_nl_dir).mkdir(parents=True, exist_ok=True)

    output_nl_sentences_path = os.path.join(temp_dir, constants.NL_DIR_NAME,
                                            constants.SENTENCES_FILE_NAME)
    expected_nl_sentences_path = os.path.join(expected_dir,
                                              constants.NL_DIR_NAME,
                                              constants.SENTENCES_FILE_NAME)
    output_db_path = os.path.join(temp_dir, "datacommons.sql")
    expected_db_path = os.path.join(expected_dir, "datacommons.sql")
    output_topic_cache_json_path = os.path.join(temp_dir, constants.NL_DIR_NAME,
                                                constants.TOPIC_CACHE_FILE_NAME)
    expected_topic_cache_json_path = os.path.join(
        expected_dir, constants.NL_DIR_NAME, constants.TOPIC_CACHE_FILE_NAME)

    dc_client.get_property_of_entities = mock.MagicMock(return_value={})
    if remote_entity_types_path and os.path.exists(remote_entity_types_path):
      with open(remote_entity_types_path, "r") as f:
        dc_client.get_property_of_entities = mock.MagicMock(
            return_value=json.load(f))

    Runner(config_file_path=config_path,
           input_dir_path=input_dir,
           output_dir_path=temp_dir,
           mode=run_mode).run()

    if is_write_mode():
      write_full_db_to_file(db_path=db_path, output_path=expected_db_path)
      if os.path.exists(output_nl_sentences_path):
        shutil.copy(output_nl_sentences_path, expected_nl_sentences_path)
      if os.path.exists(output_topic_cache_json_path):
        shutil.copy(output_topic_cache_json_path,
                    expected_topic_cache_json_path)
      return

    write_full_db_to_file(db_path=db_path, output_path=output_db_path)
    compare_files(test, output_db_path, expected_db_path,
                  f"{test_name}: database")
    if os.path.exists(expected_nl_sentences_path):
      compare_csv_files(test, output_nl_sentences_path,
                        expected_nl_sentences_path,
                        f"{test_name}: nl sentences")
    if os.path.exists(expected_topic_cache_json_path) and os.path.exists(
        output_topic_cache_json_path):
      compare_files(test, output_topic_cache_json_path,
                    expected_topic_cache_json_path, f"{test_name}: topic cache")


class TestRunner(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    use_fake_gzip_time()

  def test_config_driven(self):
    _test_runner(self,
                 "config_driven",
                 config_path=os.path.join(_CONFIG_DIR, "config_driven.json"))

  def test_config_driven_invalid(self):
    with self.assertRaises(ValueError):
      _test_runner(self,
                   "config_driven",
                   config_path=os.path.join(_CONFIG_DIR,
                                            "config_driven_invalid.json"))

  def test_config_with_wildcards(self):
    _test_runner(self,
                 "config_with_wildcards",
                 config_path=os.path.join(_CONFIG_DIR,
                                          "config_with_wildcards.json"))

  def test_input_dir_driven(self):
    _test_runner(self, "input_dir_driven")

  def test_input_dir_driven_with_existing_old_schema_data(self):
    _test_runner(self,
                 "input_dir_driven_with_existing_old_schema_data",
                 input_db_file_name="sqlite_old_schema_populated.sql")

  def test_generate_svg_hierarchy(self):
    _test_runner(self, "generate_svg_hierarchy")

  def test_sv_nl_sentences(self):
    _test_runner(self, "sv_nl_sentences")

  def test_topic_nl_sentences(self):
    _test_runner(self, "topic_nl_sentences")

  def test_remote_entity_types(self):
    _test_runner(self, "remote_entity_types")

  def test_schema_update_only(self):
    _test_runner(self,
                 "schema_update_only",
                 run_mode=RunMode.SCHEMA_UPDATE,
                 input_db_file_name="sqlite_old_schema_populated.sql")

  def test_empty_input(self):
    with self.assertRaises(FileNotFoundError):
      _test_runner(self, "empty")

  def test_missing_config(self):
    with self.assertRaises(FileNotFoundError):
      _test_runner(self,
                   "empty",
                   config_path=os.path.join(_CONFIG_DIR, "nonexistent.json"))

  def test_empty_input_schema_update(self):
    """Schema update mode, input dir-driven, empty input, no database to start

    Expected output: initialized, empty database
    """
    _test_runner(self,
                 "empty",
                 output_dir_name="empty_initialized_db",
                 run_mode=RunMode.SCHEMA_UPDATE)

  def test_missing_config_schema_update(self):
    """Schema update mode, config file-driven, empty input, no database to start

    Expected output: initialized, empty database
    """
    _test_runner(self,
                 "missing_config_schema_update",
                 config_path=os.path.join(_CONFIG_DIR, "nonexistent.json"),
                 output_dir_name="empty_initialized_db",
                 run_mode=RunMode.SCHEMA_UPDATE)

  def test_with_subdirs_excluded(self):
    _test_runner(self,
                 "with_subdirs",
                 config_path=os.path.join(_CONFIG_DIR,
                                          "config_exclude_subdirs.json"),
                 output_dir_name="with_subdirs_excluded")

  def test_with_subdirs_included(self):
    _test_runner(self,
                 "with_subdirs",
                 config_path=os.path.join(_CONFIG_DIR,
                                          "config_include_subdirs.json"),
                 output_dir_name="with_subdirs_included")

  def test_namespace_prefixes(self):
    _test_runner(self, "namespace_prefixes")

  @mock.patch.dict(os.environ, {ENV_REDIS_HOST: "localhost"})
  def test_with_redis_db_cache(self):
    fake_redis = FakeRedis()
    fake_redis.set("somekey", "somevalue")
    self.assertEqual(1, len(fake_redis.keys("*")))
    with mock.patch("redis.Redis", return_value=fake_redis):
      _test_runner(self, "input_dir_driven")
      # Redis cache should be cleared.
      self.assertEqual(0, len(fake_redis.keys("*")))

  @mock.patch.dict(os.environ, {ENV_REDIS_HOST: "localhost"})
  def test_with_redis_db_cache_schema_update(self):
    fake_redis = FakeRedis()
    fake_redis.set("somekey", "somevalue")
    self.assertEqual(1, len(fake_redis.keys("*")))
    with mock.patch("redis.Redis", return_value=fake_redis):
      _test_runner(self,
                   "empty",
                   output_dir_name="empty_initialized_db",
                   run_mode=RunMode.SCHEMA_UPDATE)
      # Redis cache should NOT be cleared in schema update mode.
      self.assertEqual(1, len(fake_redis.keys("*")))

  def test_dcp_bridge(self):
    self.maxDiff = None
    with tempfile.TemporaryDirectory() as temp_dir:
      # Create a dedicated temp input directory
      input_dir = os.path.join(temp_dir, "input_dcp_bridge")
      os.makedirs(input_dir)

      # Copy files from input_dir_driven (except config.json)
      src_dir = os.path.join(_INPUT_DIR, "input_dir_driven")
      for filename in os.listdir(src_dir):
        if filename != "config.json":
          with open(os.path.join(src_dir, filename), "r") as src, \
               open(os.path.join(input_dir, filename), "w") as dst:
            dst.write(src.read())

      # Write a strict config.json using dcid:Provenance1
      strict_config = {
          "inputFiles": [{
              "pattern": "countries.csv",
              "importType": "observations",
              "format": "variablePerColumn",
              "entityType": "Country",
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "wikidataids.csv",
              "importType": "observations",
              "format": "variablePerColumn",
              "entityType": "Country",
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "variable_per_row.csv",
              "importType": "observations",
              "format": "variablePerRow",
              "columnMappings": {
                  "dcid:variableMeasured": "variable",
                  "dcid:observationAbout": "entity",
                  "dcid:observationDate": "date",
                  "dcid:value": "value"
              },
              "entityType": "Country",
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "author_entities.csv",
              "importType": "entities",
              "rowEntityType": "Author",
              "idColumn": "author_id",
              "entityColumns": ["author_country"],
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "article_entities.csv",
              "importType": "entities",
              "rowEntityType": "Article",
              "idColumn": "article_id",
              "entityColumns": ["article_author"],
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "*.mcf",
              "provenance": "dcid:Provenance1"
          }]
      }
      with open(os.path.join(input_dir, "config.json"), "w") as f:
        json.dump(strict_config, f)

      # Write provenance.mcf to satisfy strict validation
      provenance_mcf = ("Node: dcid:Source1\n"
                        "typeOf: dcs:Source\n"
                        "url: \"http://source1.com\"\n\n"
                        "Node: dcid:Provenance1\n"
                        "typeOf: dcs:Provenance\n"
                        "url: \"http://source1.com/provenance1\"\n"
                        "sourceLink: dcid:Source1\n")
      with open(os.path.join(input_dir, "provenance.mcf"), "w") as f:
        f.write(provenance_mcf)

      dc_client.get_property_of_entities = mock.MagicMock(return_value={})

      Runner(
          config_file_path=None,
          input_dir_path=input_dir,
          output_dir_path=temp_dir,
          mode=RunMode.DCP_BRIDGE,
      ).run()

      # Verify that NO SQLite database file is created
      db_path = os.path.join(temp_dir, "datacommons.db")
      self.assertFalse(os.path.exists(db_path))

      # Verify that NO nl directory is created (since GCS/local embeddings are stripped/disabled)
      nl_dir = os.path.join(temp_dir, "nl")
      self.assertFalse(os.path.exists(nl_dir))

      # Verify that a jsonld directory is created
      jsonld_dir = os.path.join(temp_dir, "jsonld")
      self.assertTrue(os.path.exists(jsonld_dir))

      # Find the subdirectory inside jsonld/
      subdirs = os.listdir(jsonld_dir)
      # There should be exactly 1 folder in jsonld/
      self.assertEqual(len(subdirs), 1)
      timestamped_dir = os.path.join(jsonld_dir, subdirs[0])
      self.assertTrue(os.path.isdir(timestamped_dir))

      # Ensure the timestamped directory has import subfolders
      import_dirs = os.listdir(timestamped_dir)
      self.assertGreater(len(import_dirs), 0)
      import_dir_path = os.path.join(timestamped_dir, import_dirs[0])
      self.assertTrue(os.path.isdir(import_dir_path))

      # Ensure the namespaced directory has files
      shard_files = os.listdir(import_dir_path)
      self.assertGreater(len(shard_files), 0)

      # Check that we have both node and observation shard files
      node_shards = [f for f in shard_files if f.startswith("node-")]
      obs_shards = [f for f in shard_files if f.startswith("observation-")]

      self.assertGreater(len(node_shards), 0)
      self.assertGreater(len(obs_shards), 0)

      # Verify that files are valid JSON-LD
      for filename in shard_files:
        filepath = os.path.join(import_dir_path, filename)
        self.assertTrue(filename.endswith(".jsonld"))
        with open(filepath, "r") as f:
          data = json.load(f)
          self.assertTrue(isinstance(data, (dict, list)))
          if filename.startswith("observation-"):
            graph = data.get("@graph", [])
            self.assertGreater(len(graph), 0)
            for obs in graph:
              self.assertEqual(obs.get("dcid:provenanceUrl"),
                               "http://source1.com/provenance1")

  def test_read_configs_from_subdirs(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      # Create subdirectories
      input_dir = os.path.join(temp_dir, "input")
      output_dir = os.path.join(temp_dir, "output")
      os.makedirs(input_dir)
      os.makedirs(output_dir)

      oecd_dir = os.path.join(input_dir, "oecd")
      os.makedirs(oecd_dir)
      ilo_dir = os.path.join(input_dir, "ilo", "ds1")
      os.makedirs(ilo_dir)

      oecd_config = {
          "inputFiles": [{
              "pattern": "data.csv",
              "importType": "OBSERVATIONS"
          }]
      }
      ilo_config = {
          "inputFiles": [{
              "pattern": "data.csv",
              "importType": "OBSERVATIONS"
          }]
      }

      with open(os.path.join(oecd_dir, "config.json"), "w") as f:
        json.dump(oecd_config, f)
      with open(os.path.join(ilo_dir, "config.json"), "w") as f:
        json.dump(ilo_config, f)

      # Instantiate Runner
      runner = Runner(config_file_path=None,
                      input_dir_path=input_dir,
                      output_dir_path=output_dir,
                      import_names=[constants.ALL_IMPORTS])

      # Verify merged config
      config = runner.config
      self.assertEqual(config.data["_dir_import_names"]["oecd"], "oecd")
      self.assertEqual(config.data["_dir_import_names"]["ilo/ds1"], "ilo/ds1")
      patterns = [entry["pattern"] for entry in config.data["inputFiles"]]
      self.assertIn("oecd/data.csv", patterns)
      self.assertIn("ilo/ds1/data.csv", patterns)

  @mock.patch("google.cloud.storage.Client")
  def test_dcp_bridge_multi_imports(self, mock_storage_client):
    self.maxDiff = None
    # Setup GCS mock
    mock_client_instance = mock_storage_client.return_value
    mock_bucket = mock_client_instance.bucket.return_value
    mock_blob = mock_bucket.blob.return_value

    with tempfile.TemporaryDirectory() as temp_dir:
      input_dir = os.path.join(temp_dir, "input")
      output_dir = os.path.join(temp_dir, "output")
      os.makedirs(input_dir)
      os.makedirs(output_dir)

      # Create oecd subdir
      oecd_dir = os.path.join(input_dir, "oecd")
      os.makedirs(oecd_dir)
      oecd_config = {
          "importName":
              "oecd",
          "inputFiles": [
              {
                  "pattern": "data.csv",
                  "entityType": "Country",
                  "provenance": "dcid:oecd",
                  # Assume no format = variable per row
                  "columnMappings": {
                      "dcid:variableMeasured": "variable",
                      "dcid:observationAbout": "entity",
                      "dcid:observationDate": "date",
                      "dcid:value": "value"
                  }
              },
              {
                  "pattern": "*.mcf",
                  "provenance": "dcid:oecd"
              }
          ]
      }
      with open(os.path.join(oecd_dir, "config.json"), "w") as f:
        json.dump(oecd_config, f)

      # OECD data.csv (entity, date, variable, value)
      oecd_data = "entity,date,variable,value\nUSA,2020,Average_Age_Frog,5.5\n"
      with open(os.path.join(oecd_dir, "data.csv"), "w") as f:
        f.write(oecd_data)

      # OECD oecd.mcf defining the provenance and source
      oecd_mcf = ("Node: dcid:OecdSource\n"
                  "typeOf: dcs:Source\n\n"
                  "Node: dcid:oecd\n"
                  "typeOf: dcs:Provenance\n"
                  "sourceLink: dcid:OecdSource\n")
      with open(os.path.join(oecd_dir, "oecd.mcf"), "w") as f:
        f.write(oecd_mcf)

      # Create ilo subdir
      ilo_dir = os.path.join(input_dir, "ilo", "ds1")
      os.makedirs(ilo_dir)
      ilo_config = {
          "importName":
              "ilo",
          "inputFiles": [
              {
                  "pattern": "data.csv",
                  "entityType": "Country",
                  "provenance": "dcid:ilo",
                  # Assume no format = variable per row
                  "columnMappings": {
                      "dcid:variableMeasured": "variable",
                      "dcid:observationAbout": "entity",
                      "dcid:observationDate": "date",
                      "dcid:value": "value"
                  }
              },
              {
                  "pattern": "*.mcf",
                  "provenance": "dcid:ilo"
              }
          ]
      }
      with open(os.path.join(ilo_dir, "config.json"), "w") as f:
        json.dump(ilo_config, f)

      ilo_data = "entity,date,variable,value\nUSA,2020,Count_Frog_Green,40\n"
      with open(os.path.join(ilo_dir, "data.csv"), "w") as f:
        f.write(ilo_data)

      # ILO ilo.mcf defining the provenance and source
      ilo_mcf = ("Node: dcid:IloSource\n"
                 "typeOf: dcs:Source\n\n"
                 "Node: dcid:ilo\n"
                 "typeOf: dcs:Provenance\n"
                 "sourceLink: dcid:IloSource\n")
      with open(os.path.join(ilo_dir, "ilo.mcf"), "w") as f:
        f.write(ilo_mcf)

      # Mock DC Client calls
      dc_client.get_property_of_entities = mock.MagicMock(return_value={})

      # Run runner with mocked GCS output path and workflow name
      from util.filesystem import _StoreWrapper
      original_full_path = _StoreWrapper.full_path

      def mock_full_path(self, sub_path=""):
        if "jsonld" in self.path:
          return f"gs://my-bucket/jsonld/run_timestamp"
        return original_full_path(self, sub_path)

      with mock.patch.dict(os.environ,
                           {"INGESTION_WORKFLOW_NAME": "my-workflow"}):
        with mock.patch.object(_StoreWrapper, "full_path", mock_full_path):
          runner = Runner(config_file_path=None,
                          input_dir_path=input_dir,
                          output_dir_path=output_dir,
                          mode=RunMode.DCP_BRIDGE,
                          import_names=[constants.ALL_IMPORTS])
          runner.run()

      # Verify GCS client calls were made
      mock_storage_client.assert_called_once()
      mock_client_instance.bucket.assert_called_with("my-bucket")

      # Verify that blob uploads were requested for both imports
      called_blobs = [call[0][0] for call in mock_bucket.blob.call_args_list]

      # We expect node and observation files under each namespace
      self.assertTrue(
          any(
              b.startswith("jsonld/run_timestamp/oecd/observation-")
              for b in called_blobs))
      self.assertTrue(
          any(
              b.startswith("jsonld/run_timestamp/oecd/node-")
              for b in called_blobs))
      self.assertTrue(
          any(
              b.startswith("jsonld/run_timestamp/ilo/observation-")
              for b in called_blobs))
      self.assertTrue(
          any(
              b.startswith("jsonld/run_timestamp/ilo/node-")
              for b in called_blobs))

      # Assert that trigger_workflow_info holds paths for both oecd and ilo
      self.assertEqual(len(runner.trigger_workflow_info), 2)
      trigger_names = sorted(
          [t["importName"] for t in runner.trigger_workflow_info])
      self.assertEqual(trigger_names, ["ilo", "oecd"])


class TestMain(unittest.TestCase):

  @mock.patch('stats.main.Runner')
  def test_run_with_import_name(self, mock_runner):
    from stats.main import _run
    from stats.main import FLAGS

    # Parse flags with dummy argv to avoid UnparsedFlagAccessError
    FLAGS(["test_program"])

    # Set flags
    FLAGS.input_dir = "/base/input"
    FLAGS.imports = ["oecd"]
    FLAGS.config_file = None
    FLAGS.output_dir = "/output"
    FLAGS.mode = RunMode.CUSTOM_DC

    _run()

    mock_runner.assert_called_once_with(config_file_path=None,
                                        input_dir_path="/base/input",
                                        output_dir_path="/output",
                                        mode=RunMode.CUSTOM_DC,
                                        import_names=["oecd"])
