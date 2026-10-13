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

import collections
import json
import os
from pathlib import Path
import shutil
import tempfile
import unittest
from unittest import mock

from freezegun import freeze_time
import numpy as np
from stats import constants
from stats.data import ValidationErrorType
from stats.runner import create_store as real_create_store
from stats.runner import ImportProcResult
from stats.runner import Runner
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import use_fake_gzip_time

from util import dc_client

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "runner")
_CONFIG_DIR = os.path.join(_TEST_DATA_DIR, "config")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _sort_key(graph_entry: dict) -> str:
  """Orders JSON-LD entries so comparisons ignore shard and write order."""
  return json.dumps(graph_entry, sort_keys=True)


@freeze_time("2025-01-23")
def _test_runner(test: unittest.TestCase,
                 test_name: str,
                 config_path: str = None,
                 output_dir_name: str = None):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    if config_path:
      input_dir = None
      remote_entity_types_path = None
    else:
      input_dir = os.path.join(_INPUT_DIR, test_name)
      remote_entity_types_path = os.path.join(input_dir,
                                              "remote_entity_types.json")

    output_dir_name = output_dir_name if output_dir_name else test_name
    expected_dir = os.path.join(_EXPECTED_DIR, output_dir_name)
    expected_jsonld_dir = os.path.join(expected_dir, "jsonld")
    output_jsonld_dir = os.path.join(temp_dir, "jsonld")

    dc_client.get_property_of_entities = mock.MagicMock(return_value={})
    if remote_entity_types_path and os.path.exists(remote_entity_types_path):
      with open(remote_entity_types_path, "r") as f:
        dc_client.get_property_of_entities = mock.MagicMock(
            return_value=json.load(f))

    # TODO: Run these tests with use_multiprocessing=True to match production.
    # Child processes don't inherit the dc_client mocks above, so this needs a
    # stub DC API server (e.g. via DC_API_ROOT) first.
    Runner(config_file_path=config_path,
           input_dir_path=input_dir,
           output_dir_path=temp_dir,
           use_multiprocessing=False).run()

    if is_write_mode():
      if os.path.exists(output_jsonld_dir):
        shutil.rmtree(expected_jsonld_dir, ignore_errors=True)
        shutil.copytree(output_jsonld_dir, expected_jsonld_dir)
      return

    def load_jsonld(dir_path):
      """Collects every node and observation written under dir_path."""
      nodes = []
      observations = []
      for root, _, files in os.walk(dir_path):
        for file_name in files:
          with open(os.path.join(root, file_name), "r") as f:
            graph = json.load(f)["@graph"]
          target = nodes if file_name.startswith("node-") else observations
          target.extend(graph)
      return sorted(nodes, key=_sort_key), sorted(observations, key=_sort_key)

    output_nodes, output_obs = load_jsonld(output_jsonld_dir)
    expected_nodes, expected_obs = load_jsonld(expected_jsonld_dir)

    test.assertEqual(output_nodes, expected_nodes,
                     f"{test_name}: nodes mismatch")
    test.assertEqual(output_obs, expected_obs,
                     f"{test_name}: observations mismatch")


class TestRunner(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    use_fake_gzip_time()

  def test_config_driven(self):
    """Config passed as a file, inputs pulled from its dataDownloadUrl."""
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
    """Same as config_driven, but the inputFiles patterns are globs."""
    _test_runner(self,
                 "config_with_wildcards",
                 config_path=os.path.join(_CONFIG_DIR,
                                          "config_with_wildcards.json"))

  def test_input_dir_driven(self):
    """Config discovered as config.json inside the input dir, as DCP does it."""
    _test_runner(self, "input_dir_driven")

  def test_remote_entity_types(self):
    """Proxy entity nodes typed from what Base DC reports."""
    _test_runner(self, "remote_entity_types")

  def test_entities(self):
    _test_runner(self, "entities")

  def test_events(self):
    _test_runner(self, "events")

  def test_empty_input(self):
    with self.assertRaises(FileNotFoundError):
      _test_runner(self, "empty")

  def test_missing_config(self):
    with self.assertRaises(FileNotFoundError):
      _test_runner(self,
                   "empty",
                   config_path=os.path.join(_CONFIG_DIR, "nonexistent.json"))

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
              "entityType": "Country",
              "provenance": "dcid:Provenance1",
              "columnMappings": {
                  "dcid:variableMeasured": "variable",
                  "dcid:observationAbout": "place",
                  "dcid:observationDate": "year",
                  "dcid:value": "value",
              },
              "columnsToResolve": ["place"],
          }, {
              "pattern": "wikidataids.csv",
              "importType": "observations",
              "entityType": "Country",
              "provenance": "dcid:Provenance1",
              "columnMappings": {
                  "dcid:variableMeasured": "variable",
                  "dcid:observationAbout": "wikidataid",
                  "dcid:observationDate": "year",
                  "dcid:value": "value",
              },
              "columnsToResolve": ["wikidataid"],
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
              "columnsToResolve": ["author_country"],
              "provenance": "dcid:Provenance1"
          }, {
              "pattern": "article_entities.csv",
              "importType": "entities",
              "rowEntityType": "Article",
              "idColumn": "article_id",
              "columnsToResolve": ["article_author"],
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
                        "source: dcid:Source1\n")
      with open(os.path.join(input_dir, "provenance.mcf"), "w") as f:
        f.write(provenance_mcf)

      dc_client.get_property_of_entities = mock.MagicMock(return_value={})

      Runner(
          config_file_path=None,
          input_dir_path=input_dir,
          output_dir_path=temp_dir,
          use_multiprocessing=False,
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
                  "source: dcid:OecdSource\n")
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
                 "source: dcid:IloSource\n")
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

      mock_store = mock.MagicMock()
      from stats.runner import create_store as real_create_store

      def side_effect(path, *args, **kwargs):
        if "ingestion_records" in path:
          return mock_store
        return real_create_store(path, *args, **kwargs)

      with (mock.patch("stats.runner.create_store", side_effect=side_effect),
            mock.patch.dict(
                os.environ, {
                    "INGESTION_WORKFLOW_NAME": "my-workflow",
                    "WORKFLOW_EXECUTION_ID": "my-execution",
                    "TEMP_LOCATION": "gs://my-bucket/temp"
                })):
        with mock.patch.object(_StoreWrapper, "full_path", mock_full_path):
          runner = Runner(config_file_path=None,
                          input_dir_path=input_dir,
                          output_dir_path=output_dir,
                          import_names=[constants.ALL_IMPORTS],
                          use_multiprocessing=False)
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

      # Verify GCS Handshake payload was written correctly
      entered_mock = mock_store.__enter__.return_value
      entered_mock.as_file.return_value.write.assert_called_once()
      written_str = entered_mock.as_file.return_value.write.call_args[0][0]
      written_payload = json.loads(written_str)
      self.assertIn("importList", written_payload)
      self.assertIn("generateStatVarGroups", written_payload)

      decoded_import_list = json.loads(written_payload["importList"])
      self.assertEqual(len(decoded_import_list), 2)
      trigger_names = sorted([t["importName"] for t in decoded_import_list])
      self.assertEqual(trigger_names, ["ilo", "oecd"])

  @mock.patch.dict(
      os.environ, {
          "WORKFLOW_EXECUTION_ID": "test_wf_id_fail",
          "TEMP_LOCATION": "gs://test_bucket_fail/temp"
      })
  def test_dcp_bridge_header_validation_failure_handshake(self):
    self.maxDiff = None

    mock_file_store = mock.MagicMock()
    mock_store_ctx = mock.MagicMock()
    mock_store_ctx.__enter__.return_value = mock_file_store

    def side_effect(path, *args, **kwargs):
      if "ingestion_records" in path:
        return mock_store_ctx
      return real_create_store(path, *args, **kwargs)

    with mock.patch("stats.runner.create_store",
                    side_effect=side_effect) as mock_create_store:
      with tempfile.TemporaryDirectory() as temp_dir:
        input_dir = os.path.join(temp_dir, "input_fail")
        os.makedirs(input_dir)

        csv_content = (
            "country_code,metric_name,year,metric_value,unmapped_metadata\n"
            "country/USA,var1,2023,123.4,some_metadata_value\n")
        with open(os.path.join(input_dir, "input.csv"), "w") as f:
          f.write(csv_content)

        config_data = {
            "inputFiles": [{
                "pattern": "input.csv",
                "importType": "observations",
                "format": "variablePerRow",
                "entityType": "Country",
                "provenance": "dcid:provenance/FailTest",
                "columnMappings": {
                    "dcid:observationAbout": "country_code",
                    "dcid:variableMeasured": "metric_name",
                    "dcid:observationDate": "year",
                    "dcid:value": "metric_value"
                }
            }, {
                "pattern": "provenance.mcf"
            }]
        }
        with open(os.path.join(input_dir, "config.json"), "w") as f:
          json.dump(config_data, f)

        provenance_mcf = ("Node: dcid:source/FailSource\n"
                          "typeOf: dcs:Source\n"
                          "url: \"https://fail.com\"\n\n"
                          "Node: dcid:provenance/FailTest\n"
                          "typeOf: dcs:Provenance\n"
                          "url: \"https://fail.com/provenance\"\n"
                          "source: dcid:source/FailSource\n")
        with open(os.path.join(input_dir, "provenance.mcf"), "w") as f:
          f.write(provenance_mcf)

        runner = Runner(
            config_file_path=None,
            input_dir_path=input_dir,
            output_dir_path=temp_dir,
        )

        with self.assertRaises(ValueError) as context:
          runner.run()

        self.assertIn("CSV Header Validation Failed", str(context.exception))

        expected_handshake_path = "gs://test_bucket_fail/temp/datacommons/ingestion_records/test_wf_id_fail.json"
        mock_create_store.assert_called_with(expected_handshake_path,
                                             create_if_missing=True,
                                             treat_as_file=True)

        entered_mock = mock_store_ctx.__enter__.return_value
        write_mock = entered_mock.as_file.return_value.write
        write_mock.assert_called_once()
        written_str = write_mock.call_args[0][0]
        written_payload = json.loads(written_str)

        self.assertEqual(written_payload["status"], "FAILURE")
        self.assertEqual(len(written_payload["errors"]), 1)

        err = written_payload["errors"][0]
        self.assertEqual(err["file"], "input.csv")
        self.assertEqual(err["errorType"], ValidationErrorType.UNMAPPED_COLUMNS)
        self.assertEqual(err["problemColumns"], ["unmapped_metadata"])
        self.assertIn("contains unmapped columns", err["errorMessage"])


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

    _run()

    mock_runner.assert_called_once_with(
        config_file_path=None,
        input_dir_path="/base/input",
        output_dir_path="/output",
        import_names=["oecd"],
        import_proxy_entities=True,
    )

  @mock.patch('stats.main.Runner')
  def test_deprecated_mode_flag_is_accepted_and_ignored(self, mock_runner):
    """--mode must not break existing callers, and must not reach Runner.

    DCP passes DATA_RUN_MODE=dcpbridge and the retired CDC builds pass
    customdc / maindc / schemaupdate. None of them should fail, and none of
    them should change what the importer does.
    """
    from stats.main import _run
    from stats.main import FLAGS

    FLAGS(["test_program"])
    FLAGS.input_dir = "/base/input"
    FLAGS.imports = []
    FLAGS.config_file = None
    FLAGS.output_dir = "/output"

    for mode in [
        "", "dcpbridge", "customdc", "maindc", "schemaupdate", "something_else"
    ]:
      with self.subTest(mode=mode):
        mock_runner.reset_mock()
        FLAGS.mode = mode

        _run()

        mock_runner.assert_called_once_with(
            config_file_path=None,
            input_dir_path="/base/input",
            output_dir_path="/output",
            import_names=[],
            import_proxy_entities=True,
        )
        self.assertNotIn("mode", mock_runner.call_args.kwargs)

    FLAGS.mode = ""

  def test_removed_modes_log_a_warning(self):
    from stats import main as main_module

    main_module.FLAGS(["test_program"])

    for mode in ["customdc", "maindc", "schemaupdate"]:
      with self.subTest(mode=mode):
        main_module.FLAGS.mode = mode
        with self.assertLogs(level="WARNING") as logs:
          main_module._warn_if_mode_is_set()
        self.assertTrue(any(mode in line for line in logs.output))

    for mode in ["", "dcpbridge"]:
      with self.subTest(mode=mode):
        main_module.FLAGS.mode = mode
        with mock.patch.object(main_module.logging, "warning") as mock_warn:
          main_module._warn_if_mode_is_set()
        mock_warn.assert_not_called()

    main_module.FLAGS.mode = ""

  def test_merge_split_part_aggregates_collisions_and_dcids(self):
    """Verifies that split parts are correctly merged and cross-part collisions are detected."""
    runner = Runner.__new__(Runner)
    runner._split_parts_left = {}
    runner._split_entity_dcids = {}
    runner._split_obs_hashes = {}
    runner.nodes = mock.MagicMock()
    runner.graph_writer = mock.MagicMock()
    runner.graph_writer.obs_collision_count = 0
    runner.graph_writer.file_collision_counts = collections.defaultdict(int)

    # Simulate part 0 of 2 with 2 observation hashes and entity 'dcid:Place1'
    res_part0 = ImportProcResult(
        file_rel_path="data/test.csv",
        obs_collision_count=0,
        file_collision_counts={},
        file_sample_collisions={},
        resolved_entities={},
        event_types={},
        entity_types={},
        variables={},
        sources={},
        provenances={},
        groups={},
        properties={},
        processed_imports=set(),
        num_chunks=2,
        entity_dcids=["dcid:Place1"],
        obs_hashes=np.array([100, 200], dtype=np.uint64),
    )
    # Part 0 finishes: returns False because 1 part remains
    self.assertFalse(runner._merge_split_part(res_part0))
    self.assertIn("data/test.csv", runner._split_parts_left)
    self.assertEqual(runner._split_parts_left["data/test.csv"], 1)

    # Simulate part 1 of 2 with duplicate hash 200, new hash 300, and entity 'dcid:Place2'
    res_part1 = ImportProcResult(
        file_rel_path="data/test.csv",
        obs_collision_count=0,
        file_collision_counts={},
        file_sample_collisions={},
        resolved_entities={},
        event_types={},
        entity_types={},
        variables={},
        sources={},
        provenances={},
        groups={},
        properties={},
        processed_imports=set(),
        num_chunks=2,
        entity_dcids=["dcid:Place2"],
        obs_hashes=np.array([200, 300], dtype=np.uint64),
    )
    # Part 1 finishes: returns True because all parts are completed
    self.assertTrue(runner._merge_split_part(res_part1))
    # State cleaned up for this completed file
    self.assertNotIn("data/test.csv", runner._split_parts_left)
    # Duplicate hash 200 across parts detected: 4 total - 3 unique = 1 collision
    self.assertEqual(runner.graph_writer.obs_collision_count, 1)
    self.assertEqual(runner.graph_writer.file_collision_counts["test.csv"], 1)

  def test_plan_import_tasks_splitting_and_ordering(self):
    """Verifies that large observation CSVs are split and MCF files prioritized."""
    from stats.config import Config
    from util.filesystem import File

    config_dict = {
        "inputFiles": [
            {
                "pattern": "obs_large.csv",
                "importType": "observations"
            },
            {
                "pattern": "obs_small.csv",
                "importType": "observations"
            },
            {
                "pattern": "entities.csv",
                "importType": "entities"
            },
            {
                "pattern": "prov.mcf"
            },
        ]
    }
    runner = Runner.__new__(Runner)
    runner.config = Config(config_dict)

    def make_file_mock(path, size_bytes):
      f = mock.MagicMock(spec=File)
      f.path = path
      f.name.return_value = os.path.basename(path)
      f.full_path.return_value = f"/tmp/{path}"
      f.size.return_value = size_bytes
      return f

    file_mcf = make_file_mock("prov.mcf", 1024)
    file_small = make_file_mock("obs_small.csv", 2 * 1024 * 1024)
    file_large = make_file_mock("obs_large.csv", 24 * 1024 * 1024)
    file_entities = make_file_mock("entities.csv", 20 * 1024 * 1024)

    files = [file_small, file_large, file_mcf, file_entities]
    tasks = runner._plan_import_tasks(files, num_processes=8)

    # 1 mcf + 1 small + 3 large + 1 entities = 6 tasks
    self.assertEqual(len(tasks), 6)

    # MCF file must be scheduled first
    self.assertEqual(tasks[0].file.path, "prov.mcf")

    # Tasks for large.csv should have num_chunks=3
    large_tasks = [t for t in tasks if t.file.path == "obs_large.csv"]
    self.assertEqual(len(large_tasks), 3)
    self.assertEqual([t.chunk_index for t in large_tasks], [0, 1, 2])
    for t in large_tasks:
      self.assertEqual(t.num_chunks, 3)
      self.assertEqual(t.est_bytes, 8 * 1024 * 1024)

    # Non-observation files or small files should have num_chunks=1
    small_task = next(t for t in tasks if t.file.path == "obs_small.csv")
    self.assertEqual(small_task.num_chunks, 1)

    entities_task = next(t for t in tasks if t.file.path == "entities.csv")
    self.assertEqual(entities_task.num_chunks, 1)

  def test_validate_all_headers_multi_file_concurrency(self):
    """Verifies that header validation aggregates errors across multiple files concurrently."""
    from stats.config import Config
    from stats.nodes import Nodes
    from util.filesystem import create_store

    config_dict = {
        "inputFiles": [{
            "pattern": "bad1.csv",
            "importType": "observations",
            "columnMappings": {
                "dcid:observationAbout": "entity",
                "dcid:variableMeasured": "variable",
                "dcid:observationDate": "date",
                "dcid:value": "value"
            }
        }, {
            "pattern": "bad2.csv",
            "importType": "observations",
            "columnMappings": {
                "dcid:observationAbout": "entity",
                "dcid:variableMeasured": "variable",
                "dcid:observationDate": "date",
                "dcid:value": "value"
            }
        }, {
            "pattern": "good.csv",
            "importType": "observations",
            "columnMappings": {
                "dcid:observationAbout": "entity",
                "dcid:variableMeasured": "variable",
                "dcid:observationDate": "date",
                "dcid:value": "value"
            }
        }]
    }

    with tempfile.TemporaryDirectory() as temp_dir:
      with open(os.path.join(temp_dir, "bad1.csv"), "w") as f:
        f.write(
            "entity,variable,date,value,unmapped1\ngeoId/06,Var,2020,1,foo\n")
      with open(os.path.join(temp_dir, "bad2.csv"), "w") as f:
        f.write(
            "entity,variable,date,value,unmapped2\ngeoId/06,Var,2020,1,bar\n")
      with open(os.path.join(temp_dir, "good.csv"), "w") as f:
        f.write("entity,variable,date,value\ngeoId/06,Var,2020,1\n")

      store = create_store(temp_dir)
      csv_files = [
          store.as_dir().open_file("bad1.csv"),
          store.as_dir().open_file("bad2.csv"),
          store.as_dir().open_file("good.csv"),
      ]

      runner = Runner.__new__(Runner)
      runner.config = Config(config_dict)
      runner.input_store = store
      runner.process_dir = store.as_dir()
      runner.graph_writer = mock.MagicMock()
      runner.nodes = Nodes(config=runner.config)
      runner.reporter = mock.MagicMock()
      runner.validation_reporter = mock.MagicMock()

      with self.assertRaises(ValueError) as ctx:
        runner._validate_all_headers(csv_files)

      err_msg = str(ctx.exception)
      self.assertIn("CSV Header Validation Failed", err_msg)
      self.assertIn("bad1.csv", err_msg)
      self.assertIn("unmapped1", err_msg)
      self.assertIn("bad2.csv", err_msg)
      self.assertIn("unmapped2", err_msg)
      self.assertNotIn("good.csv", err_msg)

  def test_runner_multiprocessing_end_to_end_with_splits(self):
    """End-to-end integration test with use_multiprocessing=True and CSV splitting."""
    with tempfile.TemporaryDirectory() as temp_dir:
      input_dir = os.path.join(temp_dir, "input")
      output_dir = os.path.join(temp_dir, "output")
      os.makedirs(input_dir)
      os.makedirs(output_dir)

      csv_content = ("entity,variable,date,value\n"
                     "geoId/01,Count_Person,2020,10\n"
                     "geoId/01,Count_Person,2021,11\n"
                     "geoId/02,Count_Person,2020,20\n"
                     "geoId/02,Count_Person,2021,21\n"
                     "geoId/04,Count_Person,2020,30\n"
                     "geoId/04,Count_Person,2021,31\n")
      with open(os.path.join(input_dir, "data.csv"), "w") as f:
        f.write(csv_content)

      config_data = {
          "inputFiles": [{
              "pattern": "prov.mcf",
              "provenance": "dcid:provenance/TestProv"
          }, {
              "pattern": "data.csv",
              "importType": "observations",
              "format": "variablePerRow",
              "provenance": "dcid:provenance/TestProv",
              "columnMappings": {
                  "dcid:observationAbout": "entity",
                  "dcid:variableMeasured": "variable",
                  "dcid:observationDate": "date",
                  "dcid:value": "value"
              }
          }]
      }
      with open(os.path.join(input_dir, "config.json"), "w") as f:
        json.dump(config_data, f)

      prov_mcf = ("Node: dcid:source/TestSource\n"
                  "typeOf: dcs:Source\n"
                  "url: \"https://test.org\"\n\n"
                  "Node: dcid:provenance/TestProv\n"
                  "typeOf: dcs:Provenance\n"
                  "source: dcid:source/TestSource\n"
                  "url: \"https://test.org/prov\"\n")
      with open(os.path.join(input_dir, "prov.mcf"), "w") as f:
        f.write(prov_mcf)

      with mock.patch("stats.runner._SPLIT_TARGET_BYTES", 50):
        runner = Runner(
            config_file_path=None,
            input_dir_path=input_dir,
            output_dir_path=output_dir,
            use_multiprocessing=True,
        )
        runner.run()

      obs = []
      nodes = []
      for root, _, files in os.walk(output_dir):
        for fname in files:
          if fname.endswith(".jsonld"):
            with open(os.path.join(root, fname)) as f:
              g = json.load(f).get("@graph", [])
              if fname.startswith("node-"):
                nodes.extend(g)
              elif fname.startswith("observation-"):
                obs.extend(g)

      self.assertEqual(len(obs), 6)
      node_ids = {n["@id"] for n in nodes if "@id" in n}
      self.assertIn("dcid:provenance/TestProv", node_ids)
      self.assertIn("dcid:source/TestSource", node_ids)
