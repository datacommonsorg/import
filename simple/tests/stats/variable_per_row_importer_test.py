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

import json
import os
import shutil
import tempfile
import unittest
from unittest.mock import MagicMock

from stats.config import Config
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from stats.variable_per_row_importer import VariablePerRowImporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import RecordingGraphWriter
from tests.stats.test_util import use_fake_gzip_time
from tests.stats.test_util import write_observations_df
from util.filesystem import create_store

from util import dc_client

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "variable_per_row_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")

use_fake_gzip_time()


def _test_import(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_dir = os.path.join(_INPUT_DIR, test_name)
    expected_dir = os.path.join(_EXPECTED_DIR, test_name)

    input_path = os.path.join(input_dir, "input.csv")
    config_path = os.path.join(input_dir, "config.json")

    output_path = os.path.join(temp_dir, "observations.csv")
    expected_path = os.path.join(expected_dir, "observations.csv")

    input_file = create_store(input_path).as_file()

    with open(config_path) as config_file:
      config = Config(json.load(config_file))

    graph_writer = RecordingGraphWriter()
    report_file = create_store(temp_dir).as_dir().open_file("report.json")
    reporter = FileImportReporter(input_path, ImportReporter(report_file))
    nodes = Nodes(config)

    # Mock return values for entity types
    dc_client.get_property_of_entities = MagicMock(
        return_value={
            "country/FRA": "Country",
            "country/USA": "Country",
            "country/DEU": "Country",
            "country/GBR": "Country",
            "geoId/06": "State",
            "geoId/08": "State"
        })

    VariablePerRowImporter(input_file=input_file,
                           graph_writer=graph_writer,
                           reporter=reporter,
                           nodes=nodes).do_import()

    # Verify that get_property_of_entities was called with the correct tracked place IDs
    dc_client.get_property_of_entities.assert_called_once()
    called_args = dc_client.get_property_of_entities.call_args[0][0]

    if test_name == "multi_entity_custom_dimensions":
      # Should track all country IDs from the custom source/destination columns
      test.assertEqual(
          set(called_args),
          {"country/FRA", "country/USA", "country/DEU", "country/GBR"})
    elif test_name == "multi_entity_with_primary":
      # Should track both the primary states and the custom countries
      test.assertEqual(set(called_args),
                       {"geoId/06", "country/FRA", "geoId/08", "country/DEU"})
    elif test_name == "single_entity_official_keys":
      # Should track the primary states
      test.assertEqual(set(called_args), {"geoId/06", "geoId/08"})

    graph_writer.commit_and_close()

    write_observations_df(graph_writer.observations_df, output_path)

    if is_write_mode():
      shutil.copy(output_path, expected_path)
      return

    compare_files(test, output_path, expected_path)


class TestVariablePerRowImporter(unittest.TestCase):

  def test_single_entity_official_keys(self):
    _test_import(self, "single_entity_official_keys")

  def test_multi_entity_custom_dimensions(self):
    _test_import(self, "multi_entity_custom_dimensions")

  def test_multi_entity_with_primary(self):
    _test_import(self, "multi_entity_with_primary")

  def test_unresolved_entity_raises_error(self):
    from unittest import mock

    from stats.importer import EntityResolutionError
    from util.filesystem import create_store

    from util import dc_client as dc
    with tempfile.TemporaryDirectory() as temp_dir:
      config_path = os.path.join(temp_dir, "config.json")
      csv_path = os.path.join(temp_dir, "data.csv")
      with open(config_path, "w") as f:
        f.write(
            '{"inputFiles": [{"pattern": "data.csv", "format": "variablePerRow", "entityType": "Country", "columnsToResolve": ["entity"], "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable", "dcid:observationDate": "date", "dcid:value": "value"}}]}'
        )
      with open(csv_path, "w") as f:
        f.write(
            "entity,variable,date,value\nUnknownPlace123,Count_Person,2020,10\n"
        )
      with open(config_path) as config_file:
        config = Config(json.load(config_file))
      graph_writer = RecordingGraphWriter()
      nodes = Nodes(config)
      report_file = create_store(temp_dir).as_dir().open_file("report.json")
      reporter = FileImportReporter(csv_path, ImportReporter(report_file))
      importer = VariablePerRowImporter(
          create_store(csv_path).as_file(), graph_writer, reporter, nodes)
      with mock.patch.object(dc, "resolve_entities",
                             return_value={}) as mock_resolve:
        with self.assertRaises(EntityResolutionError):
          importer.do_import()

  def test_split_import_matches_unsplit(self):
    import pandas as pd
    from tests.stats.test_util import RecordingGraphWriter
    from util.filesystem import create_store

    test_dir = os.path.join(_INPUT_DIR, "single_entity_official_keys")
    config_path = os.path.join(test_dir, "config.json")
    input_path = os.path.join(test_dir, "input.csv")

    with open(config_path) as config_file:
      config = Config(json.load(config_file))

    dc_client.get_property_of_entities = MagicMock(return_value={
        "geoId/06": "State",
        "geoId/08": "State"
    })

    # Run unsplit
    unsplit_gw = RecordingGraphWriter()
    with tempfile.TemporaryDirectory() as temp_dir:
      rep_file = create_store(temp_dir).as_dir().open_file("report1.json")
      reporter = FileImportReporter(input_path, ImportReporter(rep_file))
      importer = VariablePerRowImporter(
          input_file=create_store(input_path).as_file(),
          graph_writer=unsplit_gw,
          reporter=reporter,
          nodes=Nodes(config),
          chunk_index=0,
          num_chunks=1)
      importer.do_import()
      unsplit_gw.commit_and_close()

    # Run split across 3 parts
    split_gw = RecordingGraphWriter()
    with tempfile.TemporaryDirectory() as temp_dir:
      for chunk_idx in range(3):
        rep_file = create_store(temp_dir).as_dir().open_file(
            f"report_{chunk_idx}.json")
        reporter = FileImportReporter(input_path, ImportReporter(rep_file))
        importer = VariablePerRowImporter(
            input_file=create_store(input_path).as_file(),
            graph_writer=split_gw,
            reporter=reporter,
            nodes=Nodes(config),
            chunk_index=chunk_idx,
            num_chunks=3)
        importer.do_import()
      split_gw.commit_and_close()

    unsplit_df = unsplit_gw.observations_df.sort_values(
        by=list(unsplit_gw.observations_df.columns)).reset_index(drop=True)
    split_df = split_gw.observations_df.sort_values(
        by=list(split_gw.observations_df.columns)).reset_index(drop=True)
    pd.testing.assert_frame_equal(unsplit_df, split_df)

  def test_split_import_exact_newline_boundary(self):
    """Verifies that rows are not skipped when start_byte falls exactly on a newline."""
    config_dict = {
        "inputFiles": [{
            "pattern": "data.csv",
            "importType": "observations",
            "format": "variablePerRow",
            "entityType": "Country",
            "provenance": "dcid:prov1",
            "ignoreColumns": ["xx"],
            "columnMappings": {
                "dcid:observationAbout": "entity",
                "dcid:variableMeasured": "variable",
                "dcid:observationDate": "date",
                "dcid:value": "value"
            }
        }]
    }
    config = Config(config_dict)

    with tempfile.TemporaryDirectory() as temp_dir:
      input_path = os.path.join(temp_dir, "data.csv")
      # Each line is exactly 30 bytes. Total size = 120 bytes.
      # With num_chunks=4, chunk size = 30 bytes:
      # Chunk 0: [0, 30) -> Header
      # Chunk 1: [30, 60) -> starts on byte 30 (Row 1)
      # Chunk 2: [60, 90) -> starts on byte 60 (Row 2)
      # Chunk 3: [90, 120) -> starts on byte 90 (Row 3)
      content = ("entity,variable,date,value,xx\n"
                 "USA,Var1,2020,10,000000000000\n"
                 "USA,Var2,2020,20,000000000000\n"
                 "USA,Var3,2020,30,000000000000\n")
      with open(input_path, "w") as f:
        f.write(content)

      split_gw = RecordingGraphWriter()
      for chunk_idx in range(4):
        rep_file = create_store(temp_dir).as_dir().open_file(
            f"report_{chunk_idx}.json")
        reporter = FileImportReporter(input_path, ImportReporter(rep_file))
        importer = VariablePerRowImporter(
            input_file=create_store(input_path).as_file(),
            graph_writer=split_gw,
            reporter=reporter,
            nodes=Nodes(config),
            chunk_index=chunk_idx,
            num_chunks=4)
        importer.do_import()
      split_gw.commit_and_close()

      self.assertEqual(len(split_gw.observations_df), 3)
