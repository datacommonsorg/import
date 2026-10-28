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

    VariablePerRowImporter(input_file=input_file,
                           graph_writer=graph_writer,
                           reporter=reporter,
                           nodes=nodes).do_import()

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
