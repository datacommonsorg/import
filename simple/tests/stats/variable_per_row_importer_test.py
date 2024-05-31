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

import os
import shutil
import sqlite3
import tempfile
import unittest

import pandas as pd
from stats.config import Config
from stats.data import Observation
from stats.db import create_db
from stats.db import create_sqlite_config
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from stats.variable_per_row_importer import VariablePerRowImporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import write_observations
from util.filehandler import LocalFileHandler

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "variable_per_row_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _test_import(test: unittest.TestCase,
                 test_name: str,
                 column_mappings: dict[str, str] = {}):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_file = f"{test_name}.csv"
    input_path = os.path.join(_INPUT_DIR, input_file)
    db_path = os.path.join(temp_dir, f"{test_name}.db")

    output_path = os.path.join(temp_dir, f"{test_name}.db.csv")
    expected_path = os.path.join(_EXPECTED_DIR, f"{test_name}.db.csv")

    input_fh = LocalFileHandler(input_path)

    db = create_db(create_sqlite_config(db_path))
    report_fh = LocalFileHandler(os.path.join(temp_dir, "report.json"))
    reporter = FileImportReporter(input_path, ImportReporter(report_fh))
    nodes = Nodes(
        Config(
            {"inputFiles": {
                input_file: {
                    "columnMappings": column_mappings
                }
            }}))

    VariablePerRowImporter(input_fh=input_fh,
                           db=db,
                           reporter=reporter,
                           nodes=nodes).do_import()
    db.commit_and_close()

    write_observations(db_path, output_path)

    if is_write_mode():
      shutil.copy(output_path, expected_path)
      return

    compare_files(test, output_path, expected_path)


class TestVariablePerRowImporter(unittest.TestCase):

  def test_default_column_names(self):
    _test_import(self, "default_column_names")

  def test_custom_column_names(self):
    _test_import(self,
                 "custom_column_names",
                 column_mappings={
                     "variable": "Observed Variable",
                     "entity": "Observed Location"
                 })

  def test_namespace_prefixes(self):
    _test_import(self, "namespace_prefixes")
