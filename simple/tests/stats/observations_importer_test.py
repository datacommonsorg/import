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

import pandas as pd
from stats.config import Config
from stats.db import create_and_update_db
from stats.db import create_sqlite_config
from stats.nodes import Nodes
from stats.observations_importer import ObservationsImporter
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import use_fake_gzip_time
from tests.stats.test_util import write_observations
from util.filesystem import create_store

from util import dc_client

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "observations_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")

use_fake_gzip_time()


def _test_import(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_store = create_store(_INPUT_DIR)
    temp_store = create_store(temp_dir)

    input_dir = os.path.join(_INPUT_DIR, test_name)
    expected_dir = os.path.join(_EXPECTED_DIR, test_name)

    input_file_name = "input.csv"
    input_path = os.path.join(input_dir, input_file_name)
    config_path = os.path.join(input_dir, "config.json")
    db_file_name = f"{test_name}.db"
    db_path = os.path.join(temp_dir, db_file_name)
    db_file = temp_store.as_dir().open_file(db_file_name)

    output_path = os.path.join(temp_dir, f"{test_name}.db.csv")
    expected_path = os.path.join(_EXPECTED_DIR, f"{test_name}.db.csv")
    output_path = os.path.join(temp_dir, "observations.db.csv")
    expected_path = os.path.join(expected_dir, "observations.db.csv")

    input_file = input_store.as_dir().open_dir(test_name).open_file(
        input_file_name)

    with open(config_path) as config_file:
      config = Config(json.load(config_file))

    db = create_and_update_db(create_sqlite_config(db_file))
    debug_resolve_file = temp_store.as_dir().open_file("debug.csv")
    report_file = temp_store.as_dir().open_file("report.json")
    reporter = FileImportReporter(input_path, ImportReporter(report_file))
    nodes = Nodes(config)

    dc_client.get_property_of_entities = MagicMock(return_value={})

    ObservationsImporter(input_file=input_file,
                         db=db,
                         debug_resolve_file=debug_resolve_file,
                         reporter=reporter,
                         nodes=nodes).do_import()
    db.commit_and_close()

    write_observations(db_path, output_path)

    if is_write_mode():
      shutil.copy(output_path, expected_path)
      return

    compare_files(test, output_path, expected_path)

    input_store.close()
    temp_store.close()


class TestObservationsImporter(unittest.TestCase):

  def test_countryalpha3codes(self):
    _test_import(self, "countryalpha3codes")

  def test_obs_props(self):
    _test_import(self, "obs_props")
