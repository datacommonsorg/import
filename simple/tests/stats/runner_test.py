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
import sqlite3
import tempfile
import unittest
from unittest.mock import MagicMock

import pandas as pd
from stats import constants
from stats.data import Observation
from stats.data import Triple
from stats.runner import RunMode
from stats.runner import Runner
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import read_full_db_from_file
from tests.stats.test_util import use_fake_gzip_time
from tests.stats.test_util import write_key_values
from tests.stats.test_util import write_observations
from tests.stats.test_util import write_triples

from util import dc_client

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "runner")
_CONFIG_DIR = os.path.join(_TEST_DATA_DIR, "config")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _test_runner(test: unittest.TestCase,
                 test_name: str,
                 is_config_driven: bool = True,
                 run_mode: RunMode = RunMode.CUSTOM_DC,
                 input_db_file_name: str = None):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    if is_config_driven:
      config_path = os.path.join(_CONFIG_DIR, f"{test_name}.json")
      input_dir = None
      remote_entity_types_path = None
    else:
      config_path = None
      input_dir = os.path.join(_INPUT_DIR, test_name)
      remote_entity_types_path = os.path.join(input_dir,
                                              "remote_entity_types.json")

    db_path = os.path.join(temp_dir, "datacommons.db")
    if (input_db_file_name):
      input_db_file = os.path.join(input_dir, input_db_file_name)
      read_full_db_from_file(db_path, input_db_file)

    expected_dir = os.path.join(_EXPECTED_DIR, test_name)
    expected_nl_dir = os.path.join(expected_dir, constants.NL_DIR_NAME)
    Path(expected_nl_dir).mkdir(parents=True, exist_ok=True)

    output_triples_path = os.path.join(temp_dir, "triples.db.csv")
    expected_triples_path = os.path.join(expected_dir, "triples.db.csv")
    output_observations_path = os.path.join(temp_dir, "observations.db.csv")
    expected_observations_path = os.path.join(expected_dir,
                                              "observations.db.csv")
    output_key_value_store_path = os.path.join(temp_dir,
                                               "key_value_store.db.csv")
    expected_key_value_store_path = os.path.join(expected_dir,
                                                 "key_value_store.db.csv")
    output_nl_sentences_path = os.path.join(temp_dir, constants.NL_DIR_NAME,
                                            constants.SENTENCES_FILE_NAME)
    expected_nl_sentences_path = os.path.join(expected_dir,
                                              constants.NL_DIR_NAME,
                                              constants.SENTENCES_FILE_NAME)
    output_topic_cache_json_path = os.path.join(temp_dir, constants.NL_DIR_NAME,
                                                constants.TOPIC_CACHE_FILE_NAME)
    expected_topic_cache_json_path = os.path.join(
        expected_dir, constants.NL_DIR_NAME, constants.TOPIC_CACHE_FILE_NAME)

    dc_client.get_property_of_entities = MagicMock(return_value={})
    if remote_entity_types_path and os.path.exists(remote_entity_types_path):
      with open(remote_entity_types_path, "r") as f:
        dc_client.get_property_of_entities = MagicMock(
            return_value=json.load(f))

    Runner(config_file=config_path,
           input_dir=input_dir,
           output_dir=temp_dir,
           mode=run_mode).run()

    write_triples(db_path, output_triples_path)
    write_observations(db_path, output_observations_path)
    write_key_values(db_path, output_key_value_store_path)

    if is_write_mode():
      shutil.copy(output_triples_path, expected_triples_path)
      shutil.copy(output_observations_path, expected_observations_path)
      shutil.copy(output_key_value_store_path, expected_key_value_store_path)
      if os.path.exists(output_nl_sentences_path):
        shutil.copy(output_nl_sentences_path, expected_nl_sentences_path)
      if os.path.exists(output_topic_cache_json_path):
        shutil.copy(output_topic_cache_json_path,
                    expected_topic_cache_json_path)
      return

    compare_files(test, output_triples_path, expected_triples_path,
                  f"{test_name}: triples")
    compare_files(test, output_observations_path, expected_observations_path,
                  f"{test_name}: observations")
    compare_files(test, output_key_value_store_path,
                  expected_key_value_store_path,
                  f"{test_name}: key_value_store")
    if os.path.exists(expected_nl_sentences_path):
      compare_files(test, output_nl_sentences_path, expected_nl_sentences_path,
                    f"{test_name}: nl sentences")
    if os.path.exists(expected_topic_cache_json_path):
      compare_files(test, output_topic_cache_json_path,
                    expected_topic_cache_json_path, f"{test_name}: topic cache")


class TestRunner(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    use_fake_gzip_time()

  def test_config_driven(self):
    _test_runner(self, "config_driven")

  def test_config_with_wildcards(self):
    _test_runner(self, "config_with_wildcards")

  def test_input_dir_driven(self):
    _test_runner(self, "input_dir_driven", is_config_driven=False)

  def test_input_dir_driven_with_existing_old_schema_data(self):
    _test_runner(self,
                 "input_dir_driven_with_existing_old_schema_data",
                 is_config_driven=False,
                 input_db_file_name="sqlite_old_schema_populated.sql")

  def test_generate_svg_hierarchy(self):
    _test_runner(self, "generate_svg_hierarchy", is_config_driven=False)

  def test_sv_nl_sentences(self):
    _test_runner(self, "sv_nl_sentences", is_config_driven=False)

  def test_topic_nl_sentences(self):
    _test_runner(self, "topic_nl_sentences", is_config_driven=False)

  def test_remote_entity_types(self):
    _test_runner(self, "remote_entity_types", is_config_driven=False)

  def test_schema_update_only(self):
    _test_runner(self,
                 "schema_update_only",
                 is_config_driven=False,
                 run_mode=RunMode.SCHEMA_UPDATE,
                 input_db_file_name="sqlite_old_schema_populated.sql")
