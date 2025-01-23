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

from freezegun import freeze_time
from stats import constants
from stats.runner import RunMode
from stats.runner import Runner
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
    _test_runner(self,
                 "config_driven",
                 config_path=os.path.join(_CONFIG_DIR, "config_driven.json"))

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
