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

import os
from pathlib import Path
import shutil
import sqlite3
import tempfile
import unittest

import pandas as pd
from stats import constants
from stats.data import Observation
from stats.data import Triple
from stats.runner import Runner
from tests.stats.test_util import is_write_mode

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "runner")
_CONFIG_DIR = os.path.join(_TEST_DATA_DIR, "config")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


def _write_observations(db_path: str, output_path: str):
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from observations").fetchall()
    observations = [Observation(*row) for row in rows]
    pd.DataFrame(observations).to_csv(output_path, index=False)


def _write_triples(db_path: str, output_path: str):
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from triples").fetchall()
    triples = [Triple(*row) for row in rows]
    pd.DataFrame(triples).to_csv(output_path, index=False)


def _test_runner(test: unittest.TestCase,
                 test_name: str,
                 is_config_driven: bool = True):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    if is_config_driven:
      config_path = os.path.join(_CONFIG_DIR, f"{test_name}.json")
      input_dir = None
    else:
      config_path = None
      input_dir = os.path.join(_INPUT_DIR, test_name)

    db_path = os.path.join(temp_dir, "datacommons.db")

    expected_dir = os.path.join(_EXPECTED_DIR, test_name)
    expected_nl_dir = os.path.join(expected_dir, constants.NL_DIR_NAME)
    Path(expected_nl_dir).mkdir(parents=True, exist_ok=True)

    output_triples_path = os.path.join(temp_dir, "triples.db.csv")
    expected_triples_path = os.path.join(expected_dir, "triples.db.csv")
    output_observations_path = os.path.join(temp_dir, "observations.db.csv")
    expected_observations_path = os.path.join(expected_dir,
                                              "observations.db.csv")
    output_nl_sentences_path = os.path.join(temp_dir, constants.NL_DIR_NAME,
                                            constants.SENTENCES_FILE_NAME)
    expected_nl_sentences_path = os.path.join(expected_dir,
                                              constants.NL_DIR_NAME,
                                              constants.SENTENCES_FILE_NAME)

    Runner(config_file=config_path, input_dir=input_dir,
           output_dir=temp_dir).run()

    _write_triples(db_path, output_triples_path)
    _write_observations(db_path, output_observations_path)

    if is_write_mode():
      shutil.copy(output_triples_path, expected_triples_path)
      shutil.copy(output_observations_path, expected_observations_path)
      shutil.copy(output_nl_sentences_path, expected_nl_sentences_path)
      return

    _compare_files(test, output_triples_path, expected_triples_path)
    _compare_files(test, output_observations_path, expected_observations_path)
    _compare_files(test, output_nl_sentences_path, expected_nl_sentences_path)


class TestRunner(unittest.TestCase):

  def test_config_driven(self):
    _test_runner(self, "config_driven")

  def test_config_with_wildcards(self):
    _test_runner(self, "config_with_wildcards")

  def test_input_dir_driven(self):
    _test_runner(self, "input_dir_driven", is_config_driven=False)

  def test_generate_svg_hierarchy(self):
    _test_runner(self, "generate_svg_hierarchy", is_config_driven=False)

  def test_sv_nl_sentences(self):
    _test_runner(self, "sv_nl_sentences", is_config_driven=False)

  def test_topic_nl_sentences(self):
    _test_runner(self, "topic_nl_sentences", is_config_driven=False)
