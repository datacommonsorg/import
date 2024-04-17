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
import tempfile
import unittest

import pandas as pd
from stats.data import Triple
import stats.nl as nl
from tests.stats.test_util import is_write_mode
from util.filehandler import LocalFileHandler

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "nl")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _read_triples_csv(path: str) -> list[Triple]:
  df = pd.read_csv(path)
  return [Triple(**kwargs) for kwargs in df.to_dict(orient='records')]


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


def _test_generate_nl_sentences(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_triples_path = os.path.join(_INPUT_DIR, f"{test_name}.csv")
    input_triples = _read_triples_csv(input_triples_path)

    output_sentences_csv_path = os.path.join(temp_dir,
                                             f"{test_name}_sentences.csv")
    expected_sentences_csv_path = os.path.join(_EXPECTED_DIR,
                                               f"{test_name}_sentences.csv")

    output_sentences_csv_fh = LocalFileHandler(output_sentences_csv_path)

    nl.generate_nl_sentences(input_triples, output_sentences_csv_fh)

    if is_write_mode():
      shutil.copy(output_sentences_csv_path, expected_sentences_csv_path)
      return

    _compare_files(test, output_sentences_csv_path, expected_sentences_csv_path)


class TestData(unittest.TestCase):

  def test_generate_nl_sentences_for_sv_triples(self):
    _test_generate_nl_sentences(self, "sv_triples")

  def test_generate_nl_sentences_for_topic_triples(self):
    _test_generate_nl_sentences(self, "topic_triples")
