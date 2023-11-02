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
import tempfile
import unittest

from stats.data import StatVar
import stats.nl as nl
from tests.stats.test_util import is_write_mode
from util.filehandler import LocalFileHandler

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "nl")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


_TEST_STAT_VARS = [
    StatVar("foo", "Foo Name", "Foo Description"),
    StatVar("bar",
            "Bar Name",
            "Bar Description",
            nl_sentences=["Bar Sentence1", "Bar Sentence 2"]),
]


class TestData(unittest.TestCase):

  def test_generate_sv_sentences(self):
    expected_sv_sentences_path = os.path.join(_EXPECTED_DIR, "sentences.csv")

    if is_write_mode():
      expected_sentences_fh = LocalFileHandler(expected_sv_sentences_path)
      print("Writing sv sentences to:", expected_sentences_fh)
      nl.generate_sv_sentences(_TEST_STAT_VARS, expected_sentences_fh)
      return

    with tempfile.TemporaryDirectory() as temp_dir:
      actual_sv_sentences_path = os.path.join(temp_dir, "sentences.csv")
      actual_sentences_fh = LocalFileHandler(actual_sv_sentences_path)

      nl.generate_sv_sentences(_TEST_STAT_VARS, actual_sentences_fh)

      _compare_files(self, actual_sv_sentences_path, expected_sv_sentences_path)
