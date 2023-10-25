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
import unittest

from stats.data import StatVar
import stats.embeddings as embeddings
from util.filehandler import LocalFileHandler

# If $TEST_MODE is set to "write", it will write the embeddings to the _TEST_DATA folder.
_TEST_MODE = os.getenv("TEST_MODE", "")

_TEST_DATA_DIR = "test_data"
_TEST_DATA_DIR_FH = LocalFileHandler(
    os.path.join(os.path.dirname(os.path.abspath(__file__)), _TEST_DATA_DIR))

_BASIC_EMBEDDINGS_FILE_NAME = "basic_embeddings.csv"

_DCID_SENTENCE_COLUMNS = ["dcid", "sentence"]

_TEST_STAT_VARS = [
    StatVar("foo", "Foo Name", "Foo Description"),
    StatVar("bar",
            "Bar Name",
            "Bar Description",
            nl_sentences=["Bar Sentence1", "Bar Sentence 2"]),
]


class TestData(unittest.TestCase):

  def test_basic_embeddings(self):
    embeddings_fh = _TEST_DATA_DIR_FH.make_file(_BASIC_EMBEDDINGS_FILE_NAME)
    if _TEST_MODE == "write":
      dataframe = embeddings.build(_TEST_STAT_VARS, None)
      print("Writing dcids and sentences from the embeddings dataframe to:",
            embeddings_fh)
      embeddings_fh.write_string(
          dataframe[_DCID_SENTENCE_COLUMNS].to_csv(index=False))
      return

    dataframe = embeddings.build(_TEST_STAT_VARS, None)
    self.assertEqual(dataframe[_DCID_SENTENCE_COLUMNS].to_csv(index=False),
                     embeddings_fh.read_string())
