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

import gzip
import os
import sqlite3
import unittest

import pandas as pd
from stats.data import Observation
from stats.data import Triple

# If $TEST_MODE is set to "write", the test will write the goldens.
_TEST_MODE = os.getenv("TEST_MODE", "")
_WRITE_MODE = "write"


def is_write_mode() -> bool:
  return _TEST_MODE == _WRITE_MODE


def compare_files(test: unittest.TestCase,
                  actual_path: str,
                  expected_path: str,
                  message: str = None):
  """
  Compares the content of the actual and expected files and asserts their equality.
  """
  with open(actual_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want, message)


def read_triples_csv(path: str) -> list[Triple]:
  """
  Reads a triples CSV into a list of Triple objects.
  """
  df = pd.read_csv(path, keep_default_na=False)
  return [Triple(**kwargs) for kwargs in df.to_dict(orient='records')]


def write_observations(db_path: str, output_path: str):
  """
  Fetches all observations from a sqlite db at db_path
  and writes it to the output_path CSV.
  """
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from observations").fetchall()
    observations = [Observation(*row) for row in rows]
    pd.DataFrame(observations).to_csv(output_path, index=False)


def write_triples(db_path: str, output_path: str):
  """
  Fetches all triples from a sqlite db at db_path
  and writes it to the output_path CSV.
  """
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from triples").fetchall()
    triples = [Triple(*row) for row in rows]
    write_triples_list(triples, output_path)


def write_triples_list(triples: list[Triple], output_path: str):
  """
  Writes the list of triples to the output_path CSV.
  """
  pd.DataFrame(triples).to_csv(output_path, index=False)


def write_key_values(db_path: str, output_path: str):
  """
  Fetches all key values from a sqlite db at db_path
  and writes it to the output_path CSV.
  """
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from key_value_store").fetchall()
    pd.DataFrame(rows, columns=["lookup_key", "value"]).to_csv(output_path,
                                                               index=False)


class FakeGzipTime:

  def __init__(self, timestamp=0) -> None:
    self.timestamp = timestamp

  def time(self):
    return self.timestamp


# GZIP encodes a timestamp in the gzipped content which makes test results inconsistent.
# Use this method to make tests use fixed timestamps.
def use_fake_gzip_time(timestamp=0):
  gzip.time = FakeGzipTime(timestamp)
