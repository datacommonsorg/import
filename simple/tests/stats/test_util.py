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

import pandas as pd
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
