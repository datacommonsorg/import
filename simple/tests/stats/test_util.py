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
import unittest

import pandas as pd
from stats.data import Observation
from stats.data import OBSERVATION_FIELD_NAMES
from stats.data import Triple
from stats.graph_writer import GraphWriter
from util.filesystem import File

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
  # Pass if neither actual nor existing file exists.
  # Fail if only one exists.
  actual_file_exists = os.path.exists(actual_path)
  expected_file_exists = os.path.exists(expected_path)
  test.assertEqual(
      actual_file_exists, expected_file_exists,
      f"Actual file existence does not match expected file existence: {message}"
  )
  if (expected_file_exists == False):
    return

  with open(actual_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want, message)


def compare_csv_files(test: unittest.TestCase,
                      actual_path: str,
                      expected_path: str,
                      message: str = None):
  """
  Compares CSV files in an order-independent way by sorting both before comparison.
  """
  # Pass if neither actual nor existing file exists.
  # Fail if only one exists.
  actual_file_exists = os.path.exists(actual_path)
  expected_file_exists = os.path.exists(expected_path)
  test.assertEqual(
      actual_file_exists, expected_file_exists,
      f"Actual file existence does not match expected file existence: {message}"
  )
  if (expected_file_exists == False):
    return

  # Check if files are empty (0 bytes) to prevent pandas EmptyDataError
  actual_empty = not os.path.exists(actual_path) or not open(
      actual_path).read().strip()
  expected_empty = not os.path.exists(expected_path) or not open(
      expected_path).read().strip()

  if actual_empty and expected_empty:
    return  # Both are empty, which is a perfect match!

  if actual_empty != expected_empty:
    test.fail(
        f"CSV emptiness mismatch: actual_empty={actual_empty}, expected_empty={expected_empty}. {message}"
    )

  # Read both CSVs (guaranteed to contain data now)
  actual_df = pd.read_csv(actual_path)
  expected_df = pd.read_csv(expected_path)

  # Sort both dataframes by all columns for deterministic comparison
  actual_sorted = actual_df.sort_values(by=list(actual_df.columns)).reset_index(
      drop=True)
  expected_sorted = expected_df.sort_values(
      by=list(expected_df.columns)).reset_index(drop=True)

  # Convert to CSV strings for comparison
  actual_csv = actual_sorted.to_csv(index=False)
  expected_csv = expected_sorted.to_csv(index=False)

  test.assertEqual(actual_csv, expected_csv, message)


def write_triples_list(triples: list[Triple], output_path: str):
  """Writes triples to output_path as CSV.

  Triples are written in the normalized form a GraphWriter persists, i.e. with
  namespaces stripped from the subject and object ids.
  """
  normalized = [Triple(*triple.normalized_tuple()) for triple in triples]
  pd.DataFrame(normalized).to_csv(output_path, index=False)


def write_observations_df(observations_df: pd.DataFrame, output_path: str):
  """
  Writes the observations DataFrame to the output_path CSV using the
  observation column order in which a GraphWriter persists them.
  """
  observations_df.to_csv(output_path,
                         index=False,
                         columns=OBSERVATION_FIELD_NAMES)


class FakeGzipTime:

  def __init__(self, timestamp=0) -> None:
    self.timestamp = timestamp

  def time(self):
    return self.timestamp


# GZIP encodes a timestamp in the gzipped content which makes test results inconsistent.
# Use this method to make tests use fixed timestamps.
def use_fake_gzip_time(timestamp=0):
  gzip.time = FakeGzipTime(timestamp)


class RecordingGraphWriter(GraphWriter):
  """An in-memory GraphWriter that records what was written to it.

  Importer tests use this to assert on the triples and observations an importer
  produces, without depending on a real storage backend.
  """

  def __init__(self) -> None:
    self.triples: list[Triple] = []
    self.observation_dfs: list[pd.DataFrame] = []
    self.committed = False
    self.closed = False

  def write_triples(self,
                    triples: list[Triple],
                    input_file: File = None,
                    provenance_dir: str = None):
    self.triples.extend(triples)

  def write_observations(self, observations_df: pd.DataFrame, input_file: File):
    self.observation_dfs.append(observations_df)

  @property
  def observations_df(self) -> pd.DataFrame:
    """All inserted observations concatenated in insertion order."""
    if not self.observation_dfs:
      return pd.DataFrame(columns=OBSERVATION_FIELD_NAMES)
    return pd.concat(self.observation_dfs, ignore_index=True)

  def commit(self):
    self.committed = True

  def commit_and_close(self):
    self.commit()
    self.closed = True
