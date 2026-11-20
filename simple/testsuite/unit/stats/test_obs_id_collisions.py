# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Observation @id collisions between files."""

import os
import random
from unittest import mock

import numpy as np
import pytest
from stats import obs_id_collisions
from stats.obs_id_collisions import find_cross_file_collisions
from stats.obs_id_collisions import save_obs_ids

_MAX_ID = (1 << 64) - 1


def _files(tmp_path, *id_sets: set[int]) -> list[tuple[str, str]]:
  files = []
  for i, ids in enumerate(id_sets):
    path = os.path.join(tmp_path, f"{i}.npy")
    save_obs_ids(np.array(sorted(ids), dtype=np.uint64), path)
    files.append((f"f{i}.csv", path))
  return files


def test_files_without_shared_ids_do_not_collide(tmp_path):
  result = find_cross_file_collisions(_files(tmp_path, {1, 2}, {3, _MAX_ID}))
  assert result.count == 0
  assert result.file_counts == {}


def test_shared_id_is_counted_for_the_later_file(tmp_path):
  result = find_cross_file_collisions(
      _files(tmp_path, {1, 0xabc}, {2}, {0xabc, 3}))
  assert result.file_counts == {"f2.csv": 1}
  assert result.file_samples == {
      "f2.csv": [
          "@id='dcid:obs_0000000000000abc...' also produced by 'f0.csv'"
      ]
  }


def test_id_shared_by_k_files_is_k_minus_one_collisions(tmp_path):
  result = find_cross_file_collisions(
      _files(tmp_path, {_MAX_ID}, {_MAX_ID}, {_MAX_ID}))
  assert result.count == 2
  assert result.file_counts == {"f1.csv": 1, "f2.csv": 1}
  assert result.file_samples["f2.csv"] == [
      f"@id='dcid:obs_{_MAX_ID:016x}...' also produced by 'f1.csv'"
  ]


def test_samples_per_file_are_capped(tmp_path):
  shared = set(range(10))
  result = find_cross_file_collisions(_files(tmp_path, shared, shared))
  assert result.file_counts == {"f1.csv": 10}
  assert len(result.file_samples["f1.csv"]) == 3


def test_files_with_the_same_key_are_summed(tmp_path):
  files = _files(tmp_path, {1}, {1}, {1})
  files = [("same.csv", path) for _, path in files]
  result = find_cross_file_collisions(files)
  assert result.file_counts == {"same.csv": 2}
  assert len(result.file_samples["same.csv"]) == 2


def test_ids_of_several_parts_of_a_file_are_its_own_collisions(tmp_path):
  """As if the file were imported whole: an id in k parts is k - 1 collisions.

  Parts of a file are consecutive entries with its path.
  """
  files = _files(tmp_path, {1, 2}, {2, 3}, {2, 3, 4}, {3, 5})
  files = [("a.csv", files[0][1]), ("a.csv", files[1][1]),
           ("a.csv", files[2][1]), ("b.csv", files[3][1])]
  result = find_cross_file_collisions(files)
  # Id 2 is in three parts of a.csv, id 3 in two; b.csv repeats id 3.
  assert result.file_counts == {"a.csv": 3, "b.csv": 1}
  assert result.file_samples == {
      "a.csv": [
          "@id='dcid:obs_0000000000000002...' also produced by another part of"
          " the file",
          "@id='dcid:obs_0000000000000002...' also produced by another part of"
          " the file",
          "@id='dcid:obs_0000000000000003...' also produced by another part of"
          " the file",
      ],
      "b.csv": ["@id='dcid:obs_0000000000000003...' also produced by 'a.csv'"],
  }


@pytest.mark.parametrize("bucket_size", [1, 3, 1000])
def test_bucketing_does_not_change_the_result(tmp_path, bucket_size):
  rng = random.Random(0)
  shared = {rng.getrandbits(64) for _ in range(50)}
  id_sets = [
      {rng.getrandbits(64) for _ in range(200)} | shared for _ in range(3)
  ]
  with mock.patch.object(obs_id_collisions, "_BUCKET_SIZE", bucket_size):
    result = find_cross_file_collisions(_files(tmp_path, *id_sets))
  assert result.file_counts == {"f1.csv": 50, "f2.csv": 50}


@pytest.mark.parametrize("chunk_size", [1, 7, 1000])
def test_obs_ids_find_the_same_repeats_as_a_set(chunk_size):
  """Every id after its first is a repeat, within a chunk or across chunks."""
  rng = random.Random(chunk_size)
  ids = [rng.choice([rng.getrandbits(64), _MAX_ID, 0, 5]) for _ in range(5000)]
  obs_ids, seen, expected, actual = obs_id_collisions.ObsIds(), set(), [], []
  for start in range(0, len(ids), chunk_size):
    chunk = ids[start:start + chunk_size]
    actual.extend(obs_ids.add(np.array(chunk, dtype=np.uint64)).tolist())
    for obs_id in chunk:
      expected.append(obs_id in seen)
      seen.add(obs_id)
  assert actual == expected
  assert len(obs_ids) == len(seen)
  assert obs_ids.sorted().tolist() == sorted(seen)
