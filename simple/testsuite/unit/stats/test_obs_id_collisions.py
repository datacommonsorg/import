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

import pytest
from stats import obs_id_collisions
from stats.obs_id_collisions import find_cross_file_collisions
from stats.obs_id_collisions import save_obs_ids

_MAX_ID = (1 << 64) - 1


def _files(tmp_path, *id_sets: set[int]) -> list[tuple[str, str]]:
  files = []
  for i, ids in enumerate(id_sets):
    path = os.path.join(tmp_path, f"{i}.npy")
    save_obs_ids(ids, path)
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
