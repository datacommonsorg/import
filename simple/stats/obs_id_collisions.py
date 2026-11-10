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
"""Finds observation @id collisions between files.

Each file is imported in its own worker process, whose `JsonLdStreamWriter`
finds the collisions within that file only. To find collisions between files,
each worker saves the ids of its file (`save_obs_ids`) and the coordinator
compares the saved ids of every file once all files are imported
(`find_cross_file_collisions`).

An id is the first 64 bits of the observation's sha256 hash, as in
`JsonLdStreamWriter.track_observation_hash`.

Memory: a worker saves its file's distinct ids as a sorted uint64 array, 8
bytes per observation, in a local temporary directory. The coordinator memory
maps those arrays and compares them one range of ids (a bucket) at a time, so
it holds about `_BUCKET_SIZE` ids at once (~70 MB with the sort's
temporaries) however many observations the import has. Ids are hashes, so
splitting the id space evenly splits the ids evenly.
"""

from dataclasses import dataclass
from dataclasses import field
from typing import Collection

import numpy as np
from stats.jsonld_stream_writer import MAX_SAMPLES_PER_FILE

# Ids compared at once by the coordinator.
_BUCKET_SIZE = 1 << 21
_ID_SPACE = 1 << 64


def save_obs_ids(obs_ids: Collection[int], path: str) -> None:
  """Saves `obs_ids` (distinct 64-bit ids) to `path` as a sorted .npy array."""
  ids = np.fromiter(obs_ids, dtype=np.uint64, count=len(obs_ids))
  ids.sort()
  np.save(path, ids, allow_pickle=False)


@dataclass
class CrossFileCollisions:
  """Collisions between files, each counted for the later of the two files.

  `file_counts` and `file_samples` are keyed like the writer's per-file
  collision stats. A sample names the 64-bit id prefix and an earlier file
  that produced the same id.
  """
  file_counts: dict[str, int] = field(default_factory=dict)
  file_samples: dict[str, list[str]] = field(default_factory=dict)

  @property
  def count(self) -> int:
    return sum(self.file_counts.values())


def find_cross_file_collisions(
    files: list[tuple[str, str]]) -> CrossFileCollisions:
  """Finds the ids that more than one file produced.

  Args:
    files: (file key, path written by `save_obs_ids`) for every file that
      produced observations, in import order.

  An id that k files produce is k - 1 collisions, one for each file but the
  first. With the collisions that each worker counts within its file, the
  total is the number of observations minus the number of distinct ids.
  """
  arrays = [np.load(path, mmap_mode="r") for _, path in files]
  num_buckets = max(1, -(-sum(len(a) for a in arrays) // _BUCKET_SIZE))
  inner_bounds = np.array(
      [b * _ID_SPACE // num_buckets for b in range(1, num_buckets)],
      dtype=np.uint64)
  # cuts[i][b]:cuts[i][b + 1] is the slice of arrays[i] in bucket b.
  cuts = [
      np.concatenate(([0], np.searchsorted(a, inner_bounds), [len(a)]))
      for a in arrays
  ]

  result = CrossFileCollisions()
  for b in range(num_buckets):
    ids, owners, prev_owners = _bucket_collisions(
        [a[c[b]:c[b + 1]] for a, c in zip(arrays, cuts)])
    for i in np.unique(owners).tolist():
      key = files[i][0]
      mine = owners == i
      count = int(np.count_nonzero(mine))
      result.file_counts[key] = result.file_counts.get(key, 0) + count
      samples = result.file_samples.setdefault(key, [])
      needed = max(0, MAX_SAMPLES_PER_FILE - len(samples))
      for obs_id, prev in zip(ids[mine][:needed].tolist(),
                              prev_owners[mine][:needed].tolist()):
        samples.append(f"@id='dcid:obs_{obs_id:016x}...' also produced by"
                       f" '{files[prev][0]}'")
  return result


def _bucket_collisions(
    parts: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
  """Returns (id, file, earlier file) arrays, one entry per collision.

  `parts[i]` is file i's sorted, distinct ids in one bucket.
  """
  ids = np.concatenate(parts)
  owners = np.repeat(np.arange(len(parts), dtype=np.uint32),
                     [len(p) for p in parts])
  # A stable sort keeps the files of one id in import order, so every id after
  # the first of a run collides with the file just before it.
  order = np.argsort(ids, kind="stable")
  ids = ids[order]
  owners = owners[order]
  dups = np.flatnonzero(ids[1:] == ids[:-1]) + 1
  return ids[dups], owners[dups], owners[dups - 1]
