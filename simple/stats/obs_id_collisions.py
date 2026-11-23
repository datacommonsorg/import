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
(`find_cross_file_collisions`). A large CSV may be imported in parts, each in
its own worker (see `stats.csv_split`): each part saves its ids, and
collisions between the parts of a file are counted as the file's own.

An id is the first 64 bits of the observation's sha256 hash, as in
`JsonLdStreamWriter.track_observation_hash`.

Memory: a worker keeps its file's distinct ids in numpy arrays (`ObsIds`), 8
bytes per observation, and saves them as a sorted uint64 array in a local
temporary directory. The coordinator memory
maps those arrays and compares them one range of ids (a bucket) at a time, so
it holds about `_BUCKET_SIZE` ids at once (~70 MB with the sort's
temporaries) however many observations the import has. Ids are hashes, so
splitting the id space evenly splits the ids evenly.
"""

import concurrent.futures
from dataclasses import dataclass
from dataclasses import field
import os

import numpy as np

# Collision samples kept per file for the end-of-run summary.
MAX_SAMPLES_PER_FILE = 3
# Ids compared at once per bucket (~16 MB on the collision-free fast path).
_BUCKET_SIZE = 1 << 21
# Cap worker threads so concurrent buckets stay memory-bounded (~0.5-2 GiB).
_MAX_BUCKET_WORKERS = 32
_ID_SPACE = 1 << 64


class ObsIds:
  """The distinct observation ids that one file produced so far.

  Not a Python set: the garbage collector's full passes walk every item of a
  set, and they keep happening while pandas reads the file, so a file's import
  time would grow with the square of its rows. numpy arrays aren't walked, and
  take 8 bytes per id instead of ~60-100.

  The ids are kept in sorted runs, each more than twice as long as the next, so
  an id is looked up in O(log) runs and moved O(log) times by merges.
  """

  def __init__(self):
    self._runs: list[np.ndarray] = []

  def __len__(self) -> int:
    return sum(len(run) for run in self._runs)

  def add(self, ids: np.ndarray) -> np.ndarray:
    """Adds uint64 `ids`; returns which ones were seen before, in order.

    An id seen before is one already added, or one earlier in `ids`.
    """
    unique, first, inverse = np.unique(ids,
                                       return_index=True,
                                       return_inverse=True)
    known = np.zeros(len(unique), dtype=bool)
    for run in self._runs:
      positions = np.minimum(np.searchsorted(run, unique), len(run) - 1)
      known |= run[positions] == unique
    seen = known[inverse.reshape(-1)]
    repeated = np.ones(len(ids), dtype=bool)
    repeated[first] = False
    self._add_run(unique[~known])
    return seen | repeated

  def sorted(self) -> np.ndarray:
    """Returns every id, sorted."""
    ids = np.concatenate(self._runs) if self._runs else np.empty(0, np.uint64)
    ids.sort(kind="stable")
    return ids

  def _add_run(self, run: np.ndarray) -> None:
    if not len(run):
      return
    self._runs.append(run)
    while len(self._runs) > 1 and len(
        self._runs[-2]) <= 2 * len(self._runs[-1]):
      last = self._runs.pop()
      merged = np.concatenate((self._runs.pop(), last))
      merged.sort(kind="stable")  # Merges the two sorted runs.
      self._runs.append(merged)


def save_obs_ids(ids: np.ndarray, path: str) -> None:
  """Saves sorted, distinct uint64 `ids` (see `ObsIds.sorted`) to `path`."""
  np.save(path, ids, allow_pickle=False)


@dataclass
class CrossFileCollisions:
  """Collisions between files, each counted for the later of the two files.

  `file_counts` and `file_samples` are keyed by input file path, like the
  writer's per-file collision stats. A sample names the 64-bit id prefix and
  an earlier file that produced the same id, or says that another part of the
  same file did.
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
    files: (input file path, path written by `save_obs_ids`) for every file
      that produced observations, in import order. A file imported in parts
      has an entry per part with observations, one after the other.

  An id that k files produce is k - 1 collisions, one for each file but the
  first. With the collisions that each worker counts within its file, the
  total is the number of observations minus the number of distinct ids. The
  same goes for the parts of a file: an id that k of its parts produce is
  k - 1 collisions of the file, so its count is the same as if it were
  imported whole.

  Example: if a.csv, b.csv and c.csv all produce id X, b.csv gets one
  collision with a.csv, and c.csv one with b.csv.
  """
  if len(files) < 2:
    return CrossFileCollisions()
  arrays = [np.load(path, mmap_mode="r") for _, path in files]
  num_buckets = max(1, -(-sum(len(a) for a in arrays) // _BUCKET_SIZE))
  inner_bounds = np.array(
      [b * _ID_SPACE // num_buckets for b in range(1, num_buckets)],
      dtype=np.uint64)

  # cuts[i][b]:cuts[i][b + 1] is the slice of arrays[i] in bucket b.
  def _file_cuts(a: np.ndarray) -> np.ndarray:
    return np.concatenate(([0], np.searchsorted(a, inner_bounds), [len(a)]))

  def _check_bucket(b: int) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
    return _bucket_collisions([a[c[b]:c[b + 1]] for a, c in zip(arrays, cuts)])

  result = CrossFileCollisions()

  def _accumulate(bucket_results) -> None:
    for ids, owners, prev_owners in bucket_results:
      for i in np.unique(owners).tolist():
        key = files[i][0]
        mine = owners == i
        count = int(np.count_nonzero(mine))
        result.file_counts[key] = result.file_counts.get(key, 0) + count
        samples = result.file_samples.setdefault(key, [])
        needed = max(0, MAX_SAMPLES_PER_FILE - len(samples))
        for obs_id, prev in zip(ids[mine][:needed].tolist(),
                                prev_owners[mine][:needed].tolist()):
          if files[prev][0] == key:
            samples.append(f"@id='dcid:obs_{obs_id:016x}...' also produced by"
                           " another part of the file")
          else:
            samples.append(f"@id='dcid:obs_{obs_id:016x}...' also produced by"
                           f" '{files[prev][0]}'")

  workers = min(_MAX_BUCKET_WORKERS, os.cpu_count() or 1)
  if workers > 1:
    with concurrent.futures.ThreadPoolExecutor(max_workers=workers) as pool:
      cuts = list(pool.map(_file_cuts, arrays))
      _accumulate(pool.map(_check_bucket, range(num_buckets)))
  else:
    cuts = [_file_cuts(a) for a in arrays]
    _accumulate(_check_bucket(b) for b in range(num_buckets))
  return result


def _bucket_collisions(
    parts: list[np.ndarray]) -> tuple[np.ndarray, np.ndarray, np.ndarray]:
  """Returns (id, file, earlier file) arrays, one entry per collision.

  `parts[i]` is file i's sorted, distinct ids in one bucket.
  """
  ids = np.concatenate(parts)
  if len(ids) < 2:
    empty_u64 = np.empty(0, dtype=np.uint64)
    empty_u32 = np.empty(0, dtype=np.uint32)
    return empty_u64, empty_u32, empty_u32
  # Fast path: in-place stable sort (exploits the pre-sorted per-part runs)
  # without owner/index arrays when no duplicates exist in this bucket.
  ids.sort(kind="stable")
  if not np.any(ids[1:] == ids[:-1]):
    empty_u64 = np.empty(0, dtype=np.uint64)
    empty_u32 = np.empty(0, dtype=np.uint32)
    return empty_u64, empty_u32, empty_u32
  # At least one collision exists in this bucket: re-gather with file owners
  # and stable-sort so every id after the first of a run collides with the
  # file just before it in import order.
  ids = np.concatenate(parts)
  owners = np.repeat(np.arange(len(parts), dtype=np.uint32),
                     [len(p) for p in parts])
  order = np.argsort(ids, kind="stable")
  ids = ids[order]
  owners = owners[order]
  dups = np.flatnonzero(ids[1:] == ids[:-1]) + 1
  return ids[dups], owners[dups], owners[dups - 1]
