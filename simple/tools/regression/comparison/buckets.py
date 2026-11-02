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
"""Hash-partitioned spill files, so a comparison never holds a whole side.

Records of both sides are appended to NUM_BUCKETS files by a hash of their
key; bucket i of the baseline is then compared with bucket i of the
candidate, one bucket at a time. Memory is bounded by the write buffer plus
two buckets, i.e. about 1/NUM_BUCKETS of the output.
"""

import concurrent.futures
import os

from tools.regression.comparison.categories import Findings
from tools.regression.comparison.shards import OutputTree

NUM_BUCKETS = 256
# Buffered bytes across all buckets before they are appended to disk.
_FLUSH_BYTES = 64 << 20


class SpillBuckets:
  """Buffers byte chunks per bucket and appends them to one file per bucket."""

  def __init__(self, directory: str) -> None:
    os.makedirs(directory, exist_ok=True)
    self._directory = directory
    self._pending: list[list[bytes]] = [[] for _ in range(NUM_BUCKETS)]
    self._pending_bytes = 0

  def append(self, bucket: int, data: bytes) -> None:
    self._pending[bucket].append(data)
    self._pending_bytes += len(data)
    if self._pending_bytes >= _FLUSH_BYTES:
      self.flush()

  def flush(self) -> None:
    for bucket, chunks in enumerate(self._pending):
      if not chunks:
        continue
      with open(self.path(bucket), "ab") as f:
        f.writelines(chunks)
      chunks.clear()
    self._pending_bytes = 0

  def path(self, bucket: int) -> str:
    return os.path.join(self._directory, f"{bucket:03d}")

  def read(self, bucket: int) -> bytes:
    """Returns the bytes of a bucket. Call flush() first."""
    path = self.path(bucket)
    if not os.path.exists(path):
      return b""
    with open(path, "rb") as f:
      return f.read()


class BucketComparer:
  """Base of the observation and node comparers.

  compare() of a subclass calls _spill_and_compare(), which spills each side
  with _spill_side() and then calls _compare_bucket() on bucket i of both
  sides, for every i.
  """

  def __init__(self, findings: Findings, pool: concurrent.futures.Executor,
               spill_dir: str) -> None:
    self._findings = findings
    self._pool = pool
    self._spill_dir = spill_dir
    # The canonical @context values of the shards of each side.
    self.contexts: tuple[set[str], set[str]] = (set(), set())

  def _spill_and_compare(self, baseline: OutputTree,
                         candidate: OutputTree) -> None:
    spills = [SpillBuckets(f"{self._spill_dir}/{side}") for side in range(2)]
    for side, tree in enumerate((baseline, candidate)):
      self._spill_side(side, tree, spills[side])
      spills[side].flush()
    for bucket in range(NUM_BUCKETS):
      self._compare_bucket(spills[0].read(bucket), spills[1].read(bucket))

  def _spill_side(self, side: int, tree: OutputTree,
                  spill: SpillBuckets) -> None:
    raise NotImplementedError

  def _compare_bucket(self, baseline: bytes, candidate: bytes) -> None:
    raise NotImplementedError
