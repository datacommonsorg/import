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
"""Reading byte ranges of a file as one stream."""

import io
from typing import Callable

# Returns bytes [start, end) of a file.
FetchRange = Callable[[int, int], bytes]


class RangesReader(io.RawIOBase):
  """Reads byte ranges of a file one after the other, a chunk per fetch.

  `fetch(start, end)` returns bytes [start, end) of the file, e.g. with a
  ranged GCS request; it's called for at most `chunk_size` bytes at a time.
  Reads return as many bytes as asked for until the end, like a local file's,
  and reading at the end fetches nothing.
  """

  def __init__(self, fetch: FetchRange, ranges: list[tuple[int, int]],
               chunk_size: int, name: str) -> None:
    """`name` is the file's path or URL, for errors."""
    super().__init__()
    self._fetch = fetch
    self._ranges = _merge_adjacent(ranges)
    self._chunk_size = chunk_size
    self._name = name
    self._chunk = b""  # The last bytes fetched.
    self._chunk_pos = 0  # Bytes of _chunk already read.

  def readable(self) -> bool:
    return True

  def readinto(self, buffer) -> int:
    view = memoryview(buffer).cast("B")
    filled = 0
    while filled < len(view):
      if self._chunk_pos == len(self._chunk) and not self._fetch_chunk():
        break
      n = min(len(view) - filled, len(self._chunk) - self._chunk_pos)
      view[filled:filled + n] = self._chunk[self._chunk_pos:self._chunk_pos + n]
      filled += n
      self._chunk_pos += n
    return filled

  def _fetch_chunk(self) -> bool:
    """Fetches the next chunk; returns False after the last range."""
    while self._ranges and self._ranges[0][0] >= self._ranges[0][1]:
      self._ranges.pop(0)
    if not self._ranges:
      return False
    start, range_end = self._ranges[0]
    end = min(start + self._chunk_size, range_end)
    data = self._fetch(start, end)
    if len(data) != end - start:
      raise OSError(f"Expected {end - start} bytes at offset {start} of"
                    f" {self._name}, got {len(data)}")
    self._ranges[0] = (end, range_end)
    self._chunk, self._chunk_pos = data, 0
    return True


def _merge_adjacent(ranges: list[tuple[int, int]]) -> list[tuple[int, int]]:
  """Merges ranges that end where the next starts, to fetch them together."""
  merged = []
  for start, end in ranges:
    if merged and merged[-1][1] == start:
      merged[-1] = (merged[-1][0], end)
    else:
      merged.append((start, end))
  return merged
