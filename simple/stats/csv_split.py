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
"""Splits large observation CSVs into byte-range parts to import in parallel.

Each part is a byte range parsed after the file's header. Boundaries are the
first newline after evenly spaced offsets, found with small ranged reads so the
file isn't read whole. If a boundary lands inside a quoted field, that part
fails to parse (EOF inside string) and the runner falls back to importing the
file whole.
"""

import concurrent.futures
import dataclasses
import io
import logging
from typing import Callable

import pandas as pd
from stats.config import Config
from stats.data import ImportType
from stats.jsonld_stream_writer import observation_shard_stem
from stats.jsonld_stream_writer import part_shards_could_collide
from util.filesystem import File

# Files larger than this are split, by default (see `num_parts`).
SPLIT_CSV_BYTES = 256 * 1024 * 1024
# Parts are at least this fraction of the split size: 64 MiB by default.
_MIN_PART_FRACTION = 4
# Bytes read at a time to find a newline; doubled while none is found.
WINDOW_BYTES = 64 * 1024
_MAX_PLANNING_THREADS = 32

_UTF8_BOM = b"\xef\xbb\xbf"
_BLANK = b" \t\r\n"

# Returns bytes [start, end) of a file.
ReadRange = Callable[[int, int], bytes]


@dataclasses.dataclass(frozen=True)
class FilePart:
  """Bytes [start, end) of a file, parsed after its header [0, header_end)."""
  index: int
  start: int
  end: int
  header_end: int

  @property
  def size(self) -> int:
    return self.header_end + self.end - self.start


def num_parts(size: int, max_parts: int, split_bytes: int) -> int:
  """Returns how many parts to import a file of `size` bytes in."""
  if size <= split_bytes:
    return 1
  min_part_bytes = max(1, split_bytes // _MIN_PART_FRACTION)
  return max(1, min(max_parts, size // min_part_bytes))


def plan_splits(csv_files: list[File],
                config: Config,
                max_workers: int,
                split_bytes: int | None = None) -> dict[str, list[FilePart]]:
  """Returns byte-range parts for each large observation CSV, by file path."""
  threshold = split_bytes or SPLIT_CSV_BYTES
  stems = {observation_shard_stem(f.path) for f in csv_files}
  candidates = [
      (f, parts)
      for f in csv_files
      if (parts := num_parts(f.size() or 0, max_workers, threshold)) > 1 and
      config.import_type(f) == ImportType.OBSERVATIONS and
      not part_shards_could_collide(f.path, stems)
  ]
  if not candidates:
    return {}
  with concurrent.futures.ThreadPoolExecutor(
      max_workers=min(_MAX_PLANNING_THREADS, len(candidates))) as executor:
    plans = list(executor.map(lambda c: _plan_file_or_none(*c), candidates))
  parts_by_path = {
      f.path: parts for (f, _), parts in zip(candidates, plans) if parts
  }
  if parts_by_path:
    logging.info("Splitting %d large files into %d parts", len(parts_by_path),
                 sum(len(p) for p in parts_by_path.values()))
  return parts_by_path


def plan_parts(read: ReadRange,
               size: int,
               parts: int,
               window_bytes: int = WINDOW_BYTES) -> list[FilePart] | None:
  """Returns up to `parts` parts of a CSV of `size` bytes, or None to read whole."""
  if parts < 2:
    return None
  prefix = _read_prefix(read, size, window_bytes)
  header_end = _first_non_blank_line_end(prefix)
  if header_end is None or not _header_parses_alone(prefix, header_end):
    return None
  offsets = [size * k // parts for k in range(parts)] + [size]
  starts = [header_end]
  for k in range(1, parts):
    start = _after_first_newline(read, max(offsets[k], header_end),
                                 offsets[k + 1], window_bytes)
    if start is not None and start < size:
      starts.append(start)
  if len(starts) < 2:
    return None
  return [
      FilePart(index=i, start=start, end=end, header_end=header_end)
      for i, (start, end) in enumerate(zip(starts, starts[1:] + [size]))
  ]


def _plan_file_or_none(file: File, parts: int) -> list[FilePart] | None:
  size = file.size()
  if parts < 2 or size is None or not file.can_read_ranges():
    return None
  try:
    return plan_parts(file.read_bytes, size, parts)
  except Exception:  # pylint: disable=broad-except
    logging.exception("Failed to plan parts of %s; importing it whole",
                      file.path)
    return None


def _read_prefix(read: ReadRange, size: int, window_bytes: int) -> bytes:
  """Reads from offset 0 through at least the header and first data line."""
  length = window_bytes
  while length < size:
    data = read(0, length)
    header_end = _first_non_blank_line_end(data)
    if header_end is not None and data.find(b"\n", header_end) >= 0:
      return data
    length *= 2
  return read(0, size)


def _first_non_blank_line_end(data: bytes) -> int | None:
  """Returns the offset after the first non-blank line's newline, or None."""
  pos = 0
  while (nl := data.find(b"\n", pos)) >= 0:
    line = data[pos:nl + 1]
    if pos == 0:
      line = line.removeprefix(_UTF8_BOM)
    if line.strip(_BLANK):
      return nl + 1
    pos = nl + 1
  return None


def _after_first_newline(read: ReadRange, start: int, end: int,
                         window_bytes: int) -> int | None:
  """Returns the offset after the first newline in [start, end), or None."""
  length = window_bytes
  while start < end:
    window_end = min(start + length, end)
    nl = read(start, window_end).find(b"\n")
    if nl >= 0:
      return start + nl + 1
    start = window_end
    length *= 2
  return None


def _header_parses_alone(data: bytes, header_end: int) -> bool:
  """Checks that the header parses cleanly alone and matches the first row."""
  header = _parse_csv(data[:header_end])
  first_row_nl = data.find(b"\n", header_end)
  first_row_end = first_row_nl + 1 if first_row_nl >= 0 else len(data)
  sample = _parse_csv(data[:first_row_end])
  return (header is not None and sample is not None and header.empty and
          list(header.columns) == list(sample.columns) and
          isinstance(sample.index, pd.RangeIndex))


def _parse_csv(data: bytes) -> pd.DataFrame | None:
  try:
    return pd.read_csv(io.BytesIO(data), dtype=str)
  except (ValueError, pd.errors.ParserError):
    return None
