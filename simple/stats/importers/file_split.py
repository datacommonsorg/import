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
"""Splits large CSV and MCF files into byte-range parts to import in parallel.

Each part is a byte range parsed after the file's header (the CSV header row,
or any leading `Context:` block before the first `Node:` line in MCF).
Boundaries are found with small ranged reads so the file isn't read whole. If
a part fails to parse (e.g. a CSV split inside a quoted field, or an MCF node
whose `dcid:` is in another block), the runner deletes partial shards and falls
back to importing the file whole.
"""

import collections
import concurrent.futures
import dataclasses
import io
import logging
import re
from typing import Callable

import pandas as pd
from stats.config.config import Config
from stats.graph.data import ImportType
from stats.writer.shard_naming import file_shard_stem
from stats.writer.shard_naming import node_part_stem
from stats.writer.shard_naming import part_shards_could_collide
from util.filesystem import File

# Files larger than this are split, by default (see `num_parts`).
SPLIT_FILE_BYTES = 256 * 1024 * 1024
# Parts are at least this fraction of the split size: 64 MiB by default.
_MIN_PART_FRACTION = 4
# Bytes read at a time to find a boundary; doubled while none is found.
WINDOW_BYTES = 64 * 1024
_MAX_PLANNING_THREADS = 32

_UTF8_BOM = b"\xef\xbb\xbf"
_BLANK = b" \t\r\n"
_MCF_NODE_LINE_RE = re.compile(rb"[ \t]*Node[ \t]*:")
_MCF_NEXT_NODE_RE = re.compile(rb"\n[ \t]*Node[ \t]*:")

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


def _is_mcf(file: File) -> bool:
  return file.path.lower().endswith(".mcf")


def _is_obs_csv(file: File, config: Config) -> bool:
  return not _is_mcf(file) and config.import_type(
      file) == ImportType.OBSERVATIONS


def _can_split(file: File, stem: str,
               stem_counts: collections.Counter[str]) -> bool:
  if stem_counts[stem] > 1:
    return False
  return not part_shards_could_collide(file.path, set(stem_counts))


def plan_splits(files: list[File],
                config: Config,
                max_workers: int,
                split_bytes: int | None = None) -> dict[str, list[FilePart]]:
  """Returns byte-range parts for each large input file, by path."""
  threshold = split_bytes or SPLIT_FILE_BYTES
  obs_stems = collections.Counter(
      file_shard_stem(f.path) for f in files if _is_obs_csv(f, config))
  node_stems = collections.Counter(
      node_part_stem(f.path) for f in files if not _is_obs_csv(f, config))
  candidates = [
      (f, parts)
      for f in files
      if (parts := num_parts(f.size() or 0, max_workers, threshold)) > 1 and
      (_can_split(f, file_shard_stem(f.path), obs_stems) if _is_obs_csv(
          f, config) else _can_split(f, node_part_stem(f.path), node_stems))
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


def plan_csv_parts(read: ReadRange,
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
  return _build_parts(
      size, parts, header_end,
      lambda start, end: _after_first_newline(read, start, end, window_bytes))


def plan_mcf_parts(read: ReadRange,
                   size: int,
                   parts: int,
                   window_bytes: int = WINDOW_BYTES) -> list[FilePart] | None:
  """Returns up to `parts` parts of an MCF of `size` bytes, or None to read whole."""
  if parts < 2 or size == 0:
    return None
  header_end = _first_mcf_node_offset(read, size, window_bytes)
  if header_end is None:
    return None
  return _build_parts(
      size, parts, header_end,
      lambda start, end: _find_mcf_node_after(read, start, end, window_bytes))


def _build_parts(
    size: int, parts: int, header_end: int,
    find_start: Callable[[int, int], int | None]) -> list[FilePart] | None:
  """Splits [header_end, size) into up to `parts` contiguous FileParts."""
  offsets = [size * k // parts for k in range(parts)] + [size]
  starts = [header_end]
  for k in range(1, parts):
    start = find_start(max(offsets[k], header_end), offsets[k + 1])
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
  planner = plan_mcf_parts if _is_mcf(file) else plan_csv_parts
  try:
    return planner(file.read_bytes, size, parts)
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


def _first_mcf_node_offset(read: ReadRange, size: int,
                           window_bytes: int) -> int | None:
  """Returns the byte offset of the first `Node:` line, or None."""
  head = read(0, min(max(window_bytes, 64), size))
  if _MCF_NODE_LINE_RE.match(head):
    return 0
  return _find_mcf_node_after(read, 0, size, window_bytes)


def _find_mcf_node_after(read: ReadRange, start: int, end: int,
                         window_bytes: int) -> int | None:
  """Returns the offset of the first `Node:` line after a `\\n` in [start, end)."""
  pos = start
  length = window_bytes
  while pos < end:
    window_end = min(pos + length, end)
    chunk = read(pos, window_end)
    if not chunk:
      return None
    if (match := _MCF_NEXT_NODE_RE.search(chunk)) is not None:
      return pos + match.start() + 1
    if window_end >= end:
      return None
    # Overlap on the last newline so a `\nNode:` straddling the window edge is
    # seen intact on the next read.
    last_nl = chunk.rfind(b"\n")
    if last_nl < 0:
      pos = window_end
      length = window_bytes
    elif last_nl == 0:
      length *= 2
    else:
      pos += last_nl
      length = window_bytes
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
