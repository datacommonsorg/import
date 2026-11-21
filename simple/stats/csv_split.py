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
"""Splits a large CSV file into parts, at row boundaries, to import in parallel.

Each part is a byte range of the file, parsed after the file's header. Parts
start at the first newline after evenly spaced offsets, found by reading a small
window there: the file isn't read whole. Such a newline may be inside a quoted
field. The part that ends there then fails to parse, with pandas' "EOF inside
string", because it starts on a row boundary (by induction from the header),
and the runner imports the file whole instead. So either every part parses, and
the parts give the rows of the whole file, or a part fails. The header, read
from the start of the file, is found as pandas' C parser finds quoted fields
(default dialect: `,` separator, `"` quote, `""` escape): a quote opens a field
only at the start of a field, so `a"b` is a literal quote.

The plan depends only on the file's bytes and the number of parts, so part
boundaries, and the names of the shards that parts write, are reproducible.

One known exception to "the same rows", from a pandas bug: pandas drops the
leading spaces of a value in a row that starts with spaces and straddles its
2^18-character read buffer; splitting moves those buffer boundaries.
"""

import dataclasses
import io
import re
from typing import Callable

import pandas as pd
from util.filesystem import File

# Files larger than this are split, by default (see `num_parts`).
SPLIT_CSV_BYTES = 256 * 1024 * 1024
# Parts are at least this fraction of the split size: 64 MiB by default.
_MIN_PART_FRACTION = 4
# Bytes read at a time to find a newline; doubled while none is found.
WINDOW_BYTES = 64 * 1024

_UTF8_BOM = b"\xef\xbb\xbf"
# A quoted field, from its opening quote to its closing quote, or to the end of
# the file if it has none. `""` inside it is an escaped quote.
_QUOTED_FIELD = re.compile(rb'"(?:[^"]+|"")*(?:"|\Z)')
# The bytes after which a quote opens a quoted field: a separator or line break.
_FIELD_STARTS = b",\r\n"
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
  """Returns how many parts to import a file of `size` bytes in.

  A file larger than `split_bytes` is split into up to `max_parts` parts (one
  per worker), each at least a quarter of `split_bytes`; others are imported
  whole (one part).
  """
  if size <= split_bytes:
    return 1
  min_part_bytes = max(1, split_bytes // _MIN_PART_FRACTION)
  return max(1, min(max_parts, size // min_part_bytes))


def plan_file_parts(file: File, parts: int) -> list[FilePart] | None:
  """Returns about `parts` parts to import `file` in, or None to import it whole.

  A file whose byte ranges can't be read (e.g. gzip-encoded on GCS) is
  imported whole.
  """
  size = file.size()
  if parts < 2 or size is None or not file.can_read_ranges():
    return None
  return plan_parts(file.read_bytes, size, parts)


def plan_parts(read: ReadRange,
               size: int,
               parts: int,
               window_bytes: int = WINDOW_BYTES) -> list[FilePart] | None:
  """Returns up to `parts` parts to import a CSV file of `size` bytes in.

  Or None to import it whole: if `parts` < 2, or the file has no newline to
  split at, or its first rows are ones whose parsing depends on the rows
  around them (see `_header_parses_alone`). Fewer parts than asked for are
  returned if the file has fewer rows than that.
  """
  if parts < 2:
    return None
  first_rows = _read_first_rows(read, size, window_bytes)
  header_end = _RowBoundaries(first_rows).header_end()
  if header_end is None or not _header_parses_alone(first_rows, header_end):
    return None
  # Part k starts after the first newline in [offsets[k], offsets[k + 1]).
  offsets = [size * k // parts for k in range(parts)] + [size]
  starts = [header_end]
  for k in range(1, parts):
    start = _after_first_newline(read, max(offsets[k], header_end),
                                 offsets[k + 1], window_bytes)
    if start is not None and start < size:
      starts.append(start)
  if len(starts) < 2:
    return None
  ends = starts[1:] + [size]
  return [
      FilePart(index=i, start=start, end=end, header_end=header_end)
      for i, (start, end) in enumerate(zip(starts, ends))
  ]


def _read_first_rows(read: ReadRange, size: int, window_bytes: int) -> bytes:
  """Returns the file's first bytes: through its first row after the header.

  Or the whole file, if it has no such row.
  """
  length = window_bytes
  while length < size:
    data = read(0, length)
    boundaries = _RowBoundaries(data)
    header_end = boundaries.header_end()
    if header_end is not None and boundaries.next(header_end) is not None:
      return data
    length *= 2
  return read(0, size)


def _after_first_newline(read: ReadRange, start: int, end: int,
                         window_bytes: int) -> int | None:
  """Returns the offset after the first newline in [start, end), or None."""
  length = window_bytes
  while start < end:
    window_end = min(start + length, end)
    newline = read(start, window_end).find(b"\n")
    if newline >= 0:
      return start + newline + 1
    start = window_end
    length *= 2
  return None


class _RowBoundaries:
  """Finds the offsets right after the newlines that end rows.

  Quoted fields are found by scanning the file from its start, as far as
  queries need, so each query must be at or after the previous result. `data`
  may be the file's first bytes only: a quoted field that runs past their end
  hides the newlines after it, so every boundary found is one of the file's.
  """

  def __init__(self, data: bytes) -> None:
    self._data = data
    # (start, end) of the first quoted field that doesn't end before the last
    # newline seen, or None if there is none.
    self._field = self._quoted_field_from(0)

  def next(self, pos: int) -> int | None:
    """Returns the first row boundary at or after `pos`, or None."""
    while True:
      newline = self._data.find(b"\n", pos)
      if newline < 0:
        return None
      while self._field is not None and self._field[1] <= newline:
        self._field = self._quoted_field_from(self._field[1])
      if self._field is None or newline < self._field[0]:
        return newline + 1
      pos = self._field[1]

  def _quoted_field_from(self, pos: int) -> tuple[int, int] | None:
    """Returns the first quoted field at or after `pos`, outside any field.

    A quote opens a field only at the start of one; elsewhere, as in `a"b`,
    it is a literal quote.
    """
    while True:
      quote = self._data.find(b'"', pos)
      if quote < 0:
        return None
      if self._starts_field(quote):
        return quote, _QUOTED_FIELD.match(self._data, quote).end()
      pos = quote + 1

  def _starts_field(self, pos: int) -> bool:
    if pos == 0 or (pos == len(_UTF8_BOM) and self._data[:pos] == _UTF8_BOM):
      return True
    return self._data[pos - 1] in _FIELD_STARTS

  def header_end(self) -> int | None:
    """Returns the end of the header row: the first row that isn't blank.

    pandas skips blank lines, including lines of spaces and tabs.
    """
    start = 0
    while True:
      end = self.next(start)
      if end is None:
        return None
      row = self._data[start:end]
      if start == 0:
        row = row.removeprefix(_UTF8_BOM)
      if row.strip(_BLANK):
        return end
      start = end


def _header_parses_alone(data: bytes, header_end: int) -> bool:
  """Whether the header parses the same with or without the rows after it.

  Checks, with pandas, that the header alone gives no rows, and that the first
  row after it gives the same columns and no implicit index (a first row with
  one more field than the header makes its first field the index of every
  row, so later parts would be parsed differently).
  """
  header = _parse(data[:header_end])
  first_row_end = _RowBoundaries(data).next(header_end)
  if first_row_end is None:
    first_row_end = len(data)
  first_rows = _parse(data[:first_row_end])
  return (header is not None and first_rows is not None and header.empty and
          list(header.columns) == list(first_rows.columns) and
          isinstance(first_rows.index, pd.RangeIndex))


def _parse(data: bytes) -> pd.DataFrame | None:
  try:
    return pd.read_csv(io.BytesIO(data), dtype=str)
  except (ValueError, pd.errors.ParserError):
    return None
