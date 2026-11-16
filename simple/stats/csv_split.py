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

Each part is a byte range of the file that starts and ends on a row boundary.
A part is parsed as the file's header followed by its range, so pandas reads
the same rows from the parts as from the whole file. A row boundary is a
newline outside any quoted field, found as pandas' C parser finds quoted
fields (default dialect: `,` separator, `"` quote, `""` escape): a quote opens
a field only at the start of a field, so `a"b` is a literal quote.

The plan depends only on the file's bytes, never on the machine, so part
boundaries, and the names of the shards that parts write, are reproducible.

One known exception to "the same rows", from a pandas bug: pandas drops the
leading spaces of a value in a row that starts with spaces and straddles its
2^18-character read buffer; splitting moves those buffer boundaries.
"""

import dataclasses
import io
import math
import mmap
import re

import pandas as pd
from util.filesystem import File

# Files larger than this are split into parts of about this size.
SPLIT_TARGET_BYTES = 16 * 1024 * 1024
# Parts per file, at most: larger files get larger parts.
MAX_PARTS = 64

_UTF8_BOM = b"\xef\xbb\xbf"
# A quoted field, from its opening quote to its closing quote, or to the end of
# the file if it has none. `""` inside it is an escaped quote.
_QUOTED_FIELD = re.compile(rb'"(?:[^"]+|"")*(?:"|\Z)')
# The bytes after which a quote opens a quoted field: a separator or line break.
_FIELD_STARTS = b",\r\n"
_BLANK = b" \t\r\n"


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


def plan_file_parts(file: File) -> list[FilePart] | None:
  """Returns the parts to import `file` in, or None to import it whole.

  Reads the whole file once, from a local copy if it isn't on local disk.
  A file whose byte ranges can't be read (e.g. gzip-encoded on GCS) is
  imported whole.
  """
  if not file.can_read_ranges():
    return None
  with file.local_path() as path, open(path, "rb") as f:
    with mmap.mmap(f.fileno(), 0, access=mmap.ACCESS_READ) as data:
      return plan_parts(data, SPLIT_TARGET_BYTES)


def plan_parts(data: bytes, target_bytes: int) -> list[FilePart] | None:
  """Returns the parts to import `data`, a CSV file's bytes, or None.

  None means the file is imported whole: it is not larger than
  `target_bytes`, or has no row boundary to split at, or its first rows are
  ones whose parsing depends on the rows around them (see
  `_header_parses_alone`).
  """
  num_parts = min(math.ceil(len(data) / target_bytes), MAX_PARTS)
  if num_parts < 2:
    return None
  header_end = _RowBoundaries(data).header_end()
  if header_end is None or not _header_parses_alone(data, header_end):
    return None
  boundaries = _RowBoundaries(data)
  starts = [header_end]
  for k in range(1, num_parts):
    start = boundaries.next(max(len(data) * k // num_parts, starts[-1] + 1))
    if start is None or start >= len(data):
      break
    starts.append(start)
  if len(starts) < 2:
    return None
  ends = starts[1:] + [len(data)]
  return [
      FilePart(index=i, start=start, end=end, header_end=header_end)
      for i, (start, end) in enumerate(zip(starts, ends))
  ]


class _RowBoundaries:
  """Finds the offsets right after the newlines that end rows.

  Quoted fields are found by scanning the file from its start, as far as
  queries need, so each query must be at or after the previous result.
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
