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
"""Parts of a CSV file parse to the rows of the whole file, or one fails."""

import io

import pandas as pd
import pytest
from stats import csv_split

_H = b"entity,date,value\n"

# (name, file bytes). Each is split into every number of parts from 2 up to
# its size in bytes.
FILES = [
    ("plain rows", _H + b"a,1,1\nb,2,2\nc,3,3\n"),
    ("quoted newline across boundaries",
     _H + b'a,1,"x\ny\nz"\nb,2,"\n"\nc,3,3\n'),
    ("escaped quotes", _H + b'a,1,"say ""hi""\n"\nb,2,""""\nc,3,3\n'),
    ("newlines after escaped quotes in a quoted field",
     _H + b'a,1,"x""\n""y""\n"\nb,2,2\nc,3,3\n'),
    ("literal quote mid-field, then a quoted newline",
     _H + b'a,1,1\nb"c,2,2\nd,3,"x\ny"\ne,4,4\n'),
    ("text after a closing quote",
     _H + b'a,1,1\n"b"c,2,2\nd,3,"x\ny"\ne,4,4\n'),
    ("CRLF",
     _H.replace(b"\n", b"\r\n") + b'a,1,1\r\nb,2,"x\r\ny"\r\nc,3,3\r\n'),
    ("byte order mark and quoted first field",
     b'\xef\xbb\xbf"entity",date,value\n"a\n",1,1\nb,2,2\n'),
    ("no final newline", _H + b"a,1,1\nb,2,2"),
    ("blank lines", b"\n  \n" + _H + b"a,1,1\n\nb,2,2\n\n"),
    ("header only", _H),
    ("one row", _H + b"a,1,1\n"),
    ("unterminated quote", _H + b'a,1,1\nb,2,"x\nc,3,3\n'),
    ("quoted multiline header", b'entity,"da\nte",value\na,1,1\nb,2,2\n'),
    ("multibyte characters", _H + "é,1,1\n中,2,\"é\n中\"\n".encode()),
]
# Window sizes to find newlines with: rows are longer than the smaller ones.
WINDOWS = [1, 3, csv_split.WINDOW_BYTES]


def _plan(data: bytes,
          parts: int,
          window: int = csv_split.WINDOW_BYTES) -> list[csv_split.FilePart]:
  return csv_split.plan_parts(lambda start, end: data[start:end], len(data),
                              parts, window)


def _parse(data: bytes) -> pd.DataFrame:
  return pd.read_csv(io.BytesIO(data), dtype=str)


def _parse_parts(data: bytes, parts: list[csv_split.FilePart]) -> pd.DataFrame:
  frames = [
      _parse(data[:part.header_end] + data[part.start:part.end])
      for part in parts
  ]
  return pd.concat(frames, ignore_index=True)


def _is_row_boundary(data: bytes, offset: int) -> bool:
  """Whether `offset` is right after a newline outside any quoted field."""
  return csv_split._RowBoundaries(data).next(offset - 1) == offset


@pytest.mark.parametrize("window", WINDOWS)
@pytest.mark.parametrize("name,data", FILES, ids=[n for n, _ in FILES])
def test_parts_parse_to_the_rows_of_the_whole_file_or_one_fails(
    name, data, window):
  """A part fails exactly when a part boundary is inside a quoted field."""
  del name
  try:
    whole = _parse(data)
  except pd.errors.ParserError:
    whole = None
  for num_parts in range(2, len(data) + 1):
    parts = _plan(data, num_parts, window)
    if parts is None:
      continue
    assert parts[0].start == parts[0].header_end
    assert [p.end for p in parts[:-1]] == [p.start for p in parts[1:]]
    assert parts[-1].end == len(data)
    assert len(parts) <= num_parts
    assert [p.index for p in parts] == list(range(len(parts)))
    on_row_boundaries = all(_is_row_boundary(data, p.start) for p in parts[1:])
    if whole is None or not on_row_boundaries:
      # The runner then imports the file whole.
      with pytest.raises(pd.errors.ParserError):
        _parse_parts(data, parts)
    else:
      pd.testing.assert_frame_equal(_parse_parts(data, parts), whole)


@pytest.mark.parametrize(
    "data",
    [
        _H + b"a,1,1\n",  # One row.
        _H + b"a,1,1,extra\nb,2,2,extra\n",  # Implicit index.
        b"entity,date,value",  # No row boundary.
    ])
def test_files_that_are_not_split(data):
  assert _plan(data, 4) is None


def test_a_file_is_split_into_the_parts_asked_for_if_it_has_the_rows():
  data = _H + b"a,1,1\n" * 1000
  assert len(_plan(data, 7)) == 7
  assert _plan(data, 1) is None
  # Three rows, so at most three parts.
  assert len(_plan(_H + b"a,1,1\n" * 3, 10)) == 3


@pytest.mark.parametrize(
    "size, max_parts, split_bytes, expected",
    [
        (100, 8, 100, 1),  # Not larger than split_bytes.
        (101, 8, 100, 4),  # Parts of at least a quarter of split_bytes.
        (1000, 8, 100, 8),  # One part per worker.
        (1000, 1, 100, 1),  # One worker.
        (5, 8, 1, 5),  # Parts of at least 1 byte.
        (
            19 * 2**30,  # EPA's file: 304 parts of 64 MiB, so one per worker.
            64,
            csv_split.SPLIT_CSV_BYTES,
            64),
        (300 * 2**20, 8, csv_split.SPLIT_CSV_BYTES, 4),
    ])
def test_num_parts(size, max_parts, split_bytes, expected):
  assert csv_split.num_parts(size, max_parts, split_bytes) == expected


def test_a_plan_reads_a_window_per_part_not_the_whole_file():
  data = _H + b"a,2020,1\n" * 100_000
  reads = []

  def read(start: int, end: int) -> bytes:
    reads.append(end - start)
    return data[start:end]

  parts = csv_split.plan_parts(read, len(data), 8, window_bytes=64)
  assert len(parts) == 8
  assert reads == [64] * 8  # The header, then each part's start.
