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
"""Parts of a CSV file parse to the same rows as the whole file."""

import io
import math

import pandas as pd
import pytest
from stats import csv_split

_H = b"entity,date,value\n"

# (name, file bytes). Each is split with every target size from 1 byte up.
FILES = [
    ("plain rows", _H + b"a,1,1\nb,2,2\nc,3,3\n"),
    ("quoted newline across boundaries",
     _H + b'a,1,"x\ny\nz"\nb,2,"\n"\nc,3,3\n'),
    ("escaped quotes", _H + b'a,1,"say ""hi""\n"\nb,2,""""\nc,3,3\n'),
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
    ("multibyte characters", _H + "é,1,1\n中,2,2\n".encode()),
]


def _parse(data: bytes) -> pd.DataFrame:
  return pd.read_csv(io.BytesIO(data), dtype=str)


def _parse_parts(data: bytes, parts: list[csv_split.FilePart]) -> pd.DataFrame:
  frames = [
      _parse(data[:part.header_end] + data[part.start:part.end])
      for part in parts
  ]
  return pd.concat(frames, ignore_index=True)


@pytest.mark.parametrize("name,data", FILES, ids=[n for n, _ in FILES])
def test_parts_parse_to_the_rows_of_the_whole_file(name, data):
  del name
  try:
    whole = _parse(data)
  except pd.errors.ParserError:
    whole = None
  for target in range(1, len(data) + 1):
    parts = csv_split.plan_parts(data, target)
    if parts is None:
      continue
    assert parts[0].start == parts[0].header_end
    assert [p.end for p in parts[:-1]] == [p.start for p in parts[1:]]
    assert parts[-1].end == len(data)
    if whole is None:
      # The import fails too (the runner then reports the whole file's error).
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
  assert csv_split.plan_parts(data, 4) is None


def test_part_count_is_the_size_over_the_target_up_to_a_cap():
  data = _H + b"a,1,1\n" * 1000
  assert len(csv_split.plan_parts(data, 1000)) == math.ceil(len(data) / 1000)
  assert len(csv_split.plan_parts(data, 1)) == csv_split.MAX_PARTS
