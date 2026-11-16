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
"""
CSV files: the same data, written differently, gives the same records.

Each CSV import type has one table. Every case writes the same three rows,
whose last column holds the cells that need CSV quoting.
"""

import pytest
from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

# Input on GCS is streamed in ranged reads, local input is not: CSV parsing,
# headers included, must not depend on where the input is.
_INPUT_MODES = pytest.mark.parametrize("mode", ["disk_input", "gcs_input"])

# --- Observation CSVs ---------------------------------------------------------

_OBS_HEADER = b.OBS_HEADER + ["notes"]
_OBS_ROWS = [
    b.obs_row("country/USA", value="1") + ["a, b"],
    b.obs_row("country/FRA", value="2") + ['line one\nline "two"'],
    b.obs_row("country/DEU", value="3") + ["plain"],
]
_OBS_EXPECTED = [
    b.obs("country/USA", value=1),
    b.obs("country/FRA", value=2),
    b.obs("country/DEU", value=3),
]

OBSERVATION_FORMATS = [
    Case("commas, quotes and newlines inside cells",
         given={"case.csv": b.csv_file(*_OBS_ROWS, header=_OBS_HEADER)},
         expect=_OBS_EXPECTED),
    Case("CRLF line endings",
         given={
             "case.csv":
                 b.csv_file(*_OBS_ROWS, header=_OBS_HEADER, line_end="\r\n")
         },
         expect=_OBS_EXPECTED),
    Case("every cell quoted",
         given={
             "case.csv":
                 b.csv_file(*_OBS_ROWS, header=_OBS_HEADER, quote_all=True)
         },
         expect=_OBS_EXPECTED),
    Case("byte order mark before the header",
         given={
             "case.csv": b.csv_file(*_OBS_ROWS, header=_OBS_HEADER, bom=True)
         },
         expect=_OBS_EXPECTED),
    Case("ignored column first",
         given={
             "case.csv":
                 b.csv_file(
                     ["a, b", "country/USA", "Count_Person", "2020", "1"], [
                         'line one\nline "two"', "country/FRA", "Count_Person",
                         "2020", "2"
                     ], ["plain", "country/DEU", "Count_Person", "2020", "3"],
                     header=["notes"] + b.OBS_HEADER)
         },
         expect=_OBS_EXPECTED),
    Case("rows in another order",
         given={
             "case.csv": b.csv_file(*reversed(_OBS_ROWS), header=_OBS_HEADER)
         },
         expect=_OBS_EXPECTED),
    Case("rows split across two files",
         given={
             "case.csv": b.csv_file(*_OBS_ROWS[:1], header=_OBS_HEADER),
             "case_2.csv": b.csv_file(*_OBS_ROWS[1:], header=_OBS_HEADER),
         },
         expect=_OBS_EXPECTED),
]


@_INPUT_MODES
@cases(*OBSERVATION_FORMATS)
def test_observation_csv_format(given, expect, mode):
  result = b.run(given,
                 b.obs_config(b.OBS_MAPPINGS, ignoreColumns=["notes"]),
                 mode=mode)
  assert result.error is None, result.error
  b.assert_records(result.observations, expect)


# An ignored column whose name is longer than the first read of a header.
_LONG_NAME = "x" * 300_000

HEADER_FORMATS = [
    Case("header only",
         given=(b.csv_file(header=_OBS_HEADER), "notes"),
         expect=[]),
    Case("header only, without a final newline",
         given=(",".join(_OBS_HEADER), "notes"),
         expect=[]),
    Case("quoted header cell with a newline",
         given=(b.csv_file(*_OBS_ROWS,
                           header=b.OBS_HEADER + ["two\nlines"]), "two\nlines"),
         expect=_OBS_EXPECTED),
    Case("header longer than its first read",
         given=(b.csv_file(*_OBS_ROWS,
                           header=b.OBS_HEADER + [_LONG_NAME]), _LONG_NAME),
         expect=_OBS_EXPECTED),
]


@_INPUT_MODES
@cases(*HEADER_FORMATS)
def test_observation_csv_header(given, expect, mode):
  csv_text, ignored_column = given
  result = b.run({"case.csv": csv_text},
                 b.obs_config(b.OBS_MAPPINGS, ignoreColumns=[ignored_column]),
                 mode=mode)
  assert result.error is None, result.error
  b.assert_records(result.observations, expect)


@_INPUT_MODES
def test_an_empty_csv_fails_header_validation(mode):
  result = b.run({"case.csv": ""}, b.obs_config(b.OBS_MAPPINGS), mode=mode)
  b.assert_fails(
      result, "ValueError",
      "Failed to read CSV headers for 'case.csv': No columns to parse from file"
  )


# --- Entities CSVs ------------------------------------------------------------

_ENTITY_HEADER = ["author_id", "type", "bio"]
_ENTITY_ROWS = [
    ["author1", "Author", "a, b"],
    ["author2", "Author", 'line one\nline "two"'],
    ["author3", "Author", "plain"],
]
_ENTITY_EXPECTED = [
    b.entity("author1", bio="a, b"),
    b.entity("author2", bio='line one\nline "two"'),
    b.entity("author3", bio="plain"),
]

ENTITY_FORMATS = [
    Case("commas, quotes and newlines inside cells",
         given={"case.csv": b.csv_file(*_ENTITY_ROWS, header=_ENTITY_HEADER)},
         expect=_ENTITY_EXPECTED),
    Case("CRLF line endings",
         given={
             "case.csv":
                 b.csv_file(*_ENTITY_ROWS,
                            header=_ENTITY_HEADER,
                            line_end="\r\n")
         },
         expect=_ENTITY_EXPECTED),
    Case("every cell quoted",
         given={
             "case.csv":
                 b.csv_file(*_ENTITY_ROWS,
                            header=_ENTITY_HEADER,
                            quote_all=True)
         },
         expect=_ENTITY_EXPECTED),
    Case("byte order mark before the header",
         given={
             "case.csv":
                 b.csv_file(*_ENTITY_ROWS, header=_ENTITY_HEADER, bom=True)
         },
         expect=_ENTITY_EXPECTED),
    Case("dcid column last",
         given={
             "case.csv":
                 b.csv_file(["Author", "a, b", "author1"],
                            ["Author", 'line one\nline "two"', "author2"],
                            ["Author", "plain", "author3"],
                            header=["type", "bio", "author_id"])
         },
         expect=_ENTITY_EXPECTED),
    Case("rows in another order",
         given={
             "case.csv":
                 b.csv_file(*reversed(_ENTITY_ROWS), header=_ENTITY_HEADER)
         },
         expect=_ENTITY_EXPECTED),
    Case("rows split across two files",
         given={
             "case.csv": b.csv_file(*_ENTITY_ROWS[:1], header=_ENTITY_HEADER),
             "case_2.csv": b.csv_file(*_ENTITY_ROWS[1:], header=_ENTITY_HEADER),
         },
         expect=_ENTITY_EXPECTED),
]


@_INPUT_MODES
@cases(*ENTITY_FORMATS)
def test_entity_csv_format(given, expect, mode):
  result = b.run(given,
                 b.entities_config(mappings=b.entity_mappings("bio")),
                 mode=mode)
  assert result.error is None, result.error
  b.assert_records(result.of_type("Author"), expect)
