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
"""CSV files: the same data, written differently, gives the same records.

Each CSV import type has one table. Every case writes the same three rows,
whose last column holds the cells that need CSV quoting.
"""

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

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


@cases(*OBSERVATION_FORMATS)
def test_observation_csv_format(given, expect):
  result = b.run(given, b.obs_config(ignoreColumns=["notes"]))
  b.assert_records(result.observations, expect)


# --- Entities CSVs ------------------------------------------------------------

_ENTITY_HEADER = ["author_id", "bio"]
_ENTITY_ROWS = [
    ["author1", "a, b"],
    ["author2", 'line one\nline "two"'],
    ["author3", "plain"],
]
_ENTITY_EXPECTED = [
    b.entity("author1", author_id="author1", bio="a, b"),
    b.entity("author2", author_id="author2", bio='line one\nline "two"'),
    b.entity("author3", author_id="author3", bio="plain"),
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
    Case("id column last",
         given={
             "case.csv":
                 b.csv_file(["a, b", "author1"],
                            ['line one\nline "two"', "author2"],
                            ["plain", "author3"],
                            header=["bio", "author_id"])
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


@cases(*ENTITY_FORMATS)
def test_entity_csv_format(given, expect):
  result = b.run(given, b.entities_config())
  b.assert_records(result.of_type("Author"), expect)
