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

Every format runs against each CSV import type. Each import type's last
column holds the cells that need CSV quoting (commas, quotes, newlines).
"""

import dataclasses
from typing import Callable

import pytest
from testsuite.lib import builders as b

_TRICKY_CELLS = ["a, b", 'line one\nline "two"', "plain"]


@dataclasses.dataclass(frozen=True)
class _Importer:
  header: list[str]
  rows: list[list[str]]
  config: dict
  records: Callable[[b.Result], list[dict]]  # The records to compare.
  expect: list[dict]


IMPORTERS = {
    "observations":
        _Importer(header=b.OBS_HEADER + ["notes"],
                  rows=[
                      b.obs_row(about, value=value) + [notes]
                      for about, value, notes in
                      zip(["country/USA", "country/FRA", "country/DEU"],
                          ["1", "2", "3"], _TRICKY_CELLS)
                  ],
                  config=b.obs_config(ignoreColumns=["notes"]),
                  records=lambda result: result.observations,
                  expect=[
                      b.obs("country/USA", value=1),
                      b.obs("country/FRA", value=2),
                      b.obs("country/DEU", value=3),
                  ]),
    "entities":
        _Importer(header=["author_id", "bio"],
                  rows=[[f"author{i}", bio]
                        for i, bio in enumerate(_TRICKY_CELLS, start=1)],
                  config=b.entities_config(),
                  records=lambda result:
                  [n for n in result.nodes if n.get("@type") == "dcid:Author"],
                  expect=[
                      b.entity(f"author{i}", author_id=f"author{i}", bio=bio)
                      for i, bio in enumerate(_TRICKY_CELLS, start=1)
                  ]),
}


def _last_column_first(header, rows):

  def move(row):
    return [row[-1]] + row[:-1]

  return {"case.csv": b.csv_file(*map(move, rows), header=move(header))}


# Each format turns (header, rows) into the files of one import.
FORMATS = {
    "commas, quotes and newlines inside cells":
        lambda header, rows: {
            "case.csv": b.csv_file(*rows, header=header)
        },
    "CRLF line endings":
        lambda header, rows: {
            "case.csv": b.csv_file(*rows, header=header, line_end="\r\n")
        },
    "every cell quoted":
        lambda header, rows: {
            "case.csv": b.csv_file(*rows, header=header, quote_all=True)
        },
    "byte order mark before the header":
        lambda header, rows: {
            "case.csv": b.csv_file(*rows, header=header, bom=True)
        },
    "last column first":
        _last_column_first,
    "rows in another order":
        lambda header, rows: {
            "case.csv": b.csv_file(*reversed(rows), header=header)
        },
    "rows split across two files":
        lambda header, rows: {
            "case.csv": b.csv_file(*rows[:1], header=header),
            "case_2.csv": b.csv_file(*rows[1:], header=header),
        },
}


@pytest.mark.parametrize("csv_format", FORMATS.values(), ids=FORMATS.keys())
@pytest.mark.parametrize("importer", IMPORTERS.values(), ids=IMPORTERS.keys())
def test_csv_format(importer, csv_format):
  result = b.run(csv_format(importer.header, importer.rows), importer.config)
  b.assert_records(importer.records(result), importer.expect)
