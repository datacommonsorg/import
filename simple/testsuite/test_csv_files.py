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

Each CSV import type has one table of formats, whose cases write the same
three rows; their last column holds the cells that need CSV quoting. Header
cases check the edges of reading a header. Split cases check that a large CSV
imported in parts, in parallel, gives the output of the whole file.
"""

import contextlib
import logging
from unittest import mock

import pytest
from testsuite.lib import builders as b
from testsuite.lib import run as runlib
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

# Input on GCS is streamed in ranged reads, local input is not; and in the
# split modes, every observation CSV of more than one row is imported in parts.
# CSV parsing, headers included, must depend on none of these.
_INPUT_MODES = pytest.mark.parametrize(
    "mode", ["disk_input", "gcs_input", "split", "gcs_split"])

# --- Observation CSVs ---------------------------------------------------------

_OBS_HEADER = b.OBS_HEADER + ["notes"]
_OBS_ROWS = [
    b.obs_row("country/USA", value="1") + ["a, b"],
    b.obs_row("country/FRA", value="2") + ['line one\nline "two"'],
    b.obs_row("country/DEU", value="3") + ["plain"],
]
_OBS_EXPECTED = [
    b.obs("country/USA", value="1"),
    b.obs("country/FRA", value="2"),
    b.obs("country/DEU", value="3"),
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
    Case("no newline after the last row",
         given={
             "case.csv":
                 b.csv_file(*_OBS_ROWS, header=_OBS_HEADER).removesuffix("\n")
         },
         expect=_OBS_EXPECTED),
    Case("one row",
         given={"case.csv": b.csv_file(*_OBS_ROWS[2:], header=_OBS_HEADER)},
         expect=_OBS_EXPECTED[2:]),
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

# given: (CSV text, the column to ignore).
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


# --- Split files --------------------------------------------------------------
# With the split size shrunk to 1 byte, every observation CSV of more than one
# row is imported in parts: up to one per worker. A file with a part that fails
# (e.g. a part boundary inside a quoted field) is imported whole instead.

_SPLIT_MODES = pytest.mark.parametrize("mode", ["split", "gcs_split"])
# Workers, and so parts per file: fixed, so that part boundaries don't depend
# on the machine's CPUs.
_SPLIT_RUNNER_KWARGS = {"max_workers": 8}
_SPLIT_LOG = "Splitting"
_WHOLE_LOG = "so the file is imported whole"


def _run_logged(caplog, files: dict[str, str], config: dict, mode,
                **runner_kwargs) -> tuple[b.Result, list[str]]:
  """Runs the import; returns its result and the runner's log messages."""
  caplog.clear()
  with caplog.at_level(logging.INFO):
    result = b.run_dir(files, config, mode=mode, runner_kwargs=runner_kwargs)
  return result, [r.getMessage() for r in caplog.records]


def _assert_same_output(actual: b.Result, expected: b.Result) -> None:
  """Every record (with its copies), the handshake, report and error match."""
  assert sorted(map(str, actual.output["records"])) == sorted(
      map(str, expected.output["records"]))
  for aspect in ("handshake", "ingestion_report", "error"):
    assert actual.output[aspect] == expected.output[aspect], aspect


def _rows(n: int, **kwargs) -> list[list[str]]:
  return [b.obs_row(f"country/C{i}", value=str(i), **kwargs) for i in range(n)]


# given: (CSV text, whether every part parses, so the file isn't imported
# whole).
SPLIT_FILES = [
    Case("rows", given=(b.csv_file(*_rows(20)), True), expect=None),
    Case("CRLF line endings",
         given=(b.csv_file(*_rows(20), line_end="\r\n"), True),
         expect=None),
    Case("byte order mark",
         given=(b.csv_file(*_rows(20), bom=True), True),
         expect=None),
    Case("no newline after the last row",
         given=(b.csv_file(*_rows(20)).removesuffix("\n"), True),
         expect=None),
    Case("rows longer than parts",
         given=(b.csv_file(*_rows(3, variable="V" * 200)), True),
         expect=None),
    Case("a row with an extra field, which is ignored",
         given=(b.csv_file(*_rows(5),
                           b.obs_row(date="2021") + ["x"],
                           *_rows(5, date="2022")), True),
         expect=None),
    Case("a row with a missing field, which is skipped",
         given=(b.csv_file(*_rows(5), ["country/X", "Count_Person", "2021"],
                           *_rows(5, date="2022")), True),
         expect=None),
    Case("blank lines",
         given=(b.csv_file(*_rows(5)) + "\n\n" +
                "\n".join(",".join(r) for r in _rows(5, date="2022")) + "\n",
                True),
         expect=None),
    Case("invalid values in several parts",
         given=(b.csv_file(*_rows(5), b.obs_row(value="abc"),
                           *_rows(5, date="2021"),
                           b.obs_row(value="def")), False),
         expect=None),
    Case("a long field with newlines, which part boundaries fall into",
         given=(b.csv_file(*_rows(3, date="2021"),
                           b.obs_row("country/FRA", value="2") + ["l\n" * 50],
                           *_rows(3),
                           header=_OBS_HEADER), False),
         expect=None),
]


@_SPLIT_MODES
@cases(*SPLIT_FILES)
def test_a_split_file_gives_the_output_of_the_whole_file(
    caplog, given, expect, mode):
  """Records, report, handshake and error, as without splitting."""
  del expect  # The run without splitting is the expectation.
  csv_text, parts_parse = given
  config = b.obs_config(b.OBS_MAPPINGS, ignoreColumns=["notes"])
  files = {"provenance.mcf": b.PROVENANCE_MCF, "case.csv": csv_text}
  whole, _ = _run_logged(caplog, files, config, "disk_input",
                         **_SPLIT_RUNNER_KWARGS)
  split, logs = _run_logged(caplog, files, config, mode, **_SPLIT_RUNNER_KWARGS)
  assert any(m.startswith(_SPLIT_LOG) for m in logs), logs
  assert any(_WHOLE_LOG in m for m in logs) != parts_parse, logs
  _assert_same_output(split, whole)


@contextlib.contextmanager
def _split_tiny_chunks():
  """Split, and write a shard per row, so a part writes before it fails."""
  with mock.patch("stats.csv_split.SPLIT_CSV_BYTES", 1), \
       mock.patch("stats.jsonld_stream_writer._CHUNK_SIZE", 1), \
       mock.patch.object(runlib.pd, "read_csv", runlib._read_csv_tiny_chunks):
    yield


@pytest.mark.parametrize("gcs_input", [False, True])
def test_what_the_parts_wrote_is_discarded_when_the_file_is_imported_whole(
    caplog, gcs_input):
  """A part fails after writing shards, and so is the file imported whole.

  The long field at the end of the file holds the last part boundaries, so the
  part before them writes its first rows, then fails inside the field. Each
  record is written once: by the whole file's import.
  """
  config = b.obs_config(b.OBS_MAPPINGS, ignoreColumns=["notes"])
  files = {
      "provenance.mcf":
          b.PROVENANCE_MCF,
      "case.csv":
          b.csv_file(*[row + [""] for row in _rows(12)],
                     b.obs_row("country/FRA") + ["l\n" * 400],
                     header=_OBS_HEADER),
  }
  mode = runlib.Mode("split_tiny_chunks",
                     gcs_input=gcs_input,
                     patches=_split_tiny_chunks)
  whole, _ = _run_logged(caplog, files, config, "disk_input")
  split, logs = _run_logged(caplog, files, config, mode, max_workers=4)
  assert any(_WHOLE_LOG in m and "EOF inside string" in m for m in logs), logs
  assert whole.error is None, whole.error
  _assert_same_output(split, whole)


def test_a_file_whose_part_shards_could_take_another_files_names_is_not_split(
    caplog):
  """case.csv's part 1 would write observation-case-p001-00000.jsonld."""
  files = {
      "provenance.mcf": b.PROVENANCE_MCF,
      "case.csv": b.csv_file(*_rows(3)),
      "case-p001.csv": b.csv_file(b.obs_row("country/Y")),
  }
  result, logs = _run_logged(caplog, files,
                             b.obs_config(b.OBS_MAPPINGS, pattern="case*.csv"),
                             "split", **_SPLIT_RUNNER_KWARGS)
  assert result.error is None, result.error
  assert len(result.observations) == 4
  assert not any(m.startswith(_SPLIT_LOG) for m in logs), logs


def test_an_entities_csv_is_not_split(caplog):
  result, logs = _run_logged(
      caplog, {
          "provenance.mcf": b.PROVENANCE_MCF,
          "case.csv": b.csv_file(*_ENTITY_ROWS, header=_ENTITY_HEADER)
      }, b.entities_config(mappings=b.entity_mappings("bio")), "split",
      **_SPLIT_RUNNER_KWARGS)
  assert result.error is None, result.error
  assert not any(m.startswith(_SPLIT_LOG) for m in logs), logs
