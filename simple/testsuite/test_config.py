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
"""Config: how config.json selects input files, and how bad configs fail."""

import json
import logging

import pytest
from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

_USA = b.csv_file(b.obs_row("country/USA", value="1"))
_FRA = b.csv_file(b.obs_row("country/FRA", value="2"))
_DEU = b.csv_file(b.obs_row("country/DEU", value="3"))
_GBR = b.csv_file(b.obs_row("country/GBR", value="4"))

# --- Selecting input files ----------------------------------------------------

_PATTERN_FILES = {
    "foo.csv": _USA,
    "bar.csv": _FRA,
    "dirA/foo.csv": _DEU,
    "dirB/sub/foo.csv": _GBR,
}

PATTERNS = [
    Case("exact filename matches at any depth",
         given="foo.csv",
         expect=[("country/USA", 1), ("country/DEU", 3), ("country/GBR", 4)]),
    Case("filename wildcard matches at any depth",
         given="f*.csv",
         expect=[("country/USA", 1), ("country/DEU", 3), ("country/GBR", 4)]),
    Case("root-anchored filename matches only root",
         given="/foo.csv",
         expect=[("country/USA", 1)]),
    Case("root-anchored subdirectory path",
         given="/dirA/foo.csv",
         expect=[("country/DEU", 3)]),
    Case("root-anchored single directory wildcard",
         given="/*/foo.csv",
         expect=[("country/DEU", 3)]),
    Case("root-anchored two directory wildcards",
         given="/*/*/foo.csv",
         expect=[("country/GBR", 4)]),
    Case("relative directory suffix matches at any depth",
         given="sub/foo.csv",
         expect=[("country/GBR", 4)]),
    Case("relative single directory wildcard matches nested files",
         given="*/foo.csv",
         expect=[("country/DEU", 3), ("country/GBR", 4)]),
    Case("double wildcard matches zero or more directories",
         given="**/foo.csv",
         expect=[("country/USA", 1), ("country/DEU", 3), ("country/GBR", 4)]),
    Case("root-anchored double wildcard",
         given="/**/foo.csv",
         expect=[("country/USA", 1), ("country/DEU", 3), ("country/GBR", 4)]),
    Case("brace alternation over filenames",
         given="/{foo.csv,bar.csv}",
         expect=[("country/USA", 1), ("country/FRA", 2)]),
    Case("brace alternation over directories",
         given="{dirA,dirB/sub}/foo.csv",
         expect=[("country/DEU", 3), ("country/GBR", 4)]),
    Case("brace alternation mixing root and nested patterns",
         given="{/bar.csv,**/sub/foo.csv}",
         expect=[("country/FRA", 2), ("country/GBR", 4)]),
]


@cases(*PATTERNS)
def test_input_files_pattern_matching(given, expect):
  result = b.run(_PATTERN_FILES, b.obs_config(b.OBS_MAPPINGS, pattern=given))
  b.assert_records(result.observations,
                   [b.obs(about, value=val) for about, val in expect])


_NESTED_FILES = {
    "case.csv": _USA,
    "nested/case_deeper.csv": _FRA,
    "archives/case_old.csv": _DEU,
}


def test_files_in_subdirectories_are_imported_except_archives():
  result = b.run(_NESTED_FILES, b.obs_config(b.OBS_MAPPINGS))
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])


# includeInputSubdirs is deprecated: any value is accepted, with a warning
# naming its config.json, and subdirectories are searched regardless.

_INCLUDE_INPUT_SUBDIRS_WARNING = (
    ": 'includeInputSubdirs' is deprecated and ignored")


def _include_input_subdirs_warnings(caplog) -> list[str]:
  """The config.json paths, relative to the input dir, the warnings name."""
  paths = []
  for record in caplog.records:
    path, found, _ = record.getMessage().partition(
        _INCLUDE_INPUT_SUBDIRS_WARNING)
    if found:
      # Runner reads a copy of the input dir at <work dir>/input.
      paths.append(path.split("/input/")[-1])
  return paths


@pytest.mark.parametrize("given", [True, "false"])
def test_include_input_subdirs_is_ignored_with_a_warning(given, caplog):
  with caplog.at_level(logging.WARNING):
    result = b.run(_NESTED_FILES, {
        "includeInputSubdirs": given,
        **b.obs_config(b.OBS_MAPPINGS)
    })
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])
  assert _include_input_subdirs_warnings(caplog) == [f"{b.IMPORT}/config.json"]


def test_include_input_subdirs_warning_names_its_import_config(caplog):
  files = {
      **b.import_subdir("oecd", _USA),
      **b.import_subdir("ilo", _FRA),
  }
  ilo_config = json.loads(files["ilo/config.json"])
  files["ilo/config.json"] = json.dumps({
      **ilo_config, "includeInputSubdirs": True
  })
  with caplog.at_level(logging.WARNING):
    result = b.run_dir(files, runner_kwargs={"import_names": ["oecd", "ilo"]})
  assert result.error is None, result.error
  assert _include_input_subdirs_warnings(caplog) == ["ilo/config.json"]


_PLACE_MAPPINGS = {**b.OBS_MAPPINGS, "dcid:observationAbout": "place"}

# countries.csv has a "place" column, so only an entry with _PLACE_MAPPINGS can
# import it; other.txt matches no entry and is ignored.
ENTRY_FOR_A_FILE = [
    Case("the first matching glob wins",
         given=[(_PLACE_MAPPINGS, "countries*.csv"), (b.OBS_MAPPINGS, "*.csv")],
         expect=[("country/USA", 1)]),
    Case("an exact file name wins over an earlier glob",
         given=[(b.OBS_MAPPINGS, "*.csv"), (_PLACE_MAPPINGS, "countries.csv")],
         expect=[("country/USA", 1)]),
]


@cases(*ENTRY_FOR_A_FILE)
def test_input_files_entry_for_a_file(given, expect):
  config = {
      "inputFiles": [
          *b.MCF_CONFIG["inputFiles"],
          *(b.obs_config(mappings, pattern=pattern)["inputFiles"][-1]
            for mappings, pattern in given),
      ]
  }
  result = b.run(
      {
          "countries.csv":
              b.csv_file(b.obs_row("country/USA", value="1"),
                         header=["place", "variable", "date", "value"]),
          "other.txt":
              "not,a,csv\n",
      }, config)
  b.assert_records(result.observations,
                   [b.obs(about, value=val) for about, val in expect])


def test_empty_config_writes_no_provenance_or_source():
  """
  With an input directory, an empty config.json is accepted.

  No file matches, so no metadata is imported, and there is no built-in
  provenance or source to write instead.
  """
  result = b.run({}, {})
  assert result.error is None, result.error
  b.assert_records(result.added, [])
  b.assert_records([
      r for r in result.records
      if r.get("@type") in ("dcid:Provenance", "dcid:Source")
  ], [])


def test_single_import_name_runs_only_that_subdirectory():
  result = b.run_dir(
      {
          **b.import_subdir("oecd", _USA),
          **b.import_subdir("ilo", _FRA),
      },
      runner_kwargs={"import_names": ["oecd"]})
  b.assert_records(result.observations, [b.obs("country/USA", value=1)])


def test_selected_import_names_run_only_those_subdirectories():
  result = b.run_dir(
      {
          **b.import_subdir("oecd", _USA),
          **b.import_subdir("ilo", _FRA),
          **b.import_subdir("ignored", _DEU),
      },
      runner_kwargs={"import_names": ["oecd", "ilo"]})
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])
  assert result.output["ingestion_report"]["run"]["folderNames"] == [
      "oecd", "ilo"
  ]


# Beta comes first so that processing order would favour it.
_TWO_IMPORTS = {
    **b.import_subdir("beta", provenance="BetaProvenance", source="BetaSource"),
    **b.import_subdir("alpha",
                      provenance="AlphaProvenance",
                      source="AlphaSource"),
}


def test_global_nodes_go_to_the_alphabetically_first_import_folder():
  result = b.run_dir(_TWO_IMPORTS,
                     runner_kwargs={"import_names": ["ALL_IMPORTS"]})
  assert result.error is None, result.error
  assert result.folders_of("dcid:dc/g/Root") == ["AlphaProvenance"]


# --- Invalid configs ----------------------------------------------------------

# Handshake error types of a run that fails on its config.json.
_INVALID = ("INVALID_CONFIGURATION",)

INVALID_CONFIGS = [
    Case(
        "inputFiles given as a map",
        given={"inputFiles": {
            "*.csv": {
                "provenance": f"dcid:{b.PROVENANCE}"
            }
        }},
        expect=b.Fails(
            "ValueError", "'inputFiles' must be a list of objects, each with a"
            " 'pattern' and a 'provenance'. Got: dict", _INVALID)),
    Case("inputFiles entry that is not an object",
         given={"inputFiles": ["case*.csv"]},
         expect=b.Fails(
             "ValueError", "Invalid entry in 'inputFiles': must be a JSON"
             " object. Got: case*.csv", _INVALID)),
    Case("inputFiles entry without a pattern",
         given={"inputFiles": [{
             "provenance": f"dcid:{b.PROVENANCE}"
         }]},
         expect=b.Fails(
             "ValueError", "Invalid entry in 'inputFiles': must specify"
             " a 'pattern'.", _INVALID)),
    Case("unknown top-level key",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "unknownTopLevelKey": True,
         },
         expect=b.Fails(
             "ValueError",
             "Unknown key(s) in config.json: ['unknownTopLevelKey']",
             _INVALID)),
    Case("legacy sources key is rejected",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "sources": {},
         },
         expect=b.Fails("ValueError",
                        "Unknown key(s) in config.json: ['sources']",
                        _INVALID)),
    Case("legacy variables key is rejected",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "variables": {},
         },
         expect=b.Fails("ValueError",
                        "Unknown key(s) in config.json: ['variables']",
                        _INVALID)),
    Case("legacy events key is rejected",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "events": {},
         },
         expect=b.Fails("ValueError",
                        "Unknown key(s) in config.json: ['events']", _INVALID)),
    Case("legacy customIdNamespace key is rejected",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "customIdNamespace": "custom",
         },
         expect=b.Fails("ValueError",
                        "Unknown key(s) in config.json: ['customIdNamespace']",
                        _INVALID)),
    Case("unknown inputFiles key",
         given=b.obs_config(b.OBS_MAPPINGS, unknownFileKey="val"),
         expect=b.Fails(
             "ValueError",
             "Unknown key(s) in 'inputFiles' entry for 'case*.csv':"
             " ['unknownFileKey']", _INVALID)),
    Case("non-string columnMappings value",
         given=b.obs_config({
             **b.OBS_MAPPINGS, "dcid:value": 1
         }),
         expect=b.Fails(
             "ValueError", "Invalid 'columnMappings.dcid:value' in 'inputFiles'"
             " entry for 'case*.csv': Input should be a valid string",
             _INVALID)),
    Case("legacy importProxyEntities key is rejected",
         given=b.obs_config(b.OBS_MAPPINGS, importProxyEntities=True),
         expect=b.Fails(
             "ValueError",
             "Unknown key(s) in 'inputFiles' entry for 'case*.csv':"
             " ['importProxyEntities']", _INVALID)),
    Case("legacy entityType key is rejected",
         given=b.obs_config(b.OBS_MAPPINGS, entityType="Country"),
         expect=b.Fails(
             "ValueError",
             "Unknown key(s) in 'inputFiles' entry for 'case*.csv':"
             " ['entityType']", _INVALID)),
    Case("removed reference column keys are rejected with how to migrate",
         given=b.entities_config(mappings=b.AUTHOR_MAPPINGS,
                                 columnsToResolve=["author_country"],
                                 referenceColumns=["author_country"]),
         expect=b.Fails(
             "ValueError",
             "Removed key(s) in 'inputFiles' entry for 'case*.csv':"
             " ['columnsToResolve', 'referenceColumns']. Write each"
             " reference as a cell value with a 'dcid:' prefix instead, e.g."
             " 'dcid:country/USA'.", _INVALID)),
    Case("unsupported importType",
         given=b.obs_config(b.OBS_MAPPINGS, importType="Observations"),
         expect=b.Fails("ValueError", "Unsupported import type: Observations",
                        _INVALID)),
    Case("events importType is not supported",
         given=b.obs_config(b.OBS_MAPPINGS, importType="events"),
         expect=b.Fails("ValueError", "Unsupported import type: events",
                        _INVALID)),
    Case("double wildcard in filename pattern",
         given=b.obs_config(b.OBS_MAPPINGS, pattern="**.csv"),
         expect=b.Fails(
             "ValueError",
             "Adjacent double wildcards in the name portion of a pattern are"
             " not supported", _INVALID)),
    Case("ignoreColumns that is not a list",
         given=b.obs_config(b.OBS_MAPPINGS, ignoreColumns="notes"),
         expect=b.Fails(
             "ValueError", "Invalid 'ignoreColumns' in 'inputFiles' entry for"
             " 'case*.csv': Input should be a valid list", _INVALID)),
    Case("columnMappings that is not an object",
         given=b.obs_config(list(b.OBS_MAPPINGS)),
         expect=b.Fails(
             "ValueError", "Invalid 'columnMappings' in 'inputFiles' entry for"
             " 'case*.csv': Input should be a valid dictionary", _INVALID)),
    Case("duplicate pattern",
         given={
             "inputFiles": [
                 *b.obs_config(b.OBS_MAPPINGS)["inputFiles"],
                 *b.obs_config(b.OBS_MAPPINGS)["inputFiles"][1:],
             ]
         },
         expect=b.Fails(
             "ValueError", "Duplicate pattern(s) in 'inputFiles':"
             " ['case*.csv']. Each pattern can only have one entry.",
             _INVALID)),
    Case(
        "removed rowEntityType and idColumn keys are rejected with how to"
        " migrate",
        given=b.entities_config(mappings=b.AUTHOR_MAPPINGS,
                                rowEntityType="Author",
                                idColumn="author_id"),
        expect=b.Fails(
            "ValueError",
            "Removed key(s) in 'inputFiles' entry for 'case*.csv':"
            " ['idColumn', 'rowEntityType']. Map a 'dcid:dcid' column in"
            " 'columnMappings' instead. Map a 'dcid:typeOf' column in"
            " 'columnMappings' instead.", _INVALID)),
]


@cases(*INVALID_CONFIGS)
def test_invalid_config(given, expect):
  result = b.run_csv(_USA, given)
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


# A column is either imported or ignored, for every kind of CSV.
MAPPED_AND_IGNORED = [
    Case("observations",
         given=(_USA,
                b.obs_config(b.OBS_MAPPINGS, ignoreColumns=["value", "date"])),
         expect="['date', 'value']"),
    Case("entities",
         given=(b.csv_file(["author1", "Author"], header=["author_id", "type"]),
                b.entities_config(mappings=b.entity_mappings(),
                                  ignoreColumns=["author_id"])),
         expect="['author_id']"),
]


@cases(*MAPPED_AND_IGNORED)
def test_column_both_mapped_and_ignored_fails(given, expect):
  csv_text, config = given
  result = b.run_csv(csv_text, config)
  b.assert_fails(result,
                 "ValueError",
                 "The CSV file 'case.csv' both maps and ignores column(s):"
                 f" {expect}. Remove each from 'columnMappings' or from"
                 " 'ignoreColumns'.",
                 handshake_error_types=_INVALID)


def test_every_config_problem_is_reported_at_once():
  """Unknown keys are grouped per object, then every other problem follows."""
  config = b.obs_config(b.OBS_MAPPINGS,
                        ignoreColumns="notes",
                        otherFileKey=True,
                        unknownFileKey=True)
  result = b.run_csv(_USA, {**config, "unknownTopLevelKey": True})
  b.assert_fails(
      result, "ValueError", "Found 3 problems in config.json:\n"
      "- Unknown key(s) in config.json: ['unknownTopLevelKey']. Allowed keys"
      " are: [", _INVALID)
  for problem in [
      "\n- Unknown key(s) in 'inputFiles' entry for 'case*.csv':"
      " ['otherFileKey', 'unknownFileKey']. Allowed keys are: [",
      "\n- Invalid 'ignoreColumns' in 'inputFiles' entry for 'case*.csv':"
      " Input should be a valid list",
  ]:
    assert problem in result.error["message"]


# A bad subdirectory config.json in a multi-import run is covered end to end by
# the e2e/config_validation_failure dataset.

INVALID_CONFIG_FILES = [
    Case("subdirectory config with malformed JSON",
         given={
             **b.import_subdir("oecd", _USA),
             "ilo/config.json": "{not json",
         },
         expect=b.Fails("JSONDecodeError",
                        "Expecting property name enclosed in double quotes",
                        _INVALID)),
    Case(
        "subdirectory config that is not a JSON object",
        given={
            **b.import_subdir("oecd", _USA),
            "ilo/config.json": "[]",
        },
        expect=b.Fails(
            "ValueError",
            "<input>/ilo/config.json: config.json must be a JSON object. Got: list",
            _INVALID)),
]


@cases(*INVALID_CONFIG_FILES)
def test_invalid_config_file(given, expect):
  result = b.run_dir(given, runner_kwargs={"import_names": ["ALL_IMPORTS"]})
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_csv_without_column_mappings_in_an_import_subdirectory_is_rejected():
  """Multi-import runs require columnMappings in each subdirectory's config."""
  files = {
      **b.import_subdir("oecd", _USA),
      **b.import_subdir("ilo", _FRA),
      "ilo/config.json":
          json.dumps(b.without_column_mappings(b.obs_config(b.OBS_MAPPINGS))),
  }
  result = b.run_dir(files, runner_kwargs={"import_names": ["oecd", "ilo"]})
  b.assert_fails(result,
                 "ValueError",
                 "Missing 'columnMappings' for CSV file 'ilo/case.csv' in"
                 " config.json. 'columnMappings' is required for every CSV"
                 " input file",
                 handshake_error_types=_INVALID)


def test_missing_config_json_fails():
  result = b.run({"case.csv": _USA}, None)
  b.assert_fails(result, "FileNotFoundError",
                 f"File not found: <input>/{b.IMPORT}/config.json")


def test_a_run_without_imports_fails():
  """Every import is a subdirectory with its own config.json; none is at the
  root of the input dir."""
  result = b.run_dir({"config.json": json.dumps(b.obs_config(b.OBS_MAPPINGS))})
  b.assert_fails(result,
                 "ValueError",
                 "No imports to run. Pass --imports with the import"
                 " subdirectories of the input directory to import",
                 handshake_error_types=_INVALID)
