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
  result = b.run(_PATTERN_FILES, {
      "includeInputSubdirs": True,
      **b.obs_config(b.OBS_MAPPINGS, pattern=given),
  })
  b.assert_records(result.observations,
                   [b.obs(about, value=val) for about, val in expect])


def test_files_in_subdirectories_are_skipped_by_default():
  result = b.run({
      "case.csv": _USA,
      "nested/case_deeper.csv": _FRA
  }, b.obs_config(b.OBS_MAPPINGS))
  b.assert_records(result.observations, [b.obs("country/USA", value=1)])


def test_include_input_subdirs_picks_up_nested_files():
  result = b.run({
      "case.csv": _USA,
      "nested/case_deeper.csv": _FRA
  }, {
      "includeInputSubdirs": True,
      **b.obs_config(b.OBS_MAPPINGS, pattern="**/case*.csv")
  })
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])


def test_first_matching_input_files_entry_wins():
  """
  countries_b.csv matches both CSV entries; the first one's mappings apply.

  other.txt matches no entry and is ignored.
  """
  place_mappings = {**b.OBS_MAPPINGS, "dcid:observationAbout": "place"}
  place_header = ["place", "variable", "date", "value"]
  config = b.obs_config(place_mappings, pattern="countries_*.csv")
  config["inputFiles"].append(
      b.obs_config(b.OBS_MAPPINGS, pattern="*.csv")["inputFiles"][-1])
  result = b.run(
      {
          "countries_a.csv":
              b.csv_file(b.obs_row("country/USA", value="1"),
                         header=place_header),
          "countries_b.csv":
              b.csv_file(b.obs_row("country/FRA", value="2"),
                         header=place_header),
          "other.txt":
              "not,a,csv\n",
      }, config)
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])


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


# --- StatVar groups -----------------------------------------------------------
#
# groupStatVarsByProperty asks the ingestion workflow to generate StatVarGroups
# (generateStatVarGroups in the handshake) and adds the custom root group they
# hang under. These tests only look at StatVarGroup records.

_CUSTOM_ROOT_GROUP = b.record("c/g/Root",
                              "StatVarGroup",
                              name="Custom Variables",
                              specializationOf=b.ref("dc/g/Root"))

GROUP_STAT_VARS = [
    Case("true", given=True, expect=True),
    Case("string true", given="true", expect=True),
    Case("string yes", given="yes", expect=True),
    Case("false", given=False, expect=False),
]


@cases(*GROUP_STAT_VARS)
def test_group_stat_vars_by_property(given, expect):
  result = b.run_csv(_USA, {
      **b.obs_config(b.OBS_MAPPINGS), "groupStatVarsByProperty": given
  })
  b.assert_records(result.of_type("StatVarGroup"),
                   [_CUSTOM_ROOT_GROUP] if expect else [])
  assert result.output["handshake"]["generateStatVarGroups"] is expect


def test_stat_var_groups_are_not_requested_by_default():
  result = b.run_csv(_USA)
  b.assert_records(result.of_type("StatVarGroup"), [])
  assert result.output["handshake"]["generateStatVarGroups"] is False


# --- Invalid configs ----------------------------------------------------------

# Handshake error types of a run that fails on its config.json.
_INVALID = ("INVALID_CONFIGURATION",)

_ENTITIES_BLOCK_REMOVED = (
    "The top-level 'entities' block in config.json is no longer supported.")
_ENTITIES_BLOCK_MCF_HINT = (
    "Define each entity type that is not already in the Data Commons schema"
    " as a Class node in an MCF file instead, e.g.:\n"
    "  Node: dcid:Author\n"
    "  typeOf: dcs:Class")

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
    Case("inputFiles entry without a pattern",
         given={"inputFiles": [{
             "provenance": f"dcid:{b.PROVENANCE}"
         }]},
         expect=b.Fails(
             "ValueError", "Invalid entry in 'inputFiles': must specify"
             " 'pattern' or 'filename'.", _INVALID)),
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
    Case("columnsToResolve on observations entry is rejected",
         given=b.obs_config(b.OBS_MAPPINGS, columnsToResolve=["entity"]),
         expect=b.Fails(
             "ValueError",
             "'columnsToResolve' is only supported for 'importType':"
             " 'entities', but entry 'case*.csv' has importType"
             " 'observations'.", _INVALID)),
    Case("referenceColumns on observations entry is rejected",
         given=b.obs_config(b.OBS_MAPPINGS, referenceColumns=["entity"]),
         expect=b.Fails(
             "ValueError",
             "'referenceColumns' is only supported for 'importType':"
             " 'entities', but entry 'case*.csv' has importType"
             " 'observations'.", _INVALID)),
    Case("removed entities block is rejected with a pointer to MCF",
         given={
             **b.entities_config(mappings=b.AUTHOR_MAPPINGS),
             "entities": {
                 "Author": {
                     "name": "Author",
                     "description": "A person who wrote an article",
                 },
                 "Book": {},
             },
         },
         expect=b.Fails(
             "ValueError", "The top-level 'entities' block in config.json is"
             " no longer supported. Found entries for: ['Author', 'Book']. " +
             _ENTITIES_BLOCK_MCF_HINT, _INVALID)),
    Case("empty entities block is rejected too",
         given={
             **b.obs_config(b.OBS_MAPPINGS),
             "entities": {},
         },
         expect=b.Fails("ValueError", _ENTITIES_BLOCK_REMOVED, _INVALID)),
    Case("unsupported format",
         given=b.obs_config(b.OBS_MAPPINGS, format="variablePerColumn"),
         expect=b.Fails(
             "ValueError", "Unsupported format 'variablePerColumn' for"
             " 'case*.csv'. The only supported format is 'variablePerRow'",
             _INVALID)),
    Case("unsupported importType",
         given=b.obs_config(b.OBS_MAPPINGS, importType="Observations"),
         expect=b.Fails("ValueError", "CSV Header Validation Failed!")),
    Case("events importType is not supported",
         given=b.obs_config(b.OBS_MAPPINGS, importType="events"),
         expect=b.Fails("ValueError", "Unsupported import type: events")),
    Case("double wildcard in filename pattern",
         given=b.obs_config(b.OBS_MAPPINGS, pattern="**.csv"),
         expect=b.Fails(
             "ValueError",
             "Adjacent double wildcards in the name portion of a pattern are"
             " not supported")),
]


@cases(*INVALID_CONFIGS)
def test_invalid_config(given, expect):
  result = b.run_csv(_USA, given)
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_invalid_config_is_reported_like_any_failed_run():
  """report.json, the ingestion report and the handshake all record it."""
  files = {
      **b.import_subdir("oecd", _USA),
      **b.import_subdir("ilo", _FRA),
      "ilo/config.json":
          json.dumps({
              **b.obs_config(b.OBS_MAPPINGS), "unknownTopLevelKey": True
          }),
  }
  result = b.run_dir(files, runner_kwargs={"import_names": ["oecd", "ilo"]})
  message = result.error["message"]
  assert message.startswith("<input>/ilo/config.json: Unknown key(s) in"
                            " config.json: ['unknownTopLevelKey']")
  assert result.output["handshake"] == {
      "status":
          "FAILURE",
      "errors": [{
          "file": "",
          "errorType": "INVALID_CONFIGURATION",
          "problemColumns": [],
          "errorMessage": message,
      }],
  }
  assert result.output["report"] == {
      "status": "FAILURE",
      "error": message,
      "importFiles": {},
  }
  assert result.output["ingestion_report"] == {
      "folders": [{
          "folderName": "oecd"
      }, {
          "folderName": "ilo"
      }],
      "run": {
          "folderNames": ["oecd", "ilo"],
          "mode": "INGEST",
          "outputPath": "<output>/preprocessor_reports/testsuite-run",
          "status": "FAILURE",
          "workflowExecutionId": "testsuite-run",
      },
  }


def test_entities_block_in_an_import_subdirectory_config_is_rejected():
  """Multi-import runs validate each subdirectory's config.json the same way."""
  files = {
      **b.import_subdir("oecd", _USA),
      **b.import_subdir("ilo", _FRA),
      "ilo/config.json":
          json.dumps({
              **b.obs_config(b.OBS_MAPPINGS), "entities": {
                  "Author": {
                      "name": "Author"
                  }
              }
          }),
  }
  result = b.run_dir(files, runner_kwargs={"import_names": ["oecd", "ilo"]})
  b.assert_fails(result,
                 "ValueError",
                 f"<input>/ilo/config.json: {_ENTITIES_BLOCK_REMOVED}",
                 handshake_error_types=_INVALID)


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
                 "File not found: <input>/config.json")


def test_empty_input_directory_fails():
  result = b.run_dir({})
  b.assert_fails(result, "FileNotFoundError",
                 "File not found: <input>/config.json")


def test_config_file_without_data_download_url_fails():
  result = b.run_dir({}, {}, config_file="config.json")
  b.assert_fails(result,
                 "ValueError",
                 "Data Download URLs not found in config.",
                 handshake_error_types=_INVALID)


def test_missing_config_file_fails():
  result = b.run_dir({}, config_file="nonexistent.json")
  b.assert_fails(result, "FileNotFoundError", "<input>/nonexistent.json")
