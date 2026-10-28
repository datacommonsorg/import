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

import dataclasses

from testsuite.lib.builders import assert_fails
from testsuite.lib.builders import assert_records
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import csv_file
from testsuite.lib.builders import MCF_CONFIG
from testsuite.lib.builders import obs
from testsuite.lib.builders import obs_config
from testsuite.lib.builders import OBS_MAPPINGS
from testsuite.lib.builders import obs_row
from testsuite.lib.builders import PROVENANCE
from testsuite.lib.builders import record
from testsuite.lib.builders import ref
from testsuite.lib.builders import run
from testsuite.lib.builders import run_csv

_USA = csv_file(obs_row("country/USA", value="1"))
_FRA = csv_file(obs_row("country/FRA", value="2"))

# --- Selecting input files ----------------------------------------------------


def test_files_in_subdirectories_are_skipped_by_default():
  result = run({"case.csv": _USA, "nested/case_deeper.csv": _FRA}, obs_config())
  assert_records(result.observations, [obs("country/USA", value=1)])


def test_include_input_subdirs_picks_up_nested_files():
  result = run({
      "case.csv": _USA,
      "nested/case_deeper.csv": _FRA
  }, {
      "includeInputSubdirs": True,
      **obs_config(pattern="**/case*.csv")
  })
  assert_records(result.observations,
                 [obs("country/USA", value=1),
                  obs("country/FRA", value=2)])


def test_first_matching_input_files_entry_wins():
  """countries_b.csv matches both CSV entries; the first one's mappings apply.

  other.txt matches no entry and is ignored.
  """
  place_mappings = {**OBS_MAPPINGS, "dcid:observationAbout": "place"}
  place_header = ["place", "variable", "date", "value"]
  config = obs_config(place_mappings, pattern="countries_*.csv")
  config["inputFiles"].append(obs_config(pattern="*.csv")["inputFiles"][-1])
  result = run(
      {
          "countries_a.csv":
              csv_file(obs_row("country/USA", value="1"), header=place_header),
          "countries_b.csv":
              csv_file(obs_row("country/FRA", value="2"), header=place_header),
          "other.txt":
              "not,a,csv\n",
      }, config)
  assert_records(result.observations,
                 [obs("country/USA", value=1),
                  obs("country/FRA", value=2)])


def test_empty_config_imports_only_the_default_provenance():
  """With an input directory, an empty config.json is accepted.

  No file matches, so only the default provenance and source are written.
  """
  result = run({}, {})
  assert_records(result.added, [
      record("c/p/default",
             "Provenance",
             name="Custom Import",
             source=ref("c/s/default"),
             url="custom-import"),
      record("c/s/default", "Source", name="Custom Data Commons"),
  ])


def test_provenance_linked_to_an_undefined_source_is_accepted():
  provenance = ("Node: dcid:TestProvenance\ntypeOf: dcs:Provenance\n"
                'name: "Test Provenance"\nsource: dcid:ElsewhereSource\n')
  result = run_csv(_USA, shared_files={"provenance.mcf": provenance})
  assert result.error is None, result.error
  assert_records(result.observations, [obs()])


# --- StatVar groups -----------------------------------------------------------
#
# groupStatVarsByProperty asks the ingestion workflow to generate StatVarGroups
# (generateStatVarGroups in the handshake) and adds the custom root group they
# hang under. Only the production mode writes a success handshake.

_CUSTOM_ROOT_GROUP = record("c/g/Root",
                            "StatVarGroup",
                            name="Custom Variables",
                            specializationOf=ref("dc/g/Root"))

GROUP_STAT_VARS = [
    Case("true", given=True, expect=True),
    Case("string true", given="true", expect=True),
    Case("string yes", given="yes", expect=True),
    Case("false", given=False, expect=False),
]


@cases(*GROUP_STAT_VARS)
def test_group_stat_vars_by_property(given, expect):
  result = run_csv(_USA, {
      **obs_config(), "groupStatVarsByProperty": given
  },
                   mode="production")
  assert_records(result.nodes, [_CUSTOM_ROOT_GROUP] if expect else [])
  assert result.output["handshake"]["generateStatVarGroups"] is expect


def test_stat_var_groups_are_not_requested_by_default():
  result = run_csv(_USA, mode="production")
  assert_records(result.nodes, [])
  assert result.output["handshake"]["generateStatVarGroups"] is False


# --- Invalid configs ----------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Fails:
  """How a run is expected to fail (see assert_fails)."""
  error_type: str
  message: str
  # Empty when the error is raised while Runner is being constructed, before
  # run(): then no FAILURE handshake is written and the ingestion workflow
  # gets no structured error.
  handshake: tuple[str, ...] = ("GENERIC_ERROR",)


INVALID_CONFIGS = [
    Case("inputFiles given as a map",
         given={"inputFiles": {
             "*.csv": {
                 "provenance": f"dcid:{PROVENANCE}"
             }
         }},
         expect=Fails(
             "ValueError", "'inputFiles' must be a list of objects, each with a"
             " 'pattern' and a 'provenance'. Got: dict", ())),
    Case("inputFiles entry without a pattern",
         given={"inputFiles": [{
             "provenance": f"dcid:{PROVENANCE}"
         }]},
         expect=Fails(
             "ValueError", "Invalid entry in 'inputFiles': must specify"
             " 'pattern' or 'filename'.", ())),
    Case("unsupported format",
         given=obs_config(format="variablePerColumn"),
         expect=Fails(
             "ValueError", "Unsupported format 'variablePerColumn' for"
             " 'case*.csv'. The only supported format is 'variablePerRow'",
             ())),
    Case("unsupported importType",
         given=obs_config(importType="Observations"),
         expect=Fails("ValueError", "CSV Header Validation Failed!")),
    Case("inputFiles entry without a provenance",
         given={
             "inputFiles": [
                 *MCF_CONFIG["inputFiles"], {
                     "pattern": "case*.csv",
                     "columnMappings": OBS_MAPPINGS
                 }
             ]
         },
         expect=Fails(
             "ValueError", "Could not determine import name: missing"
             " 'provenance' configuration for file 'case.csv'.")),
    Case("provenance without a namespace prefix",
         given=obs_config(provenance=PROVENANCE),
         expect=Fails(
             "ValueError", "The 'provenance' property must be a valid DCID or"
             f" URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL)."
             f" Found invalid provenance: '{PROVENANCE}'")),
    # The message talks about missing *sources*, and the error type is
    # GENERIC_ERROR, although the validator has a MISSING_PROVENANCE type.
    Case("provenance that no MCF defines",
         given=obs_config(provenance="dcid:UndefinedProvenance"),
         expect=Fails(
             "ValueError", "Metadata Validation Failed: Linked sources are"
             " missing for defined provenances:")),
]


@cases(*INVALID_CONFIGS)
def test_invalid_config(given, expect):
  result = run_csv(_USA, given)
  assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_missing_config_json_fails():
  result = run({"case.csv": _USA}, None)
  assert_fails(result,
               "FileNotFoundError",
               "File not found: <input>/config.json",
               handshake_error_types=())


def test_provenance_without_a_source_fails():
  provenance = ("Node: dcid:TestProvenance\ntypeOf: dcs:Provenance\n"
                'name: "Test Provenance"\n')
  result = run_csv(_USA, shared_files={"provenance.mcf": provenance})
  assert_fails(
      result, "ValueError",
      "Metadata Validation Failed: Linked sources are missing for defined"
      " provenances:")


def test_provenance_declared_only_in_the_config_sources_block_fails():
  """A provenance from the config's sources block does not count for MCF."""
  config = {
      "sources": {
          "Config Source": {
              "url": "https://config-source.example.org",
              "provenances": {
                  "Config Provenance": "https://config-source.example.org/p"
              },
          }
      },
      "inputFiles": [obs_config()["inputFiles"][-1]],
  }
  result = run_csv(_USA, config, shared_files={})
  assert_fails(
      result, "ValueError",
      "Metadata Validation Failed: Linked sources are missing for defined"
      " provenances:")
