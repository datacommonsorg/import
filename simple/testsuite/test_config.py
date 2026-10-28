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

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

_USA = b.csv_file(b.obs_row("country/USA", value="1"))
_FRA = b.csv_file(b.obs_row("country/FRA", value="2"))

# --- Selecting input files ----------------------------------------------------


def test_files_in_subdirectories_are_skipped_by_default():
  result = b.run({
      "case.csv": _USA,
      "nested/case_deeper.csv": _FRA
  }, b.obs_config())
  b.assert_records(result.observations, [b.obs("country/USA", value=1)])


def test_include_input_subdirs_picks_up_nested_files():
  result = b.run({
      "case.csv": _USA,
      "nested/case_deeper.csv": _FRA
  }, {
      "includeInputSubdirs": True,
      **b.obs_config(pattern="**/case*.csv")
  })
  b.assert_records(
      result.observations,
      [b.obs("country/USA", value=1),
       b.obs("country/FRA", value=2)])


def test_first_matching_input_files_entry_wins():
  """countries_b.csv matches both CSV entries; the first one's mappings apply.

  other.txt matches no entry and is ignored.
  """
  place_mappings = {**b.OBS_MAPPINGS, "dcid:observationAbout": "place"}
  place_header = ["place", "variable", "date", "value"]
  config = b.obs_config(place_mappings, pattern="countries_*.csv")
  config["inputFiles"].append(b.obs_config(pattern="*.csv")["inputFiles"][-1])
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


def test_empty_config_imports_only_the_default_provenance():
  """With an input directory, an empty config.json is accepted.

  No file matches, so only the default provenance and source are written.
  """
  result = b.run({}, {})
  b.assert_records(result.added, [
      b.record("c/p/default",
               "Provenance",
               name="Custom Import",
               source=b.ref("c/s/default"),
               url="custom-import"),
      b.record("c/s/default", "Source", name="Custom Data Commons"),
  ])


# --- StatVar groups -----------------------------------------------------------
#
# groupStatVarsByProperty asks the ingestion workflow to generate StatVarGroups
# (generateStatVarGroups in the handshake) and adds the custom root group they
# hang under. Only the production mode writes a success handshake. These tests
# only look at StatVarGroup records: in production mode a CSV worker can write
# an extra copy of the provenance (a known bug, see KNOWN_ISSUES in
# test_e2e.py), which is not what they check.

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
      **b.obs_config(), "groupStatVarsByProperty": given
  },
                     mode="production")
  b.assert_records(result.of_type("StatVarGroup"),
                   [_CUSTOM_ROOT_GROUP] if expect else [])
  assert result.output["handshake"]["generateStatVarGroups"] is expect


def test_stat_var_groups_are_not_requested_by_default():
  result = b.run_csv(_USA, mode="production")
  b.assert_records(result.of_type("StatVarGroup"), [])
  assert result.output["handshake"]["generateStatVarGroups"] is False


# --- Invalid configs ----------------------------------------------------------

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
            " 'pattern' and a 'provenance'. Got: dict", ())),
    Case("inputFiles entry without a pattern",
         given={"inputFiles": [{
             "provenance": f"dcid:{b.PROVENANCE}"
         }]},
         expect=b.Fails(
             "ValueError", "Invalid entry in 'inputFiles': must specify"
             " 'pattern' or 'filename'.", ())),
    Case("unsupported format",
         given=b.obs_config(format="variablePerColumn"),
         expect=b.Fails(
             "ValueError", "Unsupported format 'variablePerColumn' for"
             " 'case*.csv'. The only supported format is 'variablePerRow'",
             ())),
    Case("unsupported importType",
         given=b.obs_config(importType="Observations"),
         expect=b.Fails("ValueError", "CSV Header Validation Failed!")),
]


@cases(*INVALID_CONFIGS)
def test_invalid_config(given, expect):
  result = b.run_csv(_USA, given)
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_missing_config_json_fails():
  result = b.run({"case.csv": _USA}, None)
  b.assert_fails(result,
                 "FileNotFoundError",
                 "File not found: <input>/config.json",
                 handshake_error_types=())
