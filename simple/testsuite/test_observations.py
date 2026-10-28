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
"""Observations: variable-per-row CSV in, exact observation records out.

Every test imports case*.csv under the shared test provenance with the
columns entity, variable, date, value (see obs_config() in lib/builders.py).
Observation ids are content hashes, so these tests compare records without
"@id"; the e2e expected.json files pin exact ids.
"""

from testsuite.lib.builders import assert_fails
from testsuite.lib.builders import assert_records
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import csv_file
from testsuite.lib.builders import known_bug
from testsuite.lib.builders import mcf_node
from testsuite.lib.builders import obs
from testsuite.lib.builders import obs_config
from testsuite.lib.builders import OBS_HEADER
from testsuite.lib.builders import OBS_MAPPINGS
from testsuite.lib.builders import obs_row
from testsuite.lib.builders import PROVENANCE_MCF
from testsuite.lib.builders import record
from testsuite.lib.builders import ref
from testsuite.lib.builders import run
from testsuite.lib.builders import run_csv


def test_basic_rows():
  result = run_csv(
      csv_file(obs_row("country/USA", "Count_Person", "2020", "331000000"),
               obs_row("country/FRA", "Median_Age_Person", "2020", "42.3")))
  assert_records(result.observations, [
      obs("country/USA", "Count_Person", 2020, 331000000),
      obs("country/FRA", "Median_Age_Person", 2020, 42.3),
  ])


def test_physical_column_names_are_mapped():
  result = run_csv(
      csv_file(["country/USA", "Count_Person", "2021", "332000000"],
               header=["place", "stat", "year", "amount"]),
      obs_config({
          "dcid:observationAbout": "place",
          "dcid:variableMeasured": "stat",
          "dcid:observationDate": "year",
          "dcid:value": "amount",
      }))
  assert_records(result.observations, [obs(date=2021, value=332000000)])


# --- Cells --------------------------------------------------------------------

DROPPED = None  # The row produces no observation.

VALUES = [
    Case("integer", given="7", expect=7),
    Case("negative decimal", given="-3.5", expect=-3.5),
    Case("leading zeros are dropped", given="007", expect=7),
    Case("decimal with trailing zero", given="2.0", expect=2.0),
    Case("exponent is a number",
         given="1e5",
         expect=100000,
         bug="1e5 is emitted as the JSON string \"1e5\""),
    Case("empty cell drops the row", given="", expect=DROPPED),
    Case("NA drops the row", given="NA", expect=DROPPED),
    Case("null drops the row", given="null", expect=DROPPED),
    Case("dash drops the row", given="-", expect=DROPPED),
]


@cases(*VALUES)
def test_value(given, expect):
  result = run_csv(csv_file(obs_row(value=given)))
  want = [] if expect is DROPPED else [obs(value=expect)]
  assert_records(result.observations, want)


ENTITIES = [
    Case("plain dcid", given="country/USA", expect="country/USA"),
    Case("dcid prefix is stripped",
         given="dcid:country/USA",
         expect="country/USA"),
    Case("dcs prefix is stripped",
         given="dcs:country/FRA",
         expect="country/FRA"),
    Case("custom namespace becomes dcid",
         given="undata:area/001",
         expect="area/001"),
    Case("surrounding whitespace is trimmed",
         given=" country/USA ",
         expect="country/USA"),
    Case("NA is Namibia, not a missing value",
         given="NA",
         expect="NA",
         bug="pandas reads the entity id NA as a missing value, so the"
         " observation loses observationAbout"),
]


@cases(*ENTITIES)
def test_entity(given, expect):
  result = run_csv(csv_file(obs_row(entity=given)))
  assert_records(result.observations, [obs(about=expect)])


VARIABLES = [
    Case("plain dcid", given="Count_Person", expect="Count_Person"),
    Case("dcid prefix is stripped",
         given="dcid:Count_Person",
         expect="Count_Person"),
    Case("dcs prefix is stripped",
         given="dcs:Count_Person",
         expect="Count_Person"),
    Case("surrounding whitespace is trimmed",
         given=" Count_Person ",
         expect="Count_Person"),
]


@cases(*VARIABLES)
def test_variable(given, expect):
  result = run_csv(csv_file(obs_row(variable=given)))
  assert_records(result.observations, [obs(variable=expect)])


DATES = [
    Case("year is a number", given="2020", expect=2020),
    Case("year-month is a string", given="2020-06", expect="2020-06"),
    Case("full date is a string", given="2020-06-15", expect="2020-06-15"),
]


@cases(*DATES)
def test_date(given, expect):
  result = run_csv(csv_file(obs_row(date=given)))
  assert_records(result.observations, [obs(date=expect)])


def test_non_numeric_value_fails():
  result = run_csv(csv_file(obs_row(value="lots")))
  assert_fails(
      result, "ValueError",
      "Invalid non-numeric value(s) found in observation values of file"
      " 'case.csv': ['lots']")


def test_non_numeric_values_are_listed_ten_at_most():
  result = run_csv(
      csv_file(
          *[obs_row(f"country/C{i}", value=f"a{i}") for i in range(1, 13)]))
  listed = ", ".join(f"'a{i}'" for i in range(1, 11))
  assert_fails(
      result, "ValueError",
      f"Invalid non-numeric value(s) found in observation values of file"
      f" 'case.csv': [{listed}]. If these represent missing values")


# --- Observation ids ----------------------------------------------------------


def _ids(result) -> list[str]:
  """The "@id" of every added observation."""
  return [
      r["@id"]
      for r in result.added
      if r.get("@type") == "dcid:StatVarObservation"
  ]


def test_rows_with_the_same_key_keep_both_values_under_one_id():
  """The collision is only logged; both records are written."""
  result = run_csv(csv_file(obs_row(value="1"), obs_row(value="2")))
  assert_records(result.observations, [obs(value=1), obs(value=2)])
  assert len(set(_ids(result))) == 1


def test_rows_with_different_keys_get_different_ids():
  result = run_csv(csv_file(obs_row(date="2020"), obs_row(date="2021")))
  assert len(set(_ids(result))) == 2


# --- Columns ------------------------------------------------------------------


def test_optional_observation_columns():
  result = run_csv(
      csv_file(
          [
              "country/USA", "Amount_Debt", "2020", "12.5", "USDollar", "100",
              "dcs:CensusACS", "P1Y"
          ],
          header=OBS_HEADER + ["unit", "scaling", "method", "period"],
      ),
      obs_config({
          **OBS_MAPPINGS,
          "dcid:unit": "unit",
          "dcid:scalingFactor": "scaling",
          "dcid:measurementMethod": "method",
          "dcid:observationPeriod": "period",
      }))
  assert_records(result.observations, [
      obs(variable="Amount_Debt",
          value=12.5,
          unit=ref("USDollar"),
          scalingFactor=100,
          measurementMethod=ref("CensusACS"),
          observationPeriod="P1Y")
  ])


# observationProperties in an inputFiles entry apply to every row of the file.
OBSERVATION_PROPERTIES = [
    Case("unit", given={"unit": "USDollar"}, expect={"unit": ref("USDollar")}),
    Case("scalingFactor",
         given={"scalingFactor": "100"},
         expect={"scalingFactor": 100}),
    Case("measurementMethod",
         given={"measurementMethod": "CensusACS"},
         expect={"measurementMethod": ref("CensusACS")}),
    Case("observationPeriod",
         given={"observationPeriod": "P1Y"},
         expect={"observationPeriod": "P1Y"}),
]


@cases(*OBSERVATION_PROPERTIES)
def test_observation_property_from_config(given, expect):
  result = run_csv(csv_file(obs_row()), obs_config(observationProperties=given))
  assert_records(result.observations, [obs(**expect)])


def test_custom_observation_property_from_config_is_a_dimension():
  """Custom properties are also listed in observationProperties."""
  result = run_csv(csv_file(obs_row()),
                   obs_config(observationProperties={"sex": "Female"}))
  assert_records(result.observations, [obs(dims={"sex": "Female"})])


def test_column_value_overrides_observation_property_from_config():
  """The config value only fills rows whose cell is empty."""
  result = run_csv(
      csv_file(obs_row(value="1") + ["Euro"],
               obs_row(value="2") + [""],
               header=OBS_HEADER + ["unit"]),
      obs_config({
          **OBS_MAPPINGS, "dcid:unit": "unit"
      },
                 observationProperties={"unit": "USDollar"}))
  assert_records(result.observations, [
      obs(value=1, unit=ref("Euro")),
      obs(value=2, unit=ref("USDollar")),
  ])


def test_custom_entity_dimension():
  result = run(
      {
          "case.csv":
              csv_file(
                  ["country/USA", "country/FRA", "Count_Trip", "2020", "10"],
                  header=["origin", "destination", "variable", "date", "value"
                         ]),
          "dims.mcf":
              mcf_node("dcid:destination", typeOf="dcs:Property"),
      },
      obs_config({
          **OBS_MAPPINGS,
          "dcid:observationAbout": "origin",
          "dcid:destination": "destination",
      }))
  assert_records(result.nodes, [record("destination", "Property")])
  assert_records(result.observations, [
      obs(variable="Count_Trip",
          value=10,
          dims={"destination": ref("country/FRA")})
  ])


def test_ignored_column_is_dropped():
  result = run_csv(
      csv_file(obs_row() + ["anything"], header=OBS_HEADER + ["notes"]),
      obs_config(ignoreColumns=["notes"]))
  assert_records(result.observations, [obs()])


def test_unmapped_column_fails_header_validation():
  result = run_csv(
      csv_file(obs_row() + ["anything"], header=OBS_HEADER + ["notes"]))
  assert_fails(result,
               "ValueError",
               "CSV Header Validation Failed!",
               handshake_error_types=("UNMAPPED_COLUMNS",))


def test_missing_required_mapping_fails():
  result = run_csv(
      csv_file(["country/USA", "Count_Person", "1"],
               header=["entity", "variable", "value"]),
      obs_config({
          "dcid:observationAbout": "entity",
          "dcid:variableMeasured": "variable",
          "dcid:value": "value",
      }))
  assert_fails(result,
               "ValueError",
               "CSV Header Validation Failed!",
               handshake_error_types=("MISSING_REQUIRED_COLUMNS",))


# --- CSV formatting -----------------------------------------------------------
#
# Every case is the same three observations, written differently. The ignored
# notes column holds the cells that need CSV quoting.

_HEADER = OBS_HEADER + ["notes"]
_ROWS = [
    obs_row("country/USA", value="1") + ["a, b"],
    obs_row("country/FRA", value="2") + ['line one\nline "two"'],
    obs_row("country/DEU", value="3") + ["plain"],
]
_NOTES_FIRST = ["notes"] + OBS_HEADER
_SAME_THREE = [
    obs("country/USA", value=1),
    obs("country/FRA", value=2),
    obs("country/DEU", value=3),
]

FORMATS = [
    Case("commas, quotes and newlines inside cells",
         given={"case.csv": csv_file(*_ROWS, header=_HEADER)},
         expect=_SAME_THREE),
    Case("CRLF line endings",
         given={"case.csv": csv_file(*_ROWS, header=_HEADER, line_end="\r\n")},
         expect=_SAME_THREE),
    Case("every cell quoted",
         given={"case.csv": csv_file(*_ROWS, header=_HEADER, quote_all=True)},
         expect=_SAME_THREE),
    Case("byte order mark before the header",
         given={"case.csv": csv_file(*_ROWS, header=_HEADER, bom=True)},
         expect=_SAME_THREE),
    Case("ignored column first",
         given={
             "case.csv":
                 csv_file(*[[r[-1]] + r[:-1] for r in _ROWS],
                          header=_NOTES_FIRST)
         },
         expect=_SAME_THREE),
    Case("rows in another order",
         given={"case.csv": csv_file(*reversed(_ROWS), header=_HEADER)},
         expect=_SAME_THREE),
    Case("rows split across two files",
         given={
             "case.csv": csv_file(*_ROWS[:1], header=_HEADER),
             "case_2.csv": csv_file(*_ROWS[1:], header=_HEADER),
         },
         expect=_SAME_THREE),
]


@cases(*FORMATS)
def test_csv_format(given, expect):
  result = run(given, obs_config(ignoreColumns=["notes"]))
  assert_records(result.observations, expect)


# --- Provenance ---------------------------------------------------------------

_PROVENANCE_WITH_URL = {
    "provenance.mcf":
        PROVENANCE_MCF + 'url: "https://source.example.org/dataset"\n'
}


def test_provenance_url_is_copied_onto_observations():
  result = run_csv(csv_file(obs_row()), shared_files=_PROVENANCE_WITH_URL)
  assert_records(result.observations,
                 [obs(provenanceUrl="https://source.example.org/dataset")])


@known_bug(reason="with use_multiprocessing=True (production), observations"
           " lose dcid:provenanceUrl")
def test_provenance_url_survives_the_production_path():
  result = run_csv(csv_file(obs_row()),
                   mode="production",
                   shared_files=_PROVENANCE_WITH_URL)
  assert_records(result.observations,
                 [obs(provenanceUrl="https://source.example.org/dataset")])
