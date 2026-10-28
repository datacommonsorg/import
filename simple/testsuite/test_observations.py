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
columns about (observationAbout), variable, date, value (see obs_config() in
lib/builders.py).
Observation ids are content hashes, so these tests compare records without
"@id"; the e2e expected.json files pin exact ids.
"""

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases


def test_basic_rows():
  result = b.run_csv(
      b.csv_file(b.obs_row("country/USA", "Count_Person", "2020", "331000000"),
                 b.obs_row("country/FRA", "Median_Age_Person", "2020", "42.3")))
  b.assert_records(result.observations, [
      b.obs("country/USA", "Count_Person", 2020, 331000000),
      b.obs("country/FRA", "Median_Age_Person", 2020, 42.3),
  ])


def test_physical_column_names_are_mapped():
  result = b.run_csv(
      b.csv_file(["country/USA", "Count_Person", "2021", "332000000"],
                 header=["place", "stat", "year", "amount"]),
      b.obs_config({
          "dcid:observationAbout": "place",
          "dcid:variableMeasured": "stat",
          "dcid:observationDate": "year",
          "dcid:value": "amount",
      }))
  b.assert_records(result.observations, [b.obs(date=2021, value=332000000)])


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
  result = b.run_csv(b.csv_file(b.obs_row(value=given)))
  want = [] if expect is DROPPED else [b.obs(value=expect)]
  b.assert_records(result.observations, want)


# The observationAbout cell: the id of the place or entity observed.
OBSERVATION_ABOUT = [
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


@cases(*OBSERVATION_ABOUT)
def test_observation_about(given, expect):
  result = b.run_csv(b.csv_file(b.obs_row(about=given)))
  b.assert_records(result.observations, [b.obs(about=expect)])


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
  result = b.run_csv(b.csv_file(b.obs_row(variable=given)))
  b.assert_records(result.observations, [b.obs(variable=expect)])


DATES = [
    Case("year is a number", given="2020", expect=2020),
    Case("year-month is a string", given="2020-06", expect="2020-06"),
    Case("full date is a string", given="2020-06-15", expect="2020-06-15"),
]


@cases(*DATES)
def test_date(given, expect):
  result = b.run_csv(b.csv_file(b.obs_row(date=given)))
  b.assert_records(result.observations, [b.obs(date=expect)])


def test_non_numeric_value_fails():
  result = b.run_csv(b.csv_file(b.obs_row(value="lots")))
  b.assert_fails(
      result, "ValueError",
      "Invalid non-numeric value(s) found in observation values of file"
      " 'case.csv': ['lots']")


def test_non_numeric_values_are_listed_ten_at_most():
  result = b.run_csv(
      b.csv_file(
          *[b.obs_row(f"country/C{i}", value=f"a{i}") for i in range(1, 13)]))
  listed = ", ".join(f"'a{i}'" for i in range(1, 11))
  b.assert_fails(
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
  result = b.run_csv(b.csv_file(b.obs_row(value="1"), b.obs_row(value="2")))
  b.assert_records(result.observations, [b.obs(value=1), b.obs(value=2)])
  assert len(set(_ids(result))) == 1


def test_rows_with_different_keys_get_different_ids():
  result = b.run_csv(b.csv_file(b.obs_row(date="2020"), b.obs_row(date="2021")))
  assert len(set(_ids(result))) == 2


# --- Columns ------------------------------------------------------------------


def test_optional_observation_columns():
  result = b.run_csv(
      b.csv_file(
          [
              "country/USA", "Amount_Debt", "2020", "12.5", "USDollar", "100",
              "dcs:CensusACS", "P1Y"
          ],
          header=b.OBS_HEADER + ["unit", "scaling", "method", "period"],
      ),
      b.obs_config({
          **b.OBS_MAPPINGS,
          "dcid:unit": "unit",
          "dcid:scalingFactor": "scaling",
          "dcid:measurementMethod": "method",
          "dcid:observationPeriod": "period",
      }))
  b.assert_records(result.observations, [
      b.obs(variable="Amount_Debt",
            value=12.5,
            unit=b.ref("USDollar"),
            scalingFactor=100,
            measurementMethod=b.ref("CensusACS"),
            observationPeriod="P1Y")
  ])


# observationProperties in an inputFiles entry apply to every row of the file.
OBSERVATION_PROPERTIES = [
    Case("unit", given={"unit": "USDollar"},
         expect={"unit": b.ref("USDollar")}),
    Case("scalingFactor",
         given={"scalingFactor": "100"},
         expect={"scalingFactor": 100}),
    Case("measurementMethod",
         given={"measurementMethod": "CensusACS"},
         expect={"measurementMethod": b.ref("CensusACS")}),
    Case("observationPeriod",
         given={"observationPeriod": "P1Y"},
         expect={"observationPeriod": "P1Y"}),
]


@cases(*OBSERVATION_PROPERTIES)
def test_observation_property_from_config(given, expect):
  result = b.run_csv(b.csv_file(b.obs_row()),
                     b.obs_config(observationProperties=given))
  b.assert_records(result.observations, [b.obs(**expect)])


def test_custom_observation_property_from_config_is_a_dimension():
  """Custom properties are also listed in observationProperties."""
  result = b.run_csv(b.csv_file(b.obs_row()),
                     b.obs_config(observationProperties={"sex": "Female"}))
  b.assert_records(result.observations, [b.obs(dims={"sex": "Female"})])


def test_column_value_overrides_observation_property_from_config():
  """The config value only fills rows whose cell is empty."""
  result = b.run_csv(
      b.csv_file(b.obs_row(value="1") + ["Euro"],
                 b.obs_row(value="2") + [""],
                 header=b.OBS_HEADER + ["unit"]),
      b.obs_config({
          **b.OBS_MAPPINGS, "dcid:unit": "unit"
      },
                   observationProperties={"unit": "USDollar"}))
  b.assert_records(result.observations, [
      b.obs(value=1, unit=b.ref("Euro")),
      b.obs(value=2, unit=b.ref("USDollar")),
  ])


def test_second_entity_dimension():
  """A column mapped to another property is a second entity dimension."""
  result = b.run_csv(
      b.csv_file(["country/USA", "country/FRA", "Count_Trip", "2020", "10"],
                 header=["origin", "destination", "variable", "date", "value"]),
      b.obs_config({
          **b.OBS_MAPPINGS,
          "dcid:observationAbout": "origin",
          "dcid:destination": "destination",
      }))
  b.assert_records(result.observations, [
      b.obs(variable="Count_Trip",
            value=10,
            dims={"destination": b.ref("country/FRA")})
  ])


def test_ignored_column_is_dropped():
  result = b.run_csv(
      b.csv_file(b.obs_row() + ["anything"], header=b.OBS_HEADER + ["notes"]),
      b.obs_config(ignoreColumns=["notes"]))
  b.assert_records(result.observations, [b.obs()])


def test_unmapped_column_fails_header_validation():
  result = b.run_csv(
      b.csv_file(b.obs_row() + ["anything"], header=b.OBS_HEADER + ["notes"]))
  b.assert_fails(result,
                 "ValueError",
                 "CSV Header Validation Failed!",
                 handshake_error_types=("UNMAPPED_COLUMNS",))


def test_missing_required_mapping_fails():
  result = b.run_csv(
      b.csv_file(["country/USA", "Count_Person", "1"],
                 header=["about", "variable", "value"]),
      b.obs_config({
          "dcid:observationAbout": "about",
          "dcid:variableMeasured": "variable",
          "dcid:value": "value",
      }))
  b.assert_fails(result,
                 "ValueError",
                 "CSV Header Validation Failed!",
                 handshake_error_types=("MISSING_REQUIRED_COLUMNS",))
