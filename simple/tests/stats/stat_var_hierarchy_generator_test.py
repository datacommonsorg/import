# Copyright 2024 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import json
import os
import shutil
import tempfile
import unittest

from kg_util import mcf_parser
import pandas as pd
from parameterized import parameterized
from stats.data import Triple
from stats.stat_var_hierarchy_generator import *
from stats.stat_var_hierarchy_generator import _extract_svs
from stats.stat_var_hierarchy_generator import _generate_internal
from tests.stats.test_util import is_write_mode

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "stat_var_hierarchy_generator")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path,
                   message):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want, message)


def _write_triples_csv(triples: list[Triple], path: str):
  pd.DataFrame(triples).to_csv(path, index=False)


def _read_triples_csv(path: str) -> list[Triple]:
  df = pd.read_csv(path)
  return [Triple(**kwargs) for kwargs in df.to_dict(orient='records')]


def _strip_ns(v):
  return v[v.find(":") + 1:]


def _to_triple(mcf_triple: list[str]) -> Triple:
  mcf_triple = list(map(lambda x: _strip_ns(x), mcf_triple))
  [subject_id, predicate, value, value_type] = mcf_triple
  if value_type == mcf_parser.ID:
    return Triple(subject_id, predicate, object_id=value)
  return Triple(subject_id, predicate, object_value=value)


def _mcf_to_triples(mcf_path: str) -> list[Triple]:
  with open(mcf_path, "r") as mcf_file:
    return list(
        map(lambda mcf_triple: _to_triple(mcf_triple),
            mcf_parser.mcf_to_triples(mcf_file)))


def _test_generate_internal(test: unittest.TestCase,
                            test_name: str,
                            is_mcf_input: bool = False,
                            has_vertical_specs: bool = False,
                            has_schema_names: bool = False):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    if is_mcf_input:
      input_mcf_path = os.path.join(_INPUT_DIR, f"{test_name}.mcf")
      input_triples = _mcf_to_triples(input_mcf_path)
    else:
      input_triples_path = os.path.join(_INPUT_DIR, f"{test_name}.csv")
      input_triples = _read_triples_csv(input_triples_path)

    vertical_specs: list[VerticalSpec] = []
    if has_vertical_specs:
      input_vertical_specs_path = os.path.join(
          _INPUT_DIR, f"{test_name}.vertical_specs.json")
      with open(input_vertical_specs_path, "r") as file:
        vertical_specs = load_vertical_specs(file.read())

    dcid2name: dict[str, str] = {}
    if has_schema_names:
      input_schema_names_path = os.path.join(_INPUT_DIR,
                                             f"{test_name}.schema_names.json")
      with open(input_schema_names_path, "r") as file:
        dcid2name = json.load(file)

    output_svgs_json_path = os.path.join(temp_dir, f"{test_name}_svgs.json")
    expected_svgs_json_path = os.path.join(_EXPECTED_DIR,
                                           f"{test_name}_svgs.json")

    output_triples_csv_path = os.path.join(temp_dir, f"{test_name}_triples.csv")
    expected_triples_csv_path = os.path.join(_EXPECTED_DIR,
                                             f"{test_name}_triples.csv")

    hierarchy = _generate_internal(input_triples, vertical_specs, dcid2name)
    # Write SVGs json
    svgs_json = [svg.json() for _, svg in hierarchy.svgs.items()]
    with open(output_svgs_json_path, "w") as out:
      json.dump(svgs_json, out, indent=1)
    # Write SVG triples
    _write_triples_csv(hierarchy.svg_triples, output_triples_csv_path)

    if is_write_mode():
      shutil.copy(output_svgs_json_path, expected_svgs_json_path)
      shutil.copy(output_triples_csv_path, expected_triples_csv_path)
      return

    _compare_files(test, output_svgs_json_path, expected_svgs_json_path,
                   f"Comparing SVGS JSON: {test_name}")
    _compare_files(test, output_triples_csv_path, expected_triples_csv_path,
                   f"Comparing SVG TRIPLES: {test_name}")


class TestStatVarHierarchyGenerator(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    self.maxDiff = None

  def test_generate_internal_main_mcf(self):
    _test_generate_internal(self, "main", is_mcf_input=True)

  def test_generate_internal_basic(self):
    _test_generate_internal(self, "basic")

  def test_generate_internal_three_unrelated_svs(self):
    _test_generate_internal(self, "three_unrelated_svs")

  def test_generate_internal_two_related_svs(self):
    _test_generate_internal(self, "two_related_svs")

  def test_generate_internal_svs_with_mprops(self):
    _test_generate_internal(self, "svs_with_mprops")

  def test_generate_internal_verticals(self):
    _test_generate_internal(self, "verticals", has_vertical_specs=True)

  def test_generate_internal_schema_names(self):
    _test_generate_internal(self, "schema_names", has_schema_names=True)

  def test_extract_svs(self):
    input_triples: list[Triple] = [
        Triple("sv1", "typeOf", "StatisticalVariable", ""),
        Triple("sv1", "populationType", "Person", ""),
        Triple("sv1", "race", "Asian", ""),
        Triple("sv1", "gender", "Female", ""),
        Triple("sv1", "measuredProperty", "count", ""),
        Triple("sv1", "searchDescription", "", "SV1 search description"),
        Triple("non_sv1", "typeOf", "Person", ""),
        Triple("non_sv1", "gender", "Male", ""),
        Triple("non_sv1", "race", "AmericanIndianOrAlaskaNative", ""),
        Triple("non_sv1", "name", "", "Joe Doe"),
        Triple("sv2", "typeOf", "StatisticalVariable", ""),
        Triple("sv2", "populationType", "Coal", ""),
        Triple("sv2", "energySource", "CokeCoal", ""),
        Triple("sv2", "statType", "measuredValue", ""),
        Triple("sv3", "typeOf", "StatisticalVariable", ""),
    ]

    expected_svs = [
        SVPropVals(sv_id="sv1",
                   population_type="Person",
                   pvs=[PropVal("gender", "Female"),
                        PropVal("race", "Asian")],
                   measured_property="count"),
        SVPropVals(sv_id="sv2",
                   population_type="Coal",
                   pvs=[PropVal("energySource", "CokeCoal")],
                   measured_property=""),
        SVPropVals(sv_id="sv3",
                   population_type="Thing",
                   pvs=[],
                   measured_property="")
    ]

    svs = _extract_svs(input_triples)

    self.assertListEqual(svs, expected_svs)

  @parameterized.expand([
      (SVPropVals(sv_id="",
                  population_type="",
                  pvs=[PropVal("gender", ""),
                       PropVal("race", "Asian")],
                  measured_property=""),
       SVPropVals(sv_id="",
                  population_type="",
                  pvs=[PropVal("gender", "Female"),
                       PropVal("race", "Asian")],
                  measured_property=""), {}, "Female"),
      (SVPropVals(sv_id="",
                  population_type="",
                  pvs=[PropVal("gender", "Female")],
                  measured_property=""),
       SVPropVals(sv_id="",
                  population_type="",
                  pvs=[PropVal("gender", "Female"),
                       PropVal("race", "")],
                  measured_property=""), {}, "Race"),
      (SVPropVals(sv_id="",
                  population_type="",
                  pvs=[PropVal("gender", "Female")],
                  measured_property=""),
       SVPropVals(
           sv_id="",
           population_type="",
           pvs=[PropVal("gender", "Female"),
                PropVal("povertyStatus", "")],
           measured_property=""), {
               "povertyStatus": "State of poverty"
           }, "State of poverty"),
      (SVPropVals(
          sv_id="",
          population_type="",
          pvs=[PropVal("gender", "Female"),
               PropVal("povertyStatus", "")],
          measured_property=""),
       SVPropVals(sv_id="",
                  population_type="",
                  pvs=[
                      PropVal("gender", "Female"),
                      PropVal("povertyStatus",
                              "BelowPovertyLevelInThePast12Months")
                  ],
                  measured_property=""), {
                      "povertyStatus":
                          "State of poverty",
                      "BelowPovertyLevelInThePast12Months":
                          "BelowPovertyLevel in the last year"
                  }, "Below Poverty Level in the last year")
  ])
  def test_gen_specialized_name(self, parent: SVPropVals, child: SVPropVals,
                                dcid2name: dict[str, str], expected: str):
    self.assertEqual(child.gen_specialized_name(parent, dcid2name), expected)
