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

import pandas as pd
from stats.data import Triple
from stats.stat_var_hierarchy_generator import *
from stats.stat_var_hierarchy_generator import _extract_svs
from stats.stat_var_hierarchy_generator import _generate_internal
from tests.stats.test_util import is_write_mode

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "stat_var_hierarchy_generator")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


def _read_triples_csv(path: str) -> list[Triple]:
  df = pd.read_csv(path)
  return [Triple(**kwargs) for kwargs in df.to_dict(orient='records')]


def _test_generate_internal(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_triples_path = os.path.join(_INPUT_DIR, f"{test_name}.csv")

    output_svgs_json_path = os.path.join(temp_dir, f"{test_name}_svgs.json")
    expected_svgs_json_path = os.path.join(_EXPECTED_DIR,
                                           f"{test_name}_svgs.json")

    input_triples = _read_triples_csv(input_triples_path)

    hierarchy = _generate_internal(input_triples)
    svgs_json = [svg.json() for svg in hierarchy.svgs.values()]
    with open(output_svgs_json_path, "w") as out:
      json.dump(svgs_json, out, indent=1)

    if is_write_mode():
      shutil.copy(output_svgs_json_path, expected_svgs_json_path)
      return

    _compare_files(test, output_svgs_json_path, expected_svgs_json_path)


class TestStatVarHierarchyGenerator(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    self.maxDiff = None

  def test_generate_internal_basic(self):
    _test_generate_internal(self, "basic")

  def test_generate_internal_three_unrelated_svs(self):
    _test_generate_internal(self, "three_unrelated_svs")

  def test_generate_internal_two_related_svs(self):
    _test_generate_internal(self, "two_related_svs")

  def test_extract_svs(self):
    input_triples: list[Triple] = [
        Triple("sv1", "typeOf", "StatisticalVariable", ""),
        Triple("sv1", "populationType", "Person", ""),
        Triple("sv1", "race", "Asian", ""),
        Triple("sv1", "gender", "Female", ""),
        Triple("sv1", "utteranceTemplate", "", "SV1 utterance"),
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
                        PropVal("race", "Asian")]),
        SVPropVals(sv_id="sv2",
                   population_type="Coal",
                   pvs=[PropVal("energySource", "CokeCoal")]),
        SVPropVals(sv_id="sv3", population_type="Thing", pvs=[])
    ]

    svs = _extract_svs(input_triples)

    self.assertListEqual(svs, expected_svs)
