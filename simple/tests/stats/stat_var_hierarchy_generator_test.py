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

import unittest

from stats.data import Triple
from stats.stat_var_hierarchy_generator import *


class TestStatVarHierarchyGenerator(unittest.TestCase):

  def test_stat_var_pvs(self):
    input_triples: list[Triple] = [
        Triple("sv1", "typeOf", "StatisticalVariable", ""),
        Triple("sv1", "populationType", "Person", ""),
        Triple("sv1", "gender", "Female", ""),
        Triple("sv1", "race", "Asian", ""),
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

    expected_sv_id_2_population_type = {
        "sv1": "Person",
        "sv2": "Coal",
        "sv3": "Thing"
    }

    expected_sv_id_2_pvs = {
        "sv1": {
            "gender": "Female",
            "race": "Asian"
        },
        "sv2": {
            "energySource": "CokeCoal"
        },
        "sv3": {}
    }

    sv_pvs = StatVarPVs(input_triples)

    self.assertDictEqual(sv_pvs.sv_id_2_population_type,
                         expected_sv_id_2_population_type)
    self.assertDictEqual(sv_pvs.sv_id_2_pvs, expected_sv_id_2_pvs)
