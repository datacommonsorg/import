# Copyright 2023 Google Inc.
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

import pandas as pd
from stats import constants
from stats.data import McfNode
from stats.data import StatVarGroup
from stats.data import Triple
from stats.data import validate_numeric_values

SVG_ID1 = "svg_id1"
SVG_NAME1 = "SVG NAME1"
SVG_PARENT_ID1 = "svg_parent_id1"


class TestData(unittest.TestCase):

  def test_svg_triples(self):
    svg = StatVarGroup(SVG_ID1, SVG_NAME1, SVG_PARENT_ID1)
    result = svg.triples()
    expected = [
        Triple(SVG_ID1, "typeOf", object_id="StatVarGroup"),
        Triple(SVG_ID1, "name", object_value=SVG_NAME1),
        Triple(SVG_ID1, "specializationOf", object_id=SVG_PARENT_ID1)
    ]
    self.assertListEqual(result, expected)

  def test_svg_triples_without_parent(self):
    svg = StatVarGroup("dc/g/Root", "Data Commons Variables", "")
    result = svg.triples()
    expected = [
        Triple("dc/g/Root", "typeOf", object_id="StatVarGroup"),
        Triple("dc/g/Root", "name", object_value="Data Commons Variables"),
    ]
    self.assertListEqual(result, expected)

  def test_mcf_node(self):
    triples: list[Triple] = [
        Triple("sv1", "typeOf", object_id="StatisticalVariable"),
        Triple("sv1", "name", object_value="sv name"),
        Triple("sv1", "description", object_value="sv desc"),
        Triple("sv1", "memberOf", object_id="svg1"),
        Triple("sv1", "includedIn", object_id="prov1")
    ]

    node = McfNode("sv1")
    for triple in triples:
      node.add_triple(triple)

    expected = """
Node: dcid:sv1
typeOf: StatisticalVariable
name: "sv name"
description: "sv desc"
memberOf: svg1""".strip()

    self.assertEqual(node.to_mcf(), expected)


class TestValidateNumericValues(unittest.TestCase):
  """Tests for validate_numeric_values, which every import runs on its values."""

  def test_numeric_values_pass(self):
    df = pd.DataFrame({"value": [1, 2.5, -3, "4", "5.6"]})
    validate_numeric_values(df, "test.csv")

  def test_missing_value_column_is_ignored(self):
    validate_numeric_values(pd.DataFrame({"entity": ["a"]}), "test.csv")

  def test_empty_and_na_values_pass(self):
    df = pd.DataFrame({"value": [1, None, "", "NaN", "nan"]})
    validate_numeric_values(df, "test.csv")

  def test_standard_na_values_pass(self):
    df = pd.DataFrame({"value": [1] + list(constants.STANDARD_NA_VALUES)})
    validate_numeric_values(df, "test.csv")

  def test_non_numeric_value_raises(self):
    df = pd.DataFrame({"value": [1, "abc"]})
    with self.assertRaises(ValueError) as cm:
      validate_numeric_values(df, "test.csv")
    self.assertIn("abc", str(cm.exception))
    self.assertIn("test.csv", str(cm.exception))
    self.assertEqual(cm.exception.file_path, "test.csv")

  def test_error_lists_at_most_ten_examples(self):
    df = pd.DataFrame({"value": [f"bad{i}" for i in range(15)]})
    with self.assertRaises(ValueError) as cm:
      validate_numeric_values(df, "test.csv")
    # The message caps the example list at 10 entries.
    self.assertEqual(str(cm.exception).count("bad"), 10)
