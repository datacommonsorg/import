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

import os
import tempfile
import unittest
from unittest import mock

from parameterized import parameterized
from stats import schema
from stats import schema_constants as sc
from stats.data import Triple
from stats.db import create_db
from stats.db import create_sqlite_config


def _to_triples(dcid2name: dict[str, str]) -> list[Triple]:
  triples: list[Triple] = []
  for dcid, name in dcid2name.items():
    triples.append(Triple(dcid, sc.PREDICATE_NAME, object_value=name))
  return triples


class TestSchema(unittest.TestCase):

  @parameterized.expand([
      (
          "both",
          {
              "var1": "Variable 1"
          },
          {
              "prop1": "Property 1"
          },
          ["var1", "prop1"],
          {
              "var1": "Variable 1",
              "prop1": "Property 1"
          },
      ),
      (
          "db only",
          {
              "var1": "Variable 1"
          },
          {},
          ["var1", "prop1"],
          {
              "var1": "Variable 1"
          },
      ),
      (
          "remote only",
          {},
          {
              "prop1": "Property 1"
          },
          ["var1", "prop1"],
          {
              "prop1": "Property 1"
          },
      ),
      (
          "prefer db value",
          {
              "var1": "DB Var 1"
          },
          {
              "var1": "Remote Var 1"
          },
          ["var1", "prop1"],
          {
              "var1": "DB Var 1"
          },
      ),
  ])
  @mock.patch("util.dc_client.get_property_of_entities")
  def test_get_schema_names(self, desc: str, db_names: dict[str, str],
                            remote_names: dict[str,
                                               str], input_dcids: list[str],
                            output_names: dict[str, str], mock_dc_client):
    with tempfile.TemporaryDirectory() as temp_dir:
      db_file_path = os.path.join(temp_dir, "datacommons.db")
      db = create_db(create_sqlite_config(db_file_path))
      db.insert_triples(_to_triples(db_names))
      mock_dc_client.return_value = remote_names

      result = schema.get_schema_names(input_dcids, db)

      self.assertDictEqual(result, output_names, desc)
