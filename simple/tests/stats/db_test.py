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

import os
import shutil
import sqlite3
import tempfile
import unittest
from unittest import mock

from freezegun import freeze_time
from stats.data import Observation
from stats.data import Triple
from stats.db import create_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_config_from_env
from stats.db import ImportStatus
from stats.db import to_observation_tuple
from stats.db import to_triple_tuple
from tests.stats.test_util import is_write_mode

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "db")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


_TRIPLES = [
    Triple("sub1", "pred1", object_id="objid1"),
    Triple("sub2", "pred2", object_value="objval1")
]

_OBSERVATIONS = [
    Observation("e1", "v1", "2023", "123", "p1"),
    Observation("e2", "v1", "2023", "456", "p1")
]


class TestDb(unittest.TestCase):

  @freeze_time("2023-01-01")
  def test_sql_db(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      db_file_path = os.path.join(temp_dir, "datacommons.db")
      db = create_db(create_sqlite_config(db_file_path))
      db.insert_triples(_TRIPLES)
      db.insert_observations(_OBSERVATIONS, "foo.csv")
      db.insert_import_info(status=ImportStatus.SUCCESS)
      db.commit_and_close()

      sqldb = sqlite3.connect(db_file_path)

      triples = sqldb.execute("select * from triples").fetchall()
      self.assertListEqual(triples,
                           list(map(lambda x: to_triple_tuple(x), _TRIPLES)))

      observations = sqldb.execute("select * from observations").fetchall()
      self.assertListEqual(
          observations,
          list(map(lambda x: to_observation_tuple(x), _OBSERVATIONS)))

      import_tuple = sqldb.execute("select * from imports").fetchone()
      self.assertTupleEqual(
          import_tuple,
          ("2023-01-01 00:00:00", "SUCCESS", '{"numVars": 1, "numObs": 2}'))

  @freeze_time("2023-01-01")
  def test_main_dc_db(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      observations_file = os.path.join(temp_dir, "observations.csv")
      expected_observations_file = os.path.join(_EXPECTED_DIR,
                                                "observations.csv")
      tmcf_file = os.path.join(temp_dir, "observations.tmcf")
      expected_tmcf_file = os.path.join(_EXPECTED_DIR, "observations.tmcf")

      db = create_db(create_main_dc_config(temp_dir))
      db.insert_triples(_TRIPLES)
      db.insert_observations(_OBSERVATIONS, "observations.csv")
      db.insert_import_info(status=ImportStatus.SUCCESS)
      db.commit_and_close()

      if is_write_mode():
        shutil.copy(observations_file, expected_observations_file)
        shutil.copy(tmcf_file, expected_tmcf_file)
        return

      _compare_files(self, observations_file, expected_observations_file)
      _compare_files(self, tmcf_file, expected_tmcf_file)

  @mock.patch.dict(os.environ, {})
  def test_get_cloud_sql_config_from_env_empty(self):
    self.assertIsNone(get_cloud_sql_config_from_env())

  @mock.patch.dict(
      os.environ, {
          "USE_CLOUDSQL": "true",
          "CLOUDSQL_INSTANCE": "test_instance",
          "DB_USER": "test_user",
          "DB_PASS": "test_pass"
      })
  def test_get_cloud_sql_config_from_env_valid(self):
    self.assertDictEqual(
        get_cloud_sql_config_from_env(), {
            "type": "cloudsql",
            "params": {
                "instance": "test_instance",
                "db": "datacommons",
                "user": "test_user",
                "password": "test_pass"
            }
        })

  @mock.patch.dict(os.environ, {
      "USE_CLOUDSQL": "true",
      "CLOUDSQL_INSTANCE": ""
  })
  def test_get_cloud_sql_config_from_env_invalid(self):
    with self.assertRaisesRegex(
        AssertionError,
        "Environment variable CLOUDSQL_INSTANCE not specified."):
      get_cloud_sql_config_from_env()

  @mock.patch.dict(os.environ, {})
  def test_get_sqlite_config_from_env_empty(self):
    self.assertIsNone(get_sqlite_config_from_env())

  @mock.patch.dict(os.environ, {"SQLITE_PATH": "/path/datacommons.db"})
  def test_get_sqlite_config_from_env(self):
    self.assertDictEqual(get_sqlite_config_from_env(), {
        "type": "sqlite",
        "params": {
            "dbFilePath": "/path/datacommons.db"
        }
    })
