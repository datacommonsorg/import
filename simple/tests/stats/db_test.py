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
from stats.data import ObservationProperties
from stats.data import Triple
from stats.db import create_and_update_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_path_from_env
from stats.db import ImportStatus
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import read_full_db_from_file
from util.filesystem import create_store

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "db")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")

_TRIPLES = [
    Triple("sub1", "typeOf", object_id="StatisticalVariable"),
    Triple("sub1", "pred1", object_value="objval1"),
    Triple("sub1", "name", object_value="name1"),
    Triple("sub2", "typeOf", object_id="StatisticalVariable"),
    Triple("sub2", "name", object_value="name2")
]

_OBSERVATIONS = [
    Observation("e1", "v1", "2023", "123", "p1"),
    Observation("e2", "v1", "2023", "456", "p1"),
    Observation("e3",
                "v1",
                "2023",
                "789",
                "p1",
                properties=ObservationProperties.new({
                    "unit": "USD",
                    "prop1": "val1"
                }))
]

_OLD_OBSERVATION_TUPLES_AFTER_UPDATE = [
    ("e1", "v1", "2023", "123", "p1", None, None, None, None, None),
    ("e2", "v1", "2023", "456", "p1", None, None, None, None, None),
    ("e3", "v1", "2023", "789", "p1", None, None, None, None, None)
]

# The import previously recorded in sqlite_old_schema_populated.sql
_OLD_IMPORT_TUPLE = ("2022-02-02 00:00:00", "SUCCESS",
                     '{"numVars": 1, "numObs": 3}')

# The import previously recorded in sqlite_current_schema_populated.sql
_DIFFERENT_IMPORT_TUPLE = ("2021-03-03 00:00:00", "SUCCESS",
                           '{"numVars": 2, "numObs": 2}')

# The import performed during tests in this file.
_CURRENT_IMPORT_TUPLE = ("2023-01-01 00:00:00", "SUCCESS",
                         '{"numVars": 1, "numObs": 3}')

_KEY_VALUE = ("k1", "v1")

_INDEXES = [('observations_entity_variable', 'observations'),
            ('triples_subject_id', 'triples'),
            ('triples_subject_id_predicate', 'triples'),
            ('observations_variable', 'observations')]


class TestDb(unittest.TestCase):

  def _seed_db_from_input(self, db_file_path: str, input_db_file_name: str):
    input_db_file = os.path.join(_INPUT_DIR, input_db_file_name)
    read_full_db_from_file(db_file_path, input_db_file)

  def _verify_db_contents(self, db_file_path: str, triples: list[tuple],
                          observations: list[tuple], key_values: list[tuple],
                          imports: list[tuple], indexes: list[tuple]):
    db = sqlite3.connect(db_file_path)

    actual_triples = db.execute("select * from triples").fetchall()
    self.assertListEqual(actual_triples, triples)

    actual_observations = db.execute("select * from observations").fetchall()
    self.assertListEqual(actual_observations, observations)

    actual_key_values = db.execute("select * from key_value_store").fetchall()
    self.assertListEqual(actual_key_values, key_values)

    actual_imports = db.execute("select * from imports").fetchall()
    self.assertListEqual(actual_imports, imports)

    actual_indexes = db.execute(
        "select name, tbl_name from sqlite_master where type = 'index'"
    ).fetchall()
    self.assertListEqual(sorted(actual_indexes), sorted(indexes))

    db.close()

  @freeze_time("2023-01-01")
  def test_sql_db(self):
    """ Tests database creation and insertion of triples, observations, and
    import info in SQL mode.
    Compares resulting DB contents with expected values.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db_file_name = "datacommons.db"
      db_file_path = os.path.join(temp_dir, db_file_name)
      db_file = temp_store.as_dir().open_file(db_file_name)
      db = create_and_update_db(create_sqlite_config(db_file))
      db.insert_triples(_TRIPLES)
      foo_file = temp_store.as_dir().open_file("foo.csv")
      db.insert_observations(_OBSERVATIONS, foo_file)
      db.insert_key_value(_KEY_VALUE[0], _KEY_VALUE[1])
      db.insert_import_info(status=ImportStatus.SUCCESS)

      sv_triples = db.select_triples_by_subject_type("StatisticalVariable")
      self.assertListEqual(sv_triples, _TRIPLES)

      svg_triples = db.select_triples_by_subject_type("StatVarGroup")
      self.assertListEqual(svg_triples, [])

      entity_names = db.select_entity_names(["sub1", "sub2", "sub3"])
      self.assertDictEqual(entity_names, {"sub1": "name1", "sub2": "name2"})

      db.commit_and_close()

      self._verify_db_contents(
          db_file_path,
          triples=list(map(lambda x: x.db_tuple(), _TRIPLES)),
          observations=list(map(lambda x: x.db_tuple(), _OBSERVATIONS)),
          key_values=[_KEY_VALUE],
          imports=[_CURRENT_IMPORT_TUPLE],
          indexes=_INDEXES)

  @freeze_time("2023-01-01")
  def test_sql_db_schema_update(self):
    """ Tests that db.create() updates the schema of an existing SQL database
    without modifying existing data.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db_file_name = "datacommons.db"
      db_file_path = os.path.join(temp_dir, db_file_name)
      db_file = temp_store.as_dir().open_file(db_file_name)
      self._seed_db_from_input(db_file_path, "sqlite_old_schema_populated.sql")

      db = create_and_update_db(create_sqlite_config(db_file))
      db.commit_and_close()

      self._verify_db_contents(
          db_file_path,
          triples=list(map(lambda x: x.db_tuple(), _TRIPLES)),
          observations=_OLD_OBSERVATION_TUPLES_AFTER_UPDATE,
          key_values=[_KEY_VALUE],
          imports=[_OLD_IMPORT_TUPLE],
          indexes=_INDEXES)

  @freeze_time("2023-01-01")
  def test_sql_db_reimport_with_schema_update(self):
    """ Tests that reimporting data to a SQL database succeeds when the existing
    database has an old schema.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db_file_name = "datacommons.db"
      db_file_path = os.path.join(temp_dir, db_file_name)
      db_file = temp_store.as_dir().open_file(db_file_name)
      self._seed_db_from_input(db_file_path, "sqlite_old_schema_populated.sql")

      db = create_and_update_db(create_sqlite_config(db_file))

      db.maybe_clear_before_import()

      db.insert_triples(_TRIPLES)
      foo_file = temp_store.as_dir().open_file("foo.csv")
      db.insert_observations(_OBSERVATIONS, foo_file)
      db.insert_key_value(_KEY_VALUE[0], _KEY_VALUE[1])
      db.insert_import_info(status=ImportStatus.SUCCESS)

      db.commit_and_close()

      self._verify_db_contents(
          db_file_path,
          triples=list(map(lambda x: x.db_tuple(), _TRIPLES)),
          observations=list(map(lambda x: x.db_tuple(), _OBSERVATIONS)),
          key_values=[_KEY_VALUE],
          imports=[_OLD_IMPORT_TUPLE, _CURRENT_IMPORT_TUPLE],
          indexes=_INDEXES)

  @freeze_time("2023-01-01")
  def test_sql_db_reimport_without_schema_update(self):
    """ Tests that importing new data to a SQL database replaces the contents of
    all tables except the imports table.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      db_file_name = "datacommons.db"
      db_file_path = os.path.join(temp_dir, db_file_name)
      db_file = temp_store.as_dir().open_file(db_file_name)
      self._seed_db_from_input(db_file_path,
                               "sqlite_current_schema_populated.sql")

      db = create_and_update_db(create_sqlite_config(db_file))

      db.maybe_clear_before_import()

      db.insert_triples(_TRIPLES)
      foo_file = temp_store.as_dir().open_file("foo.csv")
      db.insert_observations(_OBSERVATIONS, foo_file)
      db.insert_key_value(_KEY_VALUE[0], _KEY_VALUE[1])
      db.insert_import_info(status=ImportStatus.SUCCESS)

      db.commit_and_close()

      self._verify_db_contents(
          db_file_path,
          triples=list(map(lambda x: x.db_tuple(), _TRIPLES)),
          observations=list(map(lambda x: x.db_tuple(), _OBSERVATIONS)),
          key_values=[_KEY_VALUE],
          imports=[_DIFFERENT_IMPORT_TUPLE, _CURRENT_IMPORT_TUPLE],
          indexes=_INDEXES)

  @freeze_time("2023-01-01")
  def test_main_dc_db(self):
    """ Tests database creation and insertion of triples, observations, and
    import info in main DC mode.
    Compares output observation CSV, observation TMCF, and schema MCF with goldens.
    In write mode, replaces the goldens instead.
    """
    with tempfile.TemporaryDirectory() as temp_dir:
      temp_store = create_store(temp_dir)
      observations_file_name = "observations.csv"
      observations_file_path = os.path.join(temp_dir, observations_file_name)
      expected_observations_file = os.path.join(_EXPECTED_DIR,
                                                "observations.csv")
      tmcf_file = os.path.join(temp_dir, "observations.tmcf")
      expected_tmcf_file = os.path.join(_EXPECTED_DIR, "observations.tmcf")
      mcf_file = os.path.join(temp_dir, "schema.mcf")
      expected_mcf_file = os.path.join(_EXPECTED_DIR, "schema.mcf")

      db = create_and_update_db(create_main_dc_config(temp_store.as_dir()))
      db.insert_triples(_TRIPLES)
      observations_file = temp_store.as_dir().open_file(observations_file_name)
      db.insert_observations(_OBSERVATIONS, observations_file)
      db.insert_import_info(status=ImportStatus.SUCCESS)
      db.commit_and_close()

      if is_write_mode():
        shutil.copy(observations_file_path, expected_observations_file)
        shutil.copy(tmcf_file, expected_tmcf_file)
        shutil.copy(mcf_file, expected_mcf_file)
        return

      compare_files(self, observations_file_path, expected_observations_file)
      compare_files(self, tmcf_file, expected_tmcf_file)
      compare_files(self, mcf_file, expected_mcf_file)

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
  def test_get_sqlite_path_from_env_empty(self):
    self.assertIsNone(get_sqlite_path_from_env())

  @mock.patch.dict(os.environ, {"SQLITE_PATH": "/path/datacommons.db"})
  def test_get_sqlite_path_from_env(self):
    self.assertEqual(get_sqlite_path_from_env(), "/path/datacommons.db")
