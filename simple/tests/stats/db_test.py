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
import pandas as pd
from stats.data import Observation
from stats.data import ObservationProperties
from stats.data import Triple
from stats.db import _CLEAR_TABLE_FOR_IMPORT_STATEMENTS
from stats.db import BulkImportContext
from stats.db import create_and_update_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_datacommons_platform_config_from_env
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


def _observations_to_df(observations: list[Observation]) -> pd.DataFrame:
  """Helper to convert list of Observation objects to DataFrame."""
  from stats import constants
  return pd.DataFrame([obs.db_tuple() for obs in observations],
                      columns=[
                          constants.COLUMN_ENTITY, constants.COLUMN_VARIABLE,
                          constants.COLUMN_DATE, constants.COLUMN_VALUE,
                          constants.COLUMN_PROVENANCE, 'unit', 'scaling_factor',
                          'measurement_method', 'observation_period',
                          'properties'
                      ])


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
      db.insert_observations(_observations_to_df(_OBSERVATIONS), foo_file)
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
      db.insert_observations(_observations_to_df(_OBSERVATIONS), foo_file)
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
      db.insert_observations(_observations_to_df(_OBSERVATIONS), foo_file)
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
      db.insert_observations(_observations_to_df(_OBSERVATIONS),
                             observations_file)
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

  @mock.patch.dict(
      os.environ, {
          "USE_DATA_COMMONS_PLATFORM": "true",
          "DATA_COMMONS_PLATFORM_URL": "https://test_url"
      })
  def test_get_datacommons_platform_config_from_env(self):
    self.assertEqual(
        get_datacommons_platform_config_from_env(), {
            "type": "datacommons_platform",
            "params": {
                "data_commons_platform_url": "https://test_url"
            }
        })

  @mock.patch('requests.post')
  @mock.patch.dict(
      os.environ, {
          "USE_DATA_COMMONS_PLATFORM": "true",
          "DATA_COMMONS_PLATFORM_URL": "https://test_url"
      })
  def test_insert_triples_into_datacommons_platform(self, mock_post):
    config = get_datacommons_platform_config_from_env()
    db = create_and_update_db(config)

    # Configure the mock response
    mock_post.return_value.status_code = 200
    mock_post.return_value.text = "Success"

    # Execute
    db.insert_triples(_TRIPLES)

    # Assertions
    # 1. Check that the POST request was made to the correct URL
    expected_url = "https://test_url/nodes"
    mock_post.assert_called_once()
    args, kwargs = mock_post.call_args
    self.assertEqual(args[0], expected_url)

    # 2. Extract the JSON-LD payload
    sent_json = kwargs.get('json')
    self.assertIsNotNone(sent_json)
    self.assertIn('@graph', sent_json)

    # 3. Validate specific nodes in the graph
    # We look for 'sub1' and 'sub2' within the @graph list
    nodes = {node['@id']: node for node in sent_json['@graph']}

    # Check sub1
    sub1_id = "dcid:sub1"
    self.assertIn(sub1_id, nodes)
    self.assertEqual(nodes[sub1_id]['@type'], "dcid:StatisticalVariable")
    self.assertEqual(nodes[sub1_id]['dcid:pred1'], "objval1")
    self.assertEqual(nodes[sub1_id]['dcid:name'], "name1")

    # Check sub2
    sub2_id = "dcid:sub2"
    self.assertIn(sub2_id, nodes)
    self.assertEqual(nodes[sub2_id]['@type'], "dcid:StatisticalVariable")
    self.assertEqual(nodes[sub2_id]['dcid:name'], "name2")


class TestBulkImportContext(unittest.TestCase):
  """Tests for BulkImportContext used by CloudSqlDbEngine."""

  def test_enter_starts_transaction_and_clears_tables(self):
    """Test that __enter__ starts transaction, drops indexes, and clears tables."""
    mock_engine = mock.Mock()
    mock_cursor = mock.Mock()
    mock_engine.cursor = mock_cursor

    ctx = BulkImportContext(mock_engine)
    result = ctx.__enter__()

    # Returns self
    self.assertEqual(result, ctx)

    # Verify transaction started
    mock_cursor.execute.assert_any_call("START TRANSACTION")

    # Verify indexes dropped
    mock_engine._drop_indexes.assert_called_once()

    # Verify tables cleared
    for stmt in _CLEAR_TABLE_FOR_IMPORT_STATEMENTS:
      mock_cursor.execute.assert_any_call(stmt)

  def test_exit_commits_on_success(self):
    """Test that __exit__ commits and recreates indexes on success."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    ctx.__enter__()
    ctx.__exit__(None, None, None)  # No exception

    mock_engine.connection.commit.assert_called_once()
    mock_engine._create_indexes.assert_called_once()
    mock_engine.connection.rollback.assert_not_called()

  def test_exit_rolls_back_on_exception(self):
    """Test that __exit__ rolls back on exception."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    ctx.__enter__()
    ctx.__exit__(RuntimeError, RuntimeError("test error"), None)

    mock_engine.connection.rollback.assert_called_once()
    mock_engine.connection.commit.assert_not_called()
    mock_engine._create_indexes.assert_not_called()

  def test_insert_observations_calls_executemany(self):
    """Test that insert_observations calls engine.executemany."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    batch = [("e1", "v1", "2023", "100", "p1", "", "", "", "", "")]

    count = ctx.insert_observations(batch)

    self.assertEqual(count, 1)
    mock_engine.executemany.assert_called_once()
    # Verify the batch was passed
    call_args = mock_engine.executemany.call_args[0]
    self.assertEqual(call_args[1], batch)

  def test_insert_triples_calls_executemany(self):
    """Test that insert_triples calls engine.executemany."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    triples = [("sub1", "pred1", "obj1", "")]

    count = ctx.insert_triples(triples)

    self.assertEqual(count, 1)
    mock_engine.executemany.assert_called_once()

  def test_insert_kv_calls_executemany(self):
    """Test that insert_kv calls engine.executemany."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    kv_pairs = [("key1", "value1")]

    count = ctx.insert_kv(kv_pairs)

    self.assertEqual(count, 1)
    mock_engine.executemany.assert_called_once()

  def test_get_counts_tracks_inserts(self):
    """Test that get_counts returns accurate counts after inserts."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)

    # Insert some data
    ctx.insert_observations([("e1",), ("e2",)])
    ctx.insert_triples([("t1",)])
    ctx.insert_kv([("k1",), ("k2",), ("k3",)])

    counts = ctx.get_counts()

    self.assertEqual(counts['observations'], 2)
    self.assertEqual(counts['triples'], 1)
    self.assertEqual(counts['key_value_store'], 3)

  def test_validate_passes_when_counts_match(self):
    """Test that validate passes when counts match expected."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    ctx.insert_observations([("e1",), ("e2",)])
    ctx.insert_triples([("t1",)])
    ctx.insert_kv([])

    result = ctx.validate(expected_obs=2, expected_triples=1, expected_kv=0)

    self.assertTrue(result)

  def test_validate_raises_on_observation_mismatch(self):
    """Test that validate raises RuntimeError on observation count mismatch."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)
    ctx.insert_observations([("e1",)])

    with self.assertRaises(RuntimeError) as cm:
      ctx.validate(expected_obs=100)

    self.assertIn("Observation count mismatch", str(cm.exception))

  def test_validate_raises_on_triple_mismatch(self):
    """Test that validate raises RuntimeError on triple count mismatch."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)

    with self.assertRaises(RuntimeError) as cm:
      ctx.validate(expected_triples=50)

    self.assertIn("Triple count mismatch", str(cm.exception))

  def test_validate_raises_on_kv_mismatch(self):
    """Test that validate raises RuntimeError on key-value count mismatch."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)

    with self.assertRaises(RuntimeError) as cm:
      ctx.validate(expected_kv=10)

    self.assertIn("Key-value count mismatch", str(cm.exception))

  def test_empty_batch_not_inserted(self):
    """Test that empty batches don't call executemany."""
    mock_engine = mock.Mock()
    mock_engine.cursor = mock.Mock()

    ctx = BulkImportContext(mock_engine)

    ctx.insert_observations([])
    ctx.insert_triples([])
    ctx.insert_kv([])

    mock_engine.executemany.assert_not_called()
    self.assertEqual(ctx.get_counts()['observations'], 0)
