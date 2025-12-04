# Copyright 2025 Google Inc.
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
import sqlite3
import tempfile
import pytest
from unittest.mock import Mock, MagicMock
from stats.db_transfer import transfer_sqlite_to_cloud_sql


def test_transfer_validates_sqlite_exists():
  """Test that transfer fails if SQLite file doesn't exist."""
  mock_engine = Mock()
  with pytest.raises(FileNotFoundError):
    transfer_sqlite_to_cloud_sql(
        sqlite_path="/nonexistent/db.sqlite",
        cloud_sql_engine=mock_engine
    )


def test_transfer_reads_all_tables():
  """Test that transfer reads observations, triples, and key_value_store."""
  # Create temporary SQLite database
  with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
    db_path = f.name

  try:
    # Create test data
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
      CREATE TABLE observations (
        entity TEXT, variable TEXT, date TEXT, value TEXT,
        provenance TEXT, unit TEXT, scaling_factor TEXT,
        measurement_method TEXT, observation_period TEXT, properties TEXT
      )
    """)
    cursor.execute("""
      INSERT INTO observations VALUES
      ('geoId/06', 'Count_Person', '2020', '39000000', 'prov1', '', '', '', '', '')
    """)

    cursor.execute("""
      CREATE TABLE triples (
        subject_id TEXT, predicate TEXT, object_id TEXT, object_value TEXT
      )
    """)
    cursor.execute("""
      INSERT INTO triples VALUES ('sv1', 'typeOf', 'StatisticalVariable', '')
    """)

    cursor.execute("""
      CREATE TABLE key_value_store (lookup_key TEXT, value TEXT)
    """)
    cursor.execute("""
      INSERT INTO key_value_store VALUES ('test_key', 'test_value')
    """)

    conn.commit()
    conn.close()

    # Mock Cloud SQL engine with context manager
    mock_engine = Mock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = Mock(return_value=mock_ctx)
    mock_ctx.__exit__ = Mock(return_value=False)
    mock_ctx.get_counts.return_value = {
        'observations': 1,
        'triples': 1,
        'key_value_store': 1
    }
    mock_engine.bulk_import_context.return_value = mock_ctx

    # Transfer
    result = transfer_sqlite_to_cloud_sql(
        sqlite_path=db_path,
        cloud_sql_engine=mock_engine
    )

    # Verify context manager was used
    assert mock_engine.bulk_import_context.called
    assert mock_ctx.__enter__.called
    assert mock_ctx.__exit__.called

    # Verify insert methods were called
    assert mock_ctx.insert_observations.called
    assert mock_ctx.insert_triples.called
    assert mock_ctx.insert_kv.called

    # Verify validate was called
    assert mock_ctx.validate.called

    # Verify counts returned
    assert result['observations'] == 1
    assert result['triples'] == 1
    assert result['key_value_store'] == 1

  finally:
    os.unlink(db_path)


def test_transfer_rolls_back_on_error():
  """Test that transaction rolls back on error (via context manager)."""
  with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
    db_path = f.name

  try:
    # Create minimal SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE observations (
        entity TEXT, variable TEXT, date TEXT, value TEXT,
        provenance TEXT, unit TEXT, scaling_factor TEXT,
        measurement_method TEXT, observation_period TEXT, properties TEXT
      )
    """)
    cursor.execute("INSERT INTO observations VALUES ('test', '', '', '', '', '', '', '', '', '')")
    cursor.execute("CREATE TABLE triples (subject_id TEXT, predicate TEXT, object_id TEXT, object_value TEXT)")
    cursor.execute("CREATE TABLE key_value_store (lookup_key TEXT, value TEXT)")
    conn.commit()
    conn.close()

    # Mock engine that raises error during insert
    mock_engine = Mock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = Mock(return_value=mock_ctx)
    mock_ctx.__exit__ = Mock(return_value=False)
    mock_ctx.insert_observations.side_effect = Exception("Insert failed")
    mock_engine.bulk_import_context.return_value = mock_ctx

    # Transfer should fail
    with pytest.raises(Exception, match="Insert failed"):
      transfer_sqlite_to_cloud_sql(
          sqlite_path=db_path,
          cloud_sql_engine=mock_engine
      )

    # Verify __exit__ was called (which handles rollback)
    assert mock_ctx.__exit__.called
    # Check that __exit__ received exception info
    exit_args = mock_ctx.__exit__.call_args[0]
    assert exit_args[0] is not None  # exc_type

  finally:
    os.unlink(db_path)


def test_transfer_with_validation():
  """Test that validation is called with expected counts."""
  with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
    db_path = f.name

  try:
    # Create minimal SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE observations (
        entity TEXT, variable TEXT, date TEXT, value TEXT,
        provenance TEXT, unit TEXT, scaling_factor TEXT,
        measurement_method TEXT, observation_period TEXT, properties TEXT
      )
    """)
    cursor.execute("CREATE TABLE triples (subject_id TEXT, predicate TEXT, object_id TEXT, object_value TEXT)")
    cursor.execute("CREATE TABLE key_value_store (lookup_key TEXT, value TEXT)")
    conn.commit()
    conn.close()

    # Mock Cloud SQL engine
    mock_engine = Mock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = Mock(return_value=mock_ctx)
    mock_ctx.__exit__ = Mock(return_value=False)
    mock_ctx.get_counts.return_value = {
        'observations': 0,
        'triples': 0,
        'key_value_store': 0
    }
    mock_engine.bulk_import_context.return_value = mock_ctx

    # Transfer with expected counts
    transfer_sqlite_to_cloud_sql(
        sqlite_path=db_path,
        cloud_sql_engine=mock_engine,
        expected_obs=0,
        expected_triples=0,
        expected_kv=0
    )

    # Verify validate was called with expected counts
    mock_ctx.validate.assert_called_once_with(0, 0, 0)

  finally:
    os.unlink(db_path)


def test_transfer_validation_failure():
  """Test that validation failure raises RuntimeError."""
  with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
    db_path = f.name

  try:
    # Create minimal SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("""
      CREATE TABLE observations (
        entity TEXT, variable TEXT, date TEXT, value TEXT,
        provenance TEXT, unit TEXT, scaling_factor TEXT,
        measurement_method TEXT, observation_period TEXT, properties TEXT
      )
    """)
    cursor.execute("CREATE TABLE triples (subject_id TEXT, predicate TEXT, object_id TEXT, object_value TEXT)")
    cursor.execute("CREATE TABLE key_value_store (lookup_key TEXT, value TEXT)")
    conn.commit()
    conn.close()

    # Mock Cloud SQL engine where validation fails
    mock_engine = Mock()
    mock_ctx = MagicMock()
    mock_ctx.__enter__ = Mock(return_value=mock_ctx)
    mock_ctx.__exit__ = Mock(return_value=False)
    mock_ctx.get_counts.return_value = {
        'observations': 0,
        'triples': 0,
        'key_value_store': 0
    }
    mock_ctx.validate.side_effect = RuntimeError("Count mismatch")
    mock_engine.bulk_import_context.return_value = mock_ctx

    # Transfer should fail on validation
    with pytest.raises(RuntimeError, match="Count mismatch"):
      transfer_sqlite_to_cloud_sql(
          sqlite_path=db_path,
          cloud_sql_engine=mock_engine,
          expected_obs=100
      )

  finally:
    os.unlink(db_path)
