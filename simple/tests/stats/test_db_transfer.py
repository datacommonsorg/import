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
from unittest.mock import Mock
from stats.db_transfer import transfer_sqlite_to_cloud_sql, validate_transfer


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

    # Mock Cloud SQL engine
    mock_engine = Mock()
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_engine.cursor = mock_cursor
    mock_engine.connection = mock_connection
    mock_engine._drop_indexes = Mock()
    mock_engine._create_indexes = Mock()
    mock_cursor.fetchone.return_value = (1,)  # For validation

    # Transfer
    result = transfer_sqlite_to_cloud_sql(
        sqlite_path=db_path,
        cloud_sql_engine=mock_engine
    )

    # Verify transaction commands
    execute_calls = [call[0][0] for call in mock_cursor.execute.call_args_list]
    assert "START TRANSACTION" in execute_calls

    # Verify indexes were managed
    assert mock_engine._drop_indexes.called
    assert mock_engine._create_indexes.called

    # Verify commit was called
    assert mock_connection.commit.called

    # Verify executemany was called for bulk insert
    assert mock_cursor.executemany.call_count >= 3  # obs, triples, kv

    # Verify counts returned
    assert result['observations'] == 1
    assert result['triples'] == 1
    assert result['key_value_store'] == 1

  finally:
    os.unlink(db_path)


def test_transfer_rolls_back_on_error():
  """Test that transaction rolls back on error."""
  with tempfile.NamedTemporaryFile(mode='w', suffix='.db', delete=False) as f:
    db_path = f.name

  try:
    # Create minimal SQLite database
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    cursor.execute("CREATE TABLE observations (entity TEXT)")
    cursor.execute("INSERT INTO observations VALUES ('test')")
    cursor.execute("CREATE TABLE triples (subject_id TEXT)")
    cursor.execute("CREATE TABLE key_value_store (lookup_key TEXT)")
    conn.commit()
    conn.close()

    # Mock engine that raises error during insert
    mock_engine = Mock()
    mock_cursor = Mock()
    mock_connection = Mock()
    mock_engine.cursor = mock_cursor
    mock_engine.connection = mock_connection
    mock_engine._drop_indexes = Mock()
    mock_engine._create_indexes = Mock()
    mock_cursor.executemany.side_effect = Exception("Insert failed")

    # Transfer should fail and rollback
    with pytest.raises(Exception, match="Insert failed"):
      transfer_sqlite_to_cloud_sql(
          sqlite_path=db_path,
          cloud_sql_engine=mock_engine
      )

    # Verify rollback was called
    assert mock_connection.rollback.called

  finally:
    os.unlink(db_path)


def test_validate_transfer_checks_counts():
  """Test transfer validation."""
  mock_cursor = Mock()
  mock_cursor.fetchone.side_effect = [
      (100,),  # observations count
      (50,),   # triples count
      (10,)    # key_value_store count
  ]

  result = validate_transfer(
      mock_cursor,
      expected_obs=100,
      expected_triples=50,
      expected_kv=10
  )

  assert result is True


def test_validate_transfer_fails_on_mismatch():
  """Test validation fails with wrong counts."""
  mock_cursor = Mock()
  mock_cursor.fetchone.side_effect = [
      (90,),   # observations count (expected 100)
      (50,),
      (10,)
  ]

  result = validate_transfer(
      mock_cursor,
      expected_obs=100,
      expected_triples=50,
      expected_kv=10
  )

  assert result is False
