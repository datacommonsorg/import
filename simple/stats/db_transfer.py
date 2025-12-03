# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import logging
import os
import sqlite3
import time
from typing import Optional, TYPE_CHECKING

from pymysql.connections import Connection
from pymysql.cursors import Cursor
from stats.db import _INSERT_KEY_VALUE_STORE_STATEMENT
from stats.db import _INSERT_OBSERVATIONS_STATEMENT
from stats.db import _INSERT_TRIPLES_STATEMENT
from stats.db import _pymysql

if TYPE_CHECKING:
  from stats.db import CloudSqlDbEngine


def transfer_sqlite_to_cloud_sql(
    sqlite_path: str,
    cloud_sql_engine: "CloudSqlDbEngine",
    expected_obs: Optional[int] = None,
    expected_triples: Optional[int] = None,
    expected_kv: Optional[int] = None
) -> dict:
  """Transfer data from SQLite to Cloud SQL with transaction safety and index management.

  This function:
  1. Drops indexes before bulk insert which leads to faster inserts
  2. Clears and inserts data
  3. Recreates indexes after insert (having released locks)
  4. Validates and commits

  Args:
    sqlite_path: Path to SQLite database file
    cloud_sql_engine: CloudSqlDbEngine instance
    expected_obs: Expected observation count for validation (optional)
    expected_triples: Expected triple count for validation (optional)
    expected_kv: Expected key_value_store count for validation (optional)

  Returns:
    dict with counts: {'observations': int, 'triples': int, 'key_value_store': int}

  Raises:
    FileNotFoundError: If SQLite file doesn't exist
    RuntimeError: If transfer fails or validation fails
  """
  if not os.path.exists(sqlite_path):
    raise FileNotFoundError(f"SQLite database not found: {sqlite_path}")

  sqlite_size_mb = os.path.getsize(sqlite_path) / 1024 / 1024
  logging.info(f"Starting Cloud SQL transfer from SQLite ({sqlite_size_mb:.1f} MB)")

  # Connect to SQLite
  sqlite_conn = sqlite3.connect(sqlite_path)
  sqlite_cursor = sqlite_conn.cursor()

  try:
    cursor = cloud_sql_engine.cursor
    connection = cloud_sql_engine.connection

    # Start transaction
    transaction_start = time.time()
    cursor.execute("START TRANSACTION")
    logging.info("Transaction started (DB LOCKED - writes blocked)")

    try:
      # Drop indexes for faster bulk insert
      logging.info("Dropping Cloud SQL indexes...")
      cloud_sql_engine._drop_indexes()

      # Clear existing data
      logging.info("Clearing existing Cloud SQL data...")
      cursor.execute("DELETE FROM observations")
      cursor.execute("DELETE FROM triples")
      cursor.execute("DELETE FROM key_value_store")

      # Bulk transfer data
      # Transfer observations in batches
      logging.info("Transferring observations...")
      BATCH_SIZE = 1000000  # 1M rows per batch
      obs_count = 0

      sqlite_cursor.execute("SELECT * FROM observations")
      while True:
        batch = sqlite_cursor.fetchmany(BATCH_SIZE)
        if not batch:
          break

        cursor.executemany(_pymysql(_INSERT_OBSERVATIONS_STATEMENT), batch)
        obs_count += len(batch)
        logging.info(f"Transferred {obs_count:,} observations so far...")

      logging.info(f"Transferred {obs_count:,} observations total")

      # Transfer triples
      logging.info("Transferring triples...")
      sqlite_cursor.execute("SELECT * FROM triples")
      triples = sqlite_cursor.fetchall()
      triple_count = len(triples)

      if triple_count > 0:
        cursor.executemany(_pymysql(_INSERT_TRIPLES_STATEMENT), triples)
      logging.info(f"Transferred {triple_count:,} triples")

      # Transfer key_value_store
      logging.info("Transferring key_value_store...")
      sqlite_cursor.execute("SELECT * FROM key_value_store")
      kv_pairs = sqlite_cursor.fetchall()
      kv_count = len(kv_pairs)

      if kv_count > 0:
        cursor.executemany(_pymysql(_INSERT_KEY_VALUE_STORE_STATEMENT),
                           kv_pairs)
      logging.info(f"Transferred {kv_count:,} key-value pairs")

      # Validate transfer (before commit)
      if expected_obs is not None or expected_triples is not None or expected_kv is not None:
        if not validate_transfer(cursor, expected_obs, expected_triples, expected_kv):
          raise RuntimeError("Transfer validation failed")

      # Commit transaction (releases locks). Note: indexes are not yet recreated.
      connection.commit()
      lock_duration = time.time() - transaction_start
      logging.info(f"Transfer committed - DB unlocked after {lock_duration:.1f}s ({lock_duration/60:.2f} min)")

    except Exception as e:
      # Rollback on any error
      logging.error(f"Transfer failed, rolling back: {e}")
      connection.rollback()
      raise

    # Recreate indexes outside transaction (via online DDL). This allows reads during index creation.
    try:
      logging.info("Recreating Cloud SQL indexes...")
      cloud_sql_engine._create_indexes()
      logging.info("Indexes recreated successfully")

      return {
          'observations': obs_count,
          'triples': triple_count,
          'key_value_store': kv_count
      }

    except Exception as e:
      logging.error(f"Index creation failed: {e}")
      logging.warning("Database is live but without indexes - queries will be slow")
      raise

  finally:
    sqlite_conn.close()


def validate_transfer(
    cursor: Cursor,
    expected_obs: Optional[int] = None,
    expected_triples: Optional[int] = None,
    expected_kv: Optional[int] = None
) -> bool:
  """Validate transferred data counts.

  Args:
    cursor: Cloud SQL cursor
    expected_obs: Expected observation count (optional)
    expected_triples: Expected triple count (optional)
    expected_kv: Expected key_value_store count (optional)

  Returns:
    True if validation passes, False otherwise
  """
  logging.info("Validating transfer...")

  # Check observation count
  cursor.execute("SELECT COUNT(*) FROM observations")
  obs_count = cursor.fetchone()[0]
  logging.info(f"Observations: {obs_count:,}")

  if expected_obs is not None and obs_count != expected_obs:
    logging.error(f"Observation count mismatch: expected {expected_obs:,}, got {obs_count:,}")
    return False

  # Check triple count
  cursor.execute("SELECT COUNT(*) FROM triples")
  triple_count = cursor.fetchone()[0]
  logging.info(f"Triples: {triple_count:,}")

  if expected_triples is not None and triple_count != expected_triples:
    logging.error(f"Triple count mismatch: expected {expected_triples:,}, got {triple_count:,}")
    return False

  # Check key_value_store count
  cursor.execute("SELECT COUNT(*) FROM key_value_store")
  kv_count = cursor.fetchone()[0]
  logging.info(f"Key-value pairs: {kv_count:,}")

  if expected_kv is not None and kv_count != expected_kv:
    logging.error(f"Key-value count mismatch: expected {expected_kv:,}, got {kv_count:,}")
    return False

  logging.info("Validation passed")
  return True
