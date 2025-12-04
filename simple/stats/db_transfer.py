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
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
  from stats.db import CloudSqlDbEngine

BATCH_SIZE = 1_000_000

def transfer_sqlite_to_cloud_sql(
    sqlite_path: str,
    cloud_sql_engine: "CloudSqlDbEngine",
    expected_obs: Optional[int] = None,
    expected_triples: Optional[int] = None,
    expected_kv: Optional[int] = None
) -> dict:
  """Transfer data from SQLite to Cloud SQL.

  Reads data from SQLite and uses CloudSqlDbEngine.bulk_import_context()
  for transaction-safe bulk import to Cloud SQL.

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

  sqlite_conn = sqlite3.connect(sqlite_path)
  sqlite_cursor = sqlite_conn.cursor()

  try:
    with cloud_sql_engine.bulk_import_context() as ctx:
      # Transfer observations in batches
      logging.info("Transferring observations...")
      sqlite_cursor.execute("SELECT * FROM observations")

      while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        ctx.insert_observations(batch)
        logging.info(f"Transferred {ctx.get_counts()['observations']:,} observations so far...")
      logging.info(f"Transferred {ctx.get_counts()['observations']:,} observations total")

      # Transfer triples in batches
      logging.info("Transferring triples...")
      sqlite_cursor.execute("SELECT * FROM triples")

      while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        ctx.insert_triples(batch)
      logging.info(f"Transferred {ctx.get_counts()['triples']:,} triples")

      # Transfer key_value_store in batches
      logging.info("Transferring key_value_store...")
      sqlite_cursor.execute("SELECT * FROM key_value_store")

      while batch := sqlite_cursor.fetchmany(BATCH_SIZE):
        ctx.insert_kv(batch)
      logging.info(f"Transferred {ctx.get_counts()['key_value_store']:,} key-value pairs")

      # Validate before commit
      ctx.validate(expected_obs, expected_triples, expected_kv)

    # Context manager handles commit and index recreation when it exits
    return ctx.get_counts()

  finally:
    sqlite_conn.close()
