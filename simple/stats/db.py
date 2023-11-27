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

import logging
import os
import sqlite3
import tempfile

from google.cloud.sql.connector.connector import Connector
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from stats.data import Observation
from stats.data import Triple
from util.filehandler import create_file_handler
from util.filehandler import is_gcs_path

FIELD_DB_TYPE = "type"
FIELD_DB_PARAMS = "params"
TYPE_CLOUD_SQL = "cloudsql"
TYPE_SQLITE = "sqlite"

SQLITE_DB_FILE_PATH = "dbFilePath"

CLOUD_MY_SQL_INSTANCE = "instance"
CLOUD_MY_SQL_USER = "user"
CLOUD_MY_SQL_PASSWORD = "password"
CLOUD_MY_SQL_DB = "db"
CLOUD_MY_SQL_DEFAULT_DB_NAME = "datacommons"

ENV_USE_CLOUDSQL = "USE_CLOUDSQL"
ENV_CLOUDSQL_INSTANCE = "CLOUDSQL_INSTANCE"
ENV_DB_USER = "DB_USER"
ENV_DB_PASS = "DB_PASS"
ENV_DB_NAME = "DB_NAME"

_CREATE_TRIPLES_TABLE = """
create table if not exists triples (
    subject_id TEXT,
    predicate TEXT,
    object_id TEXT,
    object_value TEXT
);
"""

_DELETE_TRIPLES_STATEMENT = "delete from triples"
_INSERT_TRIPLES_STATEMENT = "insert into triples values(?, ?, ?, ?)"

_CREATE_OBSERVATIONS_TABLE = """
create table if not exists observations (
    entity TEXT,
    variable TEXT,
    date TEXT,
    value TEXT,
    provenance TEXT
);
"""

_DELETE_OBSERVATIONS_STATEMENT = "delete from observations"
_INSERT_OBSERVATIONS_STATEMENT = "insert into observations values(?, ?, ?, ?, ?)"

_INIT_STATEMENTS = [
    _CREATE_TRIPLES_TABLE,
    _CREATE_OBSERVATIONS_TABLE,
    # Clearing tables for now.
    _DELETE_TRIPLES_STATEMENT,
    _DELETE_OBSERVATIONS_STATEMENT
]


class Db:
  """Class to insert triples and observations into a DB."""

  def __init__(self, config: dict) -> None:
    self.engine = create_db_engine(config)

  def insert_triples(self, triples: list[Triple]):
    logging.info("Writing %s triples to [%s]", len(triples), self.engine)
    self.engine.executemany(_INSERT_TRIPLES_STATEMENT,
                            [to_triple_tuple(triple) for triple in triples])

  def insert_observations(self, observations: list[Observation]):
    logging.info("Writing %s observations to [%s]", len(observations),
                 self.engine)
    self.engine.executemany(
        _INSERT_OBSERVATIONS_STATEMENT,
        [to_observation_tuple(observation) for observation in observations])

  def commit_and_close(self):
    self.engine.commit_and_close()


def to_triple_tuple(triple: Triple):
  return (triple.subject_id, triple.predicate, triple.object_id,
          triple.object_value)


def to_observation_tuple(observation: Observation):
  return (observation.entity, observation.variable, observation.date,
          observation.value, observation.provenance)


class DbEngine:

  def execute(self, sql: str, parameters=None):
    pass

  def executemany(self, sql: str, parameters=None):
    pass

  def commit_and_close(self):
    pass


class SqliteDbEngine(DbEngine):

  def __init__(self, db_params: dict) -> None:
    assert db_params
    assert SQLITE_DB_FILE_PATH in db_params

    self.db_file_path = db_params[SQLITE_DB_FILE_PATH]
    # If file path is a GCS path, we create the DB in a local temp file
    # and upload to GCS on commit.
    self.local_db_file_path: str = self.db_file_path
    if is_gcs_path(self.db_file_path):
      self.local_db_file_path = tempfile.NamedTemporaryFile().name

    self.connection = sqlite3.connect(self.local_db_file_path)
    self.cursor = self.connection.cursor()
    for statement in _INIT_STATEMENTS:
      self.cursor.execute(statement)

  def __str__(self) -> str:
    return f"{TYPE_SQLITE}: {self.db_file_path}"

  def execute(self, sql: str, parameters=None):
    if not parameters:
      self.cursor.execute(sql)
    else:
      self.cursor.execute(sql, parameters)

  def executemany(self, sql: str, parameters=None):
    if not parameters:
      self.cursor.executemany(sql)
    else:
      self.cursor.executemany(sql, parameters)

  def commit_and_close(self):
    self.connection.commit()
    self.connection.close()
    # Copy file if local and actual DB file paths are different.
    if self.local_db_file_path != self.db_file_path:
      local_db = create_file_handler(self.local_db_file_path).read_bytes()
      logging.info("Writing to sqlite db: %s (%s bytes)",
                   self.local_db_file_path, len(local_db))
      create_file_handler(self.db_file_path).write_bytes(local_db)


_CLOUD_MY_SQL_CONNECT_PARAMS = [
    CLOUD_MY_SQL_USER, CLOUD_MY_SQL_PASSWORD, CLOUD_MY_SQL_DB
]
_CLOUD_MY_SQL_PARAMS = [CLOUD_MY_SQL_INSTANCE] + _CLOUD_MY_SQL_CONNECT_PARAMS


class CloudSqlDbEngine:

  def __init__(self, db_params: dict[str, str]) -> None:
    for param in _CLOUD_MY_SQL_PARAMS:
      assert param in db_params, f"{param} param not specified"
    connector = Connector()
    kwargs = {param: db_params[param] for param in _CLOUD_MY_SQL_CONNECT_PARAMS}
    logging.info("Connecting to Cloud MySQL: %s (%s)",
                 db_params[CLOUD_MY_SQL_INSTANCE], db_params[CLOUD_MY_SQL_DB])
    self.connection: Connection = connector.connect(
        db_params[CLOUD_MY_SQL_INSTANCE], "pymysql", **kwargs)
    logging.info("Connected to Cloud MySQL: %s (%s)",
                 db_params[CLOUD_MY_SQL_INSTANCE], db_params[CLOUD_MY_SQL_DB])
    self.description = f"{TYPE_CLOUD_SQL}: {db_params[CLOUD_MY_SQL_INSTANCE]} ({db_params[CLOUD_MY_SQL_DB]})"
    self.cursor: Cursor = self.connection.cursor()
    for statement in _INIT_STATEMENTS:
      self.cursor.execute(statement)

  def __str__(self) -> str:
    return self.description

  def execute(self, sql: str, parameters=None):
    self.cursor.execute(_pymysql(sql), parameters)

  def executemany(self, sql: str, parameters=None):
    self.cursor.executemany(_pymysql(sql), parameters)

  def commit_and_close(self):
    self.cursor.close()
    self.connection.commit()


# PyMySQL uses "%s" as placeholders.
# This function replaces all "?" placeholders with "%s".
def _pymysql(sql: str) -> str:
  return sql.replace("?", "%s")


_SUPPORTED_DB_TYPES = set([TYPE_CLOUD_SQL, TYPE_SQLITE])


def create_db_engine(config: dict) -> DbEngine:
  assert config
  assert FIELD_DB_TYPE in config
  assert FIELD_DB_PARAMS in config

  db_type = config[FIELD_DB_TYPE]
  assert db_type in _SUPPORTED_DB_TYPES

  db_params = config[FIELD_DB_PARAMS]

  if db_type == TYPE_CLOUD_SQL:
    return CloudSqlDbEngine(db_params)
  if db_type == TYPE_SQLITE:
    return SqliteDbEngine(db_params)

  assert False


def create_sqlite_config(sqlite_db_file_path: str) -> dict:
  return {
      FIELD_DB_TYPE: TYPE_SQLITE,
      FIELD_DB_PARAMS: {
          SQLITE_DB_FILE_PATH: sqlite_db_file_path
      }
  }


def get_cloud_sql_config_from_env() -> dict | None:
  if os.getenv(ENV_USE_CLOUDSQL, "").lower() != "true":
    return None

  db_instance = os.getenv(ENV_CLOUDSQL_INSTANCE)
  db_user = os.getenv(ENV_DB_USER)
  db_pass = os.getenv(ENV_DB_PASS)
  db_name = os.getenv(ENV_DB_NAME, CLOUD_MY_SQL_DEFAULT_DB_NAME)

  assert db_instance != None, f"Environment variable {ENV_CLOUDSQL_INSTANCE} not specified."
  assert db_user != None, f"Environment variable {ENV_DB_USER} not specified."
  assert db_pass != None, f"Environment variable {ENV_DB_PASS} not specified."

  return {
      FIELD_DB_TYPE: TYPE_CLOUD_SQL,
      FIELD_DB_PARAMS: {
          CLOUD_MY_SQL_INSTANCE: db_instance,
          CLOUD_MY_SQL_DB: db_name,
          CLOUD_MY_SQL_USER: db_user,
          CLOUD_MY_SQL_PASSWORD: db_pass
      }
  }
