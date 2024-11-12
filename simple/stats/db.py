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

from dataclasses import dataclass
from datetime import datetime
from enum import auto
from enum import Enum
import json
import logging
import os
import sqlite3
from typing import Any

from google.cloud.sql.connector.connector import Connector
import pandas as pd
from pymysql.connections import Connection
from pymysql.cursors import Cursor
from stats.data import McfNode
from stats.data import Observation
from stats.data import STAT_VAR_GROUP
from stats.data import STATISTICAL_VARIABLE
from stats.data import Triple
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File

FIELD_DB_TYPE = "type"
FIELD_DB_PARAMS = "params"
TYPE_CLOUD_SQL = "cloudsql"
TYPE_SQLITE = "sqlite"
TYPE_MAIN_DC = "maindc"

SQLITE_DB_FILE = "dbFile"

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

ENV_SQLITE_PATH = "SQLITE_PATH"

MAIN_DC_OUTPUT_DIR = "mainDcOutputDir"

_OBSERVATION_PROPERTY_COLUMNS = [
    "unit", "scaling_factor", "measurement_method", "observation_period",
    "properties"
]

_CREATE_TRIPLES_TABLE = """
create table if not exists triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
"""

_DELETE_TRIPLES_STATEMENT = "delete from triples"
_INSERT_TRIPLES_STATEMENT = "insert into triples values(?, ?, ?, ?)"

_CREATE_OBSERVATIONS_TABLE = """
create table if not exists observations (
    entity varchar(255),
    variable varchar(255),
    date varchar(255),
    value varchar(255),
    provenance varchar(255),
    unit varchar(255),
    scaling_factor varchar(255),
    measurement_method varchar(255),
    observation_period varchar(255),
    properties TEXT
);
"""

_DELETE_OBSERVATIONS_STATEMENT = "delete from observations"
_INSERT_OBSERVATIONS_STATEMENT = "insert into observations values(?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"

_CREATE_KEY_VALUE_STORE_TABLE = """
create table if not exists key_value_store (
    lookup_key varchar(255),
    value longtext
);
"""

_DELETE_KEY_VALUE_STORE_STATEMENT = "delete from key_value_store"
_INSERT_KEY_VALUE_STORE_STATEMENT = "insert into key_value_store values(?, ?)"

_CREATE_IMPORTS_TABLE = """
create table if not exists imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
"""

_INSERT_IMPORTS_STATEMENT = "insert into imports values(?, ?, ?)"

_SELECT_TRIPLES_BY_SUBJECT_TYPE = "select * from triples where subject_id in (select subject_id from triples where predicate = 'typeOf' and object_id = ?)"

_SELECT_ENTITY_NAMES = "select subject_id, object_value from triples where subject_id in (%s) and predicate = 'name' and object_value <> ''"

_INIT_TABLE_STATEMENTS = [
    _CREATE_TRIPLES_TABLE,
    _CREATE_OBSERVATIONS_TABLE,
    _CREATE_KEY_VALUE_STORE_TABLE,
    _CREATE_IMPORTS_TABLE,
]

_CLEAR_TABLE_FOR_IMPORT_STATEMENTS = [
    # Clearing tables for now (not the import tables though since we want to maintain its history).
    _DELETE_TRIPLES_STATEMENT,
    _DELETE_OBSERVATIONS_STATEMENT,
    _DELETE_KEY_VALUE_STORE_STATEMENT
]

# Schema update statements.

# Various property columns not part of the observations table originally.
# These statements add those columns.
# Neither sqlite nor mysql support an 'if not exists' statement for altering tables universally,
# so the code needs to check for existence separately before applying these statements.
_ALTER_OBSERVATIONS_TABLE_STATEMENTS = [
    "alter table observations add column unit varchar(255);",
    "alter table observations add column scaling_factor varchar(255);",
    "alter table observations add column measurement_method varchar(255);",
    "alter table observations add column observation_period varchar(255);",
    "alter table observations add column properties text;"
]

OBSERVATIONS_TMCF = """Node: E:Table->E0
typeOf: dcs:StatVarObservation
variableMeasured: C:Table->variable
observationDate: C:Table->date
observationAbout: C:Table->entity
value: C:Table->value"""

OBSERVATIONS_TMCF_FILE_NAME = "observations.tmcf"
SCHEMA_MCF_FILE_NAME = "schema.mcf"

MCF_NODE_TYPES_ALLOWLIST = set([STATISTICAL_VARIABLE, STAT_VAR_GROUP])


@dataclass
class DbIndex:
  table_name: str
  index_name: str
  indexed_columns: list[str]

  def sqlite_drop_index_statement(self) -> str:
    return f"drop index if exists {self.index_name}"

  def sqlite_create_index_statement(self) -> str:
    return f"create index if not exists {self.index_name} on {self.table_name} ({', '.join(self.indexed_columns)})"

  def mysql_drop_index_statement(self) -> str:
    return f"alter table {self.table_name} drop index {self.index_name}"

  def mysql_create_index_statement(self) -> str:
    return f"alter table {self.table_name} add index {self.index_name} ({', '.join(self.indexed_columns)})"


_DB_INDEXES: list[DbIndex] = [
    DbIndex(table_name="observations",
            index_name="observations_entity_variable",
            indexed_columns=["entity", "variable"]),
    DbIndex(table_name="triples",
            index_name="triples_subject_id",
            indexed_columns=["subject_id"])
]


class ImportStatus(Enum):
  SUCCESS = auto()
  FAILURE = auto()


class Db:
  """Abstract class to insert triples and observations into a DB.
  The "DB" could be a traditional sql db or a file system with the output being files.
  """

  def maybe_clear_before_import(self):
    pass

  def insert_triples(self, triples: list[Triple]):
    pass

  def insert_observations(self, observations: list[Observation],
                          input_file: File):
    pass

  def insert_key_value(self, key: str, value: str):
    pass

  def insert_import_info(self, status: ImportStatus):
    pass

  def commit_and_close(self):
    pass

  # Returns all triples of nodes with the specified "typeOf" predicate.
  def select_triples_by_subject_type(self, subject_type: str) -> list[Triple]:
    pass

  # Returns names of the corresponding dcids.
  def select_entity_names(self, dcids: list[str]) -> dict[str, str]:
    pass


class MainDcDb(Db):
  """Generates output for main DC.
  Observations will be output as TMCF + CSVs.
  Triples will be output as schema MCF.
  """

  def __init__(self, db_params: dict) -> None:
    assert db_params
    assert MAIN_DC_OUTPUT_DIR in db_params

    self.output_dir = db_params[MAIN_DC_OUTPUT_DIR]
    # dcid to node dict
    self.nodes: dict[str, McfNode] = {}

  def insert_triples(self, triples: list[Triple]):
    for triple in triples:
      self._add_triple(triple)

  def insert_observations(self, observations: list[Observation],
                          input_file: File):
    df = pd.DataFrame(observations)
    # Drop the provenance and properties columns.
    # Provenance is specified differently for main dc.
    # TODO: Include obs properties in main DC output.
    df = df.drop(columns=["provenance", "properties"])
    # Right now, this overwrites any file with the same name,
    # so if different input sources have files with the same relative path,
    # they will clobber each others output. Treating this as an edge case
    # for now but could resolve by allowing input sources to be mapped to output
    # locations.
    self.output_dir.open_file(input_file.path).write(df.to_csv(index=False))

  def insert_import_info(self, status: ImportStatus):
    # No-op for now.
    pass

  def commit_and_close(self):
    # MCF
    filtered = filter(lambda node: node.node_type in MCF_NODE_TYPES_ALLOWLIST,
                      self.nodes.values())
    mcf = "\n\n".join(map(lambda node: node.to_mcf(), filtered))
    self.output_dir.open_file(SCHEMA_MCF_FILE_NAME).write(mcf)

    # TMCF
    self.output_dir.open_file(OBSERVATIONS_TMCF_FILE_NAME).write(
        OBSERVATIONS_TMCF)

    # Not supported for main DC at this time.
    def select_triples_by_subject_type(self, type_of: str) -> list[Triple]:
      return []

    # Not supported for main DC at this time.
    def select_entity_names(self, dcids: list[str]) -> dict[str, str]:
      return {}

  def _add_triple(self, triple: Triple):
    node = self.nodes.get(triple.subject_id)
    if not node:
      node = McfNode(triple.subject_id)
      self.nodes[triple.subject_id] = node
    node.add_triple(triple)


class SqlDb(Db):
  """Class to insert triples and observations into a SQL DB."""

  def __init__(self, config: dict) -> None:
    self.engine = create_db_engine(config)
    self.engine.init_or_update_tables()
    self.num_observations = 0
    self.variables: set[str] = set()
    self.indexes_cleared = False

  def maybe_clear_before_import(self):
    self.engine.clear_tables_and_indexes()

  def insert_triples(self, triples: list[Triple]):
    logging.info("Writing %s triples to [%s]", len(triples), self.engine)
    if triples:
      self.engine.executemany(_INSERT_TRIPLES_STATEMENT,
                              [triple.db_tuple() for triple in triples])

  def insert_observations(self, observations: list[Observation],
                          input_file: File):
    logging.info("Writing %s observations to [%s]", len(observations),
                 self.engine)
    self.num_observations += len(observations)
    tuples = []
    for observation in observations:
      tuples.append(observation.db_tuple())
      self.variables.add(observation.variable)

    self.engine.executemany(_INSERT_OBSERVATIONS_STATEMENT, tuples)

  def insert_key_value(self, key: str, value: str):
    self.engine.execute(_INSERT_KEY_VALUE_STORE_STATEMENT, (key, value))

  def insert_import_info(self, status: ImportStatus):
    metadata = self._import_metadata()
    logging.info("Writing import: status = %s, metadata = %s", status.name,
                 metadata)
    self.engine.execute(
        _INSERT_IMPORTS_STATEMENT,
        (str(datetime.now()), status.name, json.dumps(metadata)))

  def commit_and_close(self):
    self.engine.commit_and_close()

  def select_triples_by_subject_type(self, subject_type: str) -> list[Triple]:
    tuples = self.engine.fetch_all(_SELECT_TRIPLES_BY_SUBJECT_TYPE,
                                   (subject_type,))
    return list(map(lambda tuple: from_triple_tuple(tuple), tuples))

  def select_entity_names(self, dcids: list[str]) -> dict[str, str]:
    if not dcids:
      return {}
    query = _SELECT_ENTITY_NAMES % ", ".join("?" * len(dcids))
    tuples = self.engine.fetch_all(query, dcids)
    return {dcid: name for dcid, name in tuples}

  def _import_metadata(self) -> dict:
    return {
        "numVars": len(self.variables),
        "numObs": self.num_observations,
    }


def from_triple_tuple(tuple: tuple) -> Triple:
  return Triple(*tuple)


class DbEngine:

  def init_or_update_tables(self):
    pass

  def clear_tables_and_indexes(self):
    pass

  def execute(self, sql: str, parameters=None):
    pass

  def executemany(self, sql: str, parameters=None):
    pass

  def fetch_all(self, sql: str, parameters=None) -> list[Any]:
    pass

  def commit_and_close(self):
    pass


_SQLITE_OBSERVATIONS_TABLE_INFO_STATEMENT = "pragma table_info(observations);"


class SqliteDbEngine(DbEngine):

  def __init__(self, db_params: dict) -> None:
    assert db_params
    assert SQLITE_DB_FILE in db_params

    self.db_temp_store = None
    self.db_final_file = None
    self.db_file = db_params[SQLITE_DB_FILE]
    if self.db_file.syspath() is None:
      logging.info("Copying DB to local storage from %s",
                   self.db_file.full_path())
      # Copy to disk
      self.db_temp_store = create_store("temp://")
      self.db_final_file = self.db_file
      self.db_file = self.db_temp_store.as_dir().open_file(
          "local.db", create_if_missing=True)
      self.db_final_file.copy_to(self.db_file)
      logging.info("Local copy of DB is %s", self.db_file.syspath())

    logging.info("Connecting to SQLite: %s", self.db_file.full_path())
    self.connection = sqlite3.connect(self.db_file.syspath())
    logging.info("Connected to SQLite: %s", self.db_file.full_path())

    self.cursor = self.connection.cursor()

  def _maybe_update_schema(self) -> None:
    """
    Add any sqlite schema updates here.
    Ensure that all schema updates always check if the update is necessary before applying it.
    """
    # Add property columns to observations table if it does not exist.
    rows = self.fetch_all(_SQLITE_OBSERVATIONS_TABLE_INFO_STATEMENT)
    existing_columns = set([columns[1] for columns in rows])
    if "properties" not in existing_columns:
      logging.info(
          f"properties column does not exist in the observations table. Altering table to the following property columns: {', '.join(_OBSERVATION_PROPERTY_COLUMNS)}"
      )
      for statement in _ALTER_OBSERVATIONS_TABLE_STATEMENTS:
        self.cursor.execute(statement)

  def _drop_indexes(self) -> None:
    for index in _DB_INDEXES:
      logging.info("Dropping index: %s", index.index_name)
      self.cursor.execute(index.sqlite_drop_index_statement())

  def _create_indexes(self) -> None:
    for index in _DB_INDEXES:
      logging.info("Creating index: %s", index.index_name)
      self.cursor.execute(index.sqlite_create_index_statement())
      logging.info("Index created: %s", index.index_name)

  def __str__(self) -> str:
    return f"{TYPE_SQLITE}: {self.db_file.full_path()}"

  def init_or_update_tables(self):
    for statement in _INIT_TABLE_STATEMENTS:
      self.cursor.execute(statement)
    self._maybe_update_schema()

  def clear_tables_and_indexes(self):
    for statement in _CLEAR_TABLE_FOR_IMPORT_STATEMENTS:
      self.cursor.execute(statement)

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

  def fetch_all(self, sql: str, parameters=None) -> list[Any]:
    if not parameters:
      return self.cursor.execute(sql).fetchall()
    else:
      return self.cursor.execute(sql, parameters).fetchall()

  def commit_and_close(self):
    # Create indexes before closing.
    self._create_indexes()
    self.connection.commit()
    self.connection.close()

    if self.db_temp_store:
      logging.info("Copying temp DB back to permanent storage")
      self.db_file.copy_to(self.db_final_file)
      self.db_temp_store.close()


# Parameters needed to connect to a Cloud SQL instance.
_CLOUD_MY_SQL_INSTANCE_CONNECT_PARAMS = [
    CLOUD_MY_SQL_USER, CLOUD_MY_SQL_PASSWORD
]

# Parameters needed to connect to a specific DB in a Cloud SQL instance.
_CLOUD_MY_SQL_DB_CONNECT_PARAMS = _CLOUD_MY_SQL_INSTANCE_CONNECT_PARAMS + [
    CLOUD_MY_SQL_DB
]

# All parameters that must be specified for connecting to Cloud SQL.
_CLOUD_MY_SQL_PARAMS = [CLOUD_MY_SQL_INSTANCE] + _CLOUD_MY_SQL_DB_CONNECT_PARAMS

_CLOUD_MYSQL_PROPERTIES_COLUMN_EXISTS_STATEMENT = """
  SELECT 1
    FROM INFORMATION_SCHEMA.COLUMNS
    WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'observations' AND COLUMN_NAME = 'properties';
"""


class CloudSqlDbEngine(DbEngine):

  def __init__(self, db_params: dict[str, str]) -> None:
    for param in _CLOUD_MY_SQL_PARAMS:
      assert param in db_params, f"{param} param not specified"
    connector = Connector()
    CloudSqlDbEngine._maybe_create_database(connector, db_params)
    kwargs = {
        param: db_params[param] for param in _CLOUD_MY_SQL_DB_CONNECT_PARAMS
    }
    logging.info("Connecting to Cloud MySQL: %s (%s)",
                 db_params[CLOUD_MY_SQL_INSTANCE], db_params[CLOUD_MY_SQL_DB])
    # Uses the pymysql driver to connect to the DB.
    self.connection: Connection = connector.connect(
        db_params[CLOUD_MY_SQL_INSTANCE], "pymysql", **kwargs)
    logging.info("Connected to Cloud MySQL: %s (%s)",
                 db_params[CLOUD_MY_SQL_INSTANCE], db_params[CLOUD_MY_SQL_DB])
    self.description = f"{TYPE_CLOUD_SQL}: {db_params[CLOUD_MY_SQL_INSTANCE]} ({db_params[CLOUD_MY_SQL_DB]})"
    self.cursor: Cursor = self.connection.cursor()

  def _maybe_update_schema(self) -> None:
    """
    Add any cloud sql schema updates here.
    Ensure that all schema updates always check if the update is necessary before applying it.
    """
    # Add properties column to observations table if it does not exist.
    rows = self.fetch_all(_CLOUD_MYSQL_PROPERTIES_COLUMN_EXISTS_STATEMENT)
    properties_column_exists = rows is not None and len(rows) > 0
    if not properties_column_exists:
      logging.info(
          f"properties column does not exist in the observations table. Altering table to the following property columns: {', '.join(_OBSERVATION_PROPERTY_COLUMNS)}"
      )
      for statement in _ALTER_OBSERVATIONS_TABLE_STATEMENTS:
        self.cursor.execute(statement)

  def _drop_indexes(self) -> None:
    for index in _DB_INDEXES:
      try:
        logging.info("Dropping index: %s", index.index_name)
        self.cursor.execute(index.mysql_drop_index_statement())
      except:
        logging.info("Index does not exist: %s", index.index_name)

  def _create_indexes(self) -> None:
    for index in _DB_INDEXES:
      logging.info("Creating index: %s", index.index_name)
      self.cursor.execute(index.mysql_create_index_statement())
      logging.info("Index created: %s", index.index_name)

  def _maybe_create_database(connector: Connector,
                             db_params: dict[str, str]) -> None:

    def _db_exists(cursor) -> bool:
      # Check if the database already exists.
      cursor.execute(
          f"SELECT COUNT(*) FROM INFORMATION_SCHEMA.SCHEMATA WHERE SCHEMA_NAME = '{db_name}'"
      )
      return cursor.fetchone()[0] > 0

    kwargs = {
        param: db_params[param]
        for param in _CLOUD_MY_SQL_INSTANCE_CONNECT_PARAMS
    }
    db_instance = db_params[CLOUD_MY_SQL_INSTANCE]
    db_name = db_params[CLOUD_MY_SQL_DB]
    logging.info(
        "Connecting to Cloud MySQL instance '%s' to check existence of DB '%s'.",
        db_instance, db_name)
    with connector.connect(db_instance, "pymysql", **kwargs) as conn:
      with conn.cursor() as cursor:
        if not _db_exists(cursor):
          cursor.execute(f"CREATE DATABASE {db_name}")
          conn.commit()
          logging.info(f"Database '{db_name}' created successfully.")
        else:
          logging.info(
              f"Database '{db_name}' already exists and will be used for this import."
          )

  def __str__(self) -> str:
    return self.description

  def init_or_update_tables(self):
    for statement in _INIT_TABLE_STATEMENTS:
      self.cursor.execute(statement)
    self._maybe_update_schema()

  def clear_tables_and_indexes(self):
    for statement in _CLEAR_TABLE_FOR_IMPORT_STATEMENTS:
      self.cursor.execute(statement)
    self._drop_indexes()

  def execute(self, sql: str, parameters=None):
    self.cursor.execute(_pymysql(sql), parameters)

  def executemany(self, sql: str, parameters=None):
    self.cursor.executemany(_pymysql(sql), parameters)

  def fetch_all(self, sql: str, parameters=None):
    self.cursor.execute(_pymysql(sql), parameters)
    return self.cursor.fetchall()

  def commit_and_close(self):
    # Create indexes before closing.
    self._create_indexes()
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


def create_and_update_db(config: dict) -> Db:
  """ Creates and initializes a Db, performing any setup and updates
  (e.g. table creation, table schema changes) that are needed.
  """
  db_type = config[FIELD_DB_TYPE]
  if db_type and db_type == TYPE_MAIN_DC:
    return MainDcDb(config)
  return SqlDb(config)


def create_sqlite_config(sqlite_db_file: File) -> dict:
  return {
      FIELD_DB_TYPE: TYPE_SQLITE,
      FIELD_DB_PARAMS: {
          SQLITE_DB_FILE: sqlite_db_file
      }
  }


def create_main_dc_config(output_dir: Dir) -> dict:
  return {FIELD_DB_TYPE: TYPE_MAIN_DC, MAIN_DC_OUTPUT_DIR: output_dir}


def get_sqlite_path_from_env() -> str | None:
  return os.getenv(ENV_SQLITE_PATH)


def get_cloud_sql_config_from_env() -> dict | None:
  if os.getenv(ENV_USE_CLOUDSQL, "").lower() != "true":
    return None

  db_instance = os.getenv(ENV_CLOUDSQL_INSTANCE)
  db_user = os.getenv(ENV_DB_USER)
  db_pass = os.getenv(ENV_DB_PASS)
  db_name = os.getenv(ENV_DB_NAME, CLOUD_MY_SQL_DEFAULT_DB_NAME)

  assert db_instance, f"Environment variable {ENV_CLOUDSQL_INSTANCE} not specified."
  assert db_user, f"Environment variable {ENV_DB_USER} not specified."
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
