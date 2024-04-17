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

from datetime import datetime
from enum import auto
from enum import Enum
import json
import logging
import os
import sqlite3
import tempfile
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
from util.filehandler import create_file_handler
from util.filehandler import is_gcs_path

FIELD_DB_TYPE = "type"
FIELD_DB_PARAMS = "params"
TYPE_CLOUD_SQL = "cloudsql"
TYPE_SQLITE = "sqlite"
TYPE_MAIN_DC = "maindc"

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

ENV_SQLITE_PATH = "SQLITE_PATH"

MAIN_DC_OUTPUT_DIR = "mainDcOutputDir"

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

_CREATE_TEMP_TRIPLES_TABLE = """
create table if not exists temp_triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
"""

_INSERT_TEMP_TRIPLES_STATEMENT = "insert into temp_triples values(?, ?, ?, ?)"

_DELETE_MATCHING_TRIPLES_STATEMENT = """
    DELETE FROM triples
    WHERE (subject_id, predicate) IN (
        SELECT subject_id, predicate FROM temp_triples
    )
"""

_INSERT_FROM_TEMP_TRIPLES_STATEMENT = """
    INSERT INTO triples
    SELECT * FROM temp_triples
"""

_DROP_TEMP_TRIPLES_TABLE = "DROP TABLE IF EXISTS temp_triples"

_CREATE_OBSERVATIONS_TABLE = """
create table if not exists observations (
    entity varchar(255),
    variable varchar(255),
    date varchar(255),
    value varchar(255),
    provenance varchar(255)
);
"""

_DELETE_OBSERVATIONS_STATEMENT = "delete from observations"
_INSERT_OBSERVATIONS_STATEMENT = "insert into observations values(?, ?, ?, ?, ?)"

_CREATE_TEMP_OBSERVATIONS_TABLE = """
create table if not exists temp_observations (
    entity varchar(255),
    variable varchar(255),
    date varchar(255),
    value varchar(255),
    provenance varchar(255)
);
"""

_INSERT_TEMP_OBSERVATIONS_STATEMENT = "insert into temp_observations values(?, ?, ?, ?, ?)"

_DELETE_MATCHING_OBSERVATIONS_STATEMENT = """
    DELETE FROM observations
    WHERE (entity, variable, date) IN (
        SELECT entity, variable, date FROM temp_observations
    )
"""

_INSERT_FROM_TEMP_OBSERVATIONS_STATEMENT = """
    INSERT INTO observations
    SELECT * FROM temp_observations
"""

_DROP_TEMP_OBSERVATIONS_TABLE = "DROP TABLE IF EXISTS temp_observations"

_CREATE_IMPORTS_TABLE = """
create table if not exists imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
"""

_INSERT_IMPORTS_STATEMENT = "insert into imports values(?, ?, ?)"

_SELECT_TRIPLES_BY_SUBJECT_TYPE = "select * from triples where subject_id in (select subject_id from triples where predicate = 'typeOf' and object_id = ?)"

_INIT_STATEMENTS = [
    _CREATE_TRIPLES_TABLE,
    _CREATE_OBSERVATIONS_TABLE,
    _CREATE_IMPORTS_TABLE,
    # Clearing tables for now (not the import tables though since we want to maintain its history).
    _DELETE_TRIPLES_STATEMENT,
    _DELETE_OBSERVATIONS_STATEMENT
]

_INIT_STATEMENTS_INCREMENTAL = [
    _CREATE_TRIPLES_TABLE,
    _CREATE_OBSERVATIONS_TABLE,
    _CREATE_IMPORTS_TABLE,
    _CREATE_TEMP_TRIPLES_TABLE,
    _CREATE_TEMP_OBSERVATIONS_TABLE,
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

_NAMESPACE_DELIMITER = ':'


class ImportStatus(Enum):
  SUCCESS = auto()
  FAILURE = auto()


class Db:
  """Abstract class to insert triples and observations into a DB.
  The "DB" could be a traditional sql db or a file system with the output being files.
  """

  def insert_triples(self, triples: list[Triple]):
    pass

  def insert_observations(self, observations: list[Observation],
                          input_file_name: str):
    pass

  def insert_import_info(self, status: ImportStatus):
    pass

  def commit_and_close(self):
    pass

  # Returns all triples of nodes with the specified "typeOf" predicate.
  def select_triples_by_subject_type(self, subject_type: str) -> list[Triple]:
    pass


class MainDcDb(Db):
  """Generates output for main DC.
  Observations will be output as TMCF + CSVs.
  Triples will be output as schema MCF.
  """

  def __init__(self, db_params: dict, incremental: bool) -> None:
    assert db_params
    assert MAIN_DC_OUTPUT_DIR in db_params
    assert not incremental, "Incremental mode not supported for main DC."

    self.output_dir_fh = create_file_handler(db_params[MAIN_DC_OUTPUT_DIR],
                                             is_dir=True)
    # dcid to node dict
    self.nodes: dict[str, McfNode] = {}

  def insert_triples(self, triples: list[Triple]):
    for triple in triples:
      self._add_triple(triple)

  def insert_observations(self, observations: list[Observation],
                          input_file_name: str):
    df = pd.DataFrame(observations)
    # Drop the provenance column. It is specified differently for main dc.
    df = df.drop(columns=["provenance"])
    self.output_dir_fh.make_file(input_file_name).write_string(
        df.to_csv(index=False))

  def insert_import_info(self, status: ImportStatus):
    # No-op for now.
    pass

  def commit_and_close(self):
    # MCF
    filtered = filter(lambda node: node.node_type in MCF_NODE_TYPES_ALLOWLIST,
                      self.nodes.values())
    mcf = "\n\n".join(map(lambda node: node.to_mcf(), filtered))
    self.output_dir_fh.make_file(SCHEMA_MCF_FILE_NAME).write_string(mcf)

    # TMCF
    self.output_dir_fh.make_file(OBSERVATIONS_TMCF_FILE_NAME).write_string(
        OBSERVATIONS_TMCF)

    # Not supported for main DC at this time.
    def select_triples_with_type_of(self, type_of: str) -> list[Triple]:
      return []

  def _add_triple(self, triple: Triple):
    node = self.nodes.get(triple.subject_id)
    if not node:
      node = McfNode(triple.subject_id)
      self.nodes[triple.subject_id] = node
    node.add_triple(triple)


class SqlDb(Db):
  """Class to insert triples and observations into a SQL DB."""

  def __init__(self, config: dict, incremental: bool) -> None:
    self.engine = create_db_engine(config)
    self.num_observations = 0
    self.variables: set[str] = set()
    self.incremental = incremental
    if self.incremental:
      for statement in _INIT_STATEMENTS_INCREMENTAL:
        self.engine.execute(statement)
    else:
      for statement in _INIT_STATEMENTS:
        self.engine.execute(statement)

  def insert_triples(self, triples: list[Triple]):
    logging.info("Writing %s triples to [%s]", len(triples), self.engine)
    if self.incremental:
      self.engine.executemany(_INSERT_TEMP_TRIPLES_STATEMENT,
                              [to_triple_tuple(triple) for triple in triples])
    else:
      self.engine.executemany(_INSERT_TRIPLES_STATEMENT,
                              [to_triple_tuple(triple) for triple in triples])

  def insert_observations(self, observations: list[Observation],
                          input_file_name: str):
    logging.info("Writing %s observations to [%s]", len(observations),
                 self.engine)
    self.num_observations += len(observations)
    tuples = []
    for observation in observations:
      tuples.append(to_observation_tuple(observation))
      self.variables.add(observation.variable)
    if self.incremental:
      self.engine.executemany(_INSERT_TEMP_OBSERVATIONS_STATEMENT, tuples)
    else:
      self.engine.executemany(_INSERT_OBSERVATIONS_STATEMENT, tuples)

  def insert_import_info(self, status: ImportStatus):
    metadata = self._import_metadata()
    logging.info("Writing import: status = %s, metadata = %s", status.name,
                 metadata)
    self.engine.execute(
        _INSERT_IMPORTS_STATEMENT,
        (str(datetime.now()), status.name, json.dumps(metadata)))

  def commit_and_close(self):
    if self.incremental:
      self._update_observations_and_triples()
    self.engine.commit_and_close()

  def select_triples_by_subject_type(self, subject_type: str) -> list[Triple]:
    tuples = self.engine.fetch_all(_SELECT_TRIPLES_BY_SUBJECT_TYPE,
                                   (subject_type,))
    return list(map(lambda tuple: from_triple_tuple(tuple), tuples))

  def _import_metadata(self) -> dict:
    return {
        "numVars": len(self.variables),
        "numObs": self.num_observations,
        "incremental": self.incremental,
    }

  def _update_observations_and_triples(self):
    self.engine.execute(_DELETE_MATCHING_OBSERVATIONS_STATEMENT)
    self.engine.execute(_INSERT_FROM_TEMP_OBSERVATIONS_STATEMENT)
    self.engine.execute(_DROP_TEMP_OBSERVATIONS_TABLE)

    self.engine.execute(_DELETE_MATCHING_TRIPLES_STATEMENT)
    self.engine.execute(_INSERT_FROM_TEMP_TRIPLES_STATEMENT)
    self.engine.execute(_DROP_TEMP_TRIPLES_TABLE)


def from_triple_tuple(tuple: tuple) -> Triple:
  return Triple(*tuple)


def to_triple_tuple(triple: Triple):
  return (_strip_namespace(triple.subject_id), triple.predicate,
          _strip_namespace(triple.object_id), triple.object_value)


def to_observation_tuple(observation: Observation):
  return (_strip_namespace(observation.entity),
          _strip_namespace(observation.variable), observation.date,
          observation.value, _strip_namespace(observation.provenance))


def _strip_namespace(v: str) -> str:
  return v[v.find(_NAMESPACE_DELIMITER) + 1:]


class DbEngine:

  def execute(self, sql: str, parameters=None):
    pass

  def executemany(self, sql: str, parameters=None):
    pass

  def fetch_all(self, sql: str, parameters=None) -> list[Any]:
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

    logging.info("Connecting to SQLite: %s", self.local_db_file_path)
    self.connection = sqlite3.connect(self.local_db_file_path)
    logging.info("Connected to SQLite: %s", self.local_db_file_path)

    self.cursor = self.connection.cursor()

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

  def fetch_all(self, sql: str, parameters=None) -> list[Any]:
    if not parameters:
      return self.cursor.execute(sql).fetchall()
    else:
      return self.cursor.execute(sql, parameters).fetchall()

  def commit_and_close(self):
    self.connection.commit()
    self.connection.close()
    # Copy file if local and actual DB file paths are different.
    if self.local_db_file_path != self.db_file_path:
      local_db = create_file_handler(self.local_db_file_path,
                                     is_dir=False).read_bytes()
      logging.info("Writing to sqlite db: %s (%s bytes)",
                   self.local_db_file_path, len(local_db))
      create_file_handler(self.db_file_path, is_dir=False).write_bytes(local_db)


_CLOUD_MY_SQL_CONNECT_PARAMS = [
    CLOUD_MY_SQL_USER, CLOUD_MY_SQL_PASSWORD, CLOUD_MY_SQL_DB
]
_CLOUD_MY_SQL_PARAMS = [CLOUD_MY_SQL_INSTANCE] + _CLOUD_MY_SQL_CONNECT_PARAMS


class CloudSqlDbEngine(DbEngine):

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

  def __str__(self) -> str:
    return self.description

  def execute(self, sql: str, parameters=None):
    self.cursor.execute(_pymysql(sql), parameters)

  def executemany(self, sql: str, parameters=None):
    self.cursor.executemany(_pymysql(sql), parameters)

  def fetch_all(self, sql: str, parameters=None):
    self.cursor.execute(_pymysql(sql), parameters)
    return self.cursor.fetchall()

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


def create_db(config: dict, incremental: bool) -> Db:
  db_type = config[FIELD_DB_TYPE]
  if db_type and db_type == TYPE_MAIN_DC:
    return MainDcDb(config, incremental)
  return SqlDb(config, incremental)


def create_sqlite_config(sqlite_db_file_path: str) -> dict:
  return {
      FIELD_DB_TYPE: TYPE_SQLITE,
      FIELD_DB_PARAMS: {
          SQLITE_DB_FILE_PATH: sqlite_db_file_path
      }
  }


def create_main_dc_config(output_dir: str) -> dict:
  return {FIELD_DB_TYPE: TYPE_MAIN_DC, MAIN_DC_OUTPUT_DIR: output_dir}


def get_sqlite_config_from_env() -> dict | None:
  sqlite_db_file_path = os.getenv(ENV_SQLITE_PATH)
  if not sqlite_db_file_path:
    return None
  return create_sqlite_config(sqlite_db_file_path)


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
