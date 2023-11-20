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

from stats.data import Observation
from stats.data import Triple
from util.filehandler import create_file_handler
from util.filehandler import is_gcs_path

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
  """Class to insert triples and observations into a sqlite DB."""

  def __init__(self, db_file_path: str) -> None:
    self.db_file_path = db_file_path
    # If file path is a GCS path, we create the DB in a local temp file
    # and upload to GCS on commit.
    self.local_db_file_path: str = db_file_path
    if is_gcs_path(db_file_path):
      self.local_db_file_path = tempfile.NamedTemporaryFile().name

    self.db = sqlite3.connect(self.local_db_file_path)
    for statement in _INIT_STATEMENTS:
      self.db.execute(statement)
    pass

  def insert_triples(self, triples: list[Triple]):
    with self.db:
      self.db.executemany(_INSERT_TRIPLES_STATEMENT,
                          [to_triple_tuple(triple) for triple in triples])

  def insert_observations(self, observations: list[Observation]):
    with self.db:
      self.db.executemany(
          _INSERT_OBSERVATIONS_STATEMENT,
          [to_observation_tuple(observation) for observation in observations])

  def commit_and_close(self):
    self.db.close()
    # Copy file if local and actual DB file paths are different.
    if self.local_db_file_path != self.db_file_path:
      local_db = create_file_handler(self.local_db_file_path).read_bytes()
      logging.info("Writing to sqlite db: %s (%s bytes)",
                   self.local_db_file_path, len(local_db))
      create_file_handler(self.db_file_path).write_bytes(local_db)


def to_triple_tuple(triple: Triple):
  return (triple.subject_id, triple.predicate, triple.object_id,
          triple.object_value)


def to_observation_tuple(observation: Observation):
  return (observation.entity, observation.variable, observation.date,
          observation.value, observation.provenance)
