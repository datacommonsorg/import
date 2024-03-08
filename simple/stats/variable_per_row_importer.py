# Copyright 2024 Google Inc.
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

from csv import DictReader
import logging
import random

from stats import constants
from stats.data import Observation
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filehandler import FileHandler

_COLUMNS = [
    constants.COLUMN_ENTITY, constants.COLUMN_VARIABLE, constants.COLUMN_DATE,
    constants.COLUMN_VALUE
]
_DEFAULT_COLUMN_MAPPINGS = {x: x for x in _COLUMNS}


class VariablePerRowImporter(Importer):
  """Imports a single observations input file where variables are specified in rows (aka "SVObs").
  This is in contrast to the ObservationsImporter where variables are specified in columns.

  Currently this importer only writes observations and no entities.
  It also does not resolve any entities and expects all entities to be pre-resolved. 
    """

  def __init__(self, input_fh: FileHandler, db: Db,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_fh = input_fh
    self.db = db
    self.reporter = reporter
    self.input_file_name = self.input_fh.basename()
    self.nodes = nodes
    self.config = nodes.config
    # Reassign after reading CSV.
    self.column_mappings = dict(_DEFAULT_COLUMN_MAPPINGS)
    self.reader: DictReader = None

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      self._read_csv()
      self._map_columns()
      self._write_observations()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def _read_csv(self) -> None:
    self.reader = DictReader(self.input_fh.read_string_io())

  def _map_columns(self):
    config_mappings = self.config.column_mappings(self.input_file_name)
    for key in self.column_mappings.keys():
      if key in config_mappings:
        self.column_mappings[key] = config_mappings[key]

    # Ensure that the expected columns exist.
    expected_column_names = set(self.column_mappings.values())
    logging.info("Expected column names: %s", expected_column_names)
    actual_column_names = set(self.reader.fieldnames)
    logging.info("Actual column names: %s", actual_column_names)
    difference = expected_column_names - actual_column_names
    if difference:
      raise ValueError(
          f"The following expected columns were not found: {difference}. You can specify column mappings using the columnMappings field."
      )

  def _write_observations(self) -> None:
    provenance = self.nodes.provenance(self.input_file_name).id
    observations: list[Observation] = []
    for row in self.reader:
      observation = Observation(
          entity=row[self.column_mappings[constants.COLUMN_ENTITY]],
          variable=row[self.column_mappings[constants.COLUMN_VARIABLE]],
          date=row[self.column_mappings[constants.COLUMN_DATE]],
          value=row[self.column_mappings[constants.COLUMN_VALUE]],
          provenance=provenance)
      observations.append(observation)
    self.db.insert_observations(observations, self.input_file_name)
