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

from stats import constants
from stats import schema_constants as sc
from stats.data import Observation
from stats.data import ObservationProperties
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filehandler import FileHandler

from util import dc_client as dc

# Columns for standard observation properties.
# These are optional.
_OBS_PROPERTY_COLUMNS = [
    sc.PREDICATE_UNIT,
    sc.PREDICATE_SCALING_FACTOR,
    sc.PREDICATE_MEASUREMENT_METHOD,
    sc.PREDICATE_OBSERVATION_PERIOD,
]
_REQUIRED_COLUMNS = [
    constants.COLUMN_ENTITY,
    constants.COLUMN_VARIABLE,
    constants.COLUMN_DATE,
    constants.COLUMN_VALUE,
]
_DEFAULT_COLUMN_MAPPINGS = {x: x for x in _REQUIRED_COLUMNS}


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
    # Unique entity IDs seen in this CSV.
    # Using dict instead of set to maintain insertion order which keeps results consistent for tests.
    self.entity_dcids: dict[str, bool] = {}

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      self._read_csv()
      self._map_columns()
      self._write_observations()
      self._add_entity_nodes()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def _read_csv(self) -> None:
    self.reader = DictReader(self.input_fh.read_string_io())

  def _map_columns(self):
    config_mappings = self.config.column_mappings(self.input_file_name)

    # Required columns.
    for key in self.column_mappings.keys():
      if key in config_mappings:
        self.column_mappings[key] = config_mappings[key]

    # Optional property column mappings.
    for key in _OBS_PROPERTY_COLUMNS:
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
    obs_props = ObservationProperties.new(
        self.config.observation_properties(self.input_file_name))

    observations: list[Observation] = []
    for row in self.reader:
      entity_dcid = row[self.column_mappings[constants.COLUMN_ENTITY]]
      row_obs_props = ObservationProperties.new(
          all_properties=self._get_row_obs(row), default_obs_props=obs_props)
      observation = Observation(
          entity=entity_dcid,
          variable=row[self.column_mappings[constants.COLUMN_VARIABLE]],
          date=row[self.column_mappings[constants.COLUMN_DATE]],
          value=row[self.column_mappings[constants.COLUMN_VALUE]],
          provenance=provenance,
          properties=row_obs_props)
      observations.append(observation)
      self.entity_dcids[entity_dcid] = True
    self.db.insert_observations(observations, self.input_file_name)

  def _get_row_obs(self, row: dict[str, str]) -> dict[str, str]:
    properties: dict[str, str] = {}
    for prop in _OBS_PROPERTY_COLUMNS:
      if prop in self.column_mappings:
        properties[prop] = row[self.column_mappings[prop]]
    return properties

  def _add_entity_nodes(self) -> None:
    # Get entity nodes that are not already recorded.
    new_entity_dcids = [
        dcid for dcid in self.entity_dcids
        if dcid not in self.nodes.entities.keys()
    ]

    logging.info("Found %s total entities, of which %s are already imported.",
                 len(self.entity_dcids),
                 len(self.entity_dcids) - len(new_entity_dcids))

    if not new_entity_dcids:
      return

    # Get entity types
    logging.info("Getting entity types from DC for %s entities.",
                 len(new_entity_dcids))
    dcid2type: dict[str,
                    str] = dc.get_property_of_entities(new_entity_dcids,
                                                       sc.PREDICATE_TYPE_OF)

    if dcid2type:
      logging.info("Importing %s of %s entities.", len(dcid2type),
                   len(new_entity_dcids))
      self.nodes.entities_with_types(dcid2type)
