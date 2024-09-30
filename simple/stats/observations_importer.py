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

import pandas as pd
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


class ObservationsImporter(Importer):
  """Imports a single observations input file.
    """

  def __init__(self, input_fh: FileHandler, db: Db,
               debug_resolve_fh: FileHandler, reporter: FileImportReporter,
               nodes: Nodes) -> None:
    self.input_fh = input_fh
    self.db = db
    self.debug_resolve_fh = debug_resolve_fh
    self.reporter = reporter
    self.nodes = nodes
    self.input_file_name = self.input_fh.basename()
    self.config = nodes.config
    self.entity_type = self.config.entity_type(self.input_file_name)
    self.ignore_columns = self.config.ignore_columns(self.input_file_name)
    # Reassign after reading CSV.
    self.entity_column_name = constants.COLUMN_DCID
    self.df = pd.DataFrame()
    self.debug_resolve_df = None

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      self._read_csv()
      self._drop_ignored_columns()
      self._sanitize_values()
      self._rename_columns()
      self._resolve_entities()
      self._add_entity_nodes()
      self._write_observations()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

    self._write_debug_csvs()

  def _read_csv(self) -> None:
    # Read CSVs with the following behaviors:
    # - Set 1st column (i.e. the entity column) to type str (so that geoIds like "01" are not treated as ints and converted to 1)
    # - Strip leading whitespaces
    # - Treat comma as a thousands separator
    self.df = pd.read_csv(self.input_fh.read_string_io(),
                          dtype={0: str},
                          skipinitialspace=True,
                          thousands=",")
    logging.info("Read %s rows.", self.df.index.size)
    self.entity_column_name = self.df.columns[0]
    logging.info("Entity column name: %s", self.entity_column_name)

  def _drop_ignored_columns(self):
    if self.ignore_columns:
      self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

  def _sanitize_values(self):
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()
    # Set date field to type str.
    self.df = self.df.astype({self.df.columns[1]: str})

  def _rename_columns(self) -> None:
    renamed = {}
    # Rename dcid and date columns
    renamed[self.df.columns[0]] = constants.COLUMN_DCID
    renamed[self.df.columns[1]] = constants.COLUMN_DATE

    # Rename SV columns to their IDs
    sv_column_names = self.df.columns[2:]
    sv_ids = [
        self.nodes.variable(sv_column_name, self.input_file_name).id
        for sv_column_name in sv_column_names
    ]
    renamed.update({col: id for col, id in zip(sv_column_names, sv_ids)})

    self.df = self.df.rename(columns=renamed)

  def _write_observations(self) -> None:
    # Melt dataframe so shape it similar to the observations table.
    # Convert all values to str first, otherwise it inserts ints as floats.
    observations_df = self.df.astype(str)
    observations_df = observations_df.melt(
        id_vars=[constants.COLUMN_DCID, constants.COLUMN_DATE],
        var_name=constants.COLUMN_VARIABLE,
        value_name=constants.COLUMN_VALUE,
    )

    provenance = self.nodes.provenance(self.input_file_name).id
    obs_props = ObservationProperties.new(
        self.config.observation_properties(self.input_file_name))

    observations: list[Observation] = []
    for _, row in observations_df.iterrows():
      observation = Observation(entity=row[constants.COLUMN_DCID],
                                variable=row[constants.COLUMN_VARIABLE],
                                date=row[constants.COLUMN_DATE],
                                value=row[constants.COLUMN_VALUE],
                                provenance=provenance,
                                properties=obs_props)
      if observation.value and observation.value != "<NA>":
        observations.append(observation)
    self.db.insert_observations(observations, self.input_file_name)

  def _add_entity_nodes(self) -> None:
    # Convert entity dcids to dict.
    # Using dict instead of set to maintain insertion order which keeps results consistent for tests.
    entity_dcids: dict[str, bool] = {
        dcid: True for dcid in self.df.iloc[:, 0].tolist()
    }

    # Get entity nodes that are not already recorded.
    new_entity_dcids = [
        dcid for dcid in entity_dcids if dcid not in self.nodes.entities.keys()
    ]

    logging.info("Found %s total entities, of which %s are already imported.",
                 len(entity_dcids),
                 len(entity_dcids) - len(new_entity_dcids))

    if not new_entity_dcids:
      return

    # Get entity types
    dcid2type: dict[str,
                    str] = dc.get_property_of_entities(new_entity_dcids,
                                                       sc.PREDICATE_TYPE_OF)

    if dcid2type:
      logging.info("Importing %s of %s entities.", len(dcid2type),
                   len(new_entity_dcids))
      self.nodes.entities_with_types(dcid2type)
    elif self.entity_type:
      logging.info("Importing %s entities with type %s.", len(new_entity_dcids),
                   self.entity_type)
      self.nodes.entities_with_type(new_entity_dcids, self.entity_type)

  def _resolve_entities(self) -> None:
    df = self.df
    # get entity column
    column = df[constants.COLUMN_DCID]

    pre_resolved_entities = {}

    def remove_pre_resolved(entity: str) -> bool:
      if entity.startswith(constants.DCID_OVERRIDE_PREFIX):
        pre_resolved_entities[entity] = entity[
            len(constants.DCID_OVERRIDE_PREFIX):].strip()
        return False
      return True

    entities = list(filter(remove_pre_resolved, column.tolist()))

    logging.info("Found %s entities pre-resolved.", len(pre_resolved_entities))

    logging.info("Resolving %s entities of type %s.", len(entities),
                 self.entity_type)
    dcids = self._resolve(entities=entities)
    logging.info("Resolved %s of %s entities.", len(dcids), len(entities))

    # Replace resolved entities.
    # NOTE: column.map performs much better than column.replace, hence using the former.
    column = column.map(lambda x: dcids.get(x, x))
    unresolved = set(entities).difference(set(dcids.keys()))
    unresolved_list = sorted(list(unresolved))

    # Replace pre-resolved entities without the "dcid:" prefix.
    column = column.map(lambda x: pre_resolved_entities.get(x, x))

    df[constants.COLUMN_DCID] = column
    if unresolved_list:
      logging.warning("# unresolved entities which will be dropped: %s",
                      len(unresolved_list))
      logging.warning("Dropped entities: %s", unresolved_list)
      df.drop(df[df.iloc[:, 0].isin(values=unresolved_list)].index,
              inplace=True)
    self._create_debug_resolve_dataframe(
        resolved=dcids,
        pre_resolved=pre_resolved_entities,
        unresolved=unresolved_list,
    )

  def _resolve(self, entities: list[str]) -> dict[str, str]:
    lower_case_entity_name = self.entity_column_name.lower()

    # Check if the entities can be resolved locally.
    # If so, return them by prefixing the values as appropriate.
    if lower_case_entity_name in constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES:
      prefix = constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES[
          lower_case_entity_name]
      return dict([(entity, f"{prefix}{entity}") for entity in entities])

    # Resolve entities externally.
    property_name = constants.EXTERNALLY_RESOLVED_INPUT_COLUMNS_TO_PREFIXES.get(
        lower_case_entity_name, constants.PROPERTY_DESCRIPTION)
    return dc.resolve_entities(entities=entities,
                               entity_type=self.entity_type,
                               property_name=property_name)

  def _create_debug_resolve_dataframe(
      self,
      resolved: dict[str, str],
      pre_resolved: dict[str, str],
      unresolved: list[str],
  ):
    # Add unresolved inputs first
    inputs = unresolved[:]
    dcids = [constants.DEBUG_UNRESOLVED_DCID] * len(unresolved)

    # Add pre-resolved next.
    inputs.extend(list(pre_resolved.keys()))
    dcids.extend(list(pre_resolved.values()))

    # Add resolved inputs and dcids
    inputs.extend(list(resolved.keys()))
    dcids.extend(list(resolved.values()))

    # Create browser links
    links = []
    for dcid in dcids:
      if dcid == constants.DEBUG_UNRESOLVED_DCID:
        links.append("")
      else:
        links.append(f"{constants.DC_BROWSER}/{dcid}")

    # Create dataframe
    self.debug_resolve_df = pd.DataFrame({
        constants.DEBUG_COLUMN_INPUT: inputs,
        constants.DEBUG_COLUMN_DCID: dcids,
        constants.DEBUG_COLUMN_LINK: links,
    })

  def _write_debug_csvs(self) -> None:
    if self.debug_resolve_df is not None:
      logging.info("Writing resolutions (for debugging) to: %s",
                   self.debug_resolve_fh)
      self.debug_resolve_fh.write_string(
          self.debug_resolve_df.to_csv(index=False))
