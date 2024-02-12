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

from datetime import datetime
import logging
import random

from dateutil.parser import parse as date_parse
import pandas as pd
from stats import constants
from stats.data import AggregationConfig
from stats.data import AggregationMethod
from stats.data import Event
from stats.data import Observation
from stats.data import TimePeriod
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filehandler import FileHandler

from util import dc_client as dc

# Number of entity IDs that will be sampled to resolved their entity type, if one is not specified by the user.
# Note that the importer assumes that all entities in a given CSV are all of the same type.
_SAMPLE_ENTITY_RESOLUTION_SIZE = 5


class EventsImporter(Importer):
  """Imports a single events input file.
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
    self.provenance = self.nodes.provenance(self.input_file_name).id
    # Reassign after reading CSV.
    self.entity_column_name = constants.COLUMN_DCID

    self.event_type = self.config.event_type(self.input_file_name)
    assert self.event_type, f"Event type must be specified: {self.input_file_name}"

    self.id_column = self.config.id_column(self.input_file_name)

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
      self._write_event_triples()
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

    # Rename property columns to their IDs
    property_column_names = self.df.columns[2:]
    property_ids = [
        self.nodes.property(property_column_name).dcid
        for property_column_name in property_column_names
    ]
    renamed.update({
        col: id for col, id in zip(property_column_names, property_ids)
    })

    self.df = self.df.rename(columns=renamed)

  def _write_observations(self) -> None:
    sv_names = self.config.computed_variables(self.input_file_name)
    if not sv_names:
      logging.warning("No computed variables specified: %s",
                      self.input_file_name)
      return

    for sv_name in sv_names:
      sv_dcid = self.nodes.variable(sv_name, self.input_file_name).id
      aggr_cfg = self.config.aggregation(sv_name)
      observations = self._compute_sv_observations(sv_dcid, aggr_cfg)
      self.db.insert_observations(observations, self.input_file_name)

  def _compute_sv_observations(
      self, sv_dcid: str, aggr_cfg: AggregationConfig = AggregationConfig()
  ) -> list[Observation]:
    # Create df with only dcid and date columns.
    obs_df = self.df.loc[:, [constants.COLUMN_DCID, constants.COLUMN_DATE]]

    # Convert date to aggregation period
    obs_df[constants.COLUMN_DATE] = obs_df[constants.COLUMN_DATE].apply(
        lambda x: _time_period(x, aggr_cfg.period))

    # Group by entity (dcid) and date, count each group and drop duplicates.
    # NOTE: currently we only support count per entity and date.
    # The groupby columns and transform functions will need to change
    # when we add support for more aggregation methods (sum, average, etc.)
    obs_df[constants.COLUMN_VALUE] = obs_df.groupby(
        [constants.COLUMN_DCID,
         constants.COLUMN_DATE])[constants.COLUMN_DCID].transform("count")
    obs_df.drop_duplicates(inplace=True, ignore_index=True)

    # Add variable and provenance columns.
    obs_df[constants.COLUMN_VARIABLE] = sv_dcid
    obs_df[constants.COLUMN_PROVENANCE] = self.provenance

    # Reorder columns so they are in the same order as observations
    obs_df = obs_df.reindex(columns=[
        constants.COLUMN_DCID, constants.COLUMN_VARIABLE, constants.COLUMN_DATE,
        constants.COLUMN_VALUE, constants.COLUMN_PROVENANCE
    ])

    # Map each row to an Observation object and return the list of observations.
    return [Observation(*row) for row in obs_df.itertuples(index=False)]

  def _write_event_triples(self) -> None:
    # Add event type node - it will be written to DB later.
    # This is to avoid duplicate event types in scenarios where events of the same type
    # are spread across files.
    self.nodes.event_type(self.event_type, self.input_file_name)

    # All property columns would've been renamed to their dcids by now.
    # So use the id column's dcid as the id column name.
    id_column_name = self.nodes.property(
        self.id_column).dcid if self.id_column else ""

    triples: list[Triple] = []
    for index, row in self.df.iterrows():
      # If id column is configured, use it as the event dcid else generate based on row index.
      dcid = row[
          id_column_name] if id_column_name else f"{self.event_type}_{index}"

      entity = row.iloc[0]
      date = row.iloc[1]
      properties: dict[str, str] = {}

      for i, (k, v) in enumerate(row.items()):
        if i < 2 or pd.isna(v):
          continue
        properties[k] = v

      event = Event(dcid,
                    self.event_type,
                    entity=entity,
                    date=date,
                    provenance_id=self.provenance,
                    properties=properties)
      triples.extend(event.triples())

    self.db.insert_triples(triples)

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
    logging.info("Replaced %s pre-resolved entities.",
                 len(pre_resolved_entities))

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


# Utility methods
def _time_period(date_str: str, period: TimePeriod) -> str:
  date = date_parse(date_str)
  if period == TimePeriod.DAY:
    return date.strftime("%Y-%m-%d")
  if period == TimePeriod.YEAR:
    return date.strftime("%Y")
  # Default to month
  return date.strftime("%Y-%m")
