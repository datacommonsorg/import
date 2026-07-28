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

import logging

from dateutil.parser import parse as date_parse
import pandas as pd
from stats import constants
from stats.data import AggregationConfig
from stats.data import Event
from stats.data import filter_invalid_observation_values
from stats.data import strip_namespace
from stats.data import strip_namespace_series
from stats.data import TimePeriod
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import get_namespace_prefix_and_suffix
from stats.util import has_namespace_prefix
from util.filesystem import File

from util import dc_client as dc

# Number of entity IDs that will be sampled to resolved their entity type, if one is not specified by the user.
# Note that the importer assumes that all entities in a given CSV are all of the same type.
_SAMPLE_ENTITY_RESOLUTION_SIZE = 5


class EventsImporter(Importer):
  """Imports a single events input file.
    """

  def __init__(self, input_file: File, db: Db, debug_resolve_file: File,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_file = input_file
    self.db = db
    self.debug_resolve_file = debug_resolve_file
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.entity_type = self.config.entity_type(self.input_file)
    self.ignore_columns = self.config.ignore_columns(self.input_file)
    self.provenance = self.nodes.provenance(self.input_file).id
    # Reassign after reading CSV.
    self.entity_column_name = constants.COLUMN_DCID

    self.event_type = self.config.event_type(self.input_file)
    assert self.event_type, f"Event type must be specified: {self.input_file.full_path()}"

    self.id_column = self.config.id_column(self.input_file)

    self.df = pd.DataFrame()
    self.debug_resolve_df = None
    self.all_unresolved_entities: set[str] = set()

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      self._read_csv()
      self._drop_ignored_columns()
      self._sanitize_values()
      self._resolve_entities()
      self._rename_columns()

      self.check_and_report_unresolved_entities(self.all_unresolved_entities)

      self._write_event_triples()
      self._write_observations()
      self.reporter.report_success()
    except Exception as e:
      self._write_debug_csvs()
      self.reporter.report_failure(str(e))
      raise e

    self._write_debug_csvs()

  def _read_csv(self) -> None:
    # Read CSVs with the following behaviors:
    # - Set 1st column (i.e. the entity column) to type str (so that geoIds like "01" are not treated as ints and converted to 1)
    # - Strip leading whitespaces
    # - Treat comma as a thousands separator
    self.df = pd.read_csv(self.input_file.read_string_io(),
                          dtype={0: str},
                          skipinitialspace=True,
                          thousands=",",
                          na_values=constants.STANDARD_NA_VALUES)
    logging.info("Read %s rows.", self.df.index.size)
    mappings = self.get_column_mappings()
    self.entity_column_name = mappings.get("dcid:location", "")
    self.date_column_name = mappings.get("dcid:observationDate", "")
    if self.entity_column_name:
      logging.info("Entity column name: %s", self.entity_column_name)

  def _drop_ignored_columns(self):
    if self.ignore_columns:
      self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

  def _sanitize_values(self):
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()
    # Set date field to type str.
    if self.date_column_name and self.date_column_name in self.df.columns:
      self.df = self.df.astype({self.date_column_name: str})

  def _rename_columns(self) -> None:
    renamed = {}
    reverse_mappings = self.get_reverse_column_mappings()

    # Rename dcid and date columns if present
    if self.entity_column_name:
      renamed[self.entity_column_name] = constants.COLUMN_DCID
    if self.date_column_name:
      renamed[self.date_column_name] = constants.COLUMN_DATE

    # Rename property columns to their IDs
    for col in self.df.columns:
      if col == self.entity_column_name or col == self.date_column_name:
        continue
      prop_id = reverse_mappings.get(col, self.nodes.property(col).dcid)
      if col == self.id_column:
        continue
      renamed[col] = prop_id

    self.df = self.df.rename(columns=renamed)

  def _write_observations(self) -> None:
    sv_names = self.config.computed_variables(self.input_file)
    if not sv_names:
      logging.warning("No computed variables specified: %s",
                      self.input_file.full_path())
      return

    for sv_name in sv_names:
      sv_dcid = self.nodes.variable(sv_name, self.input_file).id
      aggr_cfg = self.config.aggregation(sv_name)
      observations = self._compute_sv_observations(sv_dcid, aggr_cfg)
      self.db.insert_observations(observations, self.input_file)

  def _compute_sv_observations(
      self, sv_dcid: str, aggr_cfg: AggregationConfig = AggregationConfig()
  ) -> pd.DataFrame:
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

    # Rename dcid column to entity
    obs_df = obs_df.rename(
        columns={constants.COLUMN_DCID: constants.COLUMN_ENTITY})

    # Convert value to string
    obs_df[constants.COLUMN_VALUE] = obs_df[constants.COLUMN_VALUE].astype(str)

    # Filter out invalid values
    obs_df = filter_invalid_observation_values(obs_df)

    # Add property columns with default values
    for col in constants.OBSERVATION_PROPERTY_COLUMNS:
      obs_df[col] = ""

    # Strip namespaces from string columns
    for col in constants.COLUMNS_TO_STRIP_NAMESPACES:
      obs_df[col] = strip_namespace_series(obs_df[col])

    # Reorder columns to match database schema
    obs_df = obs_df[constants.OBSERVATION_COLUMNS]

    return obs_df

  def _write_event_triples(self) -> None:
    # Add event type node - it will be written to DB later.
    # This is to avoid duplicate event types in scenarios where events of the same type
    # are spread across files.
    self.nodes.event_type(self.event_type, self.input_file)

    # All property columns would've been renamed to their dcids by now.
    # So use the id column's dcid as the id column name.
    id_column_name = self.nodes.property(
        self.id_column).dcid if self.id_column else ""

    triples: list[Triple] = []
    for index, row in self.df.iterrows():
      if self.id_column:
        if id_column_name and id_column_name in row:
          dcid = row[id_column_name]
        elif self.id_column in row:
          dcid = row[self.id_column]
        else:
          raise ValueError(
              f"Configured idColumn '{self.id_column}' not found in CSV file '{self.input_file.path}'"
          )
      else:
        dcid = f"{self.event_type}_{index}"

      # TODO: Add row-level validation to reject rows with empty/invalid location (entity)
      # or observationDate values rather than silently omitting those triples.
      entity_val = row.get(constants.COLUMN_DCID, "")
      entity = ("" if pd.isna(entity_val) or str(entity_val)
                in ("<NA>", "nan", "") else str(entity_val).strip())
      date_val = row.get(constants.COLUMN_DATE, "")
      date = ("" if pd.isna(date_val) or str(date_val) in ("<NA>", "nan", "")
              else str(date_val).strip())
      properties: dict[str, str] = {}

      for k, v in row.items():
        if (k in (
            constants.COLUMN_DCID,
            constants.COLUMN_DATE,
            self.id_column,
            id_column_name,
        ) or pd.isna(v) or str(v) in ("<NA>", "nan", "")):
          continue
        properties[k] = v

      event = Event(dcid,
                    self.event_type,
                    entity=entity,
                    date=date,
                    provenance_id=self.provenance,
                    properties=properties)
      triples.extend(event.triples())

    self.db.insert_triples(triples, self.input_file)

  def _resolve_entities(self) -> None:
    self.df = self.resolve_specified_columns(self.df)

    prov_id = getattr(self, "provenance", "")
    entity_col = (constants.COLUMN_DCID
                  if constants.COLUMN_DCID in self.df.columns else getattr(
                      self, "entity_column_name", None))
    if entity_col and entity_col in self.df.columns:
      for dcid in self.df[entity_col].dropna().unique():
        clean_dcid = strip_namespace(dcid)
        if self.nodes.has_entity(clean_dcid):
          if prov_id:
            self.nodes.entities[clean_dcid].provenance_ids.add(prov_id)
        elif prov_id:
          self.nodes.entity_with_type(
              clean_dcid,
              self.entity_type or "Thing",
              provenance_id=prov_id,
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
    new_df = pd.DataFrame({
        constants.DEBUG_COLUMN_INPUT: inputs,
        constants.DEBUG_COLUMN_DCID: dcids,
        constants.DEBUG_COLUMN_LINK: links,
    })
    if self.debug_resolve_df is None:
      self.debug_resolve_df = new_df
    else:
      self.debug_resolve_df = pd.concat([self.debug_resolve_df, new_df],
                                        ignore_index=True).drop_duplicates()

  def _write_debug_csvs(self) -> None:
    if self.debug_resolve_df is not None:
      logging.info("Writing resolutions (for debugging) to: %s",
                   self.debug_resolve_file)
      self.debug_resolve_file.write(self.debug_resolve_df.to_csv(index=False))


# Utility methods
def _time_period(date_str: str, period: TimePeriod) -> str:
  date = date_parse(date_str)
  if period == TimePeriod.DAY:
    return date.strftime("%Y-%m-%d")
  if period == TimePeriod.YEAR:
    return date.strftime("%Y")
  # Default to month
  return date.strftime("%Y-%m")
