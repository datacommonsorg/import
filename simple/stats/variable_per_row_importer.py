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

import json
import logging

import numpy as np
import pandas as pd
from stats import constants
from stats import schema_constants as sc
from stats.data import filter_invalid_observation_values
from stats.data import ObservationProperties
from stats.data import strip_namespace
from stats.data import strip_namespace_series
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filesystem import File

from util import dc_client as dc

# Columns for standard observation properties.
# These are optional.
_OBS_PROPERTY_COLUMNS = [
    sc.PREDICATE_UNIT,
    sc.PREDICATE_SCALING_FACTOR,
    sc.PREDICATE_MEASUREMENT_METHOD,
    sc.PREDICATE_OBSERVATION_PERIOD,
]

# Mapping from official DCID keys in config.json to internal importer columns.
STANDARD_PROPERTY_MAPPING = {
    "dcid:variableMeasured": constants.COLUMN_VARIABLE,  # "variable"
    "dcid:observationDate": constants.COLUMN_DATE,  # "date"
    "dcid:value": constants.COLUMN_VALUE,  # "value"
    "dcid:unit": constants.COLUMN_UNIT,  # "unit"
    "dcid:measurementMethod":
        constants.COLUMN_MEASUREMENT_METHOD,  # "measurement_method"
    "dcid:observationPeriod":
        constants.COLUMN_OBSERVATION_PERIOD,  # "observation_period"
}


def _convert_numeric_to_string(col: pd.Series,
                               default_for_na: str = "") -> pd.Series:
  """Convert numeric column to string, preserving integer format.

  Args:
      col: Pandas Series that may contain numeric values, NaN, or strings
      default_for_na: Value to use for NaN entries (default is empty string)

  Returns:
      Series with all values converted to strings
  """

  # If not numeric, just convert to string
  if not pd.api.types.is_numeric_dtype(col):
    return col.astype(str)

    # For numeric columns, preserve integer format
  is_int_value = col.notna() & (col == col.round())
  is_na = col.isna()

  return np.where(
      is_int_value,
      col.round().astype("Int64").astype(str),
      np.where(is_na, default_for_na, col.astype(str)),
  )


def _apply_property_defaults(df: pd.DataFrame,
                             obs_props: ObservationProperties) -> pd.DataFrame:
  """Apply property defaults, using per-row values where available."""
  property_mapping = {
      sc.PREDICATE_UNIT: constants.COLUMN_UNIT,
      sc.PREDICATE_SCALING_FACTOR: constants.COLUMN_SCALING_FACTOR,
      sc.PREDICATE_MEASUREMENT_METHOD: constants.COLUMN_MEASUREMENT_METHOD,
      sc.PREDICATE_OBSERVATION_PERIOD: constants.COLUMN_OBSERVATION_PERIOD,
  }

  for prop, col_name in property_mapping.items():
    default_value = getattr(obs_props, col_name, "")
    if prop in df.columns:
      # Replace empty strings with NaN for consistent handling
      source_col = df[prop].replace("", pd.NA)

      # Check if source is numeric before filling (to preserve int format for numeric columns)
      is_source_numeric = pd.api.types.is_numeric_dtype(source_col)

      if is_source_numeric:
        df[col_name] = _convert_numeric_to_string(source_col,
                                                  default_for_na=default_value)
      else:
        df[col_name] = source_col.fillna(default_value).astype(str)

        # Drop the original property column (if different from target)
      if prop != col_name:
        df = df.drop(columns=[prop])
    else:
      # If the column doesn't exist, use default for all rows
      df[col_name] = default_value

    # Custom properties column (always empty for variable_per_row_importer)
  df[constants.COLUMN_PROPERTIES] = ""
  return df


def _format_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
  """Convert value column to string, preserving integer format."""
  df[constants.COLUMN_VALUE] = _convert_numeric_to_string(
      df[constants.COLUMN_VALUE])
  return df


def _strip_namespaces(df: pd.DataFrame, provenance: str) -> pd.DataFrame:
  """Strip namespace prefixes from DCID columns."""

  # Strip namespaces from columns that exist
  for col in constants.COLUMNS_TO_STRIP_NAMESPACES:
    if col == constants.COLUMN_PROVENANCE:
      # Provenance comes from parameter, not from dataframe
      df[constants.COLUMN_PROVENANCE] = strip_namespace(provenance)
    elif col in df.columns:
      df[col] = strip_namespace_series(df[col])

  return df


class VariablePerRowImporter(Importer):
  """Imports a single observations input file where variables are specified in rows (aka "SVObs").
  This is in contrast to the ObservationsImporter where variables are specified in columns.

  Currently this importer only writes observations and no entities.
  It also does not resolve any entities and expects all entities to be pre-resolved.
  """

  def __init__(self, input_file: File, db: Db, reporter: FileImportReporter,
               nodes: Nodes) -> None:
    self.input_file = input_file
    self.db = db
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.custom_dimensions = []
    self.column_mappings = {}
    self.df = pd.DataFrame()
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
    self.df = pd.read_csv(self.input_file.read_string_io())

  def _map_columns(self):
    config_mappings = self.config.column_mappings(self.input_file)

    if not config_mappings:
      # Legacy fallback: if no column mappings are specified, assume standard headers
      config_mappings = {
          "dcid:observationAbout": "entity",
          "dcid:variableMeasured": "variable",
          "dcid:observationDate": "date",
          "dcid:value": "value",
      }

    self.column_mappings = {}
    self.custom_dimensions = []

    # Map column mappings to internal names and identify custom dimensions
    for key, physical_col in config_mappings.items():
      if key in STANDARD_PROPERTY_MAPPING:
        internal_col = STANDARD_PROPERTY_MAPPING[key]
        self.column_mappings[internal_col] = physical_col
      else:
        # It's a custom dimension!
        self.custom_dimensions.append(key)
        self.column_mappings[key] = physical_col

    # 1. Validate strictly required columns
    for req_col in [
        constants.COLUMN_VARIABLE, constants.COLUMN_DATE, constants.COLUMN_VALUE
    ]:
      if req_col not in self.column_mappings:
        # Find the official DCID key for the error message
        official_key = [
            k for k, v in STANDARD_PROPERTY_MAPPING.items() if v == req_col
        ][0]
        raise ValueError(
            f"Missing required column mapping for: '{official_key}'")

    # 2. Validate entity dimensions count (1 to 3 allowed)
    # Since dcid:observationAbout is now treated as a custom dimension, all entity dimensions are in custom_dimensions
    entity_dims_count = len(self.custom_dimensions)

    if entity_dims_count < 1:
      raise ValueError(
          "Invalid configuration: An observation must have at least one entity dimension. "
          "Please map 'dcid:observationAbout' or map at least one custom dimension in 'columnMappings'."
      )
    if entity_dims_count > 3:
      raise ValueError(
          f"Invalid configuration: Too many entity dimensions mapped ({entity_dims_count}). "
          "A maximum of 3 entity dimensions (including 'dcid:observationAbout') is allowed."
      )

    # 3. Verify that the physical columns actually exist in the CSV DataFrame
    expected_column_names = set(self.column_mappings.values())
    actual_column_names = set(self.df.columns)
    difference = expected_column_names - actual_column_names
    if difference:
      logging.info("Expected column names: %s", expected_column_names)
      logging.info("Actual column names: %s", actual_column_names)
      raise ValueError(
          f"The following expected columns were not found in the CSV: {difference}. "
          f"Please check your 'columnMappings' and the CSV header.")

  def _write_observations(self) -> None:
    provenance = self.nodes.provenance(self.input_file).id
    obs_props = ObservationProperties.new(
        self.config.observation_properties(self.input_file))

    # Prepare observations dataframe
    observations_df = (self._apply_column_mappings(self.df).pipe(
        self._track_entity_dcids).pipe(
            _apply_property_defaults, obs_props).pipe(
                self._serialize_custom_dimensions,
                obs_props.properties).pipe(_format_numeric_values).pipe(
                    filter_invalid_observation_values).pipe(
                        self._ensure_entity_column).pipe(
                            _strip_namespaces, provenance))

    # Reorder columns to match expected DataFrame structure
    observations_df = observations_df[constants.OBSERVATION_COLUMNS]

    self.db.insert_observations(observations_df, self.input_file)

  def _apply_column_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
    """Rename physical CSV columns to logical column names."""
    reverse_mappings = {v: k for k, v in self.column_mappings.items()}
    return df.rename(columns=reverse_mappings)

  def _track_entity_dcids(self, df: pd.DataFrame) -> pd.DataFrame:
    """Track unique entity DCIDs seen in all entity dimension columns in this CSV."""
    for col in self.custom_dimensions:
      if col in df.columns:
        valid_dcids = df[col].dropna().unique()
        for dcid in valid_dcids:
          if dcid != "":
            self.entity_dcids[dcid] = True
    return df

  def _serialize_custom_dimensions(
      self, df: pd.DataFrame, static_props: dict[str, str]) -> pd.DataFrame:
    """Serializes dynamic custom dimensions and merges them with static custom properties."""
    custom_cols = [dim for dim in self.custom_dimensions if dim in df.columns]

    def row_to_json(row):
      # Start with static default custom properties
      d = dict(static_props)
      # Add/override with dynamic custom dimensions from the row
      for col in custom_cols:
        val = row[col]
        if pd.notna(val) and val != "":
          d[col] = strip_namespace(str(val))
      return json.dumps(d) if d else ""

    df[constants.COLUMN_PROPERTIES] = df.apply(row_to_json, axis=1)

    # Drop the dynamic custom dimension columns from the DataFrame now that they are serialized
    if custom_cols:
      df = df.drop(columns=custom_cols)
    return df

  def _ensure_entity_column(self, df: pd.DataFrame) -> pd.DataFrame:
    """Ensures the logical 'entity' column exists in the DataFrame, even if empty."""
    if constants.COLUMN_ENTITY not in df.columns:
      df[constants.COLUMN_ENTITY] = None
    return df

  def _add_entity_nodes(self) -> None:
    # Get entity nodes that are not already recorded.
    new_entity_dcids = [
        strip_namespace(dcid)
        for dcid in self.entity_dcids
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
