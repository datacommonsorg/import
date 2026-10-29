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

import pandas as pd
from stats import constants
from stats import schema_constants as sc
from stats.data import FileValidationError
from stats.data import filter_invalid_observation_values
from stats.data import ObservationProperties
from stats.data import strip_namespace
from stats.data import strip_namespace_series
from stats.data import ValidationErrorType
from stats.graph_writer import GraphWriter
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filesystem import File

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
    "dcid:scalingFactor": constants.COLUMN_SCALING_FACTOR,  # "scaling_factor"
}


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
    if col_name in df.columns:
      source_col = df[col_name].replace("", pd.NA)
      df[col_name] = source_col.fillna(default_value).astype(str)
    else:
      # If the column doesn't exist, use default for all rows
      df[col_name] = default_value

  # Custom properties column (always empty for variable_per_row_importer)
  df[constants.COLUMN_PROPERTIES] = ""
  return df


def _format_numeric_values(df: pd.DataFrame) -> pd.DataFrame:
  """Format value column as string."""
  df[constants.COLUMN_VALUE] = df[constants.COLUMN_VALUE].fillna("").astype(str)
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
  """Imports a single observations CSV, one observation per row ("SVObs").

  The variable is named in a column rather than implied by the column heading,
  and up to three entity dimensions can be mapped via `columnMappings`.
  All entity DCIDs are expected to arrive pre-resolved in the CSV.
  """

  def __init__(self, input_file: File, graph_writer: GraphWriter,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_file = input_file
    self.graph_writer = graph_writer
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.custom_dimensions = []
    self.column_mappings = {}

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      errors = self.validate_headers()
      if errors:
        raise ValueError("\n".join(errors))
      self._map_columns()
      self._write_observations()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def validate_headers(self) -> list[FileValidationError]:
    column_mappings, custom_dimensions = self._parse_column_mappings()
    errors = self._validate_mappings(column_mappings, custom_dimensions)

    if errors:
      return errors

    try:
      with self.input_file.open_stream() as stream:
        header_df = pd.read_csv(stream, nrows=0)
      actual_column_names = set(header_df.columns)
    except Exception as e:
      return [
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.GENERIC_ERROR,
              error_message=
              f"Failed to read CSV headers for '{self.input_file.path}': {str(e)}"
          )
      ]

    expected_column_names = set(column_mappings.values())
    difference = expected_column_names - actual_column_names
    if difference:
      errors.append(
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.MISSING_REQUIRED_COLUMNS,
              problem_columns=sorted(list(difference)),
              error_message=
              f"The following expected columns were not found in the CSV: {sorted(list(difference))}. Please check your 'columnMappings' and the CSV header."
          ))

    ignored_column_names = set(self.config.ignore_columns(self.input_file))
    all_allowed_columns = expected_column_names | ignored_column_names
    unmapped_columns = actual_column_names - all_allowed_columns
    if unmapped_columns:
      errors.append(
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.UNMAPPED_COLUMNS,
              problem_columns=sorted(list(unmapped_columns)),
              error_message=
              f"The CSV file '{self.input_file.path}' contains unmapped columns: {sorted(list(unmapped_columns))}. Please map them in 'columnMappings' or list them in 'ignoreColumns' in config.json."
          ))

    return errors

  def _map_columns(self):
    self.column_mappings, self.custom_dimensions = self._parse_column_mappings()
    errors = self._validate_mappings(self.column_mappings,
                                     self.custom_dimensions)
    if errors:
      raise ValueError(errors[0]["errorMessage"])

  def _parse_column_mappings(self) -> tuple[dict[str, str], list[str]]:
    config_mappings = self.get_column_mappings()

    if not config_mappings:
      # TODO: Remove this default fallback and require explicit columnMappings in config.json.
      config_mappings = {
          "dcid:observationAbout": "entity",
          "dcid:variableMeasured": "variable",
          "dcid:observationDate": "date",
          "dcid:value": "value",
      }

    column_mappings = {}
    custom_dimensions = []

    for key, physical_col in config_mappings.items():
      if key in STANDARD_PROPERTY_MAPPING:
        internal_col = STANDARD_PROPERTY_MAPPING[key]
        column_mappings[internal_col] = physical_col
      else:
        custom_dimensions.append(key)
        column_mappings[key] = physical_col

    return column_mappings, custom_dimensions

  def _validate_mappings(
      self, column_mappings: dict[str, str],
      custom_dimensions: list[str]) -> list[FileValidationError]:
    errors = []
    for req_col in [
        constants.COLUMN_VARIABLE, constants.COLUMN_DATE, constants.COLUMN_VALUE
    ]:
      if req_col not in column_mappings:
        official_key = [
            k for k, v in STANDARD_PROPERTY_MAPPING.items() if v == req_col
        ][0]
        errors.append(
            FileValidationError(
                file=self.input_file.path,
                error_type=ValidationErrorType.MISSING_REQUIRED_COLUMNS,
                problem_columns=[official_key],
                error_message=
                f"Missing required column mapping for: '{official_key}'"))

    entity_dims_count = len(custom_dimensions)

    if entity_dims_count < 1:
      errors.append(
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.INVALID_CONFIGURATION,
              error_message=
              "Invalid configuration: An observation must have at least one entity dimension. Please map 'dcid:observationAbout' or map at least one custom dimension in 'columnMappings'."
          ))
    if entity_dims_count > 3:
      errors.append(
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.INVALID_CONFIGURATION,
              error_message=
              f"Invalid configuration: Too many entity dimensions mapped ({entity_dims_count}). A maximum of 3 entity dimensions (including 'dcid:observationAbout') is allowed."
          ))
    return errors

  def _write_observations(self) -> None:
    provenance = self.nodes.provenance(self.input_file).id
    obs_props = ObservationProperties.new(
        self.config.observation_properties(self.input_file))

    with self.input_file.open_stream() as stream:
      reader = pd.read_csv(stream,
                           na_values=constants.STANDARD_NA_VALUES,
                           chunksize=10000,
                           dtype=str)

      for chunk_df in reader:
        if chunk_df.empty:
          continue
        observations_df = (self.resolve_specified_columns(chunk_df).pipe(
            self._apply_column_mappings).pipe(
                _apply_property_defaults, obs_props).pipe(
                    self._serialize_custom_dimensions,
                    obs_props.properties).pipe(_format_numeric_values).pipe(
                        filter_invalid_observation_values).pipe(
                            self._ensure_entity_column).pipe(
                                _strip_namespaces, provenance))
        observations_df = observations_df[constants.OBSERVATION_COLUMNS]
        self.graph_writer.write_observations(observations_df, self.input_file)

  def _apply_column_mappings(self, df: pd.DataFrame) -> pd.DataFrame:
    """Rename physical CSV columns to logical column names."""
    reverse_mappings = {v: k for k, v in self.column_mappings.items()}
    return df.rename(columns=reverse_mappings)

  def _serialize_custom_dimensions(
      self, df: pd.DataFrame, static_props: dict[str, str]) -> pd.DataFrame:
    """Serializes dynamic custom dimensions and merges them with static custom properties."""
    custom_cols = [dim for dim in self.custom_dimensions if dim in df.columns]

    if not custom_cols:
      static_json = json.dumps(static_props) if static_props else ""
      df[constants.COLUMN_PROPERTIES] = static_json
      return df

    serialized = [
        _serialize_row_dimensions(row, static_props)
        for row in df[custom_cols].to_dict(orient="records")
    ]
    df[constants.COLUMN_PROPERTIES] = serialized
    return df.drop(columns=custom_cols)

  def _ensure_entity_column(self, df: pd.DataFrame) -> pd.DataFrame:
    """Ensures the logical 'entity' column exists in the DataFrame, even if empty."""
    if constants.COLUMN_ENTITY not in df.columns:
      df[constants.COLUMN_ENTITY] = None
    return df


def _serialize_row_dimensions(row: dict[str, object],
                              static_props: dict[str, str]) -> str:
  props = dict(static_props)
  for col, val in row.items():
    if pd.notna(val) and val != "":
      props[col] = strip_namespace(str(val))
  return json.dumps(props) if props else ""
