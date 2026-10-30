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

import pandas as pd
from stats.data import FileValidationError
from stats.data import RowEntity
from stats.data import strip_namespace
from stats.data import Triple
from stats.data import ValidationErrorType
from stats.graph_writer import GraphWriter
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import has_namespace_prefix
from util.filesystem import File


def _parse_object_ids(v_str: str) -> str | list[str]:
  """Splits comma-separated entity references and strips namespace prefixes."""
  if "," in v_str:
    return [strip_namespace(x.strip()) for x in v_str.split(",")]
  return strip_namespace(v_str)


class EntitiesImporter(Importer):
  """Imports a single entities input file.

  Key behaviors at this time:
  - All un-ignored columns will be encoded as property triples.
  - If an id column was configured, it will be used as the entity dcid. Else a new dcid will be generated for each entity.
  - Columns specified as entity columns will be encoded as object_id in the triples tables. Others will be encoded as object_value.
    + Currently this importer does not resolve any entities and all entities are assumed to be pre-resolved into dcids.
    """

  def __init__(self, input_file: File, graph_writer: GraphWriter,
               reporter: FileImportReporter, nodes: Nodes) -> None:
    self.input_file = input_file
    self.graph_writer = graph_writer
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.ignore_columns = self.config.ignore_columns(self.input_file)
    self.provenance_id = ""

    self.row_entity_type = self.config.row_entity_type(self.input_file)
    assert self.row_entity_type, f"Row entity type must be specified: {self.input_file.full_path()}"

    self.id_column = self.config.id_column(self.input_file)
    # Reassigned when renaming columns.
    self.entity_columns = set(self.config.entity_columns(self.input_file))

    # Initialize reverse column mappings from config
    column_mappings = self.get_column_mappings()
    self.has_column_mappings = bool(column_mappings)
    self.reverse_mappings = self.get_reverse_column_mappings()

    self.df = pd.DataFrame()

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      errors = self.validate_headers()
      if errors:
        raise ValueError("\n".join(e.error_message for e in errors))
      self.provenance_id = self.nodes.provenance(self.input_file).id
      self._read_csv()
      self._drop_ignored_columns()
      self._rename_columns()
      self._write_row_entity_triples()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def validate_headers(self) -> list[FileValidationError]:
    errors = []
    if not self.has_column_mappings:
      return []

    try:
      with self.input_file.open_stream() as stream:
        header_df = pd.read_csv(stream, nrows=0)
      actual_columns = set(header_df.columns)
    except Exception as e:
      return [
          FileValidationError(
              file=self.input_file.path,
              error_type=ValidationErrorType.GENERIC_ERROR,
              error_message=
              f"Failed to read CSV headers for '{self.input_file.path}': {str(e)}"
          )
      ]

    mapped_columns = set(self.reverse_mappings.keys())
    ignored_columns = set(self.ignore_columns)
    id_col = {self.id_column} if self.id_column else set()
    all_allowed_columns = mapped_columns | ignored_columns | id_col
    unmapped_columns = actual_columns - all_allowed_columns
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

  def _read_csv(self) -> None:
    # Read CSVs with the following behaviors:
    # - Strip leading whitespaces
    # - Treat comma as a thousands separator
    self.df = pd.read_csv(self.input_file.read_string_io(),
                          skipinitialspace=True,
                          thousands=",")
    logging.info("Read %s rows.", self.df.index.size)
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()

  def _drop_ignored_columns(self):
    if self.ignore_columns:
      self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

  def _is_entity_reference_column(self, col: str) -> bool:
    return col in self.entity_columns

  def _rename_columns(self) -> None:
    renamed = {}
    for col in self.df.columns:
      id = self.reverse_mappings.get(col, self.nodes.property(col).dcid)
      renamed[col] = id
      if col in self.entity_columns:
        self.entity_columns.remove(col)
        self.entity_columns.add(id)
    self.df = self.df.rename(columns=renamed)

  def _extract_row_properties(
      self,
      row: pd.Series) -> tuple[dict[str, str], dict[str, str | list[str]]]:
    """Splits a CSV row into literal property values and entity reference IDs."""
    prop_object_values: dict[str, str] = {}
    prop_object_ids: dict[str, str | list[str]] = {}
    for k, v in row.items():
      if pd.isna(v):
        continue
      v_str = str(v)
      if self._is_entity_reference_column(k) or has_namespace_prefix(v_str):
        prop_object_ids[k] = _parse_object_ids(v_str)
      else:
        prop_object_values[k] = v
    return prop_object_values, prop_object_ids

  def _write_row_entity_triples(self) -> None:
    # Add entity type node - it will be written later.
    # This is to avoid duplicate entity types in scenarios where entities of the same type
    # are spread across files.
    self.nodes.entity_type(self.row_entity_type, self.input_file)

    # All property columns would've been renamed to their dcids by now.
    # So use the id column's dcid as the id column name.
    id_column_name = ""
    if self.id_column:
      id_column_name = self.reverse_mappings.get(
          self.id_column,
          self.nodes.property(self.id_column).dcid)

    triples: list[Triple] = []
    entity_type = strip_namespace(self.row_entity_type)
    for index, row in self.df.iterrows():
      raw_dcid = row[
          id_column_name] if id_column_name else f"{self.row_entity_type}_{index}"
      dcid = strip_namespace(str(raw_dcid))
      prop_object_values, prop_object_ids = self._extract_row_properties(row)
      row_entity = RowEntity(dcid,
                             entity_type,
                             provenance_id=self.provenance_id,
                             prop_object_values=prop_object_values,
                             prop_object_ids=prop_object_ids)
      triples.extend(row_entity.triples())

    self.graph_writer.write_triples(triples, self.input_file)
