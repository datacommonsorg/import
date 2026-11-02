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
from stats.graph_writer import GraphWriter
from stats.importer import column_coverage_errors
from stats.importer import duplicate_property_errors
from stats.importer import Importer
from stats.importer import missing_column_mappings_error
from stats.importer import read_csv_columns
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
  - `columnMappings` is required. Every column must be mapped or ignored, except the id column. Each mapped column is encoded as property triples under its mapped predicate.
  - If an id column was configured, it will be used as the entity dcid. Else a new dcid will be generated for each entity. The id column is only written as a property if it is mapped.
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
    # Map from CSV column name to the predicate its values are written under.
    self.reverse_mappings = self.get_reverse_column_mappings()
    # Predicates of the columns holding entity references.
    self.entity_columns = {
        self.reverse_mappings.get(col, col)
        for col in self.config.entity_columns(self.input_file)
    }

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
      self._write_row_entity_triples()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def validate_headers(self) -> list[FileValidationError]:
    if not self.get_column_mappings():
      return [missing_column_mappings_error(self.input_file.path)]
    duplicates = duplicate_property_errors(self.input_file.path,
                                           self.get_column_mappings())
    if duplicates:
      return duplicates
    actual_columns = read_csv_columns(self.input_file)
    if isinstance(actual_columns, FileValidationError):
      return [actual_columns]
    expected_columns = set(self.reverse_mappings)
    if self.id_column:
      expected_columns.add(self.id_column)
    return column_coverage_errors(self.input_file.path, actual_columns,
                                  expected_columns, set(self.ignore_columns))

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

  def _property_columns(self) -> pd.DataFrame:
    """Returns the mapped columns, renamed to their predicates.

    An unmapped id column is left out: it only supplies the entity dcids.
    """
    mapped = [col for col in self.df.columns if col in self.reverse_mappings]
    return self.df[mapped].rename(columns=self.reverse_mappings)

  def _raw_dcid(self, index: int) -> str:
    """Returns row `index`'s id column value, else a generated `<type>_<index>`."""
    if self.id_column:
      return str(self.df.at[index, self.id_column])
    return f"{self.row_entity_type}_{index}"

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
    triples: list[Triple] = []
    entity_type = strip_namespace(self.row_entity_type)
    for index, row in self._property_columns().iterrows():
      dcid = strip_namespace(self._raw_dcid(index))
      prop_object_values, prop_object_ids = self._extract_row_properties(row)
      row_entity = RowEntity(dcid,
                             entity_type,
                             provenance_id=self.provenance_id,
                             prop_object_values=prop_object_values,
                             prop_object_ids=prop_object_ids)
      triples.extend(row_entity.triples())

    self.graph_writer.write_triples(triples, self.input_file)
