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
from stats.importer import column_coverage_errors
from stats.importer import Importer
from stats.importer import mapping_errors
from stats.importer import missing_column_mappings_error
from stats.importer import read_csv_columns
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.util import has_namespace_prefix
from util.filesystem import File

# Properties whose columns give each row's DCID and type. Every entities CSV
# must map both; they are not written as properties.
_DCID_PROPERTY = "dcid"
_TYPE_OF_PROPERTY = "typeOf"
_REQUIRED_PROPERTIES = (_DCID_PROPERTY, _TYPE_OF_PROPERTY)


def _missing_required_mappings_error(
    file_path: str, properties: set[str]) -> FileValidationError | None:
  """Returns the error if `properties` lacks `dcid` or `typeOf`, else None."""
  missing = [
      f"dcid:{prop}" for prop in _REQUIRED_PROPERTIES if prop not in properties
  ]
  if not missing:
    return None
  return FileValidationError(
      file=file_path,
      error_type=ValidationErrorType.INVALID_CONFIGURATION,
      problem_columns=missing,
      error_message=
      f"Missing required column mapping(s) for entities CSV file '{file_path}'"
      f" in config.json: {missing}. Map the column holding each row's DCID to"
      " 'dcid:dcid' and the column holding its type to 'dcid:typeOf'.")


def _parse_object_ids(v_str: str) -> str | list[str]:
  """Splits comma-separated entity references and strips namespace prefixes."""
  if "," in v_str:
    return [strip_namespace(x.strip()) for x in v_str.split(",")]
  return strip_namespace(v_str)


class EntitiesImporter(Importer):
  """Imports a single entities input file.

  Key behaviors at this time:
  - `columnMappings` is required, and must map a `dcid:dcid` and a `dcid:typeOf` column: they give each row's DCID and type. Every other column must be mapped or ignored, and is encoded as property triples under its mapped predicate.
  - Cell values with a namespace prefix (e.g. `dcid:`) are entity references, encoded as object_id in the triples tables. Others are encoded as object_value.
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
    # Map from CSV column name to the predicate its values are written under.
    self.reverse_mappings = self.get_reverse_column_mappings()

    self.df = pd.DataFrame()

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      errors = self.validate_headers()
      if errors:
        raise ValueError("\n".join(e.error_message for e in errors))
      self.provenance_id = self.nodes.provenance_id(self.input_file)
      self._read_csv()
      self._drop_ignored_columns()
      self._write_row_entity_triples()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def validate_headers(self) -> list[FileValidationError]:
    mappings = self.get_column_mappings()
    if not mappings:
      return [missing_column_mappings_error(self.input_file.path)]
    errors = mapping_errors(self.input_file.path, mappings, self.ignore_columns)
    if errors:
      return errors
    missing = _missing_required_mappings_error(
        self.input_file.path, set(self.reverse_mappings.values()))
    if missing:
      return [missing]
    actual_columns = read_csv_columns(self.input_file)
    if isinstance(actual_columns, FileValidationError):
      return [actual_columns]
    return column_coverage_errors(self.input_file.path, actual_columns,
                                  set(self.reverse_mappings),
                                  set(self.ignore_columns))

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

  def _mapped_columns(self) -> pd.DataFrame:
    """Returns the mapped columns, renamed to their predicates."""
    mapped = [col for col in self.df.columns if col in self.reverse_mappings]
    return self.df[mapped].rename(columns=self.reverse_mappings)

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
      if has_namespace_prefix(v_str):
        prop_object_ids[k] = _parse_object_ids(v_str)
      else:
        prop_object_values[k] = v
    return prop_object_values, prop_object_ids

  def _write_row_entity_triples(self) -> None:
    triples: list[Triple] = []
    for _, row in self._mapped_columns().iterrows():
      dcid = strip_namespace(str(row[_DCID_PROPERTY]))
      entity_type = strip_namespace(str(row[_TYPE_OF_PROPERTY]))
      prop_object_values, prop_object_ids = self._extract_row_properties(
          row.drop(list(_REQUIRED_PROPERTIES)))
      row_entity = RowEntity(dcid,
                             entity_type,
                             provenance_id=self.provenance_id,
                             prop_object_values=prop_object_values,
                             prop_object_ids=prop_object_ids)
      triples.extend(row_entity.triples())

    self.graph_writer.write_triples(triples, self.input_file)
