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

import collections

import pandas as pd
from stats.data import FileValidationError
from stats.data import strip_namespace
from stats.data import ValidationErrorType


def missing_column_mappings_error(file_path: str) -> FileValidationError:
  """Returns the error for a CSV file whose `inputFiles` entry has no `columnMappings`."""
  return FileValidationError(
      file=file_path,
      error_type=ValidationErrorType.INVALID_CONFIGURATION,
      error_message=
      f"Missing 'columnMappings' for CSV file '{file_path}' in config.json. "
      "'columnMappings' is required for every CSV input file: map each column "
      "to a property DCID, or list it in 'ignoreColumns'.")


def read_csv_columns(input_file) -> set[str] | FileValidationError:
  """Returns the header columns of a CSV file, or the error if it can't be read."""
  try:
    with input_file.open_stream() as stream:
      return set(pd.read_csv(stream, nrows=0).columns)
  except Exception as e:
    return FileValidationError(
        file=input_file.path,
        error_type=ValidationErrorType.GENERIC_ERROR,
        error_message=f"Failed to read CSV headers for '{input_file.path}': {e}"
    )


def column_coverage_errors(
    file_path: str, actual_columns: set[str], expected_columns: set[str],
    ignored_columns: set[str]) -> list[FileValidationError]:
  """
  Checks that the CSV header and the config describe the same columns.

  Every expected column (mapped in `columnMappings`) must be in the CSV, and
  every CSV column must be expected or listed in `ignoreColumns`. Each check
  reports all of its columns at once.
  """
  errors = []
  missing = sorted(expected_columns - actual_columns)
  if missing:
    errors.append(
        FileValidationError(
            file=file_path,
            error_type=ValidationErrorType.MISSING_REQUIRED_COLUMNS,
            problem_columns=missing,
            error_message=
            f"The following expected columns were not found in the CSV: {missing}. Please check your 'columnMappings' and the CSV header."
        ))
  unmapped = sorted(actual_columns - expected_columns - ignored_columns)
  if unmapped:
    errors.append(
        FileValidationError(
            file=file_path,
            error_type=ValidationErrorType.UNMAPPED_COLUMNS,
            problem_columns=unmapped,
            error_message=
            f"The CSV file '{file_path}' contains unmapped columns: {unmapped}. Please map them in 'columnMappings' or list them in 'ignoreColumns' in config.json."
        ))
  return errors


def duplicate_property_errors(
    file_path: str, mappings: dict[str, str]) -> list[FileValidationError]:
  """
  Checks that no two columns map to the same property.

  Keys that differ only by namespace, e.g. `dcid:unit` and `unit`, name the
  same property, so one column would silently overwrite the other. Reports
  every such property at once. `mappings` is the file's `columnMappings`.
  """
  columns_by_property = collections.defaultdict(list)
  for key, column in mappings.items():
    columns_by_property[strip_namespace(key)].append(column)
  duplicates = {
      prop: columns
      for prop, columns in columns_by_property.items()
      if len(columns) > 1
  }
  if not duplicates:
    return []
  return [
      FileValidationError(
          file=file_path,
          error_type=ValidationErrorType.INVALID_CONFIGURATION,
          problem_columns=sorted(
              column for columns in duplicates.values() for column in columns),
          error_message=
          f"The CSV file '{file_path}' maps several columns to the same property: {duplicates}. Map each property from one column."
      )
  ]


def duplicate_column_errors(
    file_path: str, mappings: dict[str, str]) -> list[FileValidationError]:
  """
  Checks that no column maps to more than one property.

  The importers look properties up by column, so a column mapped twice keeps
  only one of its properties. Reports every such column at once. `mappings`
  is the file's `columnMappings`.
  """
  properties_by_column = collections.defaultdict(list)
  for key, column in mappings.items():
    properties_by_column[column].append(strip_namespace(key))
  duplicates = {
      column: sorted(properties)
      for column, properties in sorted(properties_by_column.items())
      if len(properties) > 1
  }
  if not duplicates:
    return []
  return [
      FileValidationError(
          file=file_path,
          error_type=ValidationErrorType.INVALID_CONFIGURATION,
          problem_columns=list(duplicates),
          error_message=
          f"The CSV file '{file_path}' maps the same column to several properties: {duplicates}. Map each column to one property."
      )
  ]


def mapped_and_ignored_errors(
    file_path: str, mappings: dict[str, str],
    ignore_columns: list[str]) -> list[FileValidationError]:
  """
  Checks that no column is both mapped and listed in `ignoreColumns`.

  A column is either imported or ignored; listing it in both says neither.
  Reports every such column at once.
  """
  both = sorted(set(mappings.values()) & set(ignore_columns))
  if not both:
    return []
  return [
      FileValidationError(
          file=file_path,
          error_type=ValidationErrorType.INVALID_CONFIGURATION,
          problem_columns=both,
          error_message=
          f"The CSV file '{file_path}' both maps and ignores column(s): {both}. Remove each from 'columnMappings' or from 'ignoreColumns'."
      )
  ]


def mapping_errors(file_path: str, mappings: dict[str, str],
                   ignore_columns: list[str]) -> list[FileValidationError]:
  """
  The errors of a CSV's `columnMappings` that need no header: properties or
  columns mapped twice, and columns both mapped and ignored.
  """
  return (duplicate_property_errors(file_path, mappings) +
          duplicate_column_errors(file_path, mappings) +
          mapped_and_ignored_errors(file_path, mappings, ignore_columns))


class Importer:
  """The base class for all importers."""

  def do_import(self) -> None:
    pass

  def validate_headers(self) -> list[FileValidationError]:
    """Validates CSV headers and returns a list of validation errors (empty if valid)."""
    return []

  def get_column_mappings(self) -> dict[str, str]:
    """Returns the column mappings for the input file."""
    if not hasattr(self, "config") or not hasattr(self, "input_file"):
      return {}
    return self.config.column_mappings(self.input_file)

  def get_reverse_column_mappings(self) -> dict[str, str]:
    """Returns a mapping from physical CSV column names to stripped property DCIDs."""
    return {v: strip_namespace(k) for k, v in self.get_column_mappings().items()}
