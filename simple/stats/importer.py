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
from stats.data import strip_namespace
from stats.util import get_namespace_prefix_and_suffix
from stats.util import has_namespace_prefix

from util import dc_client as dc


class EntityResolutionError(ValueError):
  """Raised when entity resolution fails during import."""

  def __init__(self,
               file_path: str = "",
               unresolved_entities: list[str] | None = None,
               message: str = "") -> None:
    super().__init__(message)
    self.file_path = file_path
    self.unresolved_entities = unresolved_entities if unresolved_entities is not None else []


class Importer:
  """The base class for all importers."""

  def do_import(self) -> None:
    pass

  def validate_headers(self) -> list[dict]:
    """Validates CSV headers and returns a list of error dicts (empty if valid)."""
    return []

  def check_and_report_unresolved_entities(
      self, unresolved_entities: set[str]) -> None:
    """Checks if there are any unresolved entities, reports them and raises EntityResolutionError."""
    if not unresolved_entities:
      return

    unresolved_list = sorted(list(unresolved_entities))
    self.reporter.report_unresolved_entities(unresolved_list)

    if hasattr(self, "_write_debug_csvs"):
      getattr(self, "_write_debug_csvs")()

    msg = (
        f"Entity resolution failed for {len(unresolved_list)} entities in file '{self.input_file.path}': "
        f"{unresolved_list[:50]}... "
        f"Please check the debug resolution CSV file for the complete list.")
    raise EntityResolutionError(self.input_file.path, unresolved_list, msg)

  def get_column_mappings(self) -> dict[str, str]:
    """Returns the column mappings for the input file."""
    if not hasattr(self, "config") or not hasattr(self, "input_file"):
      return {}
    return self.config.column_mappings(self.input_file)

  def get_reverse_column_mappings(self) -> dict[str, str]:
    """Returns a mapping from physical CSV column names to stripped property DCIDs."""
    return {v: strip_namespace(k) for k, v in self.get_column_mappings().items()}

  def resolve_specified_columns(self,
                                df: pd.DataFrame,
                                default_entity_type: str = "") -> pd.DataFrame:
    """Resolves columns specified in config's entityColumns using the DC API.

    Works identically across observations, variable_per_row, events, and
    entities.
    """

    if not hasattr(self, "config") or not hasattr(self, "input_file"):
      return df
    cols_to_resolve = self.config.entity_columns(self.input_file)
    # For backward compatibility with legacy unit tests where entityColumns is not configured
    if (not cols_to_resolve and hasattr(self, "entity_type") and
        getattr(self, "entity_type", "")):
      default_col = getattr(self, "entity_column_name", constants.COLUMN_DCID)
      if default_col in df.columns:
        cols_to_resolve = [default_col]
      elif constants.COLUMN_DCID in df.columns:
        cols_to_resolve = [constants.COLUMN_DCID]

    if not cols_to_resolve:
      return df

    entity_type = (getattr(self, "entity_type", "") or
                   getattr(self, "row_entity_type", "") or default_entity_type)

    for col_name in cols_to_resolve:
      target_col = col_name
      if target_col not in df.columns:
        entity_cols = [
            col for col in (
                constants.COLUMN_DCID,
                constants.COLUMN_ENTITY,
                "dcid:observationAbout",
            ) if col in df.columns
        ]
        if (getattr(self, "entity_column_name", None) == col_name and
            entity_cols):
          target_col = entity_cols[0]
        else:
          continue

      column = df[target_col]
      pre_resolved_entities = {}

      def remove_pre_resolved(entity: str) -> bool:
        if has_namespace_prefix(entity):
          prefix, suffix = get_namespace_prefix_and_suffix(entity)
          pre_resolved_entities[entity] = suffix.strip()
          return False
        return True

      entities = list(filter(remove_pre_resolved, column.dropna().tolist()))
      if not entities:
        column = column.map(lambda x: pre_resolved_entities.get(x, x))
        df[target_col] = column
        continue

      lower_case_entity_name = (getattr(self, "entity_column_name", target_col)
                                if target_col in (
                                    constants.COLUMN_DCID,
                                    constants.COLUMN_ENTITY,
                                    "dcid:observationAbout",
                                ) else target_col).lower()
      if (lower_case_entity_name
          in constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES):
        prefix = constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES[
            lower_case_entity_name]
        dcids = dict([(entity, f"{prefix}{entity}") for entity in entities])
      else:
        property_name = (
            constants.EXTERNALLY_RESOLVED_INPUT_COLUMNS_TO_PREFIXES.get(
                lower_case_entity_name, constants.PROPERTY_DESCRIPTION))
        dcids = dc.resolve_entities(
            entities=entities,
            entity_type=entity_type,
            property_name=property_name,
        )

      unresolved = set(entities).difference(set(dcids.keys()))
      unresolved_list = sorted(list(unresolved))
      column = column.map(
          lambda x: dcids.get(x, pre_resolved_entities.get(x, x)))
      df[target_col] = column

      if unresolved_list:
        if hasattr(self, "all_unresolved_entities"):
          self.all_unresolved_entities.update(unresolved_list)
        df.drop(df[df[target_col].isin(values=unresolved_list)].index,
                inplace=True)

      if hasattr(self, "_create_debug_resolve_dataframe"):
        self._create_debug_resolve_dataframe(
            resolved=dcids,
            pre_resolved=pre_resolved_entities,
            unresolved=unresolved_list,
        )

    return df
