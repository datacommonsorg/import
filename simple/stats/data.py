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
"""This file includes data model classes used across the simple importer."""

from collections import Counter
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Self
from urllib.parse import urlparse

import pandas as pd
from stats import constants
from stats.report.model.summary import DateRange
from stats.report.model.summary import ValidationErrorType

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_PREDICATE_URL = "url"
_PREDICATE_SOURCE = "source"
_PREDICATE_DOMAIN = "domain"
_PREDICATE_INCLUDED_IN = "includedIn"

_SOURCE = "Source"
_PROVENANCE = "Provenance"

_NAMESPACE_DELIMITER = ':'


@dataclass
class Triple:
  subject_id: str
  predicate: str
  object_id: str = ""
  object_value: str = ""


@dataclass
class Provenance:
  id: str
  source_id: str
  name: str
  url: str = ""
  # MCF triples about this node that no field above holds (other predicates,
  # other types, extra values), with `id` as their subject.
  custom_triples: list[Triple] = field(default_factory=list)

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend([
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_PROVENANCE),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
        Triple(self.id, _PREDICATE_SOURCE, object_id=self.source_id),
    ])
    if self.url:
      triples.append(Triple(self.id, _PREDICATE_URL, object_value=self.url))
    return triples + self.custom_triples


@dataclass
class Source:
  id: str
  name: str
  url: str = ""
  # MCF triples about this node that no field above holds (other predicates,
  # other types, extra values), with `id` as their subject.
  custom_triples: list[Triple] = field(default_factory=list)
  # The import folder the Source is written to (import folders are named
  # after provenances, see Config.import_name), or "" for the global folder.
  import_folder: str = ""

  @property
  def domain(self) -> str:
    return urlparse(self.url).netloc

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend([
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_SOURCE),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
    ])
    if self.url:
      triples.append(Triple(self.id, _PREDICATE_URL, object_value=self.url))
    # A domain given in MCF (a custom triple) takes precedence over the one
    # derived from the url.
    if self.domain and not _has_predicate(self.custom_triples,
                                          _PREDICATE_DOMAIN):
      triples.append(
          Triple(self.id, _PREDICATE_DOMAIN, object_value=self.domain))
    return triples + self.custom_triples


def _has_predicate(triples: list[Triple], predicate: str) -> bool:
  """Returns whether any of `triples` has `predicate`, ignoring namespaces."""
  return any(strip_namespace(t.predicate) == predicate for t in triples)


@dataclass
class ObservationProperties:
  """The observationProperties of an inputFiles entry."""
  # The standard properties' values, by column (e.g. "scaling_factor" for
  # scalingFactor; see constants.STANDARD_PROPERTY_COLUMNS).
  standard: dict[str, str] = field(default_factory=dict)
  # The other (custom) properties' values, by property.
  properties: dict[str, str] = field(default_factory=dict)

  @classmethod
  def new(cls: Self, all_properties: dict[str, str] | None = None) -> Self:
    props = cls()
    for prop, value in (all_properties or {}).items():
      column = constants.STANDARD_PROPERTY_COLUMNS.get(prop)
      if column:
        props.standard[column] = value
      else:
        props.properties[prop] = value
    return props


@dataclass
class RowEntity:
  id: str
  row_entity_type: str
  provenance_id: str = ""
  prop_object_values: dict[str, str] = field(default_factory=dict)
  prop_object_ids: dict[str, str | list[str]] = field(default_factory=dict)

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=self.row_entity_type))
    if self.provenance_id:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=self.provenance_id))
    for prop, object_value in self.prop_object_values.items():
      triples.append(Triple(self.id, prop, object_value=object_value))
    for prop, object_id in self.prop_object_ids.items():
      if isinstance(object_id, list):
        for id in object_id:
          triples.append(Triple(self.id, prop, object_id=id))
      else:
        triples.append(Triple(self.id, prop, object_id=object_id))
    return triples


class PreprocessorError(ValueError):
  """An input error, which Runner reports in the handshake as `error_type`.

  `file_path` is the input file at fault, if any.
  """

  def __init__(
      self,
      message: str,
      error_type: ValidationErrorType = ValidationErrorType.GENERIC_ERROR,
      file_path: str = "") -> None:
    super().__init__(message)
    self.error_type = error_type
    self.file_path = file_path


def invalid_configuration(message: str) -> PreprocessorError:
  """Returns a config error, which Runner reports as INVALID_CONFIGURATION."""
  return PreprocessorError(message, ValidationErrorType.INVALID_CONFIGURATION)


@dataclass
class FileValidationError:
  file: str
  error_type: ValidationErrorType
  problem_columns: list[str] = field(default_factory=list)
  error_message: str = ""

  def to_dict(self) -> dict:
    return {
        "file": self.file,
        "errorType": self.error_type,
        "problemColumns": self.problem_columns,
        "errorMessage": self.error_message,
    }


@dataclass
class ImportCounts:
  """Observations, their dates and other nodes in one imported file.

  `nodes` counts the (node, type) pairs written, except StatVarObservations,
  once per file; `nodes_by_type` counts them per type, so `nodes` is its sum.
  """

  observations: int = 0
  date_range: DateRange | None = None
  nodes: int = 0
  # Node type -> number of nodes of that type, e.g.
  # {"StatisticalVariable": 2, "Property": 4}.
  nodes_by_type: dict[str, int] = field(default_factory=Counter)

  def add(self, other: Self) -> Self:
    """Returns both counts combined.

    Observations and nodes are summed, node counts are summed per type, and
    the date range covers both.
    """
    if self.date_range is None or other.date_range is None:
      date_range = self.date_range or other.date_range
    else:
      date_range = self.date_range.widen(other.date_range)
    nodes_by_type = Counter(self.nodes_by_type) + Counter(other.nodes_by_type)
    return ImportCounts(observations=self.observations + other.observations,
                        date_range=date_range,
                        nodes=self.nodes + other.nodes,
                        nodes_by_type=nodes_by_type)


class ImportType(StrEnum):
  OBSERVATIONS = "observations"
  ENTITIES = "entities"


_STANDARD_NA_LOWER = frozenset(
    {val.lower() for val in constants.STANDARD_NA_VALUES} | {"nan", ""})
_INVALID_VARIABLE_VALUES = frozenset({"", "none", "null", "nan", "<na>"})


def strip_namespace(v: str) -> str:
  """Strips surrounding whitespace and any namespace prefix from a DCID.

  e.g. ' dcid:country/USA ' -> 'country/USA'
  """
  v = v.strip()
  idx = v.find(_NAMESPACE_DELIMITER)
  if idx != -1:
    return v[idx + 1:].strip()
  return v


def strip_namespace_series(series: pd.Series) -> pd.Series:
  """Vectorized strip_namespace for pandas Series.

  Args:
    series: pandas Series containing DCIDs potentially with namespaces

  Returns:
    Series with surrounding whitespace and namespaces stripped
  """
  if series.empty:
    return series
  delim = _NAMESPACE_DELIMITER
  out = []
  for v in series.tolist():
    if isinstance(v, str):
      s = v.strip()
      idx = s.find(delim)
      out.append(s[idx + 1:].strip() if idx != -1 else s)
    else:
      out.append(v)
  return pd.Series(out, index=series.index, dtype=series.dtype)


def filter_invalid_observation_values(df: pd.DataFrame) -> pd.DataFrame:
  """Drops rows whose observation value is empty or a standard NA token."""
  na_set = _STANDARD_NA_LOWER
  mask = [(v.strip().lower() not in na_set) if isinstance(v, str) else
          (pd.notna(v) and str(v).strip().lower() not in na_set)
          for v in df[constants.COLUMN_VALUE].tolist()]
  if all(mask):
    return df
  return df[mask].copy()


def validate_variable_values(df: pd.DataFrame, file_path: str) -> None:
  """Validates that every observation has a non-empty, non-null variable ID.

  Raises:
      PreprocessorError: If any variable is empty or a null sentinel.
  """
  invalid_set = _INVALID_VARIABLE_VALUES
  invalid = []
  seen = set()
  for v in df[constants.COLUMN_VARIABLE].tolist():
    if isinstance(v, str):
      if v.strip().lower() in invalid_set and v not in seen:
        seen.add(v)
        invalid.append(v)
    elif v is None or pd.isna(v):
      if "" not in seen:
        seen.add("")
        invalid.append("")
    else:
      s = str(v)
      if s.strip().lower() in invalid_set and s not in seen:
        seen.add(s)
        invalid.append(s)
  if invalid:
    raise PreprocessorError(
        f"Invalid or missing variable value(s) found in file '{file_path}': "
        f"{invalid[:10]}.",
        file_path=file_path)


def validate_numeric_values(df: pd.DataFrame, file_path: str) -> None:
  """Validates that all values in the 'value' column are numeric or NaN.

  Raises:
      PreprocessorError: If any non-numeric value is found.
  """
  # Convert values to numeric, coercion will map invalid strings to NaN
  converted = pd.to_numeric(df[constants.COLUMN_VALUE], errors='coerce')
  # Only perform expensive string operations on non-numeric candidates
  na_mask = converted.isna() & df[constants.COLUMN_VALUE].notna()
  if not na_mask.any():
    return
  val_series = df.loc[na_mask, constants.COLUMN_VALUE].astype(
      str).str.strip().str.lower()
  invalid_mask = ~val_series.isin(_STANDARD_NA_LOWER)
  if not invalid_mask.any():
    return
  invalid_examples = df.loc[na_mask].loc[invalid_mask,
                                         constants.COLUMN_VALUE].unique()
  raise PreprocessorError(
      f"Invalid non-numeric value(s) found in observation values of file '{file_path}': "
      f"{list(invalid_examples)[:10]}. If these represent missing values, leave the"
      f" cells empty or use one of: {constants.STANDARD_NA_VALUES}.",
      file_path=file_path)
