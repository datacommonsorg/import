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

from collections import defaultdict
from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum
from typing import Self
from urllib.parse import urlparse

import pandas as pd
from stats import constants
from stats import schema_constants as sc
from stats.util import has_namespace_prefix
from stats.util import is_uri_or_namespace

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_PREDICATE_DESCRIPTION = "description"
_PREDICATE_SPECIALIZATION_OF = "specializationOf"
_PREDICATE_URL = "url"
_PREDICATE_SOURCE = "source"
_PREDICATE_DOMAIN = "domain"
_PREDICATE_INCLUDED_IN = "includedIn"

STATISTICAL_VARIABLE = "StatisticalVariable"
STAT_VAR_GROUP = "StatVarGroup"
_SOURCE = "Source"
_PROVENANCE = "Provenance"
_PROPERTY = "Property"
_CLASS = "Class"

_MCF_PREDICATE_BLOCKLIST = set([_PREDICATE_INCLUDED_IN])

_NAMESPACE_DELIMITER = ':'


@dataclass
class Triple:
  subject_id: str
  predicate: str
  object_id: str = ""
  object_value: str = ""

  def normalized_tuple(self):
    return (strip_namespace(self.subject_id), self.predicate,
            strip_namespace(self.object_id), self.object_value)


@dataclass
class StatVarGroup:
  id: str
  name: str
  parent_id: str

  def triples(self) -> list[Triple]:
    triples: list[Triple] = [
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=STAT_VAR_GROUP),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
    ]
    if self.parent_id:
      triples.append(
          Triple(self.id,
                 _PREDICATE_SPECIALIZATION_OF,
                 object_id=self.parent_id))
    return triples


@dataclass
class Provenance:
  id: str
  source_id: str
  name: str
  url: str = ""
  properties: dict[str, str] = field(default_factory=dict)
  provenance_id: str = ""

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend([
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_PROVENANCE),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
        Triple(self.id, _PREDICATE_SOURCE, object_id=self.source_id),
    ])
    if self.url:
      triples.append(Triple(self.id, _PREDICATE_URL, object_value=self.url))

    # Dynamically serialize any custom properties
    for pred, val in self.properties.items():
      # Skip hardcoded fields to prevent duplication
      if pred in ["typeOf", "name", "sourceLink", "source", "url"]:
        continue
      if is_uri_or_namespace(val):
        triples.append(Triple(self.id, pred, object_id=val))
      else:
        triples.append(Triple(self.id, pred, object_value=val))

    return triples


@dataclass
class Source:
  id: str
  name: str
  url: str = ""
  domain: str = field(init=False)
  properties: dict[str, str] = field(default_factory=dict)
  provenance_id: str = ""

  def __post_init__(self):
    self.domain = urlparse(self.url).netloc

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend([
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_SOURCE),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
    ])
    if self.url:
      triples.append(Triple(self.id, _PREDICATE_URL, object_value=self.url))
    if self.domain:
      triples.append(
          Triple(self.id, _PREDICATE_DOMAIN, object_value=self.domain))

    # Dynamically serialize any custom properties
    for pred, val in self.properties.items():
      # Skip hardcoded fields to prevent duplication
      if pred in ["typeOf", "name", "url", "domain"]:
        continue
      if is_uri_or_namespace(val):
        triples.append(Triple(self.id, pred, object_id=val))
      else:
        triples.append(Triple(self.id, pred, object_value=val))

    return triples


@dataclass
class ObservationProperties:
  unit: str = ""
  scaling_factor: str = ""
  measurement_method: str = ""
  observation_period: str = ""
  # All custom properties other than the standard ones above go in this field.
  properties: dict[str, str] = field(default_factory=dict)

  @classmethod
  def new(cls: Self,
          all_properties: dict[str, str] = {},
          default_obs_props: Self = None) -> Self:
    if default_obs_props is None:
      default_obs_props = cls()
    unit = all_properties.get(sc.PREDICATE_UNIT, "") or default_obs_props.unit
    scaling_factor = all_properties.get(sc.PREDICATE_SCALING_FACTOR,
                                        "") or default_obs_props.scaling_factor
    measurement_method = all_properties.get(
        sc.PREDICATE_MEASUREMENT_METHOD,
        "") or default_obs_props.measurement_method
    observation_period = all_properties.get(
        sc.PREDICATE_OBSERVATION_PERIOD,
        "") or default_obs_props.observation_period
    custom_properties = {
        p: v
        for p, v in all_properties.items()
        if p not in sc.STANDARD_OBSERVATION_PROPERTIES
    }
    custom_properties = default_obs_props.properties | custom_properties
    return cls(unit, scaling_factor, measurement_method, observation_period,
               custom_properties)


@dataclass
class Property:
  dcid: str
  name: str
  provenance_ids: set[str] = field(default_factory=set)

  def triples(self) -> list[Triple]:
    return [
        Triple(self.dcid, _PREDICATE_TYPE_OF, object_id=_PROPERTY),
        Triple(self.dcid, _PREDICATE_NAME, object_value=self.name),
    ]


@dataclass
class RowEntity:
  id: str
  row_entity_type: str
  provenance_id: str = ""
  prop_object_values: dict[str, str] = field(
      default_factory=lambda: defaultdict(dict))
  prop_object_ids: dict[str, str | list[str]] = field(
      default_factory=lambda: defaultdict(dict))

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


@dataclass
class EntityType:
  id: str
  name: str
  description: str = ""
  provenance_ids: list[str] = field(default_factory=list)
  source_ids: list[str] = field(default_factory=list)

  def add_provenance(self, provenance: Provenance) -> Self:
    provenance_id = provenance.id
    source_id = provenance.source_id
    if provenance_id not in self.provenance_ids:
      self.provenance_ids.append(provenance_id)
    if source_id not in self.source_ids:
      self.source_ids.append(source_id)

    return self

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(Triple(self.id, _PREDICATE_TYPE_OF, object_id=_CLASS))
    triples.append(Triple(self.id, _PREDICATE_NAME, object_value=self.name))
    if self.description:
      triples.append(
          Triple(self.id, _PREDICATE_DESCRIPTION,
                 object_value=self.description))
    for provenance_id in self.provenance_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=provenance_id))
    for source_id in self.source_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=source_id))
    return triples


class ValidationErrorType(StrEnum):
  UNMAPPED_COLUMNS = "UNMAPPED_COLUMNS"
  MISSING_REQUIRED_COLUMNS = "MISSING_REQUIRED_COLUMNS"
  INVALID_CONFIGURATION = "INVALID_CONFIGURATION"
  UNRESOLVED_ENTITY = "UNRESOLVED_ENTITY"
  MISSING_SOURCE = "MISSING_SOURCE"
  MISSING_PROVENANCE = "MISSING_PROVENANCE"
  GENERIC_ERROR = "GENERIC_ERROR"


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


class ImportType(StrEnum):
  OBSERVATIONS = "observations"
  ENTITIES = "entities"


@dataclass
class McfNode:
  id: str
  node_type: str = ""
  properties: dict[str, str] = field(default_factory=lambda: defaultdict(dict))

  def add_triple(self, triple: Triple) -> Self:
    if triple.predicate in _MCF_PREDICATE_BLOCKLIST:
      return self

    if triple.predicate == _PREDICATE_TYPE_OF:
      self.node_type = triple.object_id

    if triple.object_id:
      self.properties[triple.predicate] = triple.object_id
    elif triple.object_value:
      self.properties[triple.predicate] = f'"{triple.object_value}"'

    return self

  def to_mcf(self) -> str:
    parts: list[str] = []
    if has_namespace_prefix(self.id):
      parts.append(f"Node: {self.id}")
    else:
      parts.append(f"Node: dcid:{self.id}")
    parts.extend([f"{p}: {v}" for p, v in self.properties.items()])
    return "\n".join(parts)


@dataclass
class VerticalSpec:
  population_type: str
  measured_properties: set[str] = field(default_factory=set)
  verticals: set[str] = field(default_factory=set)

  def from_json(json: dict) -> Self:
    return VerticalSpec(population_type=json.get("populationType",
                                                 sc.DEFAULT_POPULATION_TYPE),
                        measured_properties=set(
                            json.get("measuredProperties", [])),
                        verticals=set(json.get("verticals", [])))


def strip_namespace(v: str) -> str:
  """
  Strips namespaces from dcids.
  e.g. 'dcid:country/USA' -> 'country/USA'
  """
  return v[v.find(_NAMESPACE_DELIMITER) + 1:]


def strip_namespace_series(series: pd.Series) -> pd.Series:
  """Vectorized strip_namespace for pandas Series.

  Args:
    series: pandas Series containing DCIDs potentially with namespaces

  Returns:
    Series with namespaces stripped
  """
  return series.str.split(_NAMESPACE_DELIMITER, n=1).str[-1]


def filter_invalid_observation_values(df: pd.DataFrame) -> pd.DataFrame:
  """Filter out rows with invalid observation values.

  Removes rows where the value is:
  - NaN/None
  - The string "<NA>"
  - Empty string

  Args:
      df: DataFrame with a 'value' column

  Returns:
      Filtered DataFrame with only valid observation values
  """
  return df[(df[constants.COLUMN_VALUE].notna()) &
            (df[constants.COLUMN_VALUE] != "<NA>") &
            (df[constants.COLUMN_VALUE] != "")].copy()


def validate_numeric_values(df: pd.DataFrame, file_path: str) -> None:
  """Validates that all values in the 'value' column are numeric or NaN.

  Raises:
      ValueError: If any non-numeric value is found.
  """
  if constants.COLUMN_VALUE not in df.columns:
    return
  # Convert values to numeric, coercion will map invalid strings to NaN
  converted = pd.to_numeric(df[constants.COLUMN_VALUE], errors='coerce')
  # Only perform expensive string operations on non-numeric candidates
  na_mask = converted.isna() & df[constants.COLUMN_VALUE].notna()
  if na_mask.any():
    val_series = df.loc[na_mask, constants.COLUMN_VALUE].astype(
        str).str.strip().str.lower()
    standard_nulls = {val.lower() for val in constants.STANDARD_NA_VALUES
                     } | {"nan", ""}
    invalid_mask = ~val_series.isin(standard_nulls)
    if invalid_mask.any():
      invalid_examples = df.loc[na_mask].loc[invalid_mask,
                                             constants.COLUMN_VALUE].unique()
      ex = ValueError(
          f"Invalid non-numeric value(s) found in observation values of file '{file_path}': "
          f"{list(invalid_examples)[:10]}. If these represent missing values, please configure "
          f"them under 'naValues' in config.json.")
      ex.file_path = file_path
      raise ex
