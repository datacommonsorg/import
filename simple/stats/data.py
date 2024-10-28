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
from dataclasses import fields
from dataclasses import is_dataclass
from enum import StrEnum
import json
from typing import Self
from urllib.parse import urlparse

from stats import schema_constants as sc
from stats.util import base64_decode_and_gunzip_json
from stats.util import gzip_and_base64_encode_json

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_PREDICATE_DESCRIPTION = "description"
_PREDICATE_MEMBER_OF = "memberOf"
_PREDICATE_SPECIALIZATION_OF = "specializationOf"
_PREDICATE_URL = "url"
_PREDICATE_SOURCE = "source"
_PREDICATE_DOMAIN = "domain"
_PREDICATE_INCLUDED_IN = "includedIn"
_PREDICATE_SUB_CLASS_OF = "subClassOf"
_PREDICATE_OBSERVATION_DATE = "observationDate"
_PREDICATE_LOCATION = "location"
_PREDICATE_POPULATION_TYPE = "populationType"
_PREDICATE_MEASURED_PROPERTY = "measuredProperty"
_PREDICATE_STAT_TYPE = "statType"
_PREDICATE_SEARCH_DESCRIPTION = "searchDescription"

STATISTICAL_VARIABLE = "StatisticalVariable"
STAT_VAR_GROUP = "StatVarGroup"
_SOURCE = "Source"
_PROVENANCE = "Provenance"
_PROPERTY = "Property"
_CLASS = "Class"
_EVENT = "Event"
_THING = "schema:Thing"
_MEASURED_VALUE = "measuredValue"

_MCF_PREDICATE_BLOCKLIST = set([_PREDICATE_INCLUDED_IN])

_DCS_PREFIX = "dcs:"

_NAMESPACE_DELIMITER = ':'


@dataclass
class Triple:
  subject_id: str
  predicate: str
  object_id: str = ""
  object_value: str = ""

  def db_tuple(self):
    return (_strip_namespace(self.subject_id), self.predicate,
            _strip_namespace(self.object_id), self.object_value)


@dataclass
class StatVarGroup:
  id: str
  name: str
  parent_id: str
  provenance_ids: list[str] = field(default_factory=list)
  source_ids: list[str] = field(default_factory=list)

  def add_provenance(self, provenance: "Provenance") -> "StatVarGroup":
    provenance_id = provenance.id
    source_id = provenance.source_id
    if not provenance_id in self.provenance_ids:
      self.provenance_ids.append(provenance_id)
    if not source_id in self.source_ids:
      self.source_ids.append(source_id)

    return self

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(Triple(self.id, _PREDICATE_TYPE_OF,
                          object_id=STAT_VAR_GROUP))
    triples.append(Triple(self.id, _PREDICATE_NAME, object_value=self.name))
    triples.append(
        Triple(self.id, _PREDICATE_SPECIALIZATION_OF, object_id=self.parent_id))
    for provenance_id in self.provenance_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=provenance_id))
    for source_id in self.source_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=source_id))
    return triples


@dataclass
class StatVar:
  id: str
  name: str
  description: str = ""
  search_descriptions: list[str] = field(default_factory=list)
  group_id: str = ""
  group_path: str = ""
  provenance_ids: list[str] = field(default_factory=list)
  source_ids: list[str] = field(default_factory=list)
  properties: dict[str, str] = field(default_factory=dict)

  def __post_init__(self):
    if _PREDICATE_POPULATION_TYPE not in self.properties:
      self.properties[_PREDICATE_POPULATION_TYPE] = _THING
    # If measuredProperty is not specified, we set it to the SV dcid,
    # thereby making it schema-less.
    if _PREDICATE_MEASURED_PROPERTY not in self.properties and self.id:
      self.properties[_PREDICATE_MEASURED_PROPERTY] = self.id
    if _PREDICATE_STAT_TYPE not in self.properties:
      self.properties[_PREDICATE_STAT_TYPE] = _MEASURED_VALUE

    # Add dcs: prefix to prop values if not specified.
    for p, v in self.properties.items():
      if not ":" in v:
        self.properties[p] = f"{_DCS_PREFIX}{v}"

  def add_provenance(self, provenance: "Provenance") -> "StatVar":
    provenance_id = provenance.id
    source_id = provenance.source_id
    if not provenance_id in self.provenance_ids:
      self.provenance_ids.append(provenance_id)
    if not source_id in self.source_ids:
      self.source_ids.append(source_id)

    return self

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=STATISTICAL_VARIABLE))
    triples.append(Triple(self.id, _PREDICATE_NAME, object_value=self.name))
    if self.description:
      triples.append(
          Triple(self.id, _PREDICATE_DESCRIPTION,
                 object_value=self.description))

    # Encode search descriptions as searchDescription triples.
    for search_description in self.search_descriptions:
      triples.append(
          Triple(self.id,
                 _PREDICATE_SEARCH_DESCRIPTION,
                 object_value=search_description))

    if self.group_id:
      triples.append(
          Triple(self.id, _PREDICATE_MEMBER_OF, object_id=self.group_id))
    for provenance_id in self.provenance_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=provenance_id))
    for source_id in self.source_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=source_id))
    for p, v in self.properties.items():
      triples.append(Triple(self.id, p, object_id=v))
    return triples


@dataclass
class Entity:
  entity_dcid: str
  entity_type: str

  def triples(self) -> list[Triple]:
    # Currently only 1 triple is generated but could be more in the future (e.g. name)
    return [
        Triple(self.entity_dcid, _PREDICATE_TYPE_OF, object_id=self.entity_type)
    ]


@dataclass
class Provenance:
  id: str
  source_id: str
  name: str
  url: str = ""

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.extend([
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_PROVENANCE),
        Triple(self.id, _PREDICATE_NAME, object_value=self.name),
        Triple(self.id, _PREDICATE_SOURCE, object_id=self.source_id),
    ])
    if self.url:
      triples.append(Triple(self.id, _PREDICATE_URL, object_value=self.url))
    return triples


@dataclass
class Source:
  id: str
  name: str
  url: str = ""
  domain: str = field(init=False)

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
class Observation:
  entity: str
  variable: str
  date: str
  value: str
  provenance: str
  properties: ObservationProperties = field(
      default_factory=ObservationProperties.new)

  def db_tuple(self):
    return (_strip_namespace(self.entity), _strip_namespace(self.variable),
            self.date, self.value, _strip_namespace(self.provenance),
            _strip_namespace(self.properties.unit),
            self.properties.scaling_factor,
            _strip_namespace(self.properties.measurement_method),
            _strip_namespace(self.properties.observation_period),
            json.dumps(self.properties.properties)
            if self.properties.properties else "")


def _get_flattened_dataclass_field_names(cls) -> list[str]:
  """Flattens the field names from the specified class and any of it's nested member classes.
  In practice, this is used to flatten the fields from the Observation and ObservationProperties classes
  which is used to populate test CSVs.
  """
  field_names: list[str] = []
  for field in fields(cls):
    if is_dataclass(field.type):
      field_names.extend(_get_flattened_dataclass_field_names(field.type))
    else:
      field_names.append(field.name)
  return field_names


OBSERVATION_FIELD_NAMES = _get_flattened_dataclass_field_names(Observation)


@dataclass
class Property:
  dcid: str
  name: str

  def triples(self) -> list[Triple]:
    return [
        Triple(self.dcid, _PREDICATE_TYPE_OF, object_id=_PROPERTY),
        Triple(self.dcid, _PREDICATE_NAME, object_value=self.name),
    ]


@dataclass
class EventType:
  id: str
  name: str
  description: str = ""
  provenance_ids: list[str] = field(default_factory=list)
  source_ids: list[str] = field(default_factory=list)

  def add_provenance(self, provenance: Provenance) -> Self:
    provenance_id = provenance.id
    source_id = provenance.source_id
    if not provenance_id in self.provenance_ids:
      self.provenance_ids.append(provenance_id)
    if not source_id in self.source_ids:
      self.source_ids.append(source_id)

    return self

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(Triple(self.id, _PREDICATE_TYPE_OF, object_id=_CLASS))
    triples.append(Triple(self.id, _PREDICATE_SUB_CLASS_OF, object_id=_EVENT))
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


@dataclass
class Event:
  id: str
  event_type: str
  entity: str = ""
  date: str = ""
  provenance_id: str = ""
  properties: dict[str, str] = field(default_factory=lambda: defaultdict(dict))

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=self.event_type))
    if self.entity:
      triples.append(Triple(self.id, _PREDICATE_LOCATION,
                            object_id=self.entity))
    if self.date:
      triples.append(
          Triple(self.id, _PREDICATE_OBSERVATION_DATE, object_value=self.date))
    if self.provenance_id:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=self.provenance_id))
    for prop, value in self.properties.items():
      triples.append(Triple(self.id, prop, object_value=value))
    return triples


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
    if not provenance_id in self.provenance_ids:
      self.provenance_ids.append(provenance_id)
    if not source_id in self.source_ids:
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


class ImportType(StrEnum):
  OBSERVATIONS = "observations"
  EVENTS = "events"
  ENTITIES = "entities"


class InputFileFormat(StrEnum):
  VARIABLE_PER_ROW = "variablePerRow"
  VARIABLE_PER_COLUMN = "variablePerColumn"


class TimePeriod(StrEnum):
  DAY = "day"
  MONTH = "month"
  YEAR = "year"


class AggregationMethod(StrEnum):
  COUNT = "count"


@dataclass
class AggregationConfig:
  period: TimePeriod = TimePeriod.YEAR
  method: AggregationMethod = AggregationMethod.COUNT

  def __post_init__(self):
    if self.period not in TimePeriod._member_map_.values():
      raise ValueError(f"invalid period: {self.period}")
    if self.method not in AggregationMethod._member_map_.values():
      raise ValueError(f"invalid method: {self.method}")


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


# dict[svg id, svg specialized name]
SVGSpecializedNames = dict[str, str]

# dict[parent svg id, child SVGSpecializedNames]
ParentSVG2ChildSpecializedNames = dict[str, SVGSpecializedNames]


@dataclass
class StatVarHierarchyResult:
  svg_triples: list[Triple]
  svg_specialized_names: ParentSVG2ChildSpecializedNames


def _strip_namespace(v: str) -> str:
  return v[v.find(_NAMESPACE_DELIMITER) + 1:]
