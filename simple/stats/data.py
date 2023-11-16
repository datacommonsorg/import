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

from dataclasses import dataclass
from dataclasses import field
from urllib.parse import urlparse

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_PREDICATE_DESCRIPTION = "description"
_PREDICATE_MEMBER_OF = "memberOf"
_PREDICATE_SPECIALIZATION_OF = "specializationOf"
_PREDICATE_URL = "url"
_PREDICATE_SOURCE = "source"
_PREDICATE_DOMAIN = "domain"
_PREDICATE_INCLUDED_IN = "includedIn"

_STATISTICAL_VARIABLE = "StatisticalVariable"
_STAT_VAR_GROUP = "StatVarGroup"
_SOURCE = "Source"
_PROVENANCE = "Provenance"


@dataclass
class Triple:
  subject_id: str
  predicate: str
  object_id: str = ""
  object_value: str = ""


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
    triples.append(
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_STAT_VAR_GROUP))
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
  nl_sentences: list[str] = field(default_factory=list)
  group_id: str = ""
  group_path: str = ""
  provenance_ids: list[str] = field(default_factory=list)
  source_ids: list[str] = field(default_factory=list)

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
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_STATISTICAL_VARIABLE))
    triples.append(Triple(self.id, _PREDICATE_NAME, object_value=self.name))
    if self.description:
      triples.append(
          Triple(self.id, _PREDICATE_DESCRIPTION,
                 object_value=self.description))
    if self.group_id:
      triples.append(
          Triple(self.id, _PREDICATE_MEMBER_OF, object_id=self.group_id))
    for provenance_id in self.provenance_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=provenance_id))
    for source_id in self.source_ids:
      triples.append(
          Triple(self.id, _PREDICATE_INCLUDED_IN, object_id=source_id))
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
class Observation:
  entity: str
  variable: str
  date: str
  value: str
  provenance: str
