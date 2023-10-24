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

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_PREDICATE_DESCRIPTION = "description"
_PREDICATE_MEMBER_OF = "memberOf"
_PREDICATE_SPECIALIZATION_OF = "specializationOf"

_STATISTICAL_VARIABLE = "StatisticalVariable"
_STAT_VAR_GROUP = "StatVarGroup"


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

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(
        Triple(self.id, _PREDICATE_TYPE_OF, object_id=_STAT_VAR_GROUP))
    triples.append(Triple(self.id, _PREDICATE_NAME, object_value=self.name))
    triples.append(
        Triple(self.id, _PREDICATE_SPECIALIZATION_OF, object_id=self.parent_id))
    return triples


@dataclass
class StatVar:
  id: str
  name: str
  description: str = ""
  nl_sentences: list[str] = field(default_factory=list)
  group_id: str = ""
  group_path: str = ""

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
    return triples
