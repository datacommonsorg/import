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

import pandas as pd
from util.filehandler import FileHandler

_PREDICATE_TYPE_OF = "typeOf"
_PREDICATE_NAME = "name"
_STATISTICAL_VARIABLE = "StatisticalVariable"
_CUSTOM_SV_ID_PREFIX = "custom/statvar_"


@dataclass
class Triple:
  subject_id: str
  predicate: str
  object_id: str = ""
  object_value: str = ""


class TriplesGenerator:

  def __init__(self, sv_names: list[str], triples_fh: FileHandler) -> None:
    self.sv_names = sorted(list(set(sv_names)))
    self.triples_fh = triples_fh

  def generate(self):
    triples: list[Triple] = []
    triples.extend(self._all_sv_triples())
    df = pd.DataFrame(triples)
    self.triples_fh.write_string(df.to_csv(index=False))

  def _all_sv_triples(self) -> list[Triple]:
    triples: list[Triple] = []

    for index, sv_name in enumerate(self.sv_names):
      triples.extend(
          self._sv_triples(f"{_CUSTOM_SV_ID_PREFIX}{index + 1}", sv_name))

    return triples

  def _sv_triples(self, sv_id, sv_name) -> list[Triple]:
    triples: list[Triple] = []
    triples.append(
        Triple(sv_id, _PREDICATE_TYPE_OF, object_id=_STATISTICAL_VARIABLE))
    triples.append(Triple(sv_id, _PREDICATE_NAME, object_value=sv_name))
    return triples
