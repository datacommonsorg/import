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
from stats.config import Config
from stats.data import StatVar
from stats.data import StatVarGroup
from stats.data import Triple
from util.filehandler import FileHandler


def generate_and_save(variables: list[StatVar], groups: list[StatVarGroup],
                      triples_fh: FileHandler):
  triples = generate(variables, groups)
  df = pd.DataFrame(triples)
  triples_fh.write_string(df.to_csv(index=False))


def generate(variables: list[StatVar],
             groups: list[StatVarGroup]) -> list[Triple]:
  triples: list[Triple] = []
  for group in groups:
    triples.extend(group.triples())
  for variable in variables:
    triples.extend(variable.triples())
  return triples
