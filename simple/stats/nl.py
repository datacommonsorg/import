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
import logging

import pandas as pd
from stats.data import Triple
import stats.schema_constants as sc
from util.filehandler import FileHandler

_DCID_COL = "dcid"
_SENTENCE_COL = "sentence"
_SENTENCE_SEPARATOR = ";"


def generate_nl_sentences(triples: list[Triple], sentences_fh: FileHandler):
  """Generates NL sentences based on name and searchDescription triples.

  This method should only be called for triples of types for which NL sentences
  should be generated. Currently it is StatisticalVariable and Topic.

  This method does not do the type checks itself and the onus is on the caller 
  to filter triples.

  The dcids and sentences are written to a CSV using the specified FileHandler
  """

  dcid2candidates: dict[str, SentenceCandidates] = {}
  for triple in triples:
    dcid2candidates.setdefault(triple.subject_id,
                               SentenceCandidates()).maybe_add(triple)

  rows = []
  for dcid, candidates in dcid2candidates.items():
    sentences = candidates.sentences()
    if not sentences:
      logging.warning("No NL sentences generated for DCID: %s", dcid)
      continue
    rows.append({_DCID_COL: dcid, _SENTENCE_COL: sentences})

  dataframe = pd.DataFrame(rows)

  logging.info("Writing %s NL sentences to: %s", dataframe.size, sentences_fh)
  sentences_fh.write_string(dataframe.to_csv(index=False))


@dataclass
class SentenceCandidates:
  name: str = ""
  utteranceTemplates: list[str] = field(default_factory=list)

  def maybe_add(self, triple: Triple):
    if triple.predicate == sc.PREDICATE_SEARCH_DESCRIPTION:
      self.utteranceTemplates.append(triple.object_value)
    elif triple.predicate == sc.PREDICATE_NAME:
      self.name = triple.object_value

  def sentences(self) -> str:
    sentences: list[str] = []

    if self.utteranceTemplates:
      sentences = self.utteranceTemplates
    elif self.name:
      sentences = [self.name]

    return _SENTENCE_SEPARATOR.join(sentences)
