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
from stats.data import StatVar
from util.filehandler import FileHandler

_DCID_COL = "dcid"
_SENTENCE_COL = "sentence"
_SENTENCE_SEPARATOR = ";"


def generate_sv_sentences(svs: list[StatVar], sentences_fh: FileHandler):
  """Generates sentences based on the name, description and NL sentences of the specified SVs.

    The SV dcids and sentences are written to a CSV using the specified FileHandler
    """
  rows = []
  for sv in svs:
    rows.append({_DCID_COL: sv.id, _SENTENCE_COL: _sv_sentences(sv)})

  dataframe = pd.DataFrame(rows)

  logging.info("Writing %s SV sentences to: %s", dataframe.size, sentences_fh)
  sentences_fh.write_string(dataframe.to_csv(index=False))


def _sv_sentences(sv: StatVar) -> str:
  sentences = []
  sentences.append(sv.name)
  if sv.description:
    sentences.append(sv.description)
  for nl_sentence in sv.nl_sentences:
    if nl_sentence:
      sentences.append(nl_sentence)
  return _SENTENCE_SEPARATOR.join(sentences)
