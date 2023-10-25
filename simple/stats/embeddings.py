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
from sentence_transformers import SentenceTransformer
from stats.data import StatVar
from util.filehandler import FileHandler
from util.filehandler import LocalFileHandler

_MODEL_NAME = "all-MiniLM-L6-v2"
_DCID_COL = "dcid"
_SENTENCE_COL = "sentence"


def build(svs: list[StatVar],
          embeddings_fh: FileHandler | None) -> pd.DataFrame:
  """Builds embeddings based on the name, description and NL sentences of the specified SVs.

    If the embeddings FileHandler is specified, it writes the embeddings to that file.
    """
  texts: list[str] = []
  dcids: list[str] = []

  def _maybe_add(text: str, dcid: str):
    if text and dcid:
      texts.append(text)
      dcids.append(dcid)

  for sv in svs:
    _maybe_add(sv.name, sv.id)
    _maybe_add(sv.description, sv.id)
    for sentence in sv.nl_sentences:
      _maybe_add(sentence, sv.id)

  assert len(texts) == len(dcids)

  logging.info("Encoding %s sentences", len(texts))

  model = SentenceTransformer(_MODEL_NAME)
  embeddings = model.encode(texts, show_progress_bar=True)
  dataframe = pd.DataFrame(embeddings)
  dataframe[_DCID_COL] = dcids
  dataframe[_SENTENCE_COL] = texts

  if embeddings_fh:
    logging.info("Writing %s embeddings to: %s", dataframe.size, embeddings_fh)
    embeddings_fh.write_string(dataframe.to_csv(index=False))

  return dataframe
