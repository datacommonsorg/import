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
import yaml

_DCID_COL = "dcid"
_SENTENCE_COL = "sentence"
_SENTENCE_SEPARATOR = ";"

_CUSTOM_EMBEDDING_INDEX = "user_all_minilm_mem"
_CUSTOM_MODEL = "ft-final-v20230717230459-all-MiniLM-L6-v2"
_CUSTOM_MODEL_PATH = "gs://datcom-nl-models/ft_final_v20230717230459.all-MiniLM-L6-v2"
_EMBEDDINGS_DIR = "embeddings"
_EMBEDDINGS_FILE = "embeddings.csv"
_SENTENCES_FILE = "sentences.csv"
_CUSTOM_CATALOG_YAML = "custom_catalog.yaml"

# Source path in test catalog files will be rewritten with this fake path.
_FAKE_SOURCE_PATH = "//fake/path"


def generate_nl_sentences(triples: list[Triple], nl_dir_fh: FileHandler):
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

  sentences_fh = nl_dir_fh.make_file(_SENTENCES_FILE)
  logging.info("Writing %s NL sentences to: %s", dataframe.size, sentences_fh)
  sentences_fh.write_string(dataframe.to_csv(index=False))

  embeddings_dir_fh = nl_dir_fh.make_file(f"{_EMBEDDINGS_DIR}/")
  embeddings_dir_fh.make_dirs()
  embeddings_fh = embeddings_dir_fh.make_file(_EMBEDDINGS_FILE)
  catalog_fh = embeddings_dir_fh.make_file(_CUSTOM_CATALOG_YAML)
  catalog_dict = _catalog_dict(nl_dir_fh.path, embeddings_fh.path)
  catalog_yaml = yaml.safe_dump(catalog_dict)
  logging.info("Writing custom catalog to path %s:\n%s", catalog_fh,
               catalog_yaml)
  catalog_fh.write_string(catalog_yaml)


def _catalog_dict(nl_dir: str, embeddings_path: str) -> dict:
  return {
      "version": "1",
      "indexes": {
          _CUSTOM_EMBEDDING_INDEX: {
              "store_type": "MEMORY",
              "source_path": nl_dir,
              "embeddings_path": embeddings_path,
              "model": _CUSTOM_MODEL
          },
      },
      "models": {
          _CUSTOM_MODEL: {
              "type": "LOCAL",
              "usage": "EMBEDDINGS",
              "gcs_folder": _CUSTOM_MODEL_PATH,
              "score_threshold": 0.5
          }
      }
  }


def _rewrite_catalog_for_testing(catalog_fh: FileHandler):
  """
  Test catalog files are written to temp folders whose values change from test to test.
  To consistently test the catalog out against a golden file, we replace the root of paths in the catalog
  with a constant fake path.
  """
  catalog_dict = yaml.safe_load(catalog_fh.read_string())
  custom_index = catalog_dict["indexes"][_CUSTOM_EMBEDDING_INDEX]
  orig_source_path = custom_index["source_path"]
  custom_index["source_path"] = _FAKE_SOURCE_PATH
  orig_embeddings_path: str = custom_index["embeddings_path"]
  custom_index["embeddings_path"] = orig_embeddings_path.replace(
      orig_source_path, _FAKE_SOURCE_PATH)
  catalog_fh.write_string(yaml.safe_dump(catalog_dict))


@dataclass
class SentenceCandidates:
  name: str = ""
  searchDescriptions: list[str] = field(default_factory=list)

  def maybe_add(self, triple: Triple):
    if triple.predicate == sc.PREDICATE_SEARCH_DESCRIPTION:
      self.searchDescriptions.append(triple.object_value)
    elif triple.predicate == sc.PREDICATE_NAME:
      self.name = triple.object_value

  def sentences(self) -> str:
    sentences: list[str] = []

    if self.searchDescriptions:
      sentences = self.searchDescriptions
    elif self.name:
      sentences = [self.name]

    return _SENTENCE_SEPARATOR.join(sentences)
