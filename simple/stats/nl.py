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
import json
import logging

import pandas as pd
from stats.data import Triple
from stats.nl_constants import CUSTOM_EMBEDDINGS_INDEX
from stats.nl_constants import CUSTOM_MODEL
from stats.nl_constants import CUSTOM_MODEL_PATH
import stats.schema_constants as sc
from util.filesystem import Dir
from util.filesystem import File
import yaml

_DCID_COL = "dcid"
_SENTENCE_COL = "sentence"
_SENTENCE_SEPARATOR = ";"

_EMBEDDINGS_DIR = "embeddings"
_EMBEDDINGS_FILE = "embeddings.csv"
_SENTENCES_FILE = "sentences.csv"
_CUSTOM_CATALOG_YAML = "custom_catalog.yaml"
_TOPIC_CACHE_JSON_FILE = "custom_dc_topic_cache.json"


def generate_nl_sentences(triples: list[Triple], nl_dir: Dir):
  """Generates NL sentences based on name and searchDescription triples.

  This method should only be called for triples of types for which NL sentences
  should be generated. Currently it is StatisticalVariable and Topic.

  This method does not do the type checks itself and the onus is on the caller
  to filter triples.

  The dcids and sentences are written to a CSV using the specified File.
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

  sentences_file = nl_dir.open_file(_SENTENCES_FILE)
  logging.info("Writing %s NL sentences to: %s", dataframe.size, sentences_file)
  sentences_file.write(dataframe.to_csv(index=False))

  embeddings_dir = nl_dir.open_dir(_EMBEDDINGS_DIR)
  embeddings_file = embeddings_dir.open_file(_EMBEDDINGS_FILE)
  catalog_file = embeddings_dir.open_file(_CUSTOM_CATALOG_YAML)
  catalog_dict = _catalog_dict(nl_dir, embeddings_file)
  catalog_yaml = yaml.safe_dump(catalog_dict)
  logging.info("Writing custom catalog to path %s:\n%s", catalog_file,
               catalog_yaml)
  catalog_file.write(catalog_yaml)


def generate_topic_cache(triples: list[Triple], nl_dir: Dir):
  """Generates topic cache based on Topic and StatVarPeerGroup triples.

  This method should only be called for triples of types for which topic cache
  should be generated (Topic and StatVarPeerGroup).

  This method does not do the type checks itself and the onus is on the caller
  to filter triples.

  The topic cache is written to a custom_dc_topic_cache.json file in the specified directory.
  """

  dcid2nodes: dict[str, TopicCacheNode] = {}
  for triple in triples:
    dcid2nodes.setdefault(triple.subject_id,
                          TopicCacheNode(triple.subject_id)).maybe_add(triple)

  nodes = []
  for node in dcid2nodes.values():
    nodes.append(node.json())

  result = {"nodes": nodes}
  topic_cache_file = nl_dir.open_file(_TOPIC_CACHE_JSON_FILE)
  logging.info("Writing %s topic cache nodes to: %s", len(nodes),
               topic_cache_file)
  topic_cache_file.write(json.dumps(result, indent=1))


def _catalog_dict(nl_dir: Dir, embeddings_file: File) -> dict:
  return {
      "version": "1",
      "indexes": {
          CUSTOM_EMBEDDINGS_INDEX: {
              "store_type": "MEMORY",
              "source_path": nl_dir.full_path(),
              "embeddings_path": embeddings_file.full_path(),
              "model": CUSTOM_MODEL
          },
      },
      "models": {
          CUSTOM_MODEL: {
              "type": "LOCAL",
              "usage": "EMBEDDINGS",
              "gcs_folder": CUSTOM_MODEL_PATH,
              "score_threshold": 0.5
          }
      }
  }


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


@dataclass
class TopicCacheNode:
  dcid: str
  types: list[str] = field(default_factory=list)
  names: list[str] = field(default_factory=list)
  relevantVariables: list[str] = field(default_factory=list)
  members: list[str] = field(default_factory=list)

  def _csv_to_list(self, csv: str) -> list[str]:
    return [item.strip() for item in csv.split(",")]

  def maybe_add(self, triple: Triple):
    if triple.predicate == sc.PREDICATE_TYPE_OF:
      self.types.append(triple.object_id)
    elif triple.predicate == sc.PREDICATE_NAME:
      self.names.append(triple.object_value)
    elif triple.predicate == sc.PREDICATE_RELEVANT_VARIABLE:
      self.relevantVariables.append(triple.object_id)
    elif triple.predicate == sc.PREDICATE_RELEVANT_VARIABLE_LIST:
      self.relevantVariables.extend(self._csv_to_list(triple.object_value))
    elif triple.predicate == sc.PREDICATE_MEMBER:
      self.members.append(triple.object_id)
    elif triple.predicate == sc.PREDICATE_MEMBER_LIST:
      self.members.extend(self._csv_to_list(triple.object_value))

  def json(self) -> dict[str, any]:
    result: dict[str, any] = {}
    result["dcid"] = [self.dcid]
    if self.types:
      result["typeOf"] = self.types
    if self.names:
      result["name"] = self.names
    if self.relevantVariables:
      result["relevantVariableList"] = self.relevantVariables
    if self.members:
      result["memberList"] = self.members
    return result
