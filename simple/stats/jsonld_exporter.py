# Copyright 2026 Google Inc.
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

import json
import logging

from pyld import jsonld
from rdflib import Graph
from rdflib import URIRef

DCID_URL = "https://datacommons.org/browser/"


def expand_id(item):
  """Expands a short ID into a full URIRef."""
  if not item:
    return None
  if item.startswith("http://") or item.startswith("https://"):
    return URIRef(item)
  if item.startswith("dcid:"):
    return URIRef(f"{DCID_URL}{item[5:]}")
  return URIRef(f"{DCID_URL}{item.lstrip('/')}")


def write_shard(g: Graph,
                index: int,
                output_dir,
                ns_map: dict,
                prefix: str = "output"):
  """
  Serializes and writes an RDF graph to a JSON-LD shard.

  Args:
  -----
    g: The RDF graph to serialize.
    index: The shard index for the filename.
    output_dir: The directory to write the shard file to.
    ns_map: The namespace map for context compaction.
    prefix: The file name prefix (e.g. 'node' or 'observation').

  """
  jsonld_str = g.serialize(context=ns_map, format="json-ld", indent=4)
  expanded_jsonld = json.loads(jsonld_str)
  compacted_jsonld = jsonld.compact(expanded_jsonld, ns_map)

  if "@graph" not in compacted_jsonld:
    data_only = {k: v for k, v in compacted_jsonld.items() if k != "@context"}
    compacted_jsonld = {
        "@context": compacted_jsonld.get("@context"),
        "@graph": [data_only]
    }

  shard_name = f"{prefix}-{index:05d}.jsonld"
  output_dir.open_file(shard_name).write(json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name}")
