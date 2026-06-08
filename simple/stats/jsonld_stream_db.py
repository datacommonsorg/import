# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A DB implementation that streams JSON-LD shards directly to GCS/Disk."""

from collections import defaultdict
import concurrent.futures
from datetime import datetime
from datetime import timezone
import gc
import hashlib
import json
import logging
import os
import tempfile
import threading

from google.cloud import storage
import pandas as pd
from rdflib import Graph
from rdflib import Literal
from rdflib import Namespace
from rdflib import RDF
import requests
from stats import constants
from stats.data import strip_namespace
from stats.data import Triple
from stats.db import Db
from stats.jsonld_exporter import DCID_URL
from stats.jsonld_exporter import expand_id
from stats.jsonld_exporter import write_shard
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File

# Configuration Constants
_CHUNK_SIZE = 10000
_UPLOAD_CONCURRENCY = 32
_EXPORT_PROCESSES_MAX = 8


def _uri_ref(val):
  if not val:
    return None
  if val.startswith(("http://", "https://", "dcid:")):
    return {"@id": val}
  return {"@id": f"dcid:{val.lstrip('/')}"}


def _parse_numeric(val):
  if val is None or val == "":
    return None
  try:
    if "." in str(val):
      return float(val)
    return int(val)
  except ValueError:
    return str(val)


def _write_observation_shard(args):
  chunk, shard_index, jsonld_dir_path, ns_map, prov_urls = args
  graph_list = []

  for row in chunk:
    entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row

    key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}"
    obs_hash = hashlib.sha256(key.encode('utf-8')).hexdigest()

    obs_obj = {
        "@id": f"dcid:obs_{obs_hash}",
        "@type": "dcid:StatVarObservation",
        "dcid:observationAbout": _uri_ref(entity),
        "dcid:variableMeasured": _uri_ref(variable),
        "dcid:observationDate": _parse_numeric(date),
        "dcid:value": _parse_numeric(value),
    }

    if provenance:
      obs_obj["dcid:provenance"] = _uri_ref(provenance)
      if provenance in prov_urls and prov_urls[provenance]:
        obs_obj["dcid:provenanceUrl"] = prov_urls[provenance]
    if unit:
      obs_obj["dcid:unit"] = _uri_ref(unit)
    if scaling_factor:
      obs_obj["dcid:scalingFactor"] = _parse_numeric(scaling_factor)
    if mmethod:
      obs_obj["dcid:measurementMethod"] = _uri_ref(mmethod)
    if period:
      obs_obj["dcid:observationPeriod"] = period

    if props:
      try:
        props_dict = json.loads(props)
        for k, v in props_dict.items():
          prop_key = f"dcid:{k}" if not k.startswith(
              "dcid:") and not k.startswith("http") else k
          obs_obj[prop_key] = v
      except json.JSONDecodeError as e:
        logging.warning(
            "Failed to decode properties JSON for observation %s/%s: %s",
            entity, variable, e)

    graph_list.append(obs_obj)

  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}

  shard_name = f"observation-{shard_index:05d}.jsonld"
  with create_store(jsonld_dir_path) as store:
    output_dir = store.as_dir()
    output_dir.open_file(shard_name).write(
        json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name}")


def _write_node_shard(args):
  # TODO(gmechali): Get rid of this and keep only the "fast" mode.
  fast_export = os.getenv("FAST_NODE_EXPORT",
                          "true").lower() in ("true", "1", "yes")
  if fast_export:
    _write_node_shard_fast(args)
  else:
    _write_node_shard_rdflib(args)


def _write_node_shard_fast(args):
  chunk, shard_index, jsonld_dir_path, ns_map = args
  subjects = {}

  for row in chunk:
    sub_id = row.subject_id
    if sub_id not in subjects:
      subjects[sub_id] = {
          "@id":
              f"dcid:{sub_id.lstrip('/')}" if not sub_id.startswith("http") and
              not sub_id.startswith("dcid:") else sub_id
      }

    pred = row.predicate
    pred_key = f"dcid:{pred}" if not pred.startswith(
        "dcid:") and not pred.startswith("http") else pred

    if pred == "typeOf":
      pred_key = "@type"

    if row.object_id:
      val = _uri_ref(row.object_id)
    else:
      val = _parse_numeric(row.object_value)

    if pred_key == "@type":
      val_str = val["@id"] if isinstance(val,
                                         dict) and "@id" in val else str(val)
      if "@type" in subjects[sub_id]:
        existing = subjects[sub_id]["@type"]
        if isinstance(existing, list):
          if val_str not in existing:
            existing.append(val_str)
        elif existing != val_str:
          subjects[sub_id]["@type"] = [existing, val_str]
      else:
        subjects[sub_id]["@type"] = val_str
    else:
      if pred_key in subjects[sub_id]:
        existing = subjects[sub_id][pred_key]
        if isinstance(existing, list):
          if val not in existing:
            existing.append(val)
        elif existing != val:
          subjects[sub_id][pred_key] = [existing, val]
      else:
        subjects[sub_id][pred_key] = val

  # Sort by @id to match rdflib output order
  graph_list = sorted(list(subjects.values()), key=lambda x: x["@id"])
  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}

  shard_name = f"node-{shard_index:05d}.jsonld"
  with create_store(jsonld_dir_path) as store:
    output_dir = store.as_dir()
    output_dir.open_file(shard_name).write(
        json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name} (fast path)")


def _write_node_shard_rdflib(args):
  """
  Writes a chunk of triples to a JSON-LD shard using rdflib.
  Args:
    args: Tuple containing (chunk, shard_index, jsonld_dir_path, ns_map)
  """

  # TODO(gmechali): Completely deprecate this path after we have 100% certainty in the direct export.
  # note that this path is exponentially slower.
  chunk, shard_index, jsonld_dir_path, ns_map = args
  DCID = Namespace(DCID_URL)
  g = Graph()
  g.bind("dcid", DCID)

  for row in chunk:
    sub = expand_id(row.subject_id)
    p = expand_id(row.predicate)
    if row.object_id:
      o = expand_id(row.object_id)
    else:
      o = Literal(row.object_value)

    if row.predicate == 'typeOf':
      g.add((sub, RDF.type, o))
    else:
      g.add((sub, p, o))

  with create_store(jsonld_dir_path) as store:
    output_dir = store.as_dir()
    write_shard(g, shard_index, output_dir, ns_map, prefix="node")


class JsonLdStreamDb(Db):
  """A DB implementation that streams triples and observations directly to JSON-LD shards on GCS/Disk."""

  def __init__(self, output_dir, import_names, nodes) -> None:
    self.output_dir = output_dir
    self.import_names = import_names
    self.nodes = nodes
    self.config = nodes.config

    # Generate unique folder name based on import name and timestamp
    import_name = None
    if isinstance(import_names, list):
      if import_names == [constants.ALL_IMPORTS]:
        import_name = constants.ALL_IMPORTS
      else:
        import_name = "_".join(import_names)

    self.import_name = import_name or nodes.config.data.get(
        "importName") or "default_import_name"
    if self.import_name and "/" in self.import_name:
      self.import_name = self.import_name.replace("/", "_")

    timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
    unique_dir_name = f"{self.import_name}_{timestamp}"
    self.jsonld_dir = output_dir.open_dir("jsonld").open_dir(unique_dir_name)

    self.obs_shard_index = 0
    self.node_shard_index = 0
    self.ns_map = {"dcid": DCID_URL}
    self.lock = threading.Lock()
    self._obs_records = defaultdict(list)
    self._triples = defaultdict(list)
    self._processed_imports = set()

  def insert_observations(self, observations_df: pd.DataFrame,
                          input_file: File):
    if not observations_df.empty:
      import_name = self.config.import_name(input_file)
      records = observations_df.to_records(index=False).tolist()
      with self.lock:
        self._processed_imports.add(import_name)
        self._obs_records[import_name].extend(records)

  def insert_triples(self, triples: list[Triple], input_file: File = None):
    if triples:
      with self.lock:
        if input_file:
          import_name = self.config.import_name(input_file)
          self._processed_imports.add(import_name)
          self._triples[import_name].extend(triples)
        else:
          self._triples["_global"].extend(triples)

  def commit(self):
    pass

  def commit_and_close(self):
    # Add global triples to every processed import's triples
    global_triples = self._triples.pop("_global", [])
    for import_name in self._processed_imports:
      self._triples[import_name].extend(global_triples)

    with tempfile.TemporaryDirectory() as temp_local_dir:
      logging.info("Using local temporary directory for export buffering: %s",
                   temp_local_dir)

      if self._obs_records or self._triples:
        logging.info("Starting JSON-LD local export in sequential streaming mode")

        # Process each import sequentially to minimize peak memory usage and
        # prevent any serverless OOM crashes.
        for import_name in self._processed_imports:
          import_temp_dir = os.path.join(temp_local_dir, import_name)
          os.makedirs(import_temp_dir, exist_ok=True)

          if import_name in self._obs_records:
            logging.info("Streaming observations export for %s...", import_name)
            for args in self._generate_observation_chunks(import_name, import_temp_dir):
              _write_observation_shard(args)
            logging.info("Completed observations export for %s.", import_name)

          if import_name in self._triples:
            logging.info("Streaming triples export for %s...", import_name)
            for args in self._generate_node_chunks(import_name, import_temp_dir):
              _write_node_shard(args)
            logging.info("Completed triples export for %s.", import_name)

        self._upload_shards(temp_local_dir)

  def _generate_observation_chunks(self, import_name: str, import_temp_dir: str):
    """Generates observation chunks of size _CHUNK_SIZE, cleaning memory progressively."""
    prov_urls = {}
    for prov in self.nodes.provenances.values():
      prov_id = strip_namespace(prov.id)
      prov_urls[prov_id] = prov.url
      prov_urls[prov.id] = prov.url

    records = self._obs_records.get(import_name, [])
    shard_index = 0
    while records:
      chunk = []
      # Pop from the end to avoid O(N) list shifting overhead
      for _ in range(min(_CHUNK_SIZE, len(records))):
        chunk.append(records.pop())
      yield (chunk, shard_index, import_temp_dir, self.ns_map,
             prov_urls)
      shard_index += 1
    if import_name in self._obs_records:
      self._obs_records[import_name].clear()

  def _generate_node_chunks(self, import_name: str, import_temp_dir: str):
    """Generates node chunks of size _CHUNK_SIZE, cleaning memory progressively."""
    triples = self._triples.get(import_name, [])
    shard_index = 0
    while triples:
      chunk = []
      # Pop from the end to avoid O(N) list shifting overhead
      for _ in range(min(_CHUNK_SIZE, len(triples))):
        chunk.append(triples.pop())
      yield (chunk, shard_index, import_temp_dir, self.ns_map)
      shard_index += 1
    if import_name in self._triples:
      self._triples[import_name].clear()

  def _upload_shards(self, temp_local_dir: str):
    """Uploads files in temp_local_dir to jsonld_dir, optimizing for GCS via native SDK."""
    files_to_upload = []
    for root, _, filenames in os.walk(temp_local_dir):
      for filename in filenames:
        abs_path = os.path.join(root, filename)
        rel_path = os.path.relpath(abs_path, temp_local_dir)
        files_to_upload.append(rel_path)

    if not files_to_upload:
      return

    target_path = self.jsonld_dir.full_path()
    logging.info(
        "Bulk uploading %d JSON-LD shards to target directory %s in parallel",
        len(files_to_upload), target_path)

    if target_path.startswith("gs://"):
      self._upload_shards_gcs(temp_local_dir, files_to_upload, target_path)
    else:
      self._upload_shards_local(temp_local_dir, files_to_upload)

    logging.info("Bulk upload of JSON-LD shards completed successfully.")

  def _upload_shards_gcs(self, temp_local_dir: str, files: list[str],
                          target_path: str):
    """Performs concurrent GCS uploads using native google-cloud-storage client."""
    # Parse bucket and blob prefix
    parts = target_path[5:].split("/", 1)
    bucket_name = parts[0]
    blob_prefix = parts[1].rstrip("/") if len(parts) > 1 else ""

    client = storage.Client()

    # Configure connection pool size for concurrent GCS uploads
    adapter = requests.adapters.HTTPAdapter(
        pool_connections=_UPLOAD_CONCURRENCY, pool_maxsize=_UPLOAD_CONCURRENCY)
    client._http.mount("https://", adapter)
    client._http.mount("http://", adapter)

    bucket = client.bucket(bucket_name)

    def _upload_single(rel_path: str):
      local_file_path = os.path.join(temp_local_dir, rel_path)
      blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
      blob = bucket.blob(blob_key)
      blob.upload_from_filename(local_file_path)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_UPLOAD_CONCURRENCY) as executor:
      list(executor.map(_upload_single, files))

  def _upload_shards_local(self, temp_local_dir: str, files: list[str]):
    """Performs concurrent local file copy (for test environments)."""
    target_store = self.jsonld_dir

    parent_dirs = set(os.path.dirname(f) for f in files if os.path.dirname(f))
    for d in sorted(parent_dirs):
      target_store.open_dir(d)

    def _copy_single(rel_path: str):
      local_file_path = os.path.join(temp_local_dir, rel_path)
      with open(local_file_path, "r") as f:
        content = f.read()
      target_store.open_file(rel_path).write(content)

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_UPLOAD_CONCURRENCY) as executor:
      list(executor.map(_copy_single, files))
