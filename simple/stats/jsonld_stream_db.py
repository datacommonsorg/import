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
import multiprocessing
import os
import shutil
import tempfile
import threading
from typing import Callable, Optional

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import TooManyRequests
from google.api_core.retry import Retry
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
from stats.data import validate_numeric_values
from stats.db import Db
from stats.jsonld_exporter import DCID_URL
from stats.jsonld_exporter import expand_id
from stats.jsonld_exporter import write_shard
from stats.util import is_entity_reference
from stats.util import is_uri_or_namespace
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File

# Configuration Constants
_CHUNK_SIZE = 10000
_UPLOAD_CONCURRENCY = 4
_EXPORT_PROCESSES_MAX = 8

_KNOWN_CUSTOM_PREFIXES = set()
_STANDARD_PREFIXES = {"http", "https", "dcid", "schema", "dcs"}


def _rewrite_custom_ns_to_dcid(val: str) -> str:
  """Rewrites custom namespaces to 'dcid:' as a temporary workaround until the ingestion loader natively supports custom namespaces.

  This allows users to author schemas with their custom namespaces today, which will be fully preserved when we re-ingest after backend support is added.
  """
  if not isinstance(val, str) or not val:
    return val
  if ":" in val and " " not in val:
    prefix, suffix = val.split(":", 1)
    if prefix in _KNOWN_CUSTOM_PREFIXES:
      return f"dcid:{suffix.lstrip('/')}"
    if prefix in _STANDARD_PREFIXES:
      return val
    if prefix.isalnum() and prefix.lower() not in _STANDARD_PREFIXES:
      _KNOWN_CUSTOM_PREFIXES.add(prefix)
      return f"dcid:{suffix.lstrip('/')}"
  return val


def _uri_ref(val):
  if not val or pd.isna(val):
    return None
  val_str = str(val).strip()
  if val_str == "" or val_str.lower() in ("nan", "<na>"):
    return None
  if is_uri_or_namespace(val_str):
    return {"@id": val_str}
  return {"@id": f"dcid:{val_str.lstrip('/')}"}


def _parse_numeric(val):
  if val is None or val == "" or pd.isna(val):
    return None
  try:
    if "." in str(val):
      return float(val)
    return int(val)
  except ValueError:
    return str(val)


def _write_observation_shard(chunk_or_args,
                             shard_index: Optional[int] = None,
                             jsonld_dir_path: Optional[str] = None,
                             ns_map: Optional[dict[str, str]] = None,
                             prov_urls: Optional[dict[str, str]] = None,
                             track_hash_fn: Optional[Callable] = None,
                             file_name: str = ""):
  if isinstance(chunk_or_args, tuple):
    chunk = chunk_or_args[0]
    shard_index = chunk_or_args[1]
    jsonld_dir_path = chunk_or_args[2]
    ns_map = chunk_or_args[3]
    prov_urls = chunk_or_args[4]
  else:
    chunk = chunk_or_args

  graph_list = []
  chunk_hashes = []

  for row in chunk:
    entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row

    key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_{props}"
    obs_hash = hashlib.sha256(key.encode('utf-8')).hexdigest()
    if track_hash_fn:
      chunk_hashes.append((obs_hash, entity, variable, date, provenance))

    var_obj = _uri_ref(_rewrite_custom_ns_to_dcid(variable))
    prop_keys = None
    if props:
      try:
        props_dict = json.loads(props)
        if isinstance(props_dict, dict):
          prop_keys = [
              f"dcid:{k}" if not k.startswith(
                  ("dcid:", "http://", "https://")) else k
              for k in props_dict.keys()
          ]
          if prop_keys and var_obj:
            var_obj["dcid:observationProperties"] = prop_keys
      except json.JSONDecodeError:
        pass

    obs_obj = {
        "@id": f"dcid:obs_{obs_hash}",
        "@type": "dcid:StatVarObservation",
        "dcid:variableMeasured": var_obj,
        "dcid:observationDate": _parse_numeric(date),
        "dcid:value": _parse_numeric(value),
    }

    entity_ref = _uri_ref(_rewrite_custom_ns_to_dcid(entity))
    if entity_ref:
      obs_obj["dcid:observationAbout"] = entity_ref

    if provenance:
      obs_obj["dcid:provenance"] = _uri_ref(
          _rewrite_custom_ns_to_dcid(provenance))
      if provenance in prov_urls and prov_urls[provenance]:
        obs_obj["dcid:provenanceUrl"] = prov_urls[provenance]
    if unit:
      obs_obj["dcid:unit"] = _uri_ref(_rewrite_custom_ns_to_dcid(unit))
    if scaling_factor:
      obs_obj["dcid:scalingFactor"] = _parse_numeric(scaling_factor)
    if mmethod:
      obs_obj["dcid:measurementMethod"] = _uri_ref(
          _rewrite_custom_ns_to_dcid(mmethod))
    if period:
      obs_obj["dcid:observationPeriod"] = period

    if props:
      try:
        props_dict = json.loads(props)
        if isinstance(props_dict, dict):
          for k, v in props_dict.items():
            prop_key = f"dcid:{k}" if not k.startswith(
                "dcid:") and not k.startswith("http") else k
            if is_entity_reference(v):
              obs_obj[prop_key] = _uri_ref(_rewrite_custom_ns_to_dcid(v))
            else:
              obs_obj[prop_key] = _parse_numeric(v)
      except json.JSONDecodeError as e:
        logging.warning(
            "Failed to decode properties JSON for observation %s/%s: %s",
            entity, variable, e)

    graph_list.append(obs_obj)

  if track_hash_fn and chunk_hashes:
    track_hash_fn(chunk_hashes, file_name=file_name)

  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}

  if file_name:
    file_stem = os.path.basename(file_name).rsplit(".", 1)[0]
    if file_stem and file_stem != "data":
      shard_name = f"observation-{file_stem}-{shard_index:05d}.jsonld"
    else:
      shard_name = f"observation-{shard_index:05d}.jsonld"
  else:
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
    sub_id = _rewrite_custom_ns_to_dcid(row.subject_id)
    if sub_id not in subjects:
      subjects[sub_id] = {
          "@id":
              sub_id
              if is_uri_or_namespace(sub_id) else f"dcid:{sub_id.lstrip('/')}"
      }

    pred = _rewrite_custom_ns_to_dcid(row.predicate)
    pred_key = pred if is_uri_or_namespace(pred) else f"dcid:{pred}"

    if pred == "typeOf":
      pred_key = "@type"

    if row.object_id:
      val = _uri_ref(_rewrite_custom_ns_to_dcid(row.object_id))
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
    sub = expand_id(_rewrite_custom_ns_to_dcid(row.subject_id))
    p = expand_id(_rewrite_custom_ns_to_dcid(row.predicate))
    if row.object_id:
      o = expand_id(_rewrite_custom_ns_to_dcid(row.object_id))
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

  def __init__(self,
               output_dir,
               import_names,
               nodes,
               jsonld_dir_name: Optional[str] = None) -> None:
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

    if jsonld_dir_name:
      unique_dir_name = jsonld_dir_name
    else:
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
      unique_dir_name = f"{self.import_name}_{timestamp}"

    self.jsonld_dir = output_dir.open_dir("jsonld").open_dir(unique_dir_name)

    self.obs_shard_index = 0
    self.node_shard_index = 0
    self.ns_map = {"dcid": DCID_URL}
    self.lock = threading.Lock()
    self._temp_dir_obj = tempfile.TemporaryDirectory()
    self.temp_local_dir = self._temp_dir_obj.name
    self._triples = defaultdict(list)
    self._processed_imports = set()
    self._node_streaming_started = set()
    self.obs_hash_set: set[int] = set()
    self.obs_collision_count: int = 0
    self.obs_sample_collisions: list[str] = []
    self.file_collision_counts: dict[str, int] = defaultdict(int)
    self.file_sample_collisions: dict[str, list[str]] = defaultdict(list)

  def track_observation_hash(self,
                             obs_hash_or_chunk,
                             entity: str = "",
                             variable: str = "",
                             date: str = "",
                             provenance: str = "",
                             file_name: str = "") -> None:
    if isinstance(obs_hash_or_chunk, list):
      chunk_items = obs_hash_or_chunk
      file_key = os.path.basename(file_name) if file_name else "unknown"
      with self.lock:
        for obs_hash, ent, var, dt, prov in chunk_items:
          hash_int = int(obs_hash[:16], 16)
          if hash_int in self.obs_hash_set:
            self.obs_collision_count += 1
            self.file_collision_counts[file_key] += 1
            sample_info = (
                f"@id='dcid:obs_{obs_hash}' [entity='{ent}', variable='{var}', date='{dt}', provenance='{prov}']"
            )
            if len(self.obs_sample_collisions) < 10:
              self.obs_sample_collisions.append(sample_info)
            if len(self.file_sample_collisions[file_key]) < 3:
              self.file_sample_collisions[file_key].append(sample_info)
              logging.warning(
                  "Observation @id collision in '%s'! Duplicate metadata key produces identical %s",
                  file_key, sample_info)
            elif self.obs_collision_count % 1000 == 0:
              logging.warning(
                  "Detected %d observation @id collisions so far across processed datasets.",
                  self.obs_collision_count)
          else:
            self.obs_hash_set.add(hash_int)
    else:
      self.track_observation_hash(
          [(obs_hash_or_chunk, entity, variable, date, provenance)],
          file_name=file_name)

  def _get_prov_urls(self) -> dict[str, str]:
    if hasattr(self, 'nodes') and self.nodes and hasattr(
        self.nodes, 'get_provenance_urls'):
      return self.nodes.get_provenance_urls()
    return {}

  def _write_observations_df_to_disk(self,
                                     df: pd.DataFrame,
                                     import_name: str,
                                     file_name: str = ""):
    import_temp_dir = os.path.join(self.temp_local_dir, import_name)
    prov_urls = self._get_prov_urls()
    n = len(df)
    for i in range(0, n, _CHUNK_SIZE):
      chunk_df = df.iloc[i:i + _CHUNK_SIZE]
      chunk_records = chunk_df.to_records(index=False).tolist()
      with self.lock:
        shard_index = self.obs_shard_index
        self.obs_shard_index += 1
      _write_observation_shard(chunk_records,
                               shard_index,
                               import_temp_dir,
                               self.ns_map,
                               prov_urls,
                               track_hash_fn=self.track_observation_hash,
                               file_name=file_name)

  def insert_observations(self, observations_df: pd.DataFrame,
                          input_file: File):
    if observations_df.empty:
      return
    validate_numeric_values(observations_df, input_file.path)

    import_name = self.config.import_name(input_file)
    file_name = input_file.path if input_file else ""
    self._init_import_export_dir(import_name)
    self._write_observations_df_to_disk(observations_df,
                                        import_name,
                                        file_name=file_name)

  def _init_import_export_dir(self, import_name: str):
    import_temp_dir = os.path.join(self.temp_local_dir, import_name)
    os.makedirs(import_temp_dir, exist_ok=True)
    with self.lock:
      self._processed_imports.add(import_name)
      if import_name not in self._node_streaming_started:
        logging.info("Streaming node/observation export for %s...", import_name)
        self._node_streaming_started.add(import_name)

  def _write_triples_to_disk(self, triples: list[Triple], import_name: str):
    import_temp_dir = os.path.join(self.temp_local_dir, import_name)
    with self.lock:
      i = 0
      n = len(triples)
      while i < n:
        chunk = []
        end = min(i + _CHUNK_SIZE, n)
        chunk.extend(triples[i:end])
        i = end

        # Expand boundary to keep subject together
        if i < n:
          boundary_subject = chunk[-1].subject_id
          while i < n and triples[i].subject_id == boundary_subject:
            chunk.append(triples[i])
            i += 1

        _write_node_shard(
            (chunk, self.node_shard_index, import_temp_dir, self.ns_map))
        self.node_shard_index += 1

  def insert_triples(self, triples: list[Triple], input_file: File = None):
    if not triples:
      return

    if not input_file:
      with self.lock:
        self._triples["_global"].extend(triples)
      return

    import_name = self.config.import_name(input_file)
    self._init_import_export_dir(import_name)
    self._write_triples_to_disk(triples, import_name)

  def commit(self):
    pass

  def commit_and_close(self):
    # Add global triples to every processed import's triples
    global_triples = self._triples.pop("_global", [])
    if not self._processed_imports:
      self._processed_imports.add(self.import_name)

    # Write global triples as node shards to the local temp directory for each import
    for import_name in self._processed_imports:
      import_temp_dir = os.path.join(self.temp_local_dir, import_name)
      os.makedirs(import_temp_dir, exist_ok=True)
      if global_triples:
        for i in range(0, len(global_triples), _CHUNK_SIZE):
          chunk = global_triples[i:i + _CHUNK_SIZE]
          _write_node_shard(
              (chunk, self.node_shard_index, import_temp_dir, self.ns_map))
          self.node_shard_index += 1

    has_local_files = any(os.scandir(self.temp_local_dir)) if os.path.exists(
        self.temp_local_dir) else False
    if has_local_files:
      logging.info(
          "Finalizing JSON-LD local export and bulk uploading shards...")
      self._upload_shards(self.temp_local_dir)

    if self.obs_collision_count > 0:
      summary_lines = [
          f"Observation @ID Collision Summary: Total of {self.obs_collision_count} observation @id collisions detected across {len(self.file_collision_counts)} file(s):"
      ]
      for f_name, count in self.file_collision_counts.items():
        summary_lines.append(f"  File '{f_name}': {count} collision(s)")
        for sample in self.file_sample_collisions[f_name]:
          summary_lines.append(f"    - {sample}")
      logging.warning("\n".join(summary_lines))

    # Clean up local temporary directory
    try:
      self._temp_dir_obj.cleanup()
      logging.info("Cleaned up local temporary directory: %s",
                   self.temp_local_dir)
    except Exception as e:
      logging.warning("Failed to clean up local temporary directory %s: %s",
                      self.temp_local_dir, e)

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

    gcs_retry = Retry(
        initial=1.0,
        maximum=10.0,
        multiplier=2.0,
        deadline=120.0,
        predicate=lambda e: isinstance(e,
                                       (TooManyRequests, GoogleAPICallError,
                                        requests.exceptions.RequestException)))

    def _upload_single(rel_path: str):
      local_file_path = os.path.join(temp_local_dir, rel_path)
      blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
      blob = bucket.blob(blob_key)
      blob.upload_from_filename(local_file_path, retry=gcs_retry)

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
