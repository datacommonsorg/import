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
"""A GraphWriter implementation that streams JSON-LD shards directly to GCS/Disk."""

from collections import defaultdict
import concurrent.futures
from datetime import datetime
from datetime import timezone
import hashlib
import json
import logging
import os
import tempfile
import threading
from typing import Callable, Optional
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import TooManyRequests
from google.api_core.retry import Retry
from google.cloud import storage
import pandas as pd
import requests
from stats import constants
from stats.data import Triple
from stats.data import validate_numeric_values
from stats.graph_writer import GraphWriter
from stats.util import is_entity_reference
from stats.util import is_uri_or_namespace
from util.filesystem import create_store
from util.filesystem import File

DCID_URL = "https://datacommons.org/browser/"

# Configuration Constants
_CHUNK_SIZE = 10000
_UPLOAD_CONCURRENCY = 4

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


def _prefix_dcid(key: str) -> str:
  """Ensures `key` has a `dcid:` prefix unless it is already a URI or prefixed."""
  if key.startswith(("dcid:", "http://", "https://")):
    return key
  return f"dcid:{key}"


def _parse_props_json(props: str, entity: str, variable: str) -> dict:
  """Parses a serialized observation properties JSON string into a dict."""
  if not props:
    return {}
  try:
    parsed = json.loads(props)
    return parsed if isinstance(parsed, dict) else {}
  except json.JSONDecodeError as e:
    logging.warning(
        "Failed to decode properties JSON for observation %s/%s: %s", entity,
        variable, e)
    return {}


def _build_observation_node(
    row: tuple,
    prov_urls: Optional[dict[str, str]] = None,
) -> dict:
  """Builds a single `dcid:StatVarObservation` JSON-LD node dict from a row."""
  entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row
  key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_{props}"
  obs_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()

  var_obj = _uri_ref(_rewrite_custom_ns_to_dcid(variable))
  props_dict = _parse_props_json(props, entity, variable)
  if props_dict and var_obj:
    var_obj["dcid:observationProperties"] = [
        _prefix_dcid(k) for k in props_dict.keys()
    ]

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
    if prov_urls and provenance in prov_urls and prov_urls[provenance]:
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

  for k, v in props_dict.items():
    prop_key = _prefix_dcid(k)
    if is_entity_reference(v):
      obs_obj[prop_key] = _uri_ref(_rewrite_custom_ns_to_dcid(v))
    else:
      obs_obj[prop_key] = _parse_numeric(v)
  return obs_obj


def _write_observation_shard(chunk,
                             shard_index: Optional[int] = None,
                             jsonld_dir_path: Optional[str] = None,
                             ns_map: Optional[dict[str, str]] = None,
                             prov_urls: Optional[dict[str, str]] = None,
                             track_hash_fn: Optional[Callable] = None,
                             file_name: str = ""):
  graph_list = []
  chunk_hashes = []
  for row in chunk:
    obs_obj = _build_observation_node(row, prov_urls)
    if track_hash_fn:
      entity, variable, date, _, provenance, *_ = row
      obs_hash = obs_obj["@id"].removeprefix("dcid:obs_")
      chunk_hashes.append((obs_hash, entity, variable, date, provenance))
    graph_list.append(obs_obj)

  if track_hash_fn and chunk_hashes:
    track_hash_fn(chunk_hashes, file_name=file_name)

  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}
  if file_name:
    sanitized_stem = file_name.replace("/", "_").rsplit(".", 1)[0]
    shard_name = f"observation-{sanitized_stem}-{shard_index:05d}.jsonld"
  else:
    shard_name = f"observation-{shard_index:05d}.jsonld"
  with create_store(jsonld_dir_path) as store:
    output_dir = store.as_dir()
    output_dir.open_file(shard_name).write(
        json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name}")


def _append_unique_value(subject_dict: dict, key: str, val: object) -> None:
  """Appends `val` to `subject_dict[key]`, deduplicating and promoting to a list."""
  if key not in subject_dict:
    subject_dict[key] = val
    return
  existing = subject_dict[key]
  if isinstance(existing, list):
    if val not in existing:
      existing.append(val)
  elif existing != val:
    subject_dict[key] = [existing, val]


# TODO(gmechali): When parallelizing node shard exports (MCF and Entities CSV files),
# include the sanitized file_name in the shard name (e.g., node-{sanitized_stem}-{shard_index:05d}.jsonld)
# to prevent shard filename collisions across parallel workers, similar to observation shards.
def _write_node_shard(args):
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
    pred_key = "@type" if pred == "typeOf" else (
        pred if is_uri_or_namespace(pred) else f"dcid:{pred}")
    val = (_uri_ref(_rewrite_custom_ns_to_dcid(row.object_id))
           if row.object_id else _parse_numeric(row.object_value))
    if pred_key == "@type":
      val = val["@id"] if isinstance(val, dict) and "@id" in val else str(val)
    _append_unique_value(subjects[sub_id], pred_key, val)

  # Sort by @id to match deterministic output order
  graph_list = sorted(list(subjects.values()), key=lambda x: x["@id"])
  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}

  # TODO(gmechali): When parallelizing node shard exports, include file_name in shard_name
  unique_id = uuid.uuid4().hex[:12]
  shard_name = f"node-{shard_index:05d}-{unique_id}.jsonld"
  with create_store(jsonld_dir_path) as store:
    output_dir = store.as_dir()
    output_dir.open_file(shard_name).write(
        json.dumps(compacted_jsonld, indent=4))
  logging.info(f"Saved JSON-LD shard to {shard_name}")


class JsonLdStreamWriter(GraphWriter):
  """A GraphWriter implementation that streams triples and observations directly to JSON-LD shards on GCS/Disk."""

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

  def track_observation_hash(
      self,
      chunk_items: list[tuple[str, str, str, str, str]],
      file_name: str = "",
  ) -> None:
    """Tracks observation hashes for a shard chunk and logs any collisions."""
    file_key = os.path.basename(file_name) if file_name else "unknown"
    with self.lock:
      for obs_hash, ent, var, dt, prov in chunk_items:
        hash_int = int(obs_hash[:16], 16)
        if hash_int not in self.obs_hash_set:
          self.obs_hash_set.add(hash_int)
          continue
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

  def _get_prov_urls(self) -> dict[str, str]:
    if hasattr(self, 'nodes') and self.nodes and hasattr(
        self.nodes, 'get_provenance_urls'):
      return self.nodes.get_provenance_urls()
    return {}

  def _write_observations_df_to_disk(self,
                                     df: pd.DataFrame,
                                     import_name: str,
                                     file_name: str = ""):
    dir_name = import_name.replace("/", "_")
    import_temp_dir = os.path.join(self.temp_local_dir, dir_name)
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

  def write_observations(self, observations_df: pd.DataFrame, input_file: File):
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
    dir_name = import_name.replace("/", "_")
    import_temp_dir = os.path.join(self.temp_local_dir, dir_name)
    os.makedirs(import_temp_dir, exist_ok=True)
    with self.lock:
      self._processed_imports.add(import_name)
      if import_name not in self._node_streaming_started:
        logging.info("Streaming node/observation export for %s...", import_name)
        self._node_streaming_started.add(import_name)

  def _write_triples_to_disk(self, triples: list[Triple], import_name: str):
    dir_name = import_name.replace("/", "_")
    import_temp_dir = os.path.join(self.temp_local_dir, dir_name)
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

  def write_triples(self,
                    triples: list[Triple],
                    input_file: File = None,
                    provenance_dir: str = None):
    if not triples:
      return

    if not provenance_dir and input_file:
      provenance_dir = self.config.import_name(input_file)

    if not provenance_dir:
      with self.lock:
        self._triples["_global"].extend(triples)
      return

    self._init_import_export_dir(provenance_dir)
    self._write_triples_to_disk(triples, provenance_dir)

  def commit_and_close(self):
    global_triples = self._triples.pop("_global", [])
    if not self._processed_imports:
      self._processed_imports.add(self.import_name)

    if global_triples:
      target_import = self.import_name if self.import_name in self._processed_imports else next(
          iter(self._processed_imports))
      dir_name = target_import.replace("/", "_")
      import_temp_dir = os.path.join(self.temp_local_dir, dir_name)
      os.makedirs(import_temp_dir, exist_ok=True)
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

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_UPLOAD_CONCURRENCY) as executor:
      list(
          executor.map(
              lambda rel_path: _upload_single_gcs_blob(
                  temp_local_dir, rel_path, blob_prefix, bucket, gcs_retry),
              files))

  def _upload_shards_local(self, temp_local_dir: str, files: list[str]):
    """Performs concurrent local file copy (for test environments)."""
    target_store = self.jsonld_dir

    parent_dirs = set(os.path.dirname(f) for f in files if os.path.dirname(f))
    for d in sorted(parent_dirs):
      try:
        target_store.open_dir(d)
      except Exception:
        pass

    with concurrent.futures.ThreadPoolExecutor(
        max_workers=_UPLOAD_CONCURRENCY) as executor:
      list(
          executor.map(
              lambda rel_path: _copy_single_local_shard(
                  temp_local_dir, rel_path, target_store), files))


def _upload_single_gcs_blob(temp_local_dir: str, rel_path: str,
                            blob_prefix: str, bucket, gcs_retry: Retry) -> None:
  """Uploads a single local shard file (`rel_path`) to a GCS bucket."""
  local_file_path = os.path.join(temp_local_dir, rel_path)
  blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
  bucket.blob(blob_key).upload_from_filename(local_file_path, retry=gcs_retry)


def _copy_single_local_shard(temp_local_dir: str, rel_path: str,
                             target_store) -> None:
  """Copies a single local shard file (`rel_path`) into `target_store`."""
  local_file_path = os.path.join(temp_local_dir, rel_path)
  with open(local_file_path, "r") as f:
    content = f.read()
  target_store.open_file(rel_path).write(content)
