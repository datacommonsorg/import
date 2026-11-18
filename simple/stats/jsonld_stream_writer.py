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
import functools
import hashlib
import json
import logging
import types
from typing import Any, Callable, Mapping, Optional
import uuid

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import TooManyRequests
from google.api_core.retry import Retry
import pandas as pd
import requests
from stats.data import Triple
from stats.data import validate_numeric_values
from stats.graph_writer import GraphWriter
from stats.util import is_entity_reference
from stats.util import is_uri_or_namespace
from util.filesystem import File
from util.filesystem import gcs_client
from util.filesystem import is_gcs_path

DCID_URL = "https://datacommons.org/browser/"

# Configuration Constants
_CHUNK_SIZE = 10000
_UPLOAD_CONCURRENCY = 4
# Shards serialized but not yet written, per writer, at most: bounds memory
# while writes overlap with building the next shards.
_MAX_PENDING_SHARDS = 2 * _UPLOAD_CONCURRENCY
_SHARD_CONTENT_TYPE = "application/ld+json"

# Distinct values whose JSON-LD forms are cached, per process and per cache:
# enough for the variables, places and properties of a large file.
_CACHE_SIZE = 1 << 16

_STANDARD_PREFIXES = {"http", "https", "dcid", "schema", "dcs"}

_NO_PROPS: Mapping[str, Any] = types.MappingProxyType({})


def _to_json(doc: dict) -> str:
  """Serializes a shard compactly.

  The ingestion pipeline parses each shard as a whole JSON document, so layout
  doesn't matter. Pretty-printing (indent=...) would make shards ~1.7x larger
  and force Python's pure-Python encoder instead of the C one.
  """
  return json.dumps(doc, separators=(",", ":"))


def _rewrite_custom_ns_to_dcid(val: str) -> str:
  """Rewrites custom namespaces to 'dcid:' as a temporary workaround until the ingestion loader natively supports custom namespaces.

  This allows users to author schemas with their custom namespaces today, which will be fully preserved when we re-ingest after backend support is added.

  `_ref` caches the result, so it must stay a function of `val` alone.
  """
  if not isinstance(val, str) or not val:
    return val
  if ":" in val and " " not in val:
    prefix, suffix = val.split(":", 1)
    if prefix.isalnum() and prefix.lower() not in _STANDARD_PREFIXES:
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


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_ref(val: str) -> dict | None:
  return _uri_ref(_rewrite_custom_ns_to_dcid(val))


def _ref(val) -> dict | None:
  """Returns `_uri_ref` of `val` with its custom namespace rewritten.

  Cached per distinct `val`, and returned as a new dict, which the caller may
  change (observationProperties is set on a variable's).
  """
  if not isinstance(val, str):
    return _uri_ref(_rewrite_custom_ns_to_dcid(val))
  ref = _cached_ref(val)
  return dict(ref) if ref is not None else None


def _parse_numeric(val):
  if val is None or val == "" or pd.isna(val):
    return None
  try:
    if "." in str(val):
      return float(val)
    return int(val)
  except ValueError:
    return str(val)


# Collision samples kept per file for the end-of-run summary.
MAX_SAMPLES_PER_FILE = 3


def collision_file_key(file_path: str) -> str:
  """Returns the key under which collisions of `file_path` are summarized.

  The path relative to the input directory, so that files with the same name in
  different import folders are told apart.
  """
  return file_path or "unknown"


def _prefix_dcid(key: str) -> str:
  """Ensures `key` has a `dcid:` prefix unless it is already a URI or prefixed."""
  if key.startswith(("dcid:", "http://", "https://")):
    return key
  return f"dcid:{key}"


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _decode_props(props: str) -> tuple[Mapping[str, Any], str]:
  """The cached part of `_parse_props_json`."""
  parsed = json.loads(props)
  if not isinstance(parsed, dict) or not parsed:
    return _NO_PROPS, props
  # Read-only: rows with the same properties share it.
  return types.MappingProxyType(parsed), json.dumps(parsed, sort_keys=True)


def _parse_props_json(props: str, entity: str,
                      variable: str) -> tuple[Mapping[str, Any], str]:
  """Parses an observation's properties JSON for its node and its @id.

  Returns the properties, in their order in `props`, and their JSON with
  sorted keys, so that the @id doesn't depend on the order the config declares
  them in (or `props` itself if there are none). The properties are cached
  and shared between rows, so they're read-only.
  """
  if not props:
    return _NO_PROPS, props
  try:
    return _decode_props(props)
  except json.JSONDecodeError as e:
    logging.warning(
        "Failed to decode properties JSON for observation %s/%s: %s", entity,
        variable, e)
    return _NO_PROPS, props


def _build_observation_node(row: tuple) -> dict:
  """Builds a single `dcid:StatVarObservation` JSON-LD node dict from a row."""
  entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, props = row
  props_dict, id_props = _parse_props_json(props, entity, variable)
  key = f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_{id_props}"
  obs_hash = hashlib.sha256(key.encode("utf-8")).hexdigest()

  var_obj = _ref(variable)
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
  entity_ref = _ref(entity)
  if entity_ref:
    obs_obj["dcid:observationAbout"] = entity_ref
  if provenance:
    obs_obj["dcid:provenance"] = _ref(provenance)
  if unit:
    obs_obj["dcid:unit"] = _ref(unit)
  if scaling_factor:
    obs_obj["dcid:scalingFactor"] = _parse_numeric(scaling_factor)
  if mmethod:
    obs_obj["dcid:measurementMethod"] = _ref(mmethod)
  if period:
    obs_obj["dcid:observationPeriod"] = period

  for k, v in props_dict.items():
    prop_key = _prefix_dcid(k)
    if is_entity_reference(v):
      obs_obj[prop_key] = _ref(v)
    else:
      obs_obj[prop_key] = _parse_numeric(v)
  return obs_obj


def _observation_shard(chunk,
                       shard_index: int,
                       ns_map: dict[str, str],
                       track_hash_fn: Optional[Callable] = None,
                       file_name: str = "") -> tuple[str, str]:
  """Returns the name and JSON text of the observation shard of `chunk`."""
  graph_list = []
  chunk_hashes = []
  for row in chunk:
    obs_obj = _build_observation_node(row)
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
  return shard_name, _to_json(compacted_jsonld)


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
def _node_shard(chunk, shard_index: int, ns_map: dict[str,
                                                      str]) -> tuple[str, str]:
  """Returns the name and JSON text of the node shard of `chunk`."""
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
  return shard_name, _to_json(compacted_jsonld)


class JsonLdStreamWriter(GraphWriter):
  """A GraphWriter implementation that streams triples and observations directly to JSON-LD shards on GCS/Disk.

  Not thread-safe: the coordinator and each worker process have their own.
  """

  def __init__(self,
               output_dir,
               import_names: list[str],
               nodes,
               jsonld_dir_name: Optional[str] = None) -> None:
    self.output_dir = output_dir
    self.config = nodes.config

    # The run's name, from its imports (e.g. ["oecd", "ilo"] -> "oecd_ilo", or
    # ALL_IMPORTS): names its timestamped output folder below, and is the
    # default folder for global triples (see _global_triples_import).
    self.import_name = "_".join(import_names).replace("/", "_")

    if jsonld_dir_name:
      # A worker's writer: the coordinator's writer created the run folder.
      self.jsonld_dir = output_dir.open_dir("jsonld", create=False).open_dir(
          jsonld_dir_name, create=False)
    else:
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
      self.jsonld_dir = output_dir.open_dir("jsonld").open_dir(
          f"{self.import_name}_{timestamp}")

    self.obs_shard_index = 0
    self.node_shard_index = 0
    self.ns_map = {"dcid": DCID_URL}
    # Shards are written in the background, straight to jsonld_dir (see
    # _write_shard). The threads are started on first use, so that each forked
    # worker process starts its own.
    jsonld_path = self.jsonld_dir.full_path()
    self._gcs_jsonld_path = jsonld_path if is_gcs_path(jsonld_path) else None
    self._shard_writer: concurrent.futures.ThreadPoolExecutor | None = None
    self._pending_shards: list[concurrent.futures.Future] = []
    # Import folders made under a local jsonld_dir (GCS has no folders).
    self._local_folders: set[str] = set()
    self._triples = defaultdict(list)
    self._processed_imports = set()
    self._node_streaming_started = set()
    self.obs_hash_set: set[int] = set()
    self.obs_collision_count: int = 0
    self.file_collision_counts: dict[str, int] = defaultdict(int)
    self.file_sample_collisions: dict[str, list[str]] = defaultdict(list)

  def track_observation_hash(
      self,
      chunk_items: list[tuple[str, str, str, str, str]],
      file_name: str = "",
  ) -> None:
    """Tracks observation hashes for a shard chunk and logs any collisions.

    Only collisions with observations that this writer saw are found: a worker
    sees one file. See `stats.obs_id_collisions` for collisions between files.
    """
    file_key = collision_file_key(file_name)
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
      if len(self.file_sample_collisions[file_key]) < MAX_SAMPLES_PER_FILE:
        self.file_sample_collisions[file_key].append(sample_info)
        logging.warning(
            "Observation @id collision in '%s'! Duplicate metadata key produces identical %s",
            file_key, sample_info)
      elif self.obs_collision_count % 1000 == 0:
        logging.warning(
            "Detected %d observation @id collisions so far across processed datasets.",
            self.obs_collision_count)

  def merge_collisions(self, file_counts: dict[str, int],
                       file_samples: dict[str, list[str]]) -> None:
    """Adds collisions found elsewhere to this writer's end-of-run summary.

    Args:
      file_counts: collision count per file key (see `collision_file_key`).
      file_samples: sample descriptions per file key.
    """
    for f_name, count in file_counts.items():
      self.obs_collision_count += count
      self.file_collision_counts[f_name] += count
      samples = self.file_sample_collisions[f_name]
      samples.extend(
          file_samples.get(f_name,
                           [])[:max(0, MAX_SAMPLES_PER_FILE - len(samples))])

  def _write_observation_shards(self,
                                df: pd.DataFrame,
                                import_name: str,
                                file_name: str = ""):
    dir_name = import_name.replace("/", "_")
    n = len(df)
    for i in range(0, n, _CHUNK_SIZE):
      chunk_df = df.iloc[i:i + _CHUNK_SIZE]
      chunk_records = chunk_df.to_records(index=False).tolist()
      shard_index = self.obs_shard_index
      self.obs_shard_index += 1
      name, text = _observation_shard(chunk_records,
                                      shard_index,
                                      self.ns_map,
                                      track_hash_fn=self.track_observation_hash,
                                      file_name=file_name)
      self._write_shard(dir_name, name, text)

  def write_observations(self, observations_df: pd.DataFrame, input_file: File):
    if observations_df.empty:
      return
    validate_numeric_values(observations_df, input_file.path)

    import_name = self.config.import_name(input_file)
    file_name = input_file.path if input_file else ""
    self._add_processed_import(import_name)
    self._write_observation_shards(observations_df,
                                   import_name,
                                   file_name=file_name)

  def _add_processed_import(self, import_name: str):
    self._processed_imports.add(import_name)
    if import_name not in self._node_streaming_started:
      logging.info("Streaming node/observation export for %s...", import_name)
      self._node_streaming_started.add(import_name)

  def _write_node_shards(self, triples: list[Triple], import_name: str):
    dir_name = import_name.replace("/", "_")
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

      name, text = _node_shard(chunk, self.node_shard_index, self.ns_map)
      self._write_shard(dir_name, name, text)
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
      self._triples["_global"].extend(triples)
      return

    self._add_processed_import(provenance_dir)
    self._write_node_shards(triples, provenance_dir)

  def import_folders(self) -> list[str]:
    """Returns the sorted imports whose folders this writer writes.

    These are the processed imports plus the import that global triples go to
    (see _global_triples_import), which is written at commit_and_close.
    """
    return sorted(self._processed_imports | {self._global_triples_import()})

  def _global_triples_import(self) -> str:
    """Returns the import whose folder global triples are written to.

    The run's own import name if it has a folder (or no import has one), else
    the alphabetically first processed import, so that the choice does not
    depend on processing order.
    """
    if not self._processed_imports or self.import_name in self._processed_imports:
      return self.import_name
    return min(self._processed_imports)

  def commit_and_close(self):
    global_triples = self._triples.pop("_global", [])
    if global_triples:
      target_import = self._global_triples_import()
      self._add_processed_import(target_import)
      self._write_node_shards(global_triples, target_import)

    self._finish_shard_writes()

    if self.obs_collision_count > 0:
      summary_lines = [
          f"Observation @ID Collision Summary: Total of {self.obs_collision_count} observation @id collisions detected across {len(self.file_collision_counts)} file(s):"
      ]
      for f_name, count in self.file_collision_counts.items():
        summary_lines.append(f"  File '{f_name}': {count} collision(s)")
        for sample in self.file_sample_collisions[f_name]:
          summary_lines.append(f"    - {sample}")
      logging.warning("\n".join(summary_lines))

  def close_after_error(self) -> None:
    """Waits for the shards in flight, ignoring errors, after a failed import.

    So that nothing is still being written once the failure is reported.
    """
    self._finish_shard_writes(raise_errors=False)

  def _write_shard(self, folder: str, name: str, text: str) -> None:
    """Writes shard `text` to jsonld_dir/folder/name, in the background.

    At most _MAX_PENDING_SHARDS are in flight: a write error is raised here or
    by commit_and_close.
    """
    if self._shard_writer is None:
      self._shard_writer = concurrent.futures.ThreadPoolExecutor(
          max_workers=_UPLOAD_CONCURRENCY)
    while len(self._pending_shards) >= _MAX_PENDING_SHARDS:
      self._pending_shards.pop(0).result()
    rel_path = f"{folder}/{name}"
    if self._gcs_jsonld_path:
      write = functools.partial(_upload_gcs_shard, self._gcs_jsonld_path,
                                rel_path, text)
    else:
      if folder not in self._local_folders:
        # Another worker may make it at the same time, which open_dir allows.
        self.jsonld_dir.open_dir(folder)
        self._local_folders.add(folder)
      write = functools.partial(self._write_local_shard, rel_path, text)
    self._pending_shards.append(self._shard_writer.submit(write))

  def _write_local_shard(self, rel_path: str, text: str) -> None:
    self.jsonld_dir.open_file(rel_path).write(text)

  def _finish_shard_writes(self, raise_errors: bool = True) -> None:
    """Waits for the shards in flight; raises the first error if asked to."""
    try:
      for future in self._pending_shards:
        error = future.exception()
        if error and raise_errors:
          raise error
    finally:
      self._pending_shards = []
      if self._shard_writer is not None:
        self._shard_writer.shutdown(wait=True)
        self._shard_writer = None


_GCS_RETRY = Retry(
    initial=1.0,
    maximum=10.0,
    multiplier=2.0,
    deadline=120.0,
    predicate=lambda e: isinstance(e, (TooManyRequests, GoogleAPICallError,
                                       requests.exceptions.RequestException)))


def _upload_gcs_shard(jsonld_path: str, rel_path: str, text: str) -> None:
  """Uploads shard `text` to `jsonld_path`/`rel_path`, a gs:// path."""
  bucket_name, _, blob_prefix = jsonld_path[len("gs://"):].partition("/")
  blob_prefix = blob_prefix.rstrip("/")
  blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
  gcs_client().bucket(bucket_name).blob(blob_key).upload_from_string(
      text, content_type=_SHARD_CONTENT_TYPE, retry=_GCS_RETRY)
