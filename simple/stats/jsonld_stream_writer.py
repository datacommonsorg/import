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
import re
import types
from typing import Any, Callable, Mapping, NamedTuple, Optional
import uuid

import numpy as np
import pandas as pd
from stats.config import Config
from stats.data import ImportType
from stats.data import Triple
from stats.data import validate_numeric_values
from stats.graph_writer import GraphWriter
from stats.obs_id_collisions import MAX_SAMPLES_PER_FILE
from stats.obs_id_collisions import ObsIds
from stats.util import is_entity_reference
from stats.util import is_uri_or_namespace
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import gcs_client
from util.filesystem import GCS_RETRY
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
# enough for census-tract-scale places (~72-85k) without thrashing.
_CACHE_SIZE = 1 << 17

_STANDARD_PREFIXES = {"http", "https", "dcid", "schema", "dcs"}

_NO_PROPS: Mapping[str, Any] = types.MappingProxyType({})

# Fixed observation keys other than dcid:observationAbout (checked separately
# because observationAbout only conflicts when the entity column is also set).
_OTHER_STD_OBS_KEYS = frozenset({
    "@id",
    "@type",
    "dcid:variableMeasured",
    "dcid:observationDate",
    "dcid:value",
    "dcid:provenance",
    "dcid:unit",
    "dcid:scalingFactor",
    "dcid:measurementMethod",
    "dcid:observationPeriod",
})

_json_dumps = json.dumps


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


def _text(val) -> str | None:
  """The literal `val` as written, without surrounding whitespace; None if empty.

  Literals are written as text rather than parsed as numbers, so that "12.50"
  keeps its significant figures and "007" its leading zeros. The loader stores
  every literal as text anyway.
  """
  if val is None or pd.isna(val):
    return None
  return str(val).strip() or None


def _prefix_dcid(key: str) -> str:
  """Ensures `key` has a `dcid:` prefix unless it is already a URI or prefixed."""
  if key.startswith(("dcid:", "http://", "https://")):
    return key
  return f"dcid:{key}"


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_ref_json(val: str) -> str | None:
  ref = _cached_ref(val)
  if ref is None:
    return None
  return f'{{"@id":{_json_dumps(ref["@id"])}}}'


def _ref_json(val) -> str | None:
  if type(val) is str:
    return _cached_ref_json(val)
  ref = _ref(val)
  return f'{{"@id":{_json_dumps(ref["@id"])}}}' if ref is not None else None


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_var_json(variable: str, obs_props_suffix: str) -> str:
  ref = _cached_ref(variable)
  if ref is None:
    return "null"
  if obs_props_suffix:
    return f'{{"@id":{_json_dumps(ref["@id"])}{obs_props_suffix}}}'
  return f'{{"@id":{_json_dumps(ref["@id"])}}}'


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_text_json(val: str) -> str:
  s = val.strip()
  return _json_dumps(s) if s else "null"


def _text_json(val) -> str:
  if type(val) is str:
    s = val.strip()
    return _json_dumps(s) if s else "null"
  t = _text(val)
  return _json_dumps(t) if t is not None else "null"


class _CompiledProps(NamedTuple):
  """Pre-serialized custom properties for an observation row."""
  id_props_bytes: bytes
  obs_props_suffix: str
  fields_json: str
  parsed: Mapping[str, Any]
  has_about: bool
  has_other_std: bool


_EMPTY_COMPILED_PROPS = _CompiledProps(b"", "", "", _NO_PROPS, False, False)


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _compile_props_cached(props: str) -> _CompiledProps:
  """Parses and pre-serializes a distinct custom-properties JSON string."""
  parsed = json.loads(props)
  if not isinstance(parsed, dict) or not parsed:
    return _CompiledProps(props.encode("utf-8"), "", "", _NO_PROPS, False,
                          False)
  id_props_bytes = json.dumps(parsed, sort_keys=True).encode("utf-8")
  prefixed_keys = [_prefix_dcid(k) for k in parsed.keys()]
  obs_props_suffix = f',"dcid:observationProperties":{_to_json(prefixed_keys)}'
  parts = []
  has_about = False
  has_other_std = False
  for prop_key, v in zip(prefixed_keys, parsed.values()):
    if prop_key == "dcid:observationAbout":
      has_about = True
    elif prop_key in _OTHER_STD_OBS_KEYS:
      has_other_std = True
    val_json = (_ref_json(v) or
                "null") if is_entity_reference(v) else _text_json(v)
    parts.append(f",{_json_dumps(prop_key)}:{val_json}")
  return _CompiledProps(
      id_props_bytes=id_props_bytes,
      obs_props_suffix=obs_props_suffix,
      fields_json="".join(parts),
      parsed=types.MappingProxyType(parsed),
      has_about=has_about,
      has_other_std=has_other_std,
  )


def _compile_props(props, entity, variable) -> _CompiledProps:
  """Compiles custom properties JSON, logging and falling back on bad JSON."""
  if not props:
    return _EMPTY_COMPILED_PROPS
  try:
    return _compile_props_cached(props)
  except json.JSONDecodeError as e:
    logging.warning(
        "Failed to decode properties JSON for observation %s/%s: %s", entity,
        variable, e)
    return _CompiledProps(
        str(props).encode("utf-8"), "", "", _NO_PROPS, False, False)


def _format_optional_fields(provenance, unit, scaling_factor, mmethod,
                            period) -> str:
  """Serializes the optional fixed observation fields into a JSON fragment."""
  parts = []
  if provenance:
    parts.append(f',"dcid:provenance":{_ref_json(provenance) or "null"}')
  if unit:
    parts.append(f',"dcid:unit":{_ref_json(unit) or "null"}')
  if type(scaling_factor) is str:
    if sf := scaling_factor.strip():
      parts.append(f',"dcid:scalingFactor":{_json_dumps(sf)}')
  elif sf := _text(scaling_factor):
    parts.append(f',"dcid:scalingFactor":{_json_dumps(sf)}')
  if mmethod:
    parts.append(f',"dcid:measurementMethod":{_ref_json(mmethod) or "null"}')
  if type(period) is str:
    if p := period.strip():
      parts.append(f',"dcid:observationPeriod":{_json_dumps(p)}')
  elif p := _text(period):
    parts.append(f',"dcid:observationPeriod":{_json_dumps(p)}')
  return "".join(parts)


def _build_conflicting_observation_json(row: tuple, obs_hash: str,
                                        parsed_props: Mapping[str, Any]) -> str:
  """Dict-based fallback when custom properties overwrite a standard key."""
  entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, _ = row
  var_obj = _ref(variable)
  if parsed_props and var_obj:
    var_obj["dcid:observationProperties"] = [
        _prefix_dcid(k) for k in parsed_props.keys()
    ]
  obs_obj = {
      "@id": f"dcid:obs_{obs_hash}",
      "@type": "dcid:StatVarObservation",
      "dcid:variableMeasured": var_obj,
      "dcid:observationDate": _text(date),
      "dcid:value": _text(value),
  }
  if entity_ref := _ref(entity):
    obs_obj["dcid:observationAbout"] = entity_ref
  if provenance:
    obs_obj["dcid:provenance"] = _ref(provenance)
  if unit:
    obs_obj["dcid:unit"] = _ref(unit)
  if sf := _text(scaling_factor):
    obs_obj["dcid:scalingFactor"] = sf
  if mmethod:
    obs_obj["dcid:measurementMethod"] = _ref(mmethod)
  if p := _text(period):
    obs_obj["dcid:observationPeriod"] = p
  for k, v in parsed_props.items():
    prop_key = _prefix_dcid(k)
    obs_obj[prop_key] = _ref(v) if is_entity_reference(v) else _text(v)
  return _to_json(obs_obj)


_PART_TAG_RE = re.compile(r"p\d{3,}")
_OBS_PART_SUFFIX_RE = re.compile(r"p\d{3,}-\d{5,}\.jsonld")
_NODE_PART_SUFFIX_RE = re.compile(r"p\d{3,}-\d{5,}-[0-9a-f]+\.jsonld")


def file_shard_stem(file_path: str) -> str:
  """The part of an observation CSV's shard names that names the file.

  E.g. "a/data.csv" writes "observation-a_data-00000.jsonld": its stem is
  "a_data".
  """
  return file_path.replace("/", "_").rsplit(".", 1)[0]


def node_part_stem(file_path: str) -> str:
  """Shard name stem for a split node file, keeping its extension.

  E.g. "a/case.mcf" -> "a_case_mcf" and "a/case.csv" -> "a_case_csv", so a
  split MCF and a split entity CSV with the same base name both split without
  colliding on fallback cleanup.
  """
  return file_path.replace("/", "_").replace(".", "_")


def part_shard_tag(part_index: int) -> str:
  """Shard name tag for part `part_index` of a split file, e.g. 'p001'."""
  return f"p{part_index:03d}"


def part_shards_could_collide(file_path: str, stems: set[str]) -> bool:
  """Whether split shards of `file_path` could collide with another file's.

  A split file writes "observation-<stem>-p001-00000.jsonld", which another
  file named "<stem>-p001.csv" would also write; skip splitting such files.
  """
  prefix = f"{file_shard_stem(file_path)}-"
  return any(
      _PART_TAG_RE.fullmatch(other[len(prefix):])
      for other in stems
      if other.startswith(prefix))


def _is_part_shard(name: str, stem: str, *, is_obs: bool) -> bool:
  """Whether `name` is a shard written for a part of `stem`."""
  if is_obs:
    prefix, suffix_re = f"observation-{stem}-", _OBS_PART_SUFFIX_RE
  else:
    prefix, suffix_re = f"node-{stem}-", _NODE_PART_SUFFIX_RE
  return name.startswith(prefix) and bool(
      suffix_re.fullmatch(name[len(prefix):]))


def _observation_shard(chunk,
                       shard_index: int,
                       ns_map: dict[str, str],
                       track_hash_fn: Callable,
                       file_name: str,
                       shard_tag: str = "") -> tuple[str, str]:
  """Returns the name and JSON text of the observation shard of `chunk`.

  `file_name` is the path of the input file, relative to the input directory.
  `shard_tag`, if any, follows the file in the name (see JsonLdStreamWriter).
  """
  node_strs = []
  prefix_buf = bytearray()
  sha256 = hashlib.sha256
  compile_props = _compile_props
  cached_var_json = _cached_var_json
  cached_text_json = _cached_text_json
  json_dumps = _json_dumps

  prev_hash_key = None
  base_hasher = None
  prev_tail_key = None
  tail_json = ""
  prev_date = _NO_PROPS
  date_json = "null"
  prev_var_key = None
  var_json = "null"

  for row in chunk:
    (entity, variable, date, value, provenance, unit, scaling_factor, mmethod,
     period, props) = row

    compiled = (compile_props(props, entity, variable)
                if props else _EMPTY_COMPILED_PROPS)

    hash_key = (entity, variable, date, provenance, unit, mmethod, period)
    if hash_key != prev_hash_key:
      prev_hash_key = hash_key
      base_hasher = sha256(
          f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_".
          encode("utf-8"))
    h = base_hasher.copy()
    if compiled.id_props_bytes:
      h.update(compiled.id_props_bytes)
    digest = h.digest()
    prefix_buf.extend(digest[:8])
    obs_hash = digest.hex()

    if compiled.has_other_std or (compiled.has_about and entity):
      node_strs.append(
          _build_conflicting_observation_json(row, obs_hash, compiled.parsed))
      continue

    var_key = (variable, compiled.obs_props_suffix)
    if var_key != prev_var_key:
      prev_var_key = var_key
      if type(variable) is str:
        var_json = cached_var_json(variable, compiled.obs_props_suffix)
      else:
        vref = _ref(variable)
        if vref is None:
          var_json = "null"
        elif compiled.obs_props_suffix:
          var_json = (
              f'{{"@id":{json_dumps(vref["@id"])}{compiled.obs_props_suffix}}}')
        else:
          var_json = f'{{"@id":{json_dumps(vref["@id"])}}}'

    if date != prev_date:
      prev_date = date
      date_json = (cached_text_json(date)
                   if type(date) is str else _text_json(date))

    if type(value) is str:
      sv = value.strip()
      val_json = json_dumps(sv) if sv else "null"
    else:
      val_json = _text_json(value)

    if entity:
      eref = _ref_json(entity)
      entity_json = f',"dcid:observationAbout":{eref}' if eref else ""
    else:
      entity_json = ""

    tail_key = (provenance, unit, scaling_factor, mmethod, period)
    if tail_key != prev_tail_key:
      prev_tail_key = tail_key
      tail_json = _format_optional_fields(provenance, unit, scaling_factor,
                                          mmethod, period)

    node_strs.append(f'{{"@id":"dcid:obs_{obs_hash}",'
                     f'"@type":"dcid:StatVarObservation",'
                     f'"dcid:variableMeasured":{var_json},'
                     f'"dcid:observationDate":{date_json},'
                     f'"dcid:value":{val_json}'
                     f'{entity_json}{tail_json}{compiled.fields_json}}}')

  if prefix_buf:
    ids = np.frombuffer(prefix_buf, dtype=">u8").astype(np.uint64, copy=False)
    track_hash_fn(ids, chunk, file_name)

  tag = f"-{shard_tag}" if shard_tag else ""
  shard_name = (f"observation-{file_shard_stem(file_name)}{tag}"
                f"-{shard_index:05d}.jsonld")
  shard_text = (
      f'{{"@context":{_to_json(ns_map)},"@graph":[{",".join(node_strs)}]}}')
  return shard_name, shard_text


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


def _node_shard(chunk,
                shard_index: int,
                ns_map: dict[str, str],
                file_name: str = "",
                shard_tag: str = "") -> tuple[str, str]:
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
           if row.object_id else _text(row.object_value))
    if pred_key == "@type":
      val = val["@id"] if isinstance(val, dict) and "@id" in val else str(val)
    _append_unique_value(subjects[sub_id], pred_key, val)

  # Sort by @id to match deterministic output order
  graph_list = sorted(list(subjects.values()), key=lambda x: x["@id"])
  compacted_jsonld = {"@context": ns_map, "@graph": graph_list}

  # Every worker numbers its node shards from 0: the uuid tells them apart.
  unique_id = uuid.uuid4().hex[:12]
  if shard_tag and file_name:
    prefix = f"node-{node_part_stem(file_name)}-{shard_tag}"
  else:
    prefix = "node"
  shard_name = f"{prefix}-{shard_index:05d}-{unique_id}.jsonld"
  return shard_name, _to_json(compacted_jsonld)


class JsonLdStreamWriter(GraphWriter):
  """A GraphWriter implementation that streams triples and observations directly to JSON-LD shards on GCS/Disk.

  Not thread-safe: the coordinator and each worker process have their own.
  """

  def __init__(self,
               output_dir: Dir,
               import_names: list[str],
               config: Config,
               jsonld_dir_name: Optional[str] = None,
               shard_tag: str = "") -> None:
    """
    Args:
      jsonld_dir_name: For a worker's writer, the run folder that the
        coordinator's writer created.
      shard_tag: Added to the names of shards written for a part of a split
        file, e.g. "p001" in "observation-data-p001-00000.jsonld" or
        "node-nodes-p001-00000-<uuid>.jsonld".
    """
    self.config = config

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
    self.shard_tag = shard_tag
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
    # Triples of no import (e.g. the root StatVarGroups), written to the folder
    # of _global_triples_import at commit_and_close.
    self._global_triples: list[Triple] = []
    # The imports with shards from this writer or, on the coordinator, from
    # the workers' writers merged into it (see merge_worker).
    self._processed_imports: set[str] = set()
    self.obs_ids = ObsIds()
    self.file_collision_counts: dict[str, int] = defaultdict(int)
    self.file_sample_collisions: dict[str, list[str]] = defaultdict(list)

  def track_observation_hash(
      self,
      ids: np.ndarray,
      chunk_rows: list[tuple],
      file_name: str,
  ) -> None:
    """Tracks 64-bit observation id prefixes and records any collisions.

    Only collisions with observations that this writer saw are found: a worker
    sees one file. See `stats.obs_id_collisions` for collisions between files.
    """
    collisions = np.flatnonzero(self.obs_ids.add(ids))
    if not len(collisions):
      return
    self.file_collision_counts[file_name] += len(collisions)
    # Only the samples still missing are formatted, however many collisions.
    samples = self.file_sample_collisions[file_name]
    for i in collisions[:max(0, MAX_SAMPLES_PER_FILE - len(samples))].tolist():
      ent, var, dt, _, prov, unit, _, mmethod, period, props = chunk_rows[i]
      compiled = _compile_props(props, ent,
                                var) if props else _EMPTY_COMPILED_PROPS
      h = hashlib.sha256(
          f"{ent}_{var}_{dt}_{prov}_{unit}_{mmethod}_{period}_".encode("utf-8"))
      if compiled.id_props_bytes:
        h.update(compiled.id_props_bytes)
      samples.append(
          f"@id='dcid:obs_{h.hexdigest()}' [entity='{ent}', variable='{var}', date='{dt}', provenance='{prov}']"
      )

  def processed_imports(self) -> set[str]:
    """Returns the imports with shards from this writer (see __init__)."""
    return set(self._processed_imports)

  def merge_worker(self, imports: set[str], file_counts: dict[str, int],
                   file_samples: dict[str, list[str]]) -> None:
    """Adds a worker's writer state: its processed imports and collisions."""
    self._processed_imports.update(imports)
    self.merge_collisions(file_counts, file_samples)

  def merge_collisions(self, file_counts: dict[str, int],
                       file_samples: dict[str, list[str]]) -> None:
    """Adds collisions found elsewhere to this writer's end-of-run summary.

    Args:
      file_counts: collision count per input file path.
      file_samples: sample descriptions per input file path.
    """
    for f_name, count in file_counts.items():
      self.file_collision_counts[f_name] += count
      samples = self.file_sample_collisions[f_name]
      samples.extend(
          file_samples.get(f_name,
                           [])[:max(0, MAX_SAMPLES_PER_FILE - len(samples))])

  def _write_observation_shards(self, df: pd.DataFrame, import_name: str,
                                file_name: str):
    dir_name = import_name.replace("/", "_")
    n = len(df)
    col_lists = [df[c].tolist() for c in df.columns]
    for i in range(0, n, _CHUNK_SIZE):
      if i == 0 and n <= _CHUNK_SIZE:
        chunk_records = list(zip(*col_lists))
      else:
        end = i + _CHUNK_SIZE
        chunk_records = list(zip(*(c[i:end] for c in col_lists)))
      shard_index = self.obs_shard_index
      self.obs_shard_index += 1
      name, text = _observation_shard(chunk_records,
                                      shard_index,
                                      self.ns_map,
                                      track_hash_fn=self.track_observation_hash,
                                      file_name=file_name,
                                      shard_tag=self.shard_tag)
      self._write_shard(dir_name, name, text)

  def write_observations(self, observations_df: pd.DataFrame, input_file: File):
    if observations_df.empty:
      return
    validate_numeric_values(observations_df, input_file.path)

    import_name = self.config.import_name(input_file)
    self._processed_imports.add(import_name)
    self._write_observation_shards(observations_df,
                                   import_name,
                                   file_name=input_file.path)

  def _write_node_shards(self,
                         triples: list[Triple],
                         import_name: str,
                         file_name: str = ""):
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

      name, text = _node_shard(chunk,
                               self.node_shard_index,
                               self.ns_map,
                               file_name=file_name,
                               shard_tag=self.shard_tag)
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
      self._global_triples.extend(triples)
      return

    self._processed_imports.add(provenance_dir)
    self._write_node_shards(triples,
                            provenance_dir,
                            file_name=input_file.path if input_file else "")

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
    if self._global_triples:
      target_import = self._global_triples_import()
      self._processed_imports.add(target_import)
      self._write_node_shards(self._global_triples, target_import)
      self._global_triples = []

    self._finish_shard_writes()

  def log_collision_summary(self) -> None:
    """Logs the observation @id collisions of every file, if any.

    The coordinator calls this once its writer holds the collisions of every
    file (see `merge_collisions`).
    """
    total_collisions = sum(self.file_collision_counts.values())
    if not total_collisions:
      return
    summary_lines = [
        f"Observation @ID Collision Summary: Total of {total_collisions} observation @id collisions detected across {len(self.file_collision_counts)} file(s):"
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

  def delete_part_shards(self, input_file: File) -> None:
    """Deletes the shards of every part of `input_file`."""
    folder = self.config.import_name(input_file).replace("/", "_")
    is_obs = (not input_file.path.lower().endswith(".mcf") and
              self.config.import_type(input_file) == ImportType.OBSERVATIONS)
    stem = (file_shard_stem(input_file.path)
            if is_obs else node_part_stem(input_file.path))
    shards = [
        shard
        for shard in self.jsonld_dir.open_dir(folder, create=False).all_files()
        if _is_part_shard(shard.name(), stem, is_obs=is_obs)
    ]
    if not shards:
      return
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(_UPLOAD_CONCURRENCY, len(shards))) as pool:
      list(pool.map(File.delete, shards))

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


def _upload_gcs_shard(jsonld_path: str, rel_path: str, text: str) -> None:
  """Uploads shard `text` to `jsonld_path`/`rel_path`, a gs:// path."""
  bucket_name, _, blob_prefix = jsonld_path[len("gs://"):].partition("/")
  blob_prefix = blob_prefix.rstrip("/")
  blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
  gcs_client().bucket(bucket_name).blob(blob_key).upload_from_string(
      text, content_type=_SHARD_CONTENT_TYPE, retry=GCS_RETRY)
