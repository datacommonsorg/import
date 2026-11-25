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
"""Pure JSON-LD serialization for observation shards and node shards."""

import functools
import hashlib
import json
import logging
import types
from typing import Any, Mapping, NamedTuple

import numpy as np
import pandas as pd
from stats.graph.util import is_entity_reference
from stats.graph.util import is_uri_or_namespace
from stats.writer.shard_naming import node_shard_name
from stats.writer.shard_naming import observation_shard_name

DCID_URL = "https://datacommons.org/browser/"

# Distinct values whose JSON-LD forms are cached, per process and per cache:
# enough for census-tract-scale places (~72-85k) without thrashing.
_CACHE_SIZE = 1 << 17

_STANDARD_PREFIXES = {"http", "https", "dcid", "schema", "dcs"}

_NO_PROPS: Mapping[str, Any] = types.MappingProxyType({})

# ---------------------------------------------------------------------------
# Fast-path observation JSON-LD serialization
# ---------------------------------------------------------------------------
# Observation shards are the hottest loop in the preprocessor (~1B+ rows on
# large imports). Rather than building a Python dict per row and calling
# json.dumps() on 10k-node lists, `build_observation_shard` assembles JSON-LD
# fragments directly from cached pieces (`_CompiledProps`, `_cached_var_json`,
# `_cached_text_json`, `_format_optional_fields`). This is ~4x faster end to
# end (~1.7x faster than even dict + orjson) with zero extra dependencies and
# byte-identical output.
#
# MAINTENANCE INVARIANT: Any schema change to `dcid:StatVarObservation` fields
# MUST be kept in sync across:
#   1. `_OTHER_STD_OBS_KEYS` (conflict detection with custom properties),
#   2. `_format_optional_fields` and `build_observation_shard` (fast path), and
#   3. `_build_conflicting_observation_json` (rare key-override fallback).
#
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

_UNSET = object()


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
  if not val_str:
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


def prefix_dcid(key: str) -> str:
  """Ensures `key` has a `dcid:` prefix unless it is already a URI or prefixed."""
  if key.startswith(("dcid:", "http://", "https://")):
    return key
  return f"dcid:{key}"


def _has_value(val) -> bool:
  """Returns whether `val` is a non-empty, non-NA value (safe for pd.NA/NaN)."""
  if type(val) is str:
    return bool(val)
  return val is not None and not pd.isna(val)


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_ref_json(val: str) -> str | None:
  ref = _cached_ref(val)
  if ref is None:
    return None
  return f'{{"@id":{json.dumps(ref["@id"])}}}'


def _ref_json(val) -> str | None:
  if type(val) is str:
    return _cached_ref_json(val)
  ref = _ref(val)
  return f'{{"@id":{json.dumps(ref["@id"])}}}' if ref is not None else None


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_var_json(variable: str, obs_props_suffix: str) -> str:
  ref = _cached_ref(variable)
  if ref is None:
    return "null"
  if obs_props_suffix:
    return f'{{"@id":{json.dumps(ref["@id"])}{obs_props_suffix}}}'
  return f'{{"@id":{json.dumps(ref["@id"])}}}'


def _var_json(variable, obs_props_suffix: str) -> str:
  if type(variable) is str:
    return _cached_var_json(variable, obs_props_suffix)
  ref = _ref(variable)
  if ref is None:
    return "null"
  if obs_props_suffix:
    return f'{{"@id":{json.dumps(ref["@id"])}{obs_props_suffix}}}'
  return f'{{"@id":{json.dumps(ref["@id"])}}}'


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _cached_text_json(val: str) -> str:
  s = val.strip()
  return json.dumps(s) if s else "null"


def _optional_text_json(val) -> str | None:
  """Serializes `val` as a JSON string if non-empty, else returns None."""
  if type(val) is str:
    s = val.strip()
    return json.dumps(s) if s else None
  t = _text(val)
  return json.dumps(t) if t is not None else None


def _text_json(val) -> str:
  return _optional_text_json(val) or "null"


def _obs_hash_base(entity, variable, date, provenance, unit, mmethod, period):
  """Returns a SHA-256 hasher seeded with an observation's fixed @id fields."""
  return hashlib.sha256(
      f"{entity}_{variable}_{date}_{provenance}_{unit}_{mmethod}_{period}_".
      encode("utf-8"))


class _CompiledProps(NamedTuple):
  """Pre-serialized custom properties for an observation row.

  Attributes:
    id_props_bytes: UTF-8 JSON with sorted keys, fed into the @id SHA-256 hash.
    obs_props_suffix: `,"dcid:observationProperties":[...]` appended inside
      `dcid:variableMeasured`, or `""` when there are no custom properties.
    fields_json: Leading-comma JSON fragment for every custom property.
    parsed: Read-only mapping used only by `_build_conflicting_observation_json`.
    has_about: True if `dcid:observationAbout` appears in `parsed`.
    has_other_std: True if any key in `_OTHER_STD_OBS_KEYS` appears in `parsed`
      or two custom properties normalize to the same `dcid:` key.
  """
  id_props_bytes: bytes
  obs_props_suffix: str
  fields_json: str
  parsed: Mapping[str, Any]
  has_about: bool
  has_other_std: bool


_EMPTY_COMPILED_PROPS = _CompiledProps(b"", "", "", _NO_PROPS, False, False)


@functools.lru_cache(maxsize=_CACHE_SIZE)
def _compile_props_cached(
    props: str,
    entity_properties: frozenset[str] = frozenset(),
) -> _CompiledProps:
  """Parses and pre-serializes a distinct custom-properties JSON string."""
  parsed = json.loads(props)
  if not isinstance(parsed, dict) or not parsed:
    return _CompiledProps(props.encode("utf-8"), "", "", _NO_PROPS, False,
                          False)
  id_props_bytes = json.dumps(parsed, sort_keys=True).encode("utf-8")
  prefixed_keys = [prefix_dcid(k) for k in parsed.keys()]
  obs_props_suffix = f',"dcid:observationProperties":{_to_json(prefixed_keys)}'
  parts = []
  has_about = False
  has_other_std = len(set(prefixed_keys)) != len(prefixed_keys)
  for prop_key, v in zip(prefixed_keys, parsed.values()):
    if prop_key == "dcid:observationAbout":
      has_about = True
    elif prop_key in _OTHER_STD_OBS_KEYS:
      has_other_std = True
    val_json = (_ref_json(v) or
                "null") if (prop_key in entity_properties or
                            is_entity_reference(v)) else _text_json(v)
    parts.append(f",{json.dumps(prop_key)}:{val_json}")
  return _CompiledProps(
      id_props_bytes=id_props_bytes,
      obs_props_suffix=obs_props_suffix,
      fields_json="".join(parts),
      parsed=types.MappingProxyType(parsed),
      has_about=has_about,
      has_other_std=has_other_std,
  )


def _compile_props(
    props,
    entity,
    variable,
    entity_properties: frozenset[str] = frozenset(),
) -> _CompiledProps:
  """Compiles custom properties JSON, logging and falling back on bad JSON."""
  if not _has_value(props):
    return _EMPTY_COMPILED_PROPS
  try:
    return _compile_props_cached(props, entity_properties)
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
  if _has_value(provenance):
    parts.append(f',"dcid:provenance":{_ref_json(provenance) or "null"}')
  if _has_value(unit):
    parts.append(f',"dcid:unit":{_ref_json(unit) or "null"}')
  if sf := _optional_text_json(scaling_factor):
    parts.append(f',"dcid:scalingFactor":{sf}')
  if _has_value(mmethod):
    parts.append(f',"dcid:measurementMethod":{_ref_json(mmethod) or "null"}')
  if p := _optional_text_json(period):
    parts.append(f',"dcid:observationPeriod":{p}')
  return "".join(parts)


def _build_conflicting_observation_json(
    row: tuple,
    obs_hash: str,
    parsed_props: Mapping[str, Any],
    entity_properties: frozenset[str] = frozenset(),
) -> str:
  """Dict-based fallback when custom properties overwrite a standard key."""
  entity, variable, date, value, provenance, unit, scaling_factor, mmethod, period, _ = row
  var_obj = _ref(variable)
  if parsed_props and var_obj:
    var_obj["dcid:observationProperties"] = [
        prefix_dcid(k) for k in parsed_props.keys()
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
  if _has_value(provenance):
    obs_obj["dcid:provenance"] = _ref(provenance)
  if _has_value(unit):
    obs_obj["dcid:unit"] = _ref(unit)
  if sf := _text(scaling_factor):
    obs_obj["dcid:scalingFactor"] = sf
  if _has_value(mmethod):
    obs_obj["dcid:measurementMethod"] = _ref(mmethod)
  if p := _text(period):
    obs_obj["dcid:observationPeriod"] = p
  for k, v in parsed_props.items():
    prop_key = prefix_dcid(k)
    obs_obj[prop_key] = _ref(v) if (prop_key in entity_properties or
                                    is_entity_reference(v)) else _text(v)
  return _to_json(obs_obj)


def format_collision_sample(row: tuple) -> str:
  """Formats one colliding observation row for the end-of-run warning summary."""
  ent, var, dt, _, prov, unit, _, mmethod, period, props = row
  compiled = (_compile_props(props, ent, var)
              if _has_value(props) else _EMPTY_COMPILED_PROPS)
  h = _obs_hash_base(ent, var, dt, prov, unit, mmethod, period)
  if compiled.id_props_bytes:
    h.update(compiled.id_props_bytes)
  return (
      f"@id='dcid:obs_{h.hexdigest()}' [entity='{ent}', variable='{var}', date='{dt}', provenance='{prov}']"
  )


def col_to_list(series: pd.Series) -> list:
  """Converts a DataFrame column to a Python list, normalizing pd.NA to None.

  Replacing pandas' tri-state `pd.NA` once up front prevents row-cache tuple
  comparisons (`!=`) inside `build_observation_shard` from raising
  `TypeError: boolean value of NA is ambiguous`.
  """
  vals = series.tolist()
  if series.isna().any():
    return [None if v is pd.NA else v for v in vals]
  return vals


def build_observation_shard(
    chunk,
    shard_index: int,
    ns_map: dict[str, str],
    file_name: str,
    shard_tag: str = "",
    entity_properties: frozenset[str] = frozenset(),
) -> tuple[str, str, np.ndarray]:
  """Returns the name, JSON text, and uint64 @id prefixes of `chunk`'s shard.

  `file_name` is the path of the input file, relative to the input directory.
  `shard_tag`, if any, follows the file in the name (see JsonLdStreamWriter).
  """
  node_strs = []
  prefix_buf = bytearray()
  compile_props = _compile_props
  cached_text_json = _cached_text_json
  json_dumps = json.dumps

  # Row-to-row caches: CSVs typically group rows by variable, date, and
  # provenance/unit/period, so consecutive rows reuse the same SHA-256 prefix
  # and serialized JSON fragments without touching the LRU caches.
  prev_hash_key = None
  base_hasher = None
  prev_tail_key = None
  tail_json = ""
  prev_date = _UNSET
  date_json = "null"
  prev_var_key = None
  var_json = "null"

  for row in chunk:
    (entity, variable, date, value, provenance, unit, scaling_factor, mmethod,
     period, props) = row

    compiled = (compile_props(props, entity, variable, entity_properties)
                if _has_value(props) else _EMPTY_COMPILED_PROPS)

    hash_key = (entity, variable, date, provenance, unit, mmethod, period)
    if hash_key != prev_hash_key:
      prev_hash_key = hash_key
      base_hasher = _obs_hash_base(entity, variable, date, provenance, unit,
                                   mmethod, period)
    h = base_hasher.copy()
    if compiled.id_props_bytes:
      h.update(compiled.id_props_bytes)
    digest = h.digest()
    prefix_buf.extend(digest[:8])
    obs_hash = digest.hex()

    has_entity = _has_value(entity)
    if compiled.has_other_std or (compiled.has_about and has_entity):
      node_strs.append(
          _build_conflicting_observation_json(row, obs_hash, compiled.parsed,
                                              entity_properties))
      continue

    var_key = (variable, compiled.obs_props_suffix)
    if var_key != prev_var_key:
      prev_var_key = var_key
      var_json = _var_json(variable, compiled.obs_props_suffix)

    if date != prev_date:
      prev_date = date
      date_json = (cached_text_json(date)
                   if type(date) is str else _text_json(date))

    if type(value) is str:
      sv = value.strip()
      val_json = json_dumps(sv) if sv else "null"
    else:
      val_json = _text_json(value)

    if has_entity:
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
  else:
    ids = np.empty(0, dtype=np.uint64)

  shard_name = observation_shard_name(file_name, shard_index, shard_tag)
  shard_text = (
      f'{{"@context":{_to_json(ns_map)},"@graph":[{",".join(node_strs)}]}}')
  return shard_name, shard_text, ids


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


def build_node_shard(chunk,
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
  shard_name = node_shard_name(shard_index, file_name, shard_tag)
  return shard_name, _to_json(compacted_jsonld)
