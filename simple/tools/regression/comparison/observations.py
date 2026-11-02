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
"""Semantic, folder- and shard-independent comparison of observations.

Each observation is reduced to a fixed-size digest row: a 128-bit hash of its
semantic key, plus 64-bit hashes of its value, of its other properties and of
its provenanceUrl. Rows are spilled to hash buckets (see buckets.py) and
compared bucket by bucket, so memory stays bounded however many observations
there are. A second pass over the shards fetches the full records of the few
sampled keys.
"""

from collections import Counter
from collections import defaultdict
import concurrent.futures
import dataclasses
from typing import Any

import numpy as np
from tools.regression.comparison import buckets
from tools.regression.comparison.canonical import canonical_json
from tools.regression.comparison.canonical import digest64
from tools.regression.comparison.canonical import digest128
from tools.regression.comparison.canonical import load_json
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides
from tools.regression.comparison.shards import OBSERVATION
from tools.regression.comparison.shards import OutputTree

_VARIABLE = "dcid:variableMeasured"
# With the variable's @id, these make up the semantic key. Any property not
# listed in this module (e.g. a custom dimension) is part of the key too.
_KEY_FIELDS = ("dcid:observationAbout", "dcid:observationDate",
               "dcid:provenance", "dcid:unit", "dcid:measurementMethod",
               "dcid:observationPeriod")
_VALUE_FIELDS = ("dcid:value", "dcid:scalingFactor")
_PROPERTY_FIELDS = ("@id", "@type", _VARIABLE)
_PROVENANCE_URL = "dcid:provenanceUrl"
_NOT_DIMENSIONS = frozenset(_KEY_FIELDS + _VALUE_FIELDS + _PROPERTY_FIELDS +
                            (_PROVENANCE_URL,))

_KEY_DTYPE = np.dtype([("hi", "<u8"), ("lo", "<u8")])
DIGEST_DTYPE = np.dtype([
    ("key", _KEY_DTYPE),
    ("folder", "<u4"),
    ("value", "<u8"),
    ("props", "<u8"),
    ("purl", "<u8"),  # 0 when the observation has no provenanceUrl.
])
# Odd multipliers that combine the hashes of duplicate rows of one key
# independently of their order.
_MIX_VALUE = np.uint64(0x9E3779B97F4A7C15)
_MIX_PROPS = np.uint64(0xC2B2AE3D27D4EB4F)
_MIX_PURL = np.uint64(0x165667B19E3779F9)
_MIX_FOLDER = np.uint64(0x27D4EB2F165667C5)
_SHARDS_PER_TASK = 4

Key = tuple[int, int]


def _ref_id(value: Any) -> Any:
  return value.get("@id") if isinstance(value, dict) else value


def observation_key(record: dict) -> Key:
  """Returns the 128-bit digest of an observation's semantic key."""
  parts = [_ref_id(record.get(_VARIABLE))]
  parts += [record.get(field) for field in _KEY_FIELDS]
  dimensions = {k: v for k, v in record.items() if k not in _NOT_DIMENSIONS}
  return digest128(canonical_json([parts, dimensions]))


def _digest_record(record: dict, folder: int) -> tuple:
  value = [record.get(field) for field in _VALUE_FIELDS]
  props = [record.get(field) for field in _PROPERTY_FIELDS]
  purl = record.get(_PROVENANCE_URL)
  # The low bit is forced on so that a present URL never digests to 0.
  purl_digest = 0 if purl is None else digest64(canonical_json(purl)) | 1
  return (observation_key(record), folder, digest64(canonical_json(value)),
          digest64(canonical_json(props)), purl_digest)


def digest_shard(task: tuple[str, int]) -> tuple[np.ndarray, str]:
  """Returns the digest rows and the canonical @context of one shard."""
  path, folder = task
  data = load_json(path)
  rows = [_digest_record(r, folder) for r in data.get("@graph", [])]
  return np.array(rows,
                  dtype=DIGEST_DTYPE), canonical_json(data.get("@context"))


def find_records(task: tuple[str, str, frozenset]) -> list[tuple[Key, dict]]:
  """Returns the records of one shard whose key is in the wanted set."""
  path, folder, wanted = task
  found = []
  for record in load_json(path).get("@graph", []):
    key = observation_key(record)
    if key in wanted:
      found.append((key, {"folder": folder, "record": record}))
  return found


@dataclasses.dataclass(frozen=True)
class _KeyedRows:
  """The rows of one bucket, one entry per distinct key."""
  keys: np.ndarray
  counts: np.ndarray
  value: np.ndarray
  props: np.ndarray
  purl: np.ndarray
  folders: np.ndarray


def _group_by_key(rows: np.ndarray) -> _KeyedRows:
  """Combines the rows of each key (duplicates included) into one entry."""
  rows = np.sort(rows, order="key")
  keys, starts, counts = np.unique(rows["key"],
                                   return_index=True,
                                   return_counts=True)
  if not len(rows):
    empty = np.zeros(0, "<u8")
    return _KeyedRows(keys, counts, empty, empty, empty, empty)

  def combine(column: np.ndarray, mix: np.uint64) -> np.ndarray:
    return np.add.reduceat(column.astype("<u8") * mix, starts)

  with np.errstate(over="ignore"):
    return _KeyedRows(keys, counts, combine(rows["value"], _MIX_VALUE),
                      combine(rows["props"], _MIX_PROPS),
                      combine(rows["purl"], _MIX_PURL),
                      combine(rows["folder"] + 1, _MIX_FOLDER))


def _compare_keyed(base: _KeyedRows, cand: _KeyedRows) -> dict[str, np.ndarray]:
  """Returns the differing keys of two bucket sides, by category."""
  common, ib, ic = np.intersect1d(base.keys,
                                  cand.keys,
                                  assume_unique=True,
                                  return_indices=True)
  same_count = base.counts[ib] == cand.counts[ic]

  def differs(column: str) -> np.ndarray:
    """Masks the common keys, written equally often, whose column differs."""
    return (getattr(base, column)[ib] != getattr(cand, column)[ic]) & same_count

  only_base = np.setdiff1d(base.keys, cand.keys, assume_unique=True)
  only_cand = np.setdiff1d(cand.keys, base.keys, assume_unique=True)
  value = differs("value")
  return {
      "obs_only_baseline": only_base,
      "obs_only_candidate": only_cand,
      "obs_multiplicity_changed": common[~same_count],
      "obs_value_changed": common[value],
      "obs_properties_changed": common[differs("props") & ~value],
      "obs_provenance_url_changed": common[differs("purl")],
      "obs_folder_moved": common[differs("folders")],
  }


@dataclasses.dataclass
class _SideTotals:
  observations: int = 0
  distinct_keys: int = 0
  duplicate_keys: int = 0
  with_provenance_url: int = 0
  per_folder: Counter = dataclasses.field(default_factory=Counter)


class ObservationComparer(buckets.BucketComparer):
  """Compares the observations of two outputs, recording into findings."""

  def __init__(self, findings: Findings, pool: concurrent.futures.Executor,
               spill_dir: str) -> None:
    super().__init__(findings, pool, spill_dir)
    self._folders: list[str] = []
    self._sample_keys: dict[str, list[Key]] = defaultdict(list)
    self._totals = (_SideTotals(), _SideTotals())

  def compare(self, baseline: OutputTree,
              candidate: OutputTree) -> dict[str, Any]:
    """Runs the comparison and returns summary statistics."""
    self._folders = sorted(baseline.folders | candidate.folders)
    self._spill_and_compare(baseline, candidate)
    self._add_samples(baseline, candidate)
    return self._stats()

  def _spill_side(self, side: int, tree: OutputTree,
                  spill: buckets.SpillBuckets) -> None:
    totals = self._totals[side]
    index = {folder: i for i, folder in enumerate(self._folders)}
    tasks = [(s.path, index[s.folder]) for s in tree.of_kind(OBSERVATION)]
    for rows, context in self._pool.map(digest_shard,
                                        tasks,
                                        chunksize=_SHARDS_PER_TASK):
      self.contexts[side].add(context)
      totals.observations += len(rows)
      totals.with_provenance_url += int((rows["purl"] != 0).sum())
      per_folder = np.bincount(rows["folder"], minlength=len(self._folders))
      for i in np.flatnonzero(per_folder):
        totals.per_folder[self._folders[i]] += int(per_folder[i])
      _spill_rows(rows, spill)

  def _compare_bucket(self, baseline: bytes, candidate: bytes) -> None:
    keyed = [
        _group_by_key(np.frombuffer(rows, dtype=DIGEST_DTYPE))
        for rows in (baseline, candidate)
    ]
    for totals, side in zip(self._totals, keyed):
      totals.distinct_keys += len(side.keys)
      totals.duplicate_keys += int((side.counts > 1).sum())
    for category, keys in _compare_keyed(*keyed).items():
      self._findings.add(category, len(keys))
      room = self._findings.max_samples - len(self._sample_keys[category])
      self._sample_keys[category].extend(k.item() for k in keys[:room])

  def _add_samples(self, baseline: OutputTree, candidate: OutputTree) -> None:
    """Fetches the full records of the sampled keys, from both sides."""
    wanted = frozenset(k for keys in self._sample_keys.values() for k in keys)
    if not wanted:
      return
    found = [
        _find_all(tree, wanted, self._pool) for tree in (baseline, candidate)
    ]
    for category, keys in self._sample_keys.items():
      for key in keys:
        self._findings.add_sample(
            category, sides(found[0].get(key, []), found[1].get(key, [])))

  def _stats(self) -> dict[str, Any]:
    base, cand = self._totals
    stats = {
        field:
            [getattr(base, field), getattr(cand, field)]
        for field in ("observations", "distinct_keys", "duplicate_keys",
                      "with_provenance_url")
    }
    stats["per_folder"] = {
        f: [base.per_folder[f], cand.per_folder[f]] for f in self._folders
    }
    return stats


def _spill_rows(rows: np.ndarray, spill: buckets.SpillBuckets) -> None:
  """Appends each row to the bucket chosen by its key."""
  bucket_of = (rows["key"]["hi"] % buckets.NUM_BUCKETS).astype(np.intp)
  order = np.argsort(bucket_of, kind="stable")
  rows, bucket_of = rows[order], bucket_of[order]
  bounds = np.searchsorted(bucket_of, np.arange(buckets.NUM_BUCKETS + 1))
  for bucket in np.flatnonzero(np.diff(bounds)):
    spill.append(int(bucket), rows[bounds[bucket]:bounds[bucket + 1]].tobytes())


def _find_all(tree: OutputTree, wanted: frozenset,
              pool: concurrent.futures.Executor) -> dict[Key, list[dict]]:
  tasks = [(s.path, s.folder, wanted) for s in tree.of_kind(OBSERVATION)]
  found = defaultdict(list)
  for matches in pool.map(find_records, tasks, chunksize=_SHARDS_PER_TASK):
    for key, entry in matches:
      found[key].append(entry)
  return found
