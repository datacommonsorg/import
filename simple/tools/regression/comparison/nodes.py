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
"""Semantic comparison of node records, by @id.

All records of an @id, from any folder or shard, are merged into one set of
values per property before comparing, so splitting a subject over several
records or moving it to another shard is not a content change; those are
reported as informational categories. Records are spilled to hash buckets by
@id (see buckets.py), so only one bucket of nodes is in memory at a time.
"""

from collections import Counter
from collections import defaultdict
import concurrent.futures
import json
from typing import Any

from tools.regression.comparison import buckets
from tools.regression.comparison.canonical import canonical_json
from tools.regression.comparison.canonical import digest64
from tools.regression.comparison.canonical import load_json
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides
from tools.regression.comparison.shards import NODE
from tools.regression.comparison.shards import OutputTree

# How many entries of each grouped statistic are kept in the report.
_TOP_GROUPS = 30

# A node entry is (shard path relative to the run dir, record).
Entry = tuple[str, dict]
# The canonical JSON values of each property of a node.
PropertyValues = dict[str, set[str]]


def spill_node_shard(
    task: tuple[str, str]) -> tuple[list[tuple[int, bytes]], str]:
  """Returns (bucket, JSON line) per record of a shard, and its @context."""
  path, rel_path = task
  data = load_json(path)
  lines = []
  for record in data.get("@graph", []):
    line = canonical_json([rel_path, record]) + "\n"
    bucket = digest64(canonical_json(record.get("@id"))) % buckets.NUM_BUCKETS
    lines.append((bucket, line.encode("utf-8")))
  return lines, canonical_json(data.get("@context"))


def _by_id(spilled: bytes) -> dict[str, list[Entry]]:
  """Groups the spilled JSON lines of one bucket by canonical @id."""
  by_id: dict[str, list[Entry]] = defaultdict(list)
  for line in spilled.splitlines():
    rel_path, record = json.loads(line)
    by_id[canonical_json(record.get("@id"))].append((rel_path, record))
  return by_id


def _as_list(value: Any) -> list:
  return value if isinstance(value, list) else [value]


def _folders(entries: list[Entry]) -> list[str]:
  return sorted({rel_path.split("/", 1)[0] for rel_path, _ in entries})


def _types(entries: list[Entry]) -> str:
  """Returns the merged @type values of a node, comma-separated."""
  types = set()
  for _, record in entries:
    for item in _as_list(record.get("@type", [])):
      types.add(item if isinstance(item, str) else canonical_json(item))
  return ",".join(sorted(types))


def _merge(entries: list[Entry]) -> PropertyValues:
  """Returns the union of the values of each property over all records."""
  merged: PropertyValues = defaultdict(set)
  for _, record in entries:
    for prop, value in record.items():
      for item in _as_list(value):
        merged[prop].add(canonical_json(item))
  return merged


class NodeComparer(buckets.BucketComparer):
  """Compares the nodes of two outputs, recording into findings."""

  def __init__(self, findings: Findings, pool: concurrent.futures.Executor,
               spill_dir: str) -> None:
    super().__init__(findings, pool, spill_dir)
    self._records = [0, 0]
    self._ids = [0, 0]
    self._types = (Counter(), Counter())
    self._only_by_type = (Counter(), Counter())
    self._property_changes: Counter = Counter()
    self._folder_moves: Counter = Counter()

  def compare(self, baseline: OutputTree,
              candidate: OutputTree) -> dict[str, Any]:
    """Runs the comparison and returns summary statistics."""
    self._spill_and_compare(baseline, candidate)
    return self._stats()

  def _spill_side(self, side: int, tree: OutputTree,
                  spill: buckets.SpillBuckets) -> None:
    tasks = [(s.path, s.rel_path) for s in tree.of_kind(NODE)]
    for lines, context in self._pool.map(spill_node_shard, tasks):
      self.contexts[side].add(context)
      self._records[side] += len(lines)
      for bucket, line in lines:
        spill.append(bucket, line)

  def _compare_bucket(self, baseline: bytes, candidate: bytes) -> None:
    base, cand = _by_id(baseline), _by_id(candidate)
    for side, by_id in enumerate((base, cand)):
      self._ids[side] += len(by_id)
      self._types[side].update(_types(entries) for entries in by_id.values())
    for node_id in sorted(base.keys() - cand.keys()):
      self._add_only(0, "node_only_baseline", base[node_id])
    for node_id in sorted(cand.keys() - base.keys()):
      self._add_only(1, "node_only_candidate", cand[node_id])
    for node_id in sorted(base.keys() & cand.keys()):
      self._compare_node(base[node_id], cand[node_id])

  def _add_only(self, side: int, category: str, entries: list[Entry]) -> None:
    types = _types(entries)
    self._only_by_type[side][types] += 1
    folders, record = _folders(entries), entries[0][1]
    sample = {"types": types, "folders": folders, "record": record}
    self._findings.add(category, sample=sample)

  def _compare_node(self, base: list[Entry], cand: list[Entry]) -> None:
    context = {"@id": base[0][1].get("@id"), "types": _types(base + cand)}
    self._compare_properties(context, _merge(base), _merge(cand))
    folders = (_folders(base), _folders(cand))
    if folders[0] != folders[1]:
      self._folder_moves[(tuple(folders[0]), tuple(folders[1]))] += 1
      self._findings.add("node_folder_moved",
                         sample=dict(context, **sides(*folders)))
    if len(base) != len(cand):
      records = [sorted(p for p, _ in entries) for entries in (base, cand)]
      self._findings.add("node_records_changed",
                         sample=dict(context, **sides(*records)))

  def _compare_properties(self, context: dict[str, Any], base: PropertyValues,
                          cand: PropertyValues) -> None:
    changed = sorted(
        p for p in base.keys() | cand.keys() if base.get(p) != cand.get(p))
    if not changed:
      return
    self._findings.add("node_properties_changed")
    for prop in changed:
      self._property_changes[(context["types"], prop)] += 1
      values = [sorted(side.get(prop, ())) for side in (base, cand)]
      self._findings.add_sample("node_properties_changed",
                                dict(context, property=prop, **sides(*values)))

  def _stats(self) -> dict[str, Any]:
    base_types, cand_types = self._types
    base_only, cand_only = self._only_by_type
    types = sorted(base_types.keys() | cand_types.keys())
    top_properties = self._property_changes.most_common(_TOP_GROUPS)
    top_moves = self._folder_moves.most_common(_TOP_GROUPS)
    property_changes = [
        dict(types=t, property=p, ids=n) for (t, p), n in top_properties
    ]
    folder_moves = [
        dict(baseline=list(b), candidate=list(c), ids=n)
        for (b, c), n in top_moves
    ]
    return {
        "records": self._records,
        "ids": self._ids,
        "types": {
            t: [base_types[t], cand_types[t]] for t in types
        },
        "only_baseline_by_type": dict(base_only.most_common()),
        "only_candidate_by_type": dict(cand_only.most_common()),
        "property_changes": property_changes,
        "folder_moves": folder_moves,
    }
