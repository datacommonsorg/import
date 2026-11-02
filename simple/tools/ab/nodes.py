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

from tools.ab.buckets import NUM_BUCKETS
from tools.ab.buckets import SpillBuckets
from tools.ab.canonical import canonical_json
from tools.ab.canonical import digest64
from tools.ab.canonical import load_json
from tools.ab.categories import Findings
from tools.ab.shards import NODE
from tools.ab.shards import OutputTree

# How many entries of each grouped statistic are kept in the report.
_TOP_GROUPS = 30

# A node entry is (shard path relative to the run dir, record).
Entry = tuple[str, dict]


def spill_node_shard(
    task: tuple[str, str]) -> tuple[list[tuple[int, bytes]], str]:
  """Returns (bucket, JSON line) per record of a shard, and its @context."""
  path, rel_path = task
  data = load_json(path)
  lines = []
  for record in data.get("@graph", []):
    line = canonical_json([rel_path, record]) + "\n"
    bucket = digest64(canonical_json(record.get("@id"))) % NUM_BUCKETS
    lines.append((bucket, line.encode("utf-8")))
  return lines, canonical_json(data.get("@context"))


def _read_bucket(spill: SpillBuckets, bucket: int) -> dict[str, list[Entry]]:
  by_id: dict[str, list[Entry]] = defaultdict(list)
  for line in spill.read(bucket).splitlines():
    rel_path, record = json.loads(line)
    by_id[canonical_json(record.get("@id"))].append((rel_path, record))
  return by_id


def _folders(entries: list[Entry]) -> list[str]:
  return sorted({rel_path.split("/", 1)[0] for rel_path, _ in entries})


def _types(entries: list[Entry]) -> str:
  """Returns the merged @type values of a node, comma-separated."""
  types = set()
  for _, record in entries:
    value = record.get("@type", [])
    for item in value if isinstance(value, list) else [value]:
      types.add(item if isinstance(item, str) else canonical_json(item))
  return ",".join(sorted(types))


def _merge(entries: list[Entry]) -> dict[str, set[str]]:
  """Returns the union of the values of each property over all records."""
  merged: dict[str, set[str]] = defaultdict(set)
  for _, record in entries:
    for prop, value in record.items():
      for item in value if isinstance(value, list) else [value]:
        merged[prop].add(canonical_json(item))
  return merged


class NodeComparer:
  """Compares the nodes of two outputs, recording into findings."""

  def __init__(self, findings: Findings, pool: concurrent.futures.Executor,
               spill_dir: str) -> None:
    self._findings = findings
    self._pool = pool
    self._spill_dir = spill_dir
    self._records = [0, 0]
    self._ids = [0, 0]
    self._types = (Counter(), Counter())
    self._only_by_type = (Counter(), Counter())
    self._property_changes: Counter = Counter()
    self._folder_moves: Counter = Counter()
    self.contexts: tuple[set[str], set[str]] = (set(), set())

  def compare(self, baseline: OutputTree,
              candidate: OutputTree) -> dict[str, Any]:
    """Runs the comparison and returns summary statistics."""
    spills = [
        self._spill_side(side, tree)
        for side, tree in enumerate((baseline, candidate))
    ]
    for bucket in range(NUM_BUCKETS):
      self._compare_bucket(_read_bucket(spills[0], bucket),
                           _read_bucket(spills[1], bucket))
    return self._stats()

  def _spill_side(self, side: int, tree: OutputTree) -> SpillBuckets:
    spill = SpillBuckets(f"{self._spill_dir}/{side}")
    tasks = [(s.path, s.rel_path) for s in tree.of_kind(NODE)]
    for lines, context in self._pool.map(spill_node_shard, tasks):
      self.contexts[side].add(context)
      self._records[side] += len(lines)
      for bucket, line in lines:
        spill.append(bucket, line)
    spill.flush()
    return spill

  def _compare_bucket(self, base: dict[str, list[Entry]],
                      cand: dict[str, list[Entry]]) -> None:
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
    self._findings.add(category,
                       sample={
                           "types": types,
                           "folders": _folders(entries),
                           "record": entries[0][1]
                       })

  def _compare_node(self, base: list[Entry], cand: list[Entry]) -> None:
    node_id = base[0][1].get("@id")
    types = _types(base + cand)
    self._compare_properties(node_id, types, _merge(base), _merge(cand))
    folders = (_folders(base), _folders(cand))
    if folders[0] != folders[1]:
      self._folder_moves[(tuple(folders[0]), tuple(folders[1]))] += 1
      self._findings.add("node_folder_moved",
                         sample={
                             "@id": node_id,
                             "types": types,
                             "baseline": folders[0],
                             "candidate": folders[1]
                         })
    if len(base) != len(cand):
      self._findings.add("node_records_changed",
                         sample={
                             "@id": node_id,
                             "types": types,
                             "baseline": sorted(p for p, _ in base),
                             "candidate": sorted(p for p, _ in cand)
                         })

  def _compare_properties(self, node_id: Any, types: str, base: dict[str,
                                                                     set[str]],
                          cand: dict[str, set[str]]) -> None:
    changed = sorted(
        p for p in base.keys() | cand.keys() if base.get(p) != cand.get(p))
    if not changed:
      return
    self._findings.add("node_properties_changed")
    for prop in changed:
      self._property_changes[(types, prop)] += 1
      self._findings.add_sample(
          "node_properties_changed", {
              "@id": node_id,
              "types": types,
              "property": prop,
              "baseline": sorted(base.get(prop, ())),
              "candidate": sorted(cand.get(prop, ()))
          })

  def _stats(self) -> dict[str, Any]:
    types = sorted(self._types[0].keys() | self._types[1].keys())
    return {
        "records":
            self._records,
        "ids":
            self._ids,
        "types": {
            t: [self._types[0][t], self._types[1][t]] for t in types
        },
        "only_baseline_by_type":
            dict(self._only_by_type[0].most_common()),
        "only_candidate_by_type":
            dict(self._only_by_type[1].most_common()),
        "property_changes": [{
            "types": types,
            "property": prop,
            "ids": count
        } for (types,
               prop), count in self._property_changes.most_common(_TOP_GROUPS)],
        "folder_moves": [{
            "baseline": list(before),
            "candidate": list(after),
            "ids": count
        } for (before,
               after), count in self._folder_moves.most_common(_TOP_GROUPS)],
    }
