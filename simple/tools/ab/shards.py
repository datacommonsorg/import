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
"""Shard discovery and byte-level inventory of one preprocessor output.

An output directory holds `process/` and `jsonld/<run dir>/<folder>/*.jsonld`.
Observation shard names are deterministic and are matched by name. Node shard
names end in a random uid, so node shards are matched by content within their
folder.
"""

from collections import Counter
import concurrent.futures
import dataclasses
import glob
import hashlib
import os
from typing import Any

from tools.ab import volatile
from tools.ab.categories import Findings

OBSERVATION = "observation"
NODE = "node"
_KINDS = (OBSERVATION, NODE)
_HASH_CHUNK_BYTES = 1 << 20


@dataclasses.dataclass(frozen=True)
class Shard:
  rel_path: str  # "<folder>/<file name>", relative to the run directory.
  path: str

  @property
  def folder(self) -> str:
    return self.rel_path.split("/", 1)[0]

  @property
  def kind(self) -> str:
    return os.path.basename(self.rel_path).split("-", 1)[0]


@dataclasses.dataclass(frozen=True)
class OutputTree:
  """The shards of one output directory."""
  root: str
  run_dir_name: str | None  # None if the run wrote no JSON-LD.
  shards: tuple[Shard, ...]

  def of_kind(self, kind: str) -> list[Shard]:
    return [s for s in self.shards if s.kind == kind]

  @property
  def folders(self) -> set[str]:
    return {s.folder for s in self.shards}


def load_output_tree(output_dir: str) -> OutputTree:
  """Lists the shards under output_dir/jsonld/<run dir>."""
  jsonld_dir = os.path.join(output_dir, "jsonld")
  run_dirs = sorted(os.listdir(jsonld_dir)) if os.path.isdir(jsonld_dir) else []
  if not run_dirs:
    return OutputTree(output_dir, None, ())
  if len(run_dirs) > 1:
    raise ValueError(f"Expected one run directory under {jsonld_dir}, found "
                     f"{run_dirs}. Use a fresh output directory per run.")
  run_dir = os.path.join(jsonld_dir, run_dirs[0])
  shards = tuple(
      Shard(os.path.relpath(path, run_dir), path)
      for path in sorted(glob.glob(os.path.join(run_dir, "*", "*.jsonld"))))
  unknown = [s.rel_path for s in shards if s.kind not in _KINDS]
  if unknown:
    raise ValueError(f"Unrecognized shard names: {unknown[:5]}")
  return OutputTree(output_dir, run_dirs[0], shards)


def sha256_file(path: str) -> str:
  digest = hashlib.sha256()
  with open(path, "rb") as f:
    for chunk in iter(lambda: f.read(_HASH_CHUNK_BYTES), b""):
      digest.update(chunk)
  return digest.hexdigest()


def compare_inventory(baseline: OutputTree, candidate: OutputTree,
                      findings: Findings,
                      pool: concurrent.futures.Executor) -> dict[str, Any]:
  """Compares run directory names, shard names and shard bytes."""
  run_dirs = [
      volatile.stable_run_dir_name(t.run_dir_name or "")
      for t in (baseline, candidate)
  ]
  if run_dirs[0] != run_dirs[1]:
    findings.add("run_dir_name_changed",
                 sample={
                     "baseline": run_dirs[0],
                     "candidate": run_dirs[1]
                 })
  sha = _hash_all(baseline, candidate, pool)
  return {
      "run_dir":
          run_dirs,
      "folders": [len(baseline.folders),
                  len(candidate.folders)],
      "bytes": [_total_bytes(baseline),
                _total_bytes(candidate)],
      OBSERVATION:
          _compare_observation_shards(baseline, candidate, sha, findings),
      NODE:
          _compare_node_shards(baseline, candidate, sha, findings),
  }


def _hash_all(baseline: OutputTree, candidate: OutputTree,
              pool: concurrent.futures.Executor) -> dict[str, str]:
  """Returns the sha256 of every shard of both sides, by absolute path."""
  paths = [s.path for t in (baseline, candidate) for s in t.shards]
  return dict(zip(paths, pool.map(sha256_file, paths, chunksize=8)))


def _total_bytes(tree: OutputTree) -> int:
  return sum(os.path.getsize(s.path) for s in tree.shards)


def _compare_observation_shards(baseline: OutputTree, candidate: OutputTree,
                                sha: dict[str, str],
                                findings: Findings) -> dict[str, int]:
  base = {s.rel_path: s.path for s in baseline.of_kind(OBSERVATION)}
  cand = {s.rel_path: s.path for s in candidate.of_kind(OBSERVATION)}
  for rel_path in sorted(base.keys() ^ cand.keys()):
    side = "baseline" if rel_path in base else "candidate"
    findings.add("shard_set_changed",
                 sample={
                     "only_in": side,
                     "shard": rel_path
                 })
  common = sorted(base.keys() & cand.keys())
  identical = 0
  for rel_path in common:
    if sha[base[rel_path]] == sha[cand[rel_path]]:
      identical += 1
      continue
    findings.add("shard_bytes_changed", sample={"shard": rel_path})
  return {
      "baseline": len(base),
      "candidate": len(cand),
      "same_name": len(common),
      "byte_identical": identical
  }


def _compare_node_shards(baseline: OutputTree, candidate: OutputTree,
                         sha: dict[str,
                                   str], findings: Findings) -> dict[str, int]:
  base = Counter((s.folder, sha[s.path]) for s in baseline.of_kind(NODE))
  cand = Counter((s.folder, sha[s.path]) for s in candidate.of_kind(NODE))
  _compare_node_shard_counts(base, cand, findings)
  names = {
      (s.folder, sha[s.path]): s.rel_path for t in (baseline, candidate)
      for s in t.of_kind(NODE)
  }
  for side, unmatched in (("baseline", base - cand), ("candidate",
                                                      cand - base)):
    for key, count in sorted(unmatched.items()):
      findings.add("shard_bytes_changed",
                   count,
                   sample={
                       "only_in": side,
                       "shard": names[key]
                   })
  return {
      "baseline": sum(base.values()),
      "candidate": sum(cand.values()),
      "byte_identical_in_same_folder": sum((base & cand).values()),
  }


def _compare_node_shard_counts(base: Counter, cand: Counter,
                               findings: Findings) -> None:
  per_folder = [Counter(), Counter()]
  for counts, side in zip(per_folder, (base, cand)):
    for (folder, _), count in side.items():
      counts[folder] += count
  for folder in sorted(per_folder[0].keys() | per_folder[1].keys()):
    if per_folder[0][folder] != per_folder[1][folder]:
      findings.add(
          "shard_set_changed",
          sample={
              "folder": folder,
              "node_shards": [per_folder[0][folder], per_folder[1][folder]]
          })
