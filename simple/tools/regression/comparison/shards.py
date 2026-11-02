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

from tools.regression.comparison import volatile
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides

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


def _visible_entries(dir_path: str) -> list[str]:
  """Sorted entries of dir_path, without hidden ones such as .DS_Store."""
  if not os.path.isdir(dir_path):
    return []
  return sorted(e for e in os.listdir(dir_path) if not e.startswith("."))


def load_output_tree(output_dir: str) -> OutputTree:
  """Lists the shards under output_dir/jsonld/<run dir>."""
  jsonld_dir = os.path.join(output_dir, "jsonld")
  run_dirs = _visible_entries(jsonld_dir)
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
    findings.add("run_dir_name_changed", sample=sides(*run_dirs))
  paths = [s.path for t in (baseline, candidate) for s in t.shards]
  sha = dict(zip(paths, pool.map(sha256_file, paths, chunksize=8)))
  folders = [len(baseline.folders), len(candidate.folders)]
  total_bytes = [_total_bytes(baseline), _total_bytes(candidate)]
  observation_shards = _compare_observation_shards(baseline, candidate, sha,
                                                   findings)
  node_shards = _compare_node_shards(baseline, candidate, sha, findings)
  return {
      "run_dir": run_dirs,
      "folders": folders,
      "bytes": total_bytes,
      OBSERVATION: observation_shards,
      NODE: node_shards,
  }


def _total_bytes(tree: OutputTree) -> int:
  return sum(os.path.getsize(s.path) for s in tree.shards)


def _compare_observation_shards(baseline: OutputTree, candidate: OutputTree,
                                sha: dict[str, str],
                                findings: Findings) -> dict[str, int]:
  """Matches observation shards by name, then compares their bytes."""
  base = {s.rel_path: s.path for s in baseline.of_kind(OBSERVATION)}
  cand = {s.rel_path: s.path for s in candidate.of_kind(OBSERVATION)}
  for rel_path in sorted(base.keys() ^ cand.keys()):
    only_in = "baseline" if rel_path in base else "candidate"
    findings.add("shard_set_changed",
                 sample=dict(only_in=only_in, shard=rel_path))
  common = sorted(base.keys() & cand.keys())
  changed = [p for p in common if sha[base[p]] != sha[cand[p]]]
  for rel_path in changed:
    findings.add("shard_bytes_changed", sample={"shard": rel_path})
  return {
      "baseline": len(base),
      "candidate": len(cand),
      "same_name": len(common),
      "byte_identical": len(common) - len(changed),
  }


def _compare_node_shards(baseline: OutputTree, candidate: OutputTree,
                         sha: dict[str,
                                   str], findings: Findings) -> dict[str, int]:
  """Matches node shards by content within their folder."""
  base = Counter((s.folder, sha[s.path]) for s in baseline.of_kind(NODE))
  cand = Counter((s.folder, sha[s.path]) for s in candidate.of_kind(NODE))
  base_per_folder, cand_per_folder = _per_folder(base), _per_folder(cand)
  for folder in sorted(base_per_folder.keys() | cand_per_folder.keys()):
    counts = [base_per_folder[folder], cand_per_folder[folder]]
    if counts[0] != counts[1]:
      findings.add("shard_set_changed",
                   sample=dict(folder=folder, node_shards=counts))
  names = {
      (s.folder, sha[s.path]): s.rel_path for t in (baseline, candidate)
      for s in t.of_kind(NODE)
  }
  for only_in, unmatched in (("baseline", base - cand), ("candidate",
                                                         cand - base)):
    for key, count in sorted(unmatched.items()):
      findings.add("shard_bytes_changed",
                   count,
                   sample=dict(only_in=only_in, shard=names[key]))
  return {
      "baseline": sum(base.values()),
      "candidate": sum(cand.values()),
      "byte_identical_in_same_folder": sum((base & cand).values()),
  }


def _per_folder(shards: Counter) -> Counter:
  """Sums a Counter keyed by (folder, sha256) per folder."""
  per_folder: Counter = Counter()
  for (folder, _), count in shards.items():
    per_folder[folder] += count
  return per_folder
