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
"""Canonical form of a preprocessor run's output, for comparisons.

The raw output has details that legitimately change between runs and modes:
timestamped directory names, shard file names and boundaries, record order,
and absolute paths. The canonical form removes exactly those and keeps
everything else, so two outputs are equal iff the loader would ingest the same
records into the same import folders and the workflow would get the same
report and handshake.

    {
      "folders": {
        "<import folder>": {
          "nodes":        {"<@id>": {record}, ...},
          "observations": {"<@id>": {record}, ...}
        }
      },
      "contexts": [{"dcid": "https://datacommons.org/browser/"}],
      "report": {...},     # process/report.json, times and paths removed
      "handshake": {...},  # ingestion-workflow record, or null
      "error": {...}       # exception raised by Runner.run(), or null
    }

A record that appears more than once in one folder (e.g. a node split across
shards) is kept as a list of its copies, so duplication shows up in diffs
instead of being merged away.
"""

import difflib
import json
import os
import re
from typing import Any

OBSERVATIONS = "observations"
NODES = "nodes"


def canonical_record(record: dict) -> dict:
  """Sorts multi-value lists; keeps all keys and JSON types."""
  out = {}
  for key, value in record.items():
    if isinstance(value, list):
      value = sorted(value, key=_sort_key)
    out[key] = value
  return out


def _sort_key(value: Any) -> str:
  return json.dumps(value, sort_keys=True)


def load_jsonld(jsonld_root: str) -> tuple[dict, list]:
  """Reads every *.jsonld under jsonld_root/<run dir>/<folder>/."""
  folders: dict[str, dict[str, dict[str, list]]] = {}
  contexts: list = []
  if not os.path.isdir(jsonld_root):
    return {}, []
  for run_dir in sorted(os.listdir(jsonld_root)):
    run_path = os.path.join(jsonld_root, run_dir)
    for root, _, files in os.walk(run_path):
      for name in sorted(files):
        if not name.endswith(".jsonld"):
          continue
        folder = os.path.relpath(root, run_path)
        kind = OBSERVATIONS if name.startswith("observation") else NODES
        with open(os.path.join(root, name), encoding="utf-8") as f:
          doc = json.load(f)
        if doc.get("@context") not in contexts:
          contexts.append(doc.get("@context"))
        bucket = folders.setdefault(folder, {NODES: {}, OBSERVATIONS: {}})[kind]
        for record in doc.get("@graph", []):
          bucket.setdefault(record.get("@id"),
                            []).append(canonical_record(record))
  for kinds in folders.values():
    for kind, records in kinds.items():
      kinds[kind] = {
          rid: copies[0] if len(copies) == 1 else sorted(copies, key=_sort_key)
          for rid, copies in sorted(records.items(), key=lambda x: str(x[0]))
      }
  return folders, contexts


def normalize_report(report: dict | None, path_tokens: dict[str, str]):
  if report is None:
    return None
  out = {
      k: v for k, v in report.items() if k not in ("startTime", "lastUpdate")
  }
  files = {}
  for path, data in report.get("importFiles", {}).items():
    data = {
        k: v for k, v in data.items() if k not in ("startTime", "lastUpdate")
    }
    files[replace_paths(path, path_tokens)] = data
  if "importFiles" in report:
    out["importFiles"] = dict(sorted(files.items()))
  if "error" in out:
    out["error"] = replace_paths(out["error"], path_tokens)
  return out


def normalize_handshake(handshake: dict | None, path_tokens: dict[str, str]):
  if handshake is None:
    return None
  out = dict(handshake)
  if isinstance(out.get("importList"), str):
    out["importList"] = json.loads(out["importList"])
  text = replace_paths(json.dumps(out, sort_keys=True), path_tokens)
  # The run directory under jsonld/ is "<import name>_<timestamp>".
  text = re.sub(r"/jsonld/[^/\"]+_\d{8}_\d{6}_\d+/", "/jsonld/<run>/", text)
  return json.loads(text)


def replace_paths(text: str, path_tokens: dict[str, str]) -> str:
  # Longest first, so nested paths are replaced before their parents.
  for path, token in sorted(path_tokens.items(), key=lambda x: -len(x[0])):
    stripped = path.rstrip("/")
    if stripped:  # An empty pattern would match between every character.
      text = text.replace(stripped, token)
  return text


def dumps(value: Any) -> str:
  return json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False) + "\n"


def diff(expected: Any,
         actual: Any,
         expected_name="expected",
         actual_name="actual",
         max_lines: int = 120) -> str:
  lines = list(
      difflib.unified_diff(dumps(expected).splitlines(),
                           dumps(actual).splitlines(),
                           fromfile=expected_name,
                           tofile=actual_name,
                           lineterm="",
                           n=2))
  if len(lines) > max_lines:
    lines = lines[:max_lines] + [f"... {len(lines) - max_lines} more lines"]
  return "\n".join(lines)


def all_records(output: dict, kind: str | None = None):
  """Yields (folder, kind, @id, record) for every record copy."""
  for folder, kinds in output.get("folders", {}).items():
    for k, records in kinds.items():
      if kind and k != kind:
        continue
      for rid, rec in records.items():
        for copy in rec if isinstance(rec, list) else [rec]:
          yield folder, k, rid, copy
