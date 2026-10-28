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
"""
Canonical form of a preprocessor run's output, for comparisons.

The raw output has details that legitimately change between runs and modes:
timestamped directory names, shard file names and boundaries, record order,
and absolute paths. The canonical form removes exactly those and keeps
everything else, so two outputs are equal iff the loader would ingest the same
records into the same import folders and the workflow would get the same
report and handshake.

    {
      "records": [("<import folder>", "nodes" | "observations", {record}), ...],
      "report": {...},     # process/report.json, times and paths removed
      "handshake": {...},  # ingestion-workflow record, or null
      "error": {...}       # exception raised by Runner.run(), or null
    }

Every copy of a record is kept (e.g. a node split across shards), so
duplication shows up in comparisons instead of being merged away.
"""

import difflib
import glob
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


def load_jsonld(jsonld_root: str) -> list[tuple[str, str, dict]]:
  """
  (import folder, kind, record) for every record of every shard.

  Shards live at <jsonld_root>/<run dir>/<import folder>/*.jsonld; the kind is
  OBSERVATIONS for observation-*.jsonld shards and NODES otherwise.
  """
  pattern = os.path.join(jsonld_root, "*", "**", "*.jsonld")
  records = []
  for path in sorted(glob.glob(pattern, recursive=True)):
    *_, folder_and_file = os.path.relpath(path, jsonld_root).split(os.sep, 1)
    folder = os.path.dirname(folder_and_file) or "."
    kind = OBSERVATIONS if os.path.basename(path).startswith(
        "observation") else NODES
    with open(path, encoding="utf-8") as f:
      records += [(folder, kind, canonical_record(r))
                  for r in json.load(f).get("@graph", [])]
  return records


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
  text = re.sub(r"/jsonld/[^/\"]+_\d{8}_\d{6}_\d+/", "/jsonld/<run>/",
                text)  # The run dir is "<import name>_<timestamp>".
  return json.loads(text)


def replace_paths(text: str, path_tokens: dict[str, str]) -> str:
  """Replaces paths with tokens, longest first (nested before parents)."""
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
