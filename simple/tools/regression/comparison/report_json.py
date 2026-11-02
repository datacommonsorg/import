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
Comparison of process/report.json and the per-file report_*.json files.

Timestamps are dropped and input paths are made relative to each side's input
root (see volatile.py). Per-file reports are compared by the input file they
describe, not by their file name.
"""

import glob
import os
from typing import Any

from tools.regression.comparison import volatile
from tools.regression.comparison.canonical import load_json
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides

_IMPORT_FILES = "importFiles"
_REPORT_FILE = "report.json"
_WORKER_REPORT_GLOB = "report_*.json"


def _normalize(report: dict, input_root: str | None) -> dict:
  report = volatile.strip_report_timestamps(report)
  files = report.get(_IMPORT_FILES, {})
  report[_IMPORT_FILES] = {
      volatile.tokenize_input_root(path, input_root): entry
      for path, entry in files.items()
  }
  return report


def _load_main(output_dir: str, input_root: str | None) -> dict:
  path = os.path.join(output_dir, "process", _REPORT_FILE)
  if not os.path.exists(path):
    return {}
  return _normalize(load_json(path), input_root)


def _load_workers(output_dir: str, input_root: str | None) -> dict[str, Any]:
  """Merges the importFiles entries of every per-file report."""
  merged = {}
  pattern = os.path.join(output_dir, "process", _WORKER_REPORT_GLOB)
  for path in sorted(glob.glob(pattern)):
    merged.update(_normalize(load_json(path), input_root)[_IMPORT_FILES])
  return merged


def _diff_dicts(base: dict, cand: dict, category: str, label: str,
                findings: Findings) -> None:
  for key in sorted(base.keys() | cand.keys()):
    if base.get(key) == cand.get(key):
      continue
    sample = {label: key, **sides(base.get(key), cand.get(key))}
    findings.add(category, sample=sample)


def compare_reports(baseline_dir: str, baseline_input: str | None,
                    candidate_dir: str, candidate_input: str | None,
                    findings: Findings) -> dict[str, Any]:
  """Compares the reports of two outputs and returns summary statistics."""
  base = _load_main(baseline_dir, baseline_input)
  cand = _load_main(candidate_dir, candidate_input)
  base_files = base.pop(_IMPORT_FILES, {})
  cand_files = cand.pop(_IMPORT_FILES, {})
  _diff_dicts(base, cand, "report_json_changed", "field", findings)
  _diff_dicts(base_files, cand_files, "report_json_changed", "file", findings)
  base_workers = _load_workers(baseline_dir, baseline_input)
  cand_workers = _load_workers(candidate_dir, candidate_input)
  _diff_dicts(base_workers, cand_workers, "worker_reports_changed", "file",
              findings)
  return {
      "status": [base.get("status"), cand.get("status")],
      "import_files": [len(base_files), len(cand_files)],
      "worker_report_files": [len(base_workers),
                              len(cand_workers)],
  }
