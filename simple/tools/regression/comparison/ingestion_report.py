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
Comparison of the ingestion report,
preprocessor_reports/[<workflow execution id>/]ingestion_report.json.

The `run` block is compared field by field, without the fields listed in
volatile.py, and `folders` folder by folder.
"""

import glob
import os
from typing import Any

from tools.regression.comparison import volatile
from tools.regression.comparison.canonical import load_json
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides

_CATEGORY = "ingestion_report_changed"


def _load(output_dir: str) -> dict:
  """Returns the output's ingestion report, or {} if it has none."""
  pattern = os.path.join(output_dir, "preprocessor_reports", "**",
                         "ingestion_report.json")
  paths = glob.glob(pattern, recursive=True)
  if len(paths) > 1:
    raise ValueError(f"Expected at most one ingestion report, found {paths}")
  return load_json(paths[0]) if paths else {}


def _diff(base: dict, cand: dict, label: str, findings: Findings) -> None:
  for key in sorted(base.keys() | cand.keys()):
    if base.get(key) != cand.get(key):
      findings.add(_CATEGORY,
                   sample={
                       label: key,
                       **sides(base.get(key), cand.get(key))
                   })


def _folders_by_name(report: dict) -> dict[str, dict]:
  return {f["folderName"]: f for f in report.get("folders", [])}


def compare_ingestion_reports(baseline_dir: str, candidate_dir: str,
                              findings: Findings) -> dict[str, Any]:
  """Compares the ingestion reports of two outputs and returns statistics."""
  base = _load(baseline_dir)
  cand = _load(candidate_dir)
  base_run = volatile.strip_run_metadata(base.get("run", {}))
  cand_run = volatile.strip_run_metadata(cand.get("run", {}))
  _diff(base_run, cand_run, "run", findings)
  base_folders = _folders_by_name(base)
  cand_folders = _folders_by_name(cand)
  _diff(base_folders, cand_folders, "folder", findings)
  return {
      "status": [base_run.get("status"),
                 cand_run.get("status")],
      "folders": [len(base_folders), len(cand_folders)],
  }
