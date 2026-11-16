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
Comparison of process/report.json.

Timestamps are dropped (see volatile.py); every other top-level field is
compared.
"""

import os
from typing import Any

from tools.regression.comparison import volatile
from tools.regression.comparison.canonical import load_json
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides

_REPORT_FILE = "report.json"


def _load_main(output_dir: str) -> dict:
  path = os.path.join(output_dir, "process", _REPORT_FILE)
  if not os.path.exists(path):
    return {}
  return volatile.strip_report_timestamps(load_json(path))


def compare_reports(baseline_dir: str, candidate_dir: str,
                    findings: Findings) -> dict[str, Any]:
  """Compares the reports of two outputs and returns summary statistics."""
  base = _load_main(baseline_dir)
  cand = _load_main(candidate_dir)
  for key in sorted(base.keys() | cand.keys()):
    if base.get(key) == cand.get(key):
      continue
    sample = {"field": key, **sides(base.get(key), cand.get(key))}
    findings.add("report_json_changed", sample=sample)
  return {"status": [base.get("status"), cand.get("status")]}
