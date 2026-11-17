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
Everything that legitimately changes from one run to the next.

The comparison ignores exactly what is listed here and nothing else, so a new
source of run-to-run noise belongs in this module.
"""

import re

# Keys dropped from process/report.json.
REPORT_TIMESTAMP_KEYS = frozenset({"startTime", "lastUpdate"})

# The JSON-LD run directory is "<import name>_<YYYYmmdd_HHMMSS_ffffff>".
_RUN_DIR_TIMESTAMP = re.compile(r"_\d{8}_\d{6}_\d{6}$")

# Node shards are named "node-<index>-<random uid>.jsonld". They are never
# matched by name; see shards.py.


def stable_run_dir_name(name: str) -> str:
  """Returns the run directory name without its wall-clock timestamp."""
  return _RUN_DIR_TIMESTAMP.sub("", name)


def strip_report_timestamps(report: dict) -> dict:
  """Returns a copy of report.json without REPORT_TIMESTAMP_KEYS."""
  return {
      key: value
      for key, value in report.items()
      if key not in REPORT_TIMESTAMP_KEYS
  }
