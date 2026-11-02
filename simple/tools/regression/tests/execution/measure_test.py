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
"""Measured runs, log line counting and loading reused runs."""

import json
import os
import sys

from tools.regression.execution.measure import count_log_lines
from tools.regression.execution.measure import run_measured
from tools.regression.execution.measure import RunMetrics
from tools.regression.execution.run import load_side
from tools.regression.execution.run import RUN_JSON


def test_count_log_lines(tmp_path):
  log = tmp_path / "log"
  log.write_text("\n".join([
      "[2026-01-01 00:00:00,000 INFO runner.py:1] fine",
      "[2026-01-01 00:00:00,000 WARNING runner.py:2] careful",
      "[2026-01-01 00:00:00,000 ERROR runner.py:3] broken",
      "CRITICAL:root:very broken",
      "Traceback (most recent call last):",
      "INFO: an ERROR inside a message does not count",
  ]))
  assert count_log_lines(str(log)) == (3, 1)


def test_run_measured(tmp_path):
  log = str(tmp_path / "log")
  metrics = run_measured(
      [sys.executable, "-c", "import sys; print('WARNING: x'); sys.exit(3)"],
      cwd=str(tmp_path),
      env=dict(os.environ),
      log_path=log,
      timeout_seconds=None)
  assert metrics.exit_code == 3
  assert not metrics.timed_out
  assert metrics.max_rss_kb > 0
  assert metrics.warning_lines == 1


def test_run_measured_kills_on_timeout(tmp_path):
  metrics = run_measured([sys.executable, "-c", "import time; time.sleep(60)"],
                         cwd=str(tmp_path),
                         env=dict(os.environ),
                         log_path=str(tmp_path / "log"),
                         timeout_seconds=0.5)
  assert metrics.timed_out
  assert metrics.exit_code < 0
  assert metrics.wall_seconds < 30


def test_reused_runs_are_loaded_in_run_number_order(tmp_path):
  """run-10 comes after run-9, not between run-1 and run-2."""
  for number in range(1, 12):
    run_dir = tmp_path / "candidate" / f"run-{number}"
    os.makedirs(run_dir / "output")
    # exit_code, timed_out, wall_seconds, then zero CPU, RSS and log counts.
    metrics = RunMetrics(0, False, number, 0, 0, 0, 0, 0, 0)
    run = {"code": "HEAD", "input_dir": "/in", "metrics": vars(metrics)}
    (run_dir / RUN_JSON).write_text(json.dumps(run))
  runs = load_side(str(tmp_path / "candidate" / "run-1" / "output"), None)
  assert [m.wall_seconds for m in runs.metrics] == list(range(1, 12))
