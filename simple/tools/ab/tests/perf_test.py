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
"""Perf thresholds, run health findings and run measurement."""

import dataclasses
import os
import sys

from tools.ab.categories import Findings
from tools.ab.measure import count_log_lines
from tools.ab.measure import run_measured
from tools.ab.measure import RunMetrics
from tools.ab.perf import compare_health
from tools.ab.perf import compare_perf
from tools.ab.perf import Thresholds
from tools.ab.report import EXIT_OK
from tools.ab.report import EXIT_OUTPUT_DIFFERENT
from tools.ab.report import EXIT_PERF_REGRESSION
from tools.ab.report import Verdict
from tools.ab.run import SideRuns

THRESHOLDS = Thresholds(wall_pct=10, cpu_pct=10, rss_pct=10, min_seconds=30)
METRICS = RunMetrics(exit_code=0,
                     timed_out=False,
                     wall_seconds=100,
                     user_seconds=80,
                     sys_seconds=20,
                     max_rss_kb=1024 * 1024,
                     peak_tree_rss_kb=4 * 1024 * 1024,
                     error_lines=0,
                     warning_lines=0)


def side(*metrics: RunMetrics) -> SideRuns:
  return SideRuns("code", "/out", None, metrics, "/log")


def test_perf_regression_beyond_threshold():
  slower = dataclasses.replace(METRICS, wall_seconds=125)
  result = compare_perf(side(METRICS), side(slower), THRESHOLDS)
  assert result.regressions == ["Wall time"]


def test_perf_within_threshold_is_not_a_regression():
  slower = dataclasses.replace(METRICS, wall_seconds=105, max_rss_kb=1100000)
  result = compare_perf(side(METRICS), side(slower), THRESHOLDS)
  assert result.regressions == []


def test_perf_uses_the_median_of_repeats():
  outlier = dataclasses.replace(METRICS, wall_seconds=500)
  result = compare_perf(side(METRICS), side(METRICS, outlier, METRICS),
                        THRESHOLDS)
  assert result.regressions == []
  assert result.rows[0].candidate == 100


def test_short_runs_are_not_judged():
  fast = dataclasses.replace(METRICS, wall_seconds=2)
  result = compare_perf(side(fast),
                        side(dataclasses.replace(fast, wall_seconds=4)),
                        THRESHOLDS)
  assert result.regressions == []
  assert "perf_min_seconds" in result.not_judged_reason


def test_reused_output_without_metrics_is_not_judged():
  result = compare_perf(side(), side(METRICS), THRESHOLDS)
  assert result.regressions == []
  assert result.not_judged_reason


def test_health_findings():
  findings = Findings(max_samples=5)
  worse = dataclasses.replace(METRICS,
                              exit_code=1,
                              error_lines=3,
                              warning_lines=2)
  compare_health(side(METRICS), side(worse), findings)
  assert {
      k: v for k, v in findings.counts.items() if v
  } == {
      "exit_status_changed": 1,
      "error_lines_increased": 3,
      "warning_lines_changed": 2,
  }


def test_fewer_errors_is_not_a_finding():
  findings = Findings(max_samples=5)
  noisy = dataclasses.replace(METRICS, error_lines=3)
  compare_health(side(noisy), side(METRICS), findings)
  assert not any(findings.counts.values())


def test_verdict_exit_codes():
  assert Verdict({}, []).exit_code == EXIT_OK
  assert Verdict({}, ["Wall time"]).exit_code == EXIT_PERF_REGRESSION
  assert Verdict({
      "obs_value_changed": 1
  }, ["Wall time"]).exit_code == EXIT_OUTPUT_DIFFERENT


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
