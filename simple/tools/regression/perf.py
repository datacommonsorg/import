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
"""Compares the run metrics of the two sides: performance and run health.

Performance metrics are medians over the repeats of a side. Run health (exit
status and log lines) uses run 1, the run whose output is compared, and is
recorded as difference categories.
"""

import dataclasses
import statistics

from tools.regression.categories import Findings
from tools.regression.measure import RunMetrics
from tools.regression.run import SideRuns

_KB_PER_MIB = 1024


@dataclasses.dataclass(frozen=True)
class Thresholds:
  """Allowed slowdowns, in percent of the baseline median."""
  wall_pct: float
  cpu_pct: float
  rss_pct: float
  # Runs shorter than this (baseline median wall time) are too noisy to judge.
  min_seconds: float


@dataclasses.dataclass(frozen=True)
class MetricRow:
  name: str
  unit: str
  baseline: float | None
  candidate: float | None
  threshold_pct: float | None  # None: reported only.
  regressed: bool

  @property
  def delta_pct(self) -> float | None:
    if not self.baseline or self.candidate is None:
      return None
    return 100 * (self.candidate - self.baseline) / self.baseline


@dataclasses.dataclass(frozen=True)
class PerfResult:
  rows: tuple[MetricRow, ...]
  repeats: tuple[int, int]
  # Why the thresholds were not applied, or None if they were.
  not_judged_reason: str | None

  @property
  def regressions(self) -> list[str]:
    return [row.name for row in self.rows if row.regressed]


# (name, unit, RunMetrics accessor, Thresholds field or None).
_METRICS = (
    ("Wall time", "s", lambda m: m.wall_seconds, "wall_pct"),
    ("CPU time (user + sys)", "s", lambda m: m.cpu_seconds, "cpu_pct"),
    ("CPU user", "s", lambda m: m.user_seconds, None),
    ("CPU sys", "s", lambda m: m.sys_seconds, None),
    ("Max RSS (largest process)", "MiB", lambda m: m.max_rss_kb / _KB_PER_MIB,
     "rss_pct"),
    ("Peak RSS (process tree, sampled)", "MiB",
     lambda m: m.peak_tree_rss_kb / _KB_PER_MIB, "rss_pct"),
)


def _median(runs: tuple[RunMetrics, ...], accessor) -> float | None:
  return statistics.median(accessor(m) for m in runs) if runs else None


def _not_judged_reason(base: SideRuns, cand: SideRuns,
                       thresholds: Thresholds) -> str | None:
  if not base.metrics or not cand.metrics:
    return "a side has no metrics (reused output without run.json)"
  if base.metrics[0].exit_code or cand.metrics[0].exit_code:
    return "a side did not exit successfully"
  base_wall = _median(base.metrics, lambda m: m.wall_seconds)
  if base_wall < thresholds.min_seconds:
    return (f"the baseline ran {base_wall:.1f}s, under --perf_min_seconds="
            f"{thresholds.min_seconds:g}")
  return None


def compare_perf(base: SideRuns, cand: SideRuns,
                 thresholds: Thresholds) -> PerfResult:
  """Returns the median metrics of both sides and the regressions."""
  reason = _not_judged_reason(base, cand, thresholds)
  rows = []
  for name, unit, accessor, threshold_field in _METRICS:
    threshold = getattr(thresholds,
                        threshold_field) if threshold_field else None
    row = MetricRow(name, unit, _median(base.metrics, accessor),
                    _median(cand.metrics, accessor), threshold, False)
    regressed = (reason is None and threshold is not None and
                 row.delta_pct is not None and row.delta_pct > threshold)
    rows.append(dataclasses.replace(row, regressed=regressed))
  return PerfResult(tuple(rows), (len(base.metrics), len(cand.metrics)), reason)


def _status(metrics: RunMetrics) -> dict:
  return {"exit_code": metrics.exit_code, "timed_out": metrics.timed_out}


def compare_health(base: SideRuns, cand: SideRuns, findings: Findings) -> None:
  """Records exit status and log line changes of run 1 as findings."""
  if not base.metrics or not cand.metrics:
    return
  b, c = base.metrics[0], cand.metrics[0]
  sample = {"baseline_log": base.log_path, "candidate_log": cand.log_path}
  if b.exit_code != c.exit_code or b.timed_out != c.timed_out:
    findings.add("exit_status_changed",
                 sample={
                     "baseline": _status(b),
                     "candidate": _status(c),
                     **sample
                 })
  if c.error_lines > b.error_lines:
    findings.add("error_lines_increased",
                 c.error_lines - b.error_lines,
                 sample={
                     "baseline": b.error_lines,
                     "candidate": c.error_lines,
                     **sample
                 })
  if c.warning_lines != b.warning_lines:
    findings.add("warning_lines_changed",
                 abs(c.warning_lines - b.warning_lines),
                 sample={
                     "baseline": b.warning_lines,
                     "candidate": c.warning_lines,
                     **sample
                 })
