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
"""The verdict, the report model, and report.json."""

import dataclasses
from typing import Any

from tools.regression.comparison.categories import CATEGORIES
from tools.regression.comparison.compare import Comparison
from tools.regression.comparison.metrics import PerfResult
from tools.regression.comparison.metrics import RunFailure
from tools.regression.execution.run import SideRuns

EXIT_OK = 0
EXIT_OUTPUT_DIFFERENT = 3
EXIT_PERF_REGRESSION = 4
EXIT_RUN_FAILED = 5


@dataclasses.dataclass(frozen=True)
class Verdict:
  """
  The outcome of a comparison.

  A failed run takes precedence over output differences, which take precedence
  over perf regressions: when a side failed, its output is not a valid result.
  """

  failing: dict[str, int]  # Category name -> count, for failing categories.
  perf_regressions: list[str]
  run_failures: list[RunFailure]

  @property
  def output_label(self) -> str:
    return "DIFFERENT" if self.failing else "IDENTICAL"

  @property
  def label(self) -> str:
    return "RUN FAILED" if self.run_failures else self.output_label

  @property
  def exit_code(self) -> int:
    if self.run_failures:
      return EXIT_RUN_FAILED
    if self.failing:
      return EXIT_OUTPUT_DIFFERENT
    if self.perf_regressions:
      return EXIT_PERF_REGRESSION
    return EXIT_OK


@dataclasses.dataclass(frozen=True)
class Report:
  """Everything a report shows."""

  baseline: SideRuns
  candidate: SideRuns
  run_info: dict[str, Any]  # Input, flags, repeats, work dir.
  comparison: Comparison
  perf: PerfResult
  verdict: Verdict


def _side_json(side: SideRuns) -> dict[str, Any]:
  return {
      "code": side.code,
      "output_dir": side.output_dir,
      "log": side.log_path,
      "runs": [dataclasses.asdict(m) for m in side.metrics],
  }


def to_json(report: Report) -> dict[str, Any]:
  findings = report.comparison.findings
  metrics = [
      dict(dataclasses.asdict(row), delta_pct=row.delta_pct)
      for row in report.perf.rows
  ]
  return {
      "verdict": {
          "label": report.verdict.label,
          "run_failures": [
              dataclasses.asdict(f) for f in report.verdict.run_failures
          ],
          "output": report.verdict.output_label,
          "failing_categories": report.verdict.failing,
          "perf_regressions": report.verdict.perf_regressions,
          "exit_code": report.verdict.exit_code,
      },
      "run": report.run_info,
      "baseline": _side_json(report.baseline),
      "candidate": _side_json(report.candidate),
      "perf": {
          "repeats": report.perf.repeats,
          "not_judged_reason": report.perf.not_judged_reason,
          "metrics": metrics,
      },
      "categories": [{
          "name": c.name,
          "kind": c.kind.value,
          "meaning": c.meaning,
          "count": findings.counts[c.name],
          "samples": findings.samples[c.name],
      } for c in CATEGORIES],
      "stats": report.comparison.stats,
  }


def summary_line(report: Report) -> str:
  """A one-line summary for the console."""
  verdict = report.verdict
  failing = ", ".join(f"{k}={v:,}" for k, v in verdict.failing.items())
  output = verdict.output_label + (f" ({failing})" if failing else "")
  head = f"Verdict: {output}"
  if verdict.run_failures:
    failures = "; ".join(f.describe() for f in verdict.run_failures)
    head = f"Verdict: {verdict.label} ({failures}); output: {output}"
  regressions = ", ".join(verdict.perf_regressions)
  return (f"{head}; perf regressions: {regressions or 'none'}; exit code "
          f"{verdict.exit_code}")
