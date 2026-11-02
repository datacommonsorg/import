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
"""The verdict and the markdown and JSON reports."""

import dataclasses
import json
from typing import Any

from tools.ab.categories import CATEGORIES
from tools.ab.compare import Comparison
from tools.ab.perf import MetricRow
from tools.ab.perf import PerfResult
from tools.ab.run import SideRuns

EXIT_OK = 0
EXIT_OUTPUT_DIFFERENT = 3
EXIT_PERF_REGRESSION = 4

_SAMPLE_CHARS = 1500
_MAX_TABLE_ROWS = 30


@dataclasses.dataclass(frozen=True)
class Verdict:
  failing: dict[str, int]  # Category name -> count, for failing categories.
  perf_regressions: list[str]

  @property
  def label(self) -> str:
    return "DIFFERENT" if self.failing else "IDENTICAL"

  @property
  def exit_code(self) -> int:
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
  return {
      "verdict": {
          "output": report.verdict.label,
          "failing_categories": report.verdict.failing,
          "perf_regressions": report.verdict.perf_regressions,
          "exit_code": report.verdict.exit_code,
      },
      "run": report.run_info,
      "baseline": _side_json(report.baseline),
      "candidate": _side_json(report.candidate),
      "perf": {
          "repeats":
              report.perf.repeats,
          "not_judged_reason":
              report.perf.not_judged_reason,
          "metrics": [
              dict(dataclasses.asdict(row), delta_pct=row.delta_pct)
              for row in report.perf.rows
          ],
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


def _num(value: float | int | str | None, unit: str = "") -> str:
  if value is None:
    return "n/a"
  if isinstance(value, str):
    return f"`{value}`"
  if isinstance(value, int):
    return f"{value:,}"
  return f"{value:,.1f}{unit}"


def _pair_row(label: str, pair: list[Any]) -> str:
  base, cand = pair
  mark = "" if base == cand else " **≠**"
  return f"| {label} | {_num(base)} | {_num(cand)}{mark} |"


def _perf_row(row: MetricRow) -> str:
  delta = "n/a" if row.delta_pct is None else f"{row.delta_pct:+.1f}%"
  threshold = "-" if row.threshold_pct is None else f"{row.threshold_pct:g}%"
  status = "**REGRESSED**" if row.regressed else ""
  return (f"| {row.name} | {_num(row.baseline, row.unit)} | "
          f"{_num(row.candidate, row.unit)} | {delta} | {threshold} | "
          f"{status} |")


def _header(report: Report) -> list[str]:
  verdict = report.verdict
  output = "IDENTICAL" if not verdict.failing else ("DIFFERENT: " + ", ".join(
      f"`{name}` ({count:,})" for name, count in verdict.failing.items()))
  if report.perf.not_judged_reason:
    perf = f"not judged ({report.perf.not_judged_reason})"
  elif verdict.perf_regressions:
    perf = "REGRESSED: " + ", ".join(verdict.perf_regressions)
  else:
    perf = "no regression beyond the thresholds"
  info = report.run_info
  return [
      "# Preprocessor A/B report",
      "",
      f"## Verdict: {verdict.label}",
      "",
      f"- **Output:** {output}",
      f"- **Performance:** {perf}",
      f"- **Exit code:** {verdict.exit_code}",
      "",
      "| | Baseline | Candidate |",
      "|---|---|---|",
      f"| Code | {report.baseline.code} | {report.candidate.code} |",
      f"| Output | `{report.baseline.output_dir}` | "
      f"`{report.candidate.output_dir}` |",
      f"| Log | `{report.baseline.log_path}` | `{report.candidate.log_path}` |",
      "",
      f"Input: `{info.get('input_dir')}`. Extra flags: "
      f"`{' '.join(info.get('extra_flags', [])) or '(none)'}`.",
  ]


def _perf_section(report: Report) -> list[str]:
  perf = report.perf
  lines = [
      "",
      f"## Performance (median of {perf.repeats[0]} / {perf.repeats[1]} runs)",
      "",
      "| Metric | Baseline | Candidate | Δ | Threshold | |",
      "|---|---|---|---|---|---|",
  ]
  lines += [_perf_row(row) for row in perf.rows]
  first = [
      s.metrics[0] if s.metrics else None
      for s in (report.baseline, report.candidate)
  ]
  for label, field in (("Exit status (run 1)",
                        "exit_code"), ("Timed out (run 1)", "timed_out"),
                       ("ERROR lines + tracebacks (run 1)", "error_lines"),
                       ("WARNING lines (run 1)", "warning_lines")):
    values = [getattr(m, field) if m else None for m in first]
    lines.append(f"| {label} | {values[0]} | {values[1]} | | | |")
  return lines


def _categories_section(report: Report) -> list[str]:
  counts = report.comparison.findings.counts
  failing = report.verdict.failing
  lines = [
      "",
      "## Differences",
      "",
      "| Category | Kind | Count | Status | Meaning |",
      "|---|---|---|---|---|",
  ]
  for c in CATEGORIES:
    count = counts[c.name]
    status = "ok" if not count else (
        "**DIFF**" if c.name in failing else "info")
    lines.append(f"| `{c.name}` | {c.kind.value} | {count:,} | {status} | "
                 f"{c.meaning} |")
  return lines


def _differing(table: dict[str, list[int]]) -> dict[str, list[int]]:
  return {k: v for k, v in table.items() if v[0] != v[1]}


def _stats_section(report: Report) -> list[str]:
  stats = report.comparison.stats
  inv, reports = stats["inventory"], stats["reports"]
  obs, nodes = stats["observations"], stats["nodes"]
  obs_shards, node_shards = inv["observation"], inv["node"]
  lines = [
      "",
      "## Output summary",
      "",
      "| | Baseline | Candidate |",
      "|---|---|---|",
      _pair_row("Run directory (no timestamp)", inv["run_dir"]),
      _pair_row("Import folders", inv["folders"]),
      _pair_row("Shard bytes", inv["bytes"]),
      _pair_row("Observation shards",
                [obs_shards["baseline"], obs_shards["candidate"]]),
      f"| ... byte-identical (same name) | {obs_shards['byte_identical']:,} "
      f"| of {obs_shards['same_name']:,} |",
      _pair_row("Node shards",
                [node_shards["baseline"], node_shards["candidate"]]),
      f"| ... byte-identical (same folder) | "
      f"{node_shards['byte_identical_in_same_folder']:,} | |",
      _pair_row("Observations", obs["observations"]),
      _pair_row("Distinct observation keys", obs["distinct_keys"]),
      _pair_row("Duplicated observation keys", obs["duplicate_keys"]),
      _pair_row("Observations with provenanceUrl", obs["with_provenance_url"]),
      _pair_row("Node records", nodes["records"]),
      _pair_row("Node @ids", nodes["ids"]),
      _pair_row("report.json status", reports["status"]),
      _pair_row("report.json importFiles", reports["import_files"]),
      _pair_row("Per-file report entries", reports["worker_report_files"]),
  ]
  lines += _pair_table("Observations per folder (differing only)", "Folder",
                       _differing(obs["per_folder"]))
  lines += _pair_table("Node @ids per @type (differing only)", "@type",
                       _differing(nodes["types"]))
  lines += _list_table("Node property changes (top groups)",
                       nodes["property_changes"])
  lines += _list_table("Node folder moves (top groups)", nodes["folder_moves"])
  return lines


def _pair_table(title: str, label: str, table: dict[str,
                                                    list[int]]) -> list[str]:
  if not table:
    return []
  lines = [
      "", f"### {title}", "", f"| {label} | Baseline | Candidate |",
      "|---|---|---|"
  ]
  rows = list(table.items())
  lines += [_pair_row(f"`{k}`", v) for k, v in rows[:_MAX_TABLE_ROWS]]
  if len(rows) > _MAX_TABLE_ROWS:
    lines.append(f"| ... {len(rows) - _MAX_TABLE_ROWS:,} more | | |")
  return lines


def _list_table(title: str, rows: list[dict[str, Any]]) -> list[str]:
  if not rows:
    return []
  columns = list(rows[0])
  lines = [
      "", f"### {title}", "", "| " + " | ".join(columns) + " |",
      "|" + "---|" * len(columns)
  ]
  for row in rows:
    lines.append("| " + " | ".join(f"`{row[c]}`" for c in columns) + " |")
  return lines


def _truncated_json(value: Any) -> str:
  text = json.dumps(value, sort_keys=True)
  if len(text) > _SAMPLE_CHARS:
    return text[:_SAMPLE_CHARS] + " ...(truncated)"
  return text


def _sample_lines(sample: Any) -> list[str]:
  """Renders a sample; its baseline and candidate sides on separate lines."""
  if not isinstance(sample, dict) or "baseline" not in sample:
    return [_truncated_json(sample)]
  sides = ("baseline", "candidate")
  context = {k: v for k, v in sample.items() if k not in sides}
  lines = [_truncated_json(context)] if context else []
  lines += [f"{side}: {_truncated_json(sample.get(side))}" for side in sides]
  return lines + [""]


def _samples_section(report: Report) -> list[str]:
  findings = report.comparison.findings
  lines = ["", "## Samples"]
  for c in CATEGORIES:
    samples = findings.samples[c.name]
    if not samples:
      continue
    lines += [
        "", f"### `{c.name}` ({findings.counts[c.name]:,}, {c.kind.value})", "",
        "```json"
    ]
    for sample in samples:
      lines += _sample_lines(sample)
    if not lines[-1]:
      lines.pop()
    lines.append("```")
  if len(lines) == 2:
    lines += ["", "No differences."]
  return lines


def to_markdown(report: Report) -> str:
  sections = (_header(report) + _perf_section(report) +
              _categories_section(report) + _stats_section(report) +
              _samples_section(report))
  return "\n".join(sections) + "\n"


def summary_line(report: Report) -> str:
  """A one-line summary for the console."""
  failing = ", ".join(f"{k}={v:,}" for k, v in report.verdict.failing.items())
  regressions = ", ".join(report.verdict.perf_regressions)
  return (f"Verdict: {report.verdict.label}"
          f"{' (' + failing + ')' if failing else ''}; perf regressions: "
          f"{regressions or 'none'}; exit code {report.verdict.exit_code}")
