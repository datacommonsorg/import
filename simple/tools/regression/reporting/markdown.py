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
"""Renders a report as markdown (report.md)."""

import json
from typing import Any

from tools.regression.comparison.categories import CATEGORIES
from tools.regression.comparison.metrics import MetricRow
from tools.regression.reporting.report import Report

_SAMPLE_CHARS = 1500
_MAX_TABLE_ROWS = 30

# A markdown table cell; None renders as a blank cell.
Cell = str | None
# The run 1 health rows of the performance table: (label, RunMetrics field).
_HEALTH_ROWS = (
    ("Exit status (run 1)", "exit_code"),
    ("Timed out (run 1)", "timed_out"),
    ("ERROR lines + tracebacks (run 1)", "error_lines"),
    ("WARNING lines (run 1)", "warning_lines"),
)
_SIDES_HEADER: list[Cell] = [None, "Baseline", "Candidate"]


def _row(cells: list[Cell]) -> str:
  return "|" + "".join(" |" if c is None else f" {c} |" for c in cells)


def _table(header: list[Cell], rows: list[list[Cell]]) -> list[str]:
  return [_row(header), "|" + "---|" * len(header)] + [_row(r) for r in rows]


def _heading(title: str, level: int = 2) -> list[str]:
  return ["", "#" * level + " " + title, ""]


def _num(value: float | int | str | None, unit: str = "") -> str:
  if value is None:
    return "n/a"
  if isinstance(value, str):
    return f"`{value}`"
  if isinstance(value, int):
    return f"{value:,}"
  return f"{value:,.1f}{unit}"


def _pair_row(label: str, pair: list[Any]) -> list[Cell]:
  base, cand = pair
  mark = "" if base == cand else " **≠**"
  return [label, _num(base), _num(cand) + mark]


def _perf_row(row: MetricRow) -> list[Cell]:
  delta = "n/a" if row.delta_pct is None else f"{row.delta_pct:+.1f}%"
  threshold = "-" if row.threshold_pct is None else f"{row.threshold_pct:g}%"
  status = "**REGRESSED**" if row.regressed else ""
  return [
      row.name,
      _num(row.baseline, row.unit),
      _num(row.candidate, row.unit), delta, threshold, status
  ]


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
  base, cand = report.baseline, report.candidate
  extra_flags = " ".join(report.run_info.get("extra_flags", [])) or "(none)"
  return [
      "# Preprocessor regression report",
      *_heading(f"Verdict: {verdict.label}"),
      f"- **Output:** {output}",
      f"- **Performance:** {perf}",
      f"- **Exit code:** {verdict.exit_code}",
      "",
      *_table(_SIDES_HEADER, [
          ["Code", base.code, cand.code],
          ["Output", f"`{base.output_dir}`", f"`{cand.output_dir}`"],
          ["Log", f"`{base.log_path}`", f"`{cand.log_path}`"],
      ]),
      "",
      f"Input: `{report.run_info.get('input_dir')}`. Extra flags: "
      f"`{extra_flags}`.",
  ]


def _perf_section(report: Report) -> list[str]:
  perf = report.perf
  rows = [_perf_row(row) for row in perf.rows]
  first = [
      s.metrics[0] if s.metrics else None
      for s in (report.baseline, report.candidate)
  ]
  for label, field in _HEALTH_ROWS:
    values = [str(getattr(m, field)) if m else "None" for m in first]
    rows.append([label, *values, None, None, None])
  header = ["Metric", "Baseline", "Candidate", "Δ", "Threshold", None]
  title = f"Performance (median of {perf.repeats[0]} / {perf.repeats[1]} runs)"
  return _heading(title) + _table(header, rows)


def _category_status(name: str, count: int, failing: dict[str, int]) -> str:
  if not count:
    return "ok"
  return "**DIFF**" if name in failing else "info"


def _categories_section(report: Report) -> list[str]:
  counts = report.comparison.findings.counts
  rows = []
  for c in CATEGORIES:
    count = counts[c.name]
    status = _category_status(c.name, count, report.verdict.failing)
    rows.append([f"`{c.name}`", c.kind.value, f"{count:,}", status, c.meaning])
  header = ["Category", "Kind", "Count", "Status", "Meaning"]
  return _heading("Differences") + _table(header, rows)


def _differing(table: dict[str, list[int]]) -> dict[str, list[int]]:
  return {k: v for k, v in table.items() if v[0] != v[1]}


def _stats_section(report: Report) -> list[str]:
  stats = report.comparison.stats
  inv, reports = stats["inventory"], stats["reports"]
  obs, nodes = stats["observations"], stats["nodes"]
  obs_shards, node_shards = inv["observation"], inv["node"]
  rows = [
      _pair_row("Run directory (no timestamp)", inv["run_dir"]),
      _pair_row("Import folders", inv["folders"]),
      _pair_row("Shard bytes", inv["bytes"]),
      _pair_row("Observation shards",
                [obs_shards["baseline"], obs_shards["candidate"]]),
      [
          "... byte-identical (same name)", f"{obs_shards['byte_identical']:,}",
          f"of {obs_shards['same_name']:,}"
      ],
      _pair_row("Node shards",
                [node_shards["baseline"], node_shards["candidate"]]),
      [
          "... byte-identical (same folder)",
          f"{node_shards['byte_identical_in_same_folder']:,}", None
      ],
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
  return (_heading("Output summary") + _table(_SIDES_HEADER, rows) +
          _pair_table("Observations per folder (differing only)", "Folder",
                      _differing(obs["per_folder"])) +
          _pair_table("Node @ids per @type (differing only)", "@type",
                      _differing(nodes["types"])) +
          _list_table("Node property changes (top groups)",
                      nodes["property_changes"]) +
          _list_table("Node folder moves (top groups)", nodes["folder_moves"]))


def _pair_table(title: str, label: str, table: dict[str,
                                                    list[int]]) -> list[str]:
  """A table of up to _MAX_TABLE_ROWS (baseline, candidate) pairs."""
  if not table:
    return []
  items = list(table.items())
  rows = [_pair_row(f"`{k}`", v) for k, v in items[:_MAX_TABLE_ROWS]]
  if len(items) > _MAX_TABLE_ROWS:
    rows.append([f"... {len(items) - _MAX_TABLE_ROWS:,} more", None, None])
  return _heading(title, 3) + _table([label, "Baseline", "Candidate"], rows)


def _list_table(title: str, rows: list[dict[str, Any]]) -> list[str]:
  """A table of dicts, one column per key."""
  if not rows:
    return []
  columns = list(rows[0])
  cells = [[f"`{row[c]}`" for c in columns] for row in rows]
  return _heading(title, 3) + _table(columns, cells)


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
    title = f"`{c.name}` ({findings.counts[c.name]:,}, {c.kind.value})"
    lines += _heading(title, 3) + ["```json"]
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
