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
Regression check of the preprocessor: baseline ref vs. candidate code.

Runs stats.main of both sides on the same input with the same interpreter and
flags, compares every output file and the run metrics, and writes report.md
and report.json. See README.md.

  cd simple
  python3 -m tools.regression --input_dir=/path/to/input [--baseline=upstream/master]
      [-- <extra stats.main flags>]

Exit codes: 0 identical and no perf regression, 3 output differs, 4 perf
regression only, 1 tool error, 2 usage error.
"""

import argparse
import json
import logging
import os
import shutil
import sys
import tempfile

from tools.regression.comparison import compare
from tools.regression.execution import inputs
from tools.regression.execution import run
from tools.regression.execution import trees
from tools.regression.reporting import markdown
from tools.regression.reporting import perf
from tools.regression.reporting import report as report_lib

BASELINE = "baseline"
CANDIDATE = "candidate"
_SECONDS_PER_MINUTE = 60
_EXTRA_FLAGS_SEPARATOR = "--"


def _add_input_args(p: argparse.ArgumentParser) -> None:
  """Adds the input directory flag."""
  p.add_argument("--input_dir",
                 help="Input directory, local or gs://. Omit for runs driven "
                 "by --config_file alone (pass that after --).")


def _add_code_args(p: argparse.ArgumentParser) -> None:
  """Adds the flags that pick the code, or the reused output, of each side."""
  p.add_argument("--baseline",
                 default="upstream/master",
                 help="Git ref of the baseline code (default: %(default)s).")
  p.add_argument("--candidate",
                 help="Git ref of the candidate code (default: the working "
                 "tree, uncommitted changes included).")
  p.add_argument("--baseline_output",
                 help="Reuse this baseline output dir instead of running the "
                 "baseline, e.g. <work_dir>/baseline/run-1/output of an "
                 "earlier --keep_outputs run (its metrics are reused too).")
  p.add_argument("--candidate_output",
                 help="Reuse this candidate output dir instead of running it.")


def _add_run_args(p: argparse.ArgumentParser) -> None:
  """Adds the flags for running the sides: repeats, dirs, input staging."""
  p.add_argument("--repeats",
                 type=int,
                 default=1,
                 help="Runs per side; perf metrics are medians.")
  p.add_argument("--work_dir",
                 help="New or empty directory for outputs, logs and reports "
                 "(default: a new temporary directory). Use a real disk for "
                 "large datasets.")
  p.add_argument("--keep_outputs",
                 action="store_true",
                 help="Keep the run outputs (and staged input) after the "
                 "comparison.")
  p.add_argument("--gcs_input",
                 choices=[m.value for m in inputs.GcsInput],
                 default=inputs.GcsInput.STAGE.value,
                 help="For a gs:// --input_dir: copy it locally once (stage) "
                 "or let both sides read GCS (direct).")
  p.add_argument("--stage_dir",
                 help="Where to stage a gs:// input (default: "
                 "<work_dir>/input). Reusing a directory only syncs changes.")
  p.add_argument("--python",
                 default=sys.executable,
                 help="Interpreter both sides run with (default: this one).")
  p.add_argument("--timeout_minutes",
                 type=float,
                 default=0,
                 help="Kill a run after this long (0: no timeout).")


def _add_comparison_args(p: argparse.ArgumentParser) -> None:
  """Adds the flags that control the output comparison."""
  p.add_argument("--compare_workers",
                 type=int,
                 help="Processes that parse shards (default: one per CPU).")
  p.add_argument("--samples",
                 type=int,
                 default=5,
                 help="Samples per difference category.")
  p.add_argument("--strict",
                 action="store_true",
                 help="Fail on informational differences too (layout, bytes, "
                 "warnings).")


def _add_threshold_args(p: argparse.ArgumentParser) -> None:
  """Adds the perf regression thresholds."""
  p.add_argument("--max_wall_regression_pct", type=float, default=10)
  p.add_argument("--max_cpu_regression_pct", type=float, default=10)
  p.add_argument("--max_rss_regression_pct", type=float, default=10)
  p.add_argument("--perf_min_seconds",
                 type=float,
                 default=30,
                 help="Do not judge perf when the baseline runs shorter than "
                 "this; short runs are noise.")


def _parser() -> argparse.ArgumentParser:
  p = argparse.ArgumentParser(
      prog="python3 -m tools.regression",
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  _add_input_args(p)
  _add_code_args(p)
  _add_run_args(p)
  _add_comparison_args(p)
  _add_threshold_args(p)
  return p


def _parse_args(argv: list[str]) -> tuple[argparse.Namespace, list[str]]:
  """Splits argv at "--" into tool arguments and extra stats.main flags."""
  extra: list[str] = []
  if _EXTRA_FLAGS_SEPARATOR in argv:
    split = argv.index(_EXTRA_FLAGS_SEPARATOR)
    argv, extra = argv[:split], argv[split + 1:]
  parser = _parser()
  args = parser.parse_args(argv)
  if args.repeats < 1:
    parser.error("--repeats must be at least 1")
  return args, extra


def _make_work_dir(work_dir: str | None) -> str:
  if not work_dir:
    return tempfile.mkdtemp(prefix="preprocessor-regression-")
  work_dir = os.path.abspath(work_dir)
  if os.path.isdir(work_dir) and os.listdir(work_dir):
    raise ValueError(f"--work_dir {work_dir} is not empty")
  os.makedirs(work_dir, exist_ok=True)
  return work_dir


def _run_or_reuse(args: argparse.Namespace, spec: run.RunSpec,
                  work_dir: str) -> dict[str, run.SideRuns]:
  """Runs the sides that are not reused and loads the ones that are."""
  reused = {BASELINE: args.baseline_output, CANDIDATE: args.candidate_output}
  refs = {BASELINE: args.baseline, CANDIDATE: args.candidate}
  to_run = [side for side in (BASELINE, CANDIDATE) if not reused[side]]
  repo = trees.repo_root()
  ran: dict[str, run.SideRuns] = {}
  if to_run:
    checkout_refs = [refs[side] for side in to_run if refs[side]]
    with tempfile.TemporaryDirectory(dir=work_dir, prefix="trees-") as tmp, \
         trees.checkouts(repo, checkout_refs, tmp) as checked_out:
      code = {
          side:
              checked_out[refs[side]]
              if refs[side] else trees.working_tree(repo) for side in to_run
      }
      ran = run.run_sides(code, spec, work_dir, args.repeats)
  return {
      side: ran.get(side) or run.load_side(reused[side], spec.input_dir)
      for side in (BASELINE, CANDIDATE)
  }


def _write_reports(report: report_lib.Report, work_dir: str) -> str:
  markdown_path = os.path.join(work_dir, "report.md")
  with open(markdown_path, "w") as f:
    f.write(markdown.to_markdown(report))
  with open(os.path.join(work_dir, "report.json"), "w") as f:
    json.dump(report_lib.to_json(report), f, indent=1, default=str)
  return markdown_path


def _cleanup(args: argparse.Namespace, sides: dict[str, run.SideRuns],
             work_dir: str, stage_dir: str) -> None:
  """Deletes the outputs this invocation wrote, and its staged input."""
  if args.keep_outputs:
    return
  for side in sides.values():
    # Reused outputs live outside the (new) work dir and are never deleted.
    if side.output_dir.startswith(work_dir + os.sep):
      shutil.rmtree(side.output_dir, ignore_errors=True)
  if not args.stage_dir:
    shutil.rmtree(stage_dir, ignore_errors=True)


def _thresholds(args: argparse.Namespace) -> perf.Thresholds:
  return perf.Thresholds(wall_pct=args.max_wall_regression_pct,
                         cpu_pct=args.max_cpu_regression_pct,
                         rss_pct=args.max_rss_regression_pct,
                         min_seconds=args.perf_min_seconds)


def main(argv: list[str]) -> int:
  args, extra_flags = _parse_args(argv)
  logging.basicConfig(level=logging.INFO, format="[regression] %(message)s")
  work_dir = _make_work_dir(args.work_dir)
  stage_dir = os.path.abspath(args.stage_dir or os.path.join(work_dir, "input"))
  gcs_input = inputs.GcsInput(args.gcs_input)
  input_dir = inputs.resolve_input_dir(args.input_dir, gcs_input, stage_dir)
  timeout_seconds = args.timeout_minutes * _SECONDS_PER_MINUTE or None
  spec = run.RunSpec(args.python, input_dir, tuple(extra_flags),
                     timeout_seconds)
  sides = _run_or_reuse(args, spec, work_dir)
  base, cand = sides[BASELINE], sides[CANDIDATE]
  logging.info("Comparing %s with %s", base.output_dir, cand.output_dir)
  comparison = compare.compare_outputs(
      compare.SideOutput(base.output_dir, base.input_root),
      compare.SideOutput(cand.output_dir, cand.input_root),
      scratch_dir=work_dir,
      workers=args.compare_workers,
      max_samples=args.samples)
  perf.compare_health(base, cand, comparison.findings)
  perf_result = perf.compare_perf(base, cand, _thresholds(args))
  verdict = report_lib.Verdict(comparison.findings.failing(args.strict),
                               perf_result.regressions)
  run_info = {
      "input_dir": args.input_dir,
      "resolved_input_dir": input_dir,
      "extra_flags": extra_flags,
      "python": args.python,
      "repeats": args.repeats,
      "strict": args.strict,
      "work_dir": work_dir,
  }
  report = report_lib.Report(base, cand, run_info, comparison, perf_result,
                             verdict)
  markdown_path = _write_reports(report, work_dir)
  _cleanup(args, sides, work_dir, stage_dir)
  print(report_lib.summary_line(report))
  print(f"Report: {markdown_path} (and report.json)")
  return verdict.exit_code
