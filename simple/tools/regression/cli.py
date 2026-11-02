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
"""Regression check of the preprocessor: baseline ref vs. candidate code.

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

from tools.regression import report as report_lib
from tools.regression import trees
from tools.regression.compare import compare_outputs
from tools.regression.compare import SideOutput
from tools.regression.inputs import GcsInput
from tools.regression.inputs import resolve_input_dir
from tools.regression.perf import compare_health
from tools.regression.perf import compare_perf
from tools.regression.perf import Thresholds
from tools.regression.run import load_side
from tools.regression.run import run_sides
from tools.regression.run import RunSpec
from tools.regression.run import SideRuns

BASELINE = "baseline"
CANDIDATE = "candidate"
_SECONDS_PER_MINUTE = 60
_EXTRA_FLAGS_SEPARATOR = "--"


def _parser() -> argparse.ArgumentParser:
  p = argparse.ArgumentParser(
      prog="python3 -m tools.regression",
      description=__doc__,
      formatter_class=argparse.RawDescriptionHelpFormatter)
  p.add_argument("--input_dir",
                 help="Input directory, local or gs://. Omit for runs driven "
                 "by --config_file alone (pass that after --).")
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
                 choices=[m.value for m in GcsInput],
                 default=GcsInput.STAGE.value,
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
  p.add_argument("--max_wall_regression_pct", type=float, default=10)
  p.add_argument("--max_cpu_regression_pct", type=float, default=10)
  p.add_argument("--max_rss_regression_pct", type=float, default=10)
  p.add_argument("--perf_min_seconds",
                 type=float,
                 default=30,
                 help="Do not judge perf when the baseline runs shorter than "
                 "this; short runs are noise.")
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


def _run_or_reuse(args: argparse.Namespace, spec: RunSpec,
                  work_dir: str) -> dict[str, SideRuns]:
  """Runs the sides that are not reused and loads the ones that are."""
  reused = {BASELINE: args.baseline_output, CANDIDATE: args.candidate_output}
  refs = {BASELINE: args.baseline, CANDIDATE: args.candidate}
  to_run = [side for side in (BASELINE, CANDIDATE) if not reused[side]]
  repo = trees.repo_root()
  ran: dict[str, SideRuns] = {}
  if to_run:
    checkout_refs = [refs[side] for side in to_run if refs[side]]
    with tempfile.TemporaryDirectory(dir=work_dir, prefix="trees-") as tmp, \
         trees.checkouts(repo, checkout_refs, tmp) as checked_out:
      code = {
          side:
              checked_out[refs[side]]
              if refs[side] else trees.working_tree(repo) for side in to_run
      }
      ran = run_sides(code, spec, work_dir, args.repeats)
  return {
      side:
          ran[side] if side in ran else load_side(reused[side], spec.input_dir)
      for side in (BASELINE, CANDIDATE)
  }


def _write_reports(report: report_lib.Report, work_dir: str) -> str:
  markdown_path = os.path.join(work_dir, "report.md")
  with open(markdown_path, "w") as f:
    f.write(report_lib.to_markdown(report))
  with open(os.path.join(work_dir, "report.json"), "w") as f:
    json.dump(report_lib.to_json(report), f, indent=1, default=str)
  return markdown_path


def _cleanup(args: argparse.Namespace, sides: dict[str, SideRuns],
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


def main(argv: list[str]) -> int:
  args, extra_flags = _parse_args(argv)
  logging.basicConfig(level=logging.INFO, format="[regression] %(message)s")
  work_dir = _make_work_dir(args.work_dir)
  stage_dir = os.path.abspath(args.stage_dir or os.path.join(work_dir, "input"))
  input_dir = resolve_input_dir(args.input_dir, GcsInput(args.gcs_input),
                                stage_dir)
  spec = RunSpec(python=args.python,
                 input_dir=input_dir,
                 extra_flags=tuple(extra_flags),
                 timeout_seconds=args.timeout_minutes * _SECONDS_PER_MINUTE or
                 None)
  sides = _run_or_reuse(args, spec, work_dir)
  base, cand = sides[BASELINE], sides[CANDIDATE]
  logging.info("Comparing %s with %s", base.output_dir, cand.output_dir)
  comparison = compare_outputs(SideOutput(base.output_dir, base.input_root),
                               SideOutput(cand.output_dir, cand.input_root),
                               scratch_dir=work_dir,
                               workers=args.compare_workers,
                               max_samples=args.samples)
  compare_health(base, cand, comparison.findings)
  perf = compare_perf(
      base, cand,
      Thresholds(wall_pct=args.max_wall_regression_pct,
                 cpu_pct=args.max_cpu_regression_pct,
                 rss_pct=args.max_rss_regression_pct,
                 min_seconds=args.perf_min_seconds))
  verdict = report_lib.Verdict(comparison.findings.failing(args.strict),
                               perf.regressions)
  report = report_lib.Report(base,
                             cand,
                             run_info={
                                 "input_dir": args.input_dir,
                                 "resolved_input_dir": input_dir,
                                 "extra_flags": extra_flags,
                                 "python": args.python,
                                 "repeats": args.repeats,
                                 "strict": args.strict,
                                 "work_dir": work_dir,
                             },
                             comparison=comparison,
                             perf=perf,
                             verdict=verdict)
  markdown_path = _write_reports(report, work_dir)
  _cleanup(args, sides, work_dir, stage_dir)
  print(report_lib.summary_line(report))
  print(f"Report: {markdown_path} (and report.json)")
  return verdict.exit_code
