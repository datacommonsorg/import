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
"""Runs the preprocessor of a code tree, measured, possibly several times.

Each run gets its own directory:

  <side>/run-<n>/output/            the --output_dir of the run
  <side>/run-<n>/preprocessor.log   stdout and stderr
  <side>/run-<n>/run.json           code, command and metrics

Only the output of run 1 is kept and compared; repeats exist for timing.
"""

import dataclasses
import glob
import json
import logging
import os
import shutil

from tools.ab.measure import run_measured
from tools.ab.measure import RunMetrics
from tools.ab.trees import CodeTree

RUN_JSON = "run.json"
_OUTPUT = "output"
_LOG = "preprocessor.log"
# Inherited variables that would make a run write outside its output dir
# (the ingestion workflow handshake) or import the wrong tree's code.
_SCRUBBED_ENV = ("PYTHONPATH", "WORKFLOW_EXECUTION_ID", "TEMP_LOCATION")


@dataclasses.dataclass(frozen=True)
class RunSpec:
  """What both sides run: the same interpreter, input and flags."""
  python: str
  input_dir: str | None
  extra_flags: tuple[str, ...]
  timeout_seconds: float | None


@dataclasses.dataclass(frozen=True)
class SideRuns:
  """The runs of one side. Metrics are empty for a reused raw output."""
  code: str  # Human-readable description of the code that ran.
  output_dir: str
  input_root: str | None
  metrics: tuple[RunMetrics, ...]
  log_path: str | None


def preprocessor_command(spec: RunSpec, output_dir: str) -> list[str]:
  command = [spec.python, "-m", "stats.main", f"--output_dir={output_dir}"]
  if spec.input_dir:
    command.append(f"--input_dir={spec.input_dir}")
  return command + list(spec.extra_flags)


def _environment(tmp_dir: str) -> dict[str, str]:
  env = {k: v for k, v in os.environ.items() if k not in _SCRUBBED_ENV}
  # The preprocessor stages shards under TMPDIR; keep them next to the run
  # rather than in /tmp, which is often a RAM-backed tmpfs.
  env["TMPDIR"] = tmp_dir
  return env


def run_once(tree: CodeTree, spec: RunSpec, run_dir: str) -> RunMetrics:
  """Runs the preprocessor of tree once into run_dir and writes run.json."""
  output_dir = os.path.join(run_dir, _OUTPUT)
  tmp_dir = os.path.join(run_dir, "tmp")
  os.makedirs(tmp_dir)
  command = preprocessor_command(spec, output_dir)
  try:
    metrics = run_measured(command,
                           cwd=tree.simple_dir,
                           env=_environment(tmp_dir),
                           log_path=os.path.join(run_dir, _LOG),
                           timeout_seconds=spec.timeout_seconds)
  finally:
    shutil.rmtree(tmp_dir, ignore_errors=True)
  with open(os.path.join(run_dir, RUN_JSON), "w") as f:
    json.dump(
        {
            "code": tree.describe(),
            "tree": tree.to_json(),
            "command": command,
            "input_dir": spec.input_dir,
            "metrics": dataclasses.asdict(metrics),
        },
        f,
        indent=2)
  return metrics


def run_sides(trees: dict[str, CodeTree], spec: RunSpec, work_dir: str,
              repeats: int) -> dict[str, SideRuns]:
  """Runs every side `repeats` times, interleaving sides to spread drift."""
  metrics: dict[str, list[RunMetrics]] = {side: [] for side in trees}
  for repeat in range(1, repeats + 1):
    for side, tree in trees.items():
      run_dir = os.path.join(work_dir, side, f"run-{repeat}")
      logging.info("%s run %d/%d: %s", side, repeat, repeats, tree.describe())
      metrics[side].append(run_once(tree, spec, run_dir))
      if repeat > 1:
        shutil.rmtree(os.path.join(run_dir, _OUTPUT), ignore_errors=True)
  first = {side: os.path.join(work_dir, side, "run-1") for side in trees}
  return {
      side:
          SideRuns(code=tree.describe(),
                   output_dir=os.path.join(first[side], _OUTPUT),
                   input_root=spec.input_dir,
                   metrics=tuple(metrics[side]),
                   log_path=os.path.join(first[side], _LOG))
      for side, tree in trees.items()
  }


def _run_number(run_json_path: str) -> int:
  """The N of <side>/run-N/run.json, so that run-10 sorts after run-9."""
  run_dir_name = os.path.basename(os.path.dirname(run_json_path))
  return int(run_dir_name.removeprefix("run-"))


def load_side(output_dir: str, default_input_root: str | None) -> SideRuns:
  """Describes an existing output, reusing its run.json files if it has any.

  For an output written by this tool (<side>/run-1/output), the run.json of
  every run of that side provides the code description, input root and
  metrics. Any other output directory is compared as is, without metrics.
  """
  output_dir = os.path.abspath(output_dir)
  if not os.path.isdir(output_dir):
    raise FileNotFoundError(f"Output directory {output_dir} does not exist")
  run_dir = os.path.dirname(output_dir)
  if not os.path.exists(os.path.join(run_dir, RUN_JSON)):
    return SideRuns(f"existing output {output_dir}", output_dir,
                    default_input_root, (), None)
  side_dir = os.path.dirname(run_dir)
  runs = []
  for path in sorted(glob.glob(os.path.join(side_dir, "run-*", RUN_JSON)),
                     key=_run_number):
    with open(path) as f:
      runs.append(json.load(f))
  return SideRuns(f"{runs[0]['code']} (reused {output_dir})", output_dir,
                  runs[0]["input_dir"],
                  tuple(RunMetrics(**run["metrics"]) for run in runs),
                  os.path.join(run_dir, _LOG))
