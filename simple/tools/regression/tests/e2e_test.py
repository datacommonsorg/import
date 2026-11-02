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
"""End to end: the tool on a testsuite dataset, HEAD against HEAD."""

import json
import os
import subprocess

import pytest
from tools.regression import cli
from tools.regression.execution import trees
from tools.regression.reporting.report import EXIT_OK

DATASET = os.path.join(os.path.dirname(__file__), "..", "..", "..", "testsuite",
                       "e2e", "kitchen_sink", "input")


def _repo_or_skip() -> str:
  try:
    return trees.repo_root()
  except (subprocess.CalledProcessError, FileNotFoundError):
    pytest.skip("not in a git checkout")


def _report(work_dir) -> dict:
  with open(os.path.join(work_dir, "report.json")) as f:
    return json.load(f)


def test_head_against_head_is_identical(tmp_path):
  repo = _repo_or_skip()
  work_dir = tmp_path / "regression"
  exit_code = cli.main([
      f"--input_dir={DATASET}", "--baseline=HEAD", "--candidate=HEAD",
      f"--work_dir={work_dir}", "--keep_outputs"
  ])
  report = _report(work_dir)
  assert exit_code == EXIT_OK
  assert report["verdict"]["output"] == "IDENTICAL"
  assert report["stats"]["observations"]["observations"][0] > 0
  assert [len(report[s]["runs"]) for s in ("baseline", "candidate")] == [1, 1]
  assert (work_dir / "report.md").exists()
  worktrees = subprocess.run(["git", "-C", repo, "worktree", "list"],
                             check=True,
                             capture_output=True,
                             text=True).stdout
  assert str(work_dir) not in worktrees

  # Reusing both outputs compares them again without running anything, and
  # keeps the metrics of the first invocation.
  rerun_dir = tmp_path / "rerun"
  exit_code = cli.main([
      f"--baseline_output={work_dir}/baseline/run-1/output",
      f"--candidate_output={work_dir}/candidate/run-1/output",
      f"--work_dir={rerun_dir}"
  ])
  rerun = _report(rerun_dir)
  assert exit_code == EXIT_OK
  assert rerun["stats"] == report["stats"]
  assert rerun["baseline"]["runs"] == report["baseline"]["runs"]
