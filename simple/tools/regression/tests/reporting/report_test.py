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
"""The verdict and its exit code."""

from tools.regression.comparison.metrics import RunFailure
from tools.regression.reporting.report import EXIT_OK
from tools.regression.reporting.report import EXIT_OUTPUT_DIFFERENT
from tools.regression.reporting.report import EXIT_PERF_REGRESSION
from tools.regression.reporting.report import EXIT_RUN_FAILED
from tools.regression.reporting.report import Verdict


def test_verdict_exit_codes():
  assert Verdict({}, [], []).exit_code == EXIT_OK
  assert Verdict({}, ["Wall time"], []).exit_code == EXIT_PERF_REGRESSION
  assert Verdict({
      "obs_value_changed": 1
  }, ["Wall time"], []).exit_code == EXIT_OUTPUT_DIFFERENT


CRASH = RunFailure("candidate", 1, 1, False, "/log")


def test_run_failure_takes_precedence():
  verdict = Verdict({"exit_status_changed": 1}, ["Wall time"], [CRASH])
  assert verdict.label == "RUN FAILED"
  assert verdict.output_label == "DIFFERENT"
  assert verdict.exit_code == EXIT_RUN_FAILED


def test_run_failure_with_identical_output_is_not_ok():
  verdict = Verdict({}, [], [CRASH])
  assert verdict.label == "RUN FAILED"
  assert verdict.output_label == "IDENTICAL"
  assert verdict.exit_code == EXIT_RUN_FAILED


def test_no_run_failure_keeps_the_output_verdict():
  assert Verdict({}, [], []).label == "IDENTICAL"
  assert Verdict({"obs_value_changed": 1}, [], []).label == "DIFFERENT"
