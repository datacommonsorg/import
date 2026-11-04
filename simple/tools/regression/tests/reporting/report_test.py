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

from tools.regression.reporting.report import EXIT_OK
from tools.regression.reporting.report import EXIT_OUTPUT_DIFFERENT
from tools.regression.reporting.report import EXIT_PERF_REGRESSION
from tools.regression.reporting.report import Verdict


def test_verdict_exit_codes():
  assert Verdict({}, []).exit_code == EXIT_OK
  assert Verdict({}, ["Wall time"]).exit_code == EXIT_PERF_REGRESSION
  assert Verdict({
      "obs_value_changed": 1
  }, ["Wall time"]).exit_code == EXIT_OUTPUT_DIFFERENT
