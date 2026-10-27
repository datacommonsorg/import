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
"""Shared pytest setup for the preprocessor test suite (see README.md)."""

import logging
import os

from hypothesis import HealthCheck
from hypothesis import settings
import pytest

# Profiles for the generated (fuzz) tests. Select with HYPOTHESIS_PROFILE.
#   ci       default: a fixed, reproducible budget for presubmit
#   nightly  much deeper exploration
#   dev      quick local iteration
_COMMON = dict(deadline=None,
               suppress_health_check=[HealthCheck.too_slow],
               derandomize=False,
               print_blob=True)
settings.register_profile(
    "ci",
    max_examples=25,
    derandomize=True,
    **{
        k: v for k, v in _COMMON.items() if k != "derandomize"
    })
settings.register_profile("nightly", max_examples=1000, **_COMMON)
settings.register_profile("dev", max_examples=10, **_COMMON)
settings.load_profile(os.getenv("HYPOTHESIS_PROFILE", "ci"))


def pytest_addoption(parser):
  group = parser.getgroup("testsuite")
  group.addoption(
      "--update-expected",
      action="store_true",
      default=False,
      help="Rewrite expect blocks in cases/*.md and e2e expected outputs from"
      " the actual output. Review the diff before committing.")
  group.addoption(
      "--modes",
      default="",
      help="Comma-separated execution modes for e2e tests (default: all).")
  group.addoption(
      "--baseline",
      default="",
      help="Git ref to compare against in test_baseline.py, e.g. origin/master."
  )


def pytest_configure(config):
  # Expectation files are rewritten at the end of each test module; parallel
  # workers would race on them.
  workers = getattr(config.option, "numprocesses", None)
  if config.getoption("--update-expected") and workers:
    raise pytest.UsageError("--update-expected cannot run with pytest-xdist;"
                            " drop -n")


@pytest.fixture(scope="session")
def update_expected(request) -> bool:
  return request.config.getoption("--update-expected")


@pytest.fixture(autouse=True)
def _quiet_logs():
  # The preprocessor logs heavily at INFO; keep test output readable.
  previous = logging.root.manager.disable
  logging.disable(logging.INFO)
  yield
  logging.disable(previous)
