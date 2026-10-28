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

import pytest


def pytest_addoption(parser):
  group = parser.getgroup("testsuite")
  group.addoption(
      "--update-expected",
      action="store_true",
      default=False,
      help="Rewrite the e2e expected.json files from the actual output."
      " Review the diff before committing.")
  group.addoption(
      "--modes",
      default="",
      help="Comma-separated execution modes for e2e tests (default: all).")


def pytest_configure(config):
  # Parallel workers would race on rewriting the expected.json files.
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
