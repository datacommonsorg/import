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
"""Worker pool sizing: the CPU-based default and the max_workers override."""

import os

import pytest
from stats import runner


def test_default_max_workers_uses_cpu_affinity(monkeypatch):
  monkeypatch.setattr(os,
                      "sched_getaffinity",
                      lambda pid: {0, 3, 5},
                      raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: 64)
  assert runner.default_max_workers() == 3


def test_default_max_workers_falls_back_to_cpu_count(monkeypatch):
  monkeypatch.delattr(os, "sched_getaffinity", raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: 6)
  assert runner.default_max_workers() == 6


def test_num_import_workers_never_exceeds_the_files():
  assert runner.num_import_workers(max_workers=8, num_files=3) == 3
  assert runner.num_import_workers(max_workers=2, num_files=3) == 2


@pytest.mark.parametrize("max_workers", [0, -1])
def test_max_workers_must_be_positive(max_workers):
  with pytest.raises(ValueError, match="max_workers must be at least 1"):
    runner.Runner(config_file_path=None,
                  input_dir_path="unused",
                  output_dir_path="unused",
                  max_workers=max_workers)
