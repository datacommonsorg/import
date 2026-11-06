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
"""Workers: how many processes import files, and the max_workers override."""

import concurrent.futures
import os
from unittest import mock

import pytest
from stats import runner
from testsuite.lib import builders as b

# Three CSVs plus provenance.mcf: four files, so four imports.
_FILES = {
    "provenance.mcf": b.PROVENANCE_MCF,
    **{
        f"case_{country}.csv":
            b.csv_file(b.obs_row(f"country/{country}", value=value)) for country, value in [("USA", "1"),
                                                                                           ("FRA", "2"),
                                                                                           ("DEU", "3")]
    },
}
_CONFIG = b.obs_config(b.OBS_MAPPINGS)
_EXPECTED = [
    b.obs("country/USA", value=1),
    b.obs("country/FRA", value=2),
    b.obs("country/DEU", value=3),
]

# --- Default -----------------------------------------------------------------


def test_default_is_the_cpus_this_process_may_run_on(monkeypatch):
  monkeypatch.setattr(os,
                      "sched_getaffinity",
                      lambda pid: {0, 3, 5},
                      raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: 64)
  assert runner.default_max_workers() == 3


def test_default_falls_back_to_cpu_count_without_affinity(monkeypatch):
  monkeypatch.delattr(os, "sched_getaffinity", raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: 6)
  assert runner.default_max_workers() == 6


def test_default_is_one_when_cpu_count_is_unknown(monkeypatch):
  monkeypatch.delattr(os, "sched_getaffinity", raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: None)
  assert runner.default_max_workers() == 1


# --- Pool size ---------------------------------------------------------------


def _pool_sizes(runner_kwargs=None, *, cpus=8):
  """
  Runs _FILES on `cpus` available CPUs.

  Returns the result and the max_workers of every process pool created.
  """
  spy = mock.MagicMock(wraps=concurrent.futures.ProcessPoolExecutor)
  with mock.patch.object(concurrent.futures, "ProcessPoolExecutor", spy), \
       mock.patch.object(runner, "default_max_workers", return_value=cpus):
    result = b.run_dir(_FILES, _CONFIG, runner_kwargs=runner_kwargs)
  return result, [c.kwargs["max_workers"] for c in spy.call_args_list]


def test_pool_defaults_to_available_cpus():
  result, sizes = _pool_sizes(cpus=2)
  assert result.error is None, result.error
  b.assert_records(result.observations, _EXPECTED)
  assert sizes == [2]


def test_max_workers_overrides_the_default():
  result, sizes = _pool_sizes(runner_kwargs={"max_workers": 1}, cpus=8)
  assert result.error is None, result.error
  b.assert_records(result.observations, _EXPECTED)
  assert sizes == [1]


def test_pool_is_never_larger_than_the_number_of_files():
  result, sizes = _pool_sizes(runner_kwargs={"max_workers": 100}, cpus=100)
  assert result.error is None, result.error
  assert sizes == [len(_FILES)]


@pytest.mark.parametrize("max_workers", [0, -1])
def test_max_workers_must_be_positive(max_workers):
  with pytest.raises(ValueError, match="max_workers must be at least 1"):
    runner.Runner(config_file_path=None,
                  input_dir_path="unused",
                  output_dir_path="unused",
                  max_workers=max_workers)
