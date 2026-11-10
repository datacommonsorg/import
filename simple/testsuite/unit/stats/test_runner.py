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
"""Worker processes: pool sizing and what they write to GCS."""

import os

from google.cloud import storage
from google.cloud.storage import Blob
import pytest
from stats import runner
from testsuite.lib import builders as b


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


def _record_calls(monkeypatch, log_path, cls, method_name, detail) -> None:
  """Records calls to `cls.method_name` as "<pid> <detail>" lines in a file.

  `detail(obj)` says what to record about the called object, or None to skip
  the call. Workers are forked processes, so they report through a file.
  """
  real_method = getattr(cls, method_name)

  def recording_method(obj, *args, **kwargs):
    call_detail = detail(obj)
    if call_detail is not None:
      with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{os.getpid()} {call_detail}\n")
    return real_method(obj, *args, **kwargs)

  monkeypatch.setattr(cls, method_name, recording_method)


def _read_calls(log_path) -> list[tuple[bool, str]]:
  """Returns (made by a worker, detail) for each call recorded in `log_path`."""
  if not log_path.exists():
    return []
  calls = []
  for line in log_path.read_text(encoding="utf-8").splitlines():
    pid, call_detail = line.split(" ", 1)
    calls.append((int(pid) != os.getpid(), call_detail))
  return calls


def _import_two_csvs_in_one_worker() -> None:
  """Imports two CSVs from (fake) GCS, both in the same worker process."""
  result = b.run_dir(
      {
          "provenance.mcf": b.PROVENANCE_MCF,
          "case_a.csv": b.csv_file(b.obs_row(about="country/USA")),
          "case_b.csv": b.csv_file(b.obs_row(about="country/FRA")),
      },
      b.obs_config(b.OBS_MAPPINGS),
      mode="gcs_input",
      runner_kwargs={"max_workers": 1})
  assert result.output["error"] is None


def test_import_workers_write_no_gcs_directory_markers(tmp_path, monkeypatch):
  """Only the coordinator writes GCS directory markers, never the workers.

  Workers marking directories for every file rewrite the same marker objects
  until GCS rejects them with 429 errors. Here the worker would mark the
  folder of the shards it uploaded for the first CSV.
  """
  log_path = tmp_path / "marker_writes.txt"
  _record_calls(monkeypatch, log_path, Blob, "upload_from_string",
                lambda blob: blob.name if blob.name.endswith("/") else None)

  _import_two_csvs_in_one_worker()

  marker_writes = _read_calls(log_path)
  assert marker_writes, "the coordinator should still mark its directories"
  assert [name for by_worker, name in marker_writes if by_worker] == []


def test_import_workers_create_one_gcs_client_each(tmp_path, monkeypatch):
  """A worker reuses one GCS client for every file, store and upload."""
  log_path = tmp_path / "clients.txt"
  _record_calls(monkeypatch, log_path, storage.Client, "__init__",
                lambda client: "client")

  _import_two_csvs_in_one_worker()

  worker_clients = [c for by_worker, c in _read_calls(log_path) if by_worker]
  assert worker_clients == ["client"]
