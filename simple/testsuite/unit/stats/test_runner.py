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


def test_import_workers_write_no_gcs_directory_markers(tmp_path, monkeypatch):
  """Only the coordinator writes GCS directory markers, never the workers.

  Workers marking directories for every file rewrite the same marker objects
  until GCS rejects them with 429 errors. One worker imports both CSVs here,
  so it would mark the folder of the shards it uploaded for the first one.
  """
  marker_writes = tmp_path / "marker_writes.txt"
  real_upload_from_string = Blob.upload_from_string

  def recording_upload_from_string(blob, data, *args, **kwargs):
    # Workers are forked processes, so they report through a file.
    if blob.name.endswith("/"):
      with open(marker_writes, "a", encoding="utf-8") as f:
        f.write(f"{os.getpid()} {blob.name}\n")
    return real_upload_from_string(blob, data, *args, **kwargs)

  monkeypatch.setattr(Blob, "upload_from_string", recording_upload_from_string)
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
  writers = [line.split() for line in marker_writes.read_text().splitlines()]
  assert writers, "the coordinator should still mark its directories"
  assert [name for pid, name in writers if int(pid) != os.getpid()] == []
