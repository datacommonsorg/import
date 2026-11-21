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
"""Worker processes: pool sizing, what they write to GCS, and interrupts."""

import concurrent.futures
import json
import logging
import os
import shutil
import time
import types

from google.cloud import storage
from google.cloud.storage import Blob
import pytest
from stats import constants
from stats import runner
from stats.report.summary_generator import SummaryGenerator
from testsuite.lib import builders as b
from testsuite.lib import fake_gcs
from testsuite.lib import run as runlib
from util.filesystem import open_dir


def _set_cpus(monkeypatch, affinity: set[int] | None, cpu_count: int) -> None:
  """Fakes the CPUs available to this process (affinity None: unsupported)."""
  if affinity is None:
    monkeypatch.delattr(os, "sched_getaffinity", raising=False)
  else:
    monkeypatch.setattr(os,
                        "sched_getaffinity",
                        lambda pid: affinity,
                        raising=False)
  monkeypatch.setattr(os, "cpu_count", lambda: cpu_count)


def test_default_max_workers_uses_cpu_affinity(monkeypatch):
  _set_cpus(monkeypatch, affinity={0, 3, 5}, cpu_count=64)
  assert runner.default_max_workers(uses_gcs=False) == 3


def test_default_max_workers_falls_back_to_cpu_count(monkeypatch):
  _set_cpus(monkeypatch, affinity=None, cpu_count=6)
  assert runner.default_max_workers(uses_gcs=False) == 6


@pytest.mark.parametrize("cpus, expected", [(1, 2), (6, 12), (16, 32), (24, 32),
                                            (64, 64)])
def test_default_max_workers_on_gcs_is_two_per_cpu_up_to_32_never_below_cpus(
    monkeypatch, cpus, expected):
  _set_cpus(monkeypatch, affinity=set(range(cpus)), cpu_count=128)
  assert runner.default_max_workers(uses_gcs=True) == expected


@pytest.mark.parametrize("input_on_gcs, output_on_gcs, expected",
                         [(False, False, 3), (False, True, 6),
                          (True, False, 6)])
def test_runner_defaults_workers_by_where_it_reads_and_writes(
    tmp_path, monkeypatch, input_on_gcs, output_on_gcs, expected):
  _set_cpus(monkeypatch, affinity={0, 1, 2}, cpu_count=64)
  for name, value in fake_gcs.env().items():
    monkeypatch.setenv(name, value)
  local_input_dir = runlib.write_input_dir({}, b.obs_config(b.OBS_MAPPINGS),
                                           b.IMPORT)
  input_dir = local_input_dir
  if input_on_gcs:
    input_dir = fake_gcs.new_prefix()
    fake_gcs.upload_dir(local_input_dir, input_dir)
  output_dir = (fake_gcs.new_prefix() if output_on_gcs else str(tmp_path))
  try:
    r = runner.Runner(input_dir_path=input_dir,
                      output_dir_path=output_dir,
                      import_names=[b.IMPORT])
    r._load_config()  # Opens the input dir.
    assert r.max_workers == expected
  finally:
    shutil.rmtree(local_input_dir, ignore_errors=True)


def test_num_import_workers_never_exceeds_the_tasks():
  assert runner.num_import_workers(max_workers=8, num_tasks=3) == 3
  assert runner.num_import_workers(max_workers=2, num_tasks=3) == 2


@pytest.mark.parametrize("max_workers", [0, -1])
def test_max_workers_must_be_positive(max_workers):
  with pytest.raises(ValueError, match="max_workers must be at least 1"):
    runner.Runner(input_dir_path="unused",
                  output_dir_path="unused",
                  max_workers=max_workers)


@pytest.mark.parametrize("input_dir, output_dir, missing",
                         [("", "unused", "input_dir"),
                          ("unused", "", "output_dir")])
def test_input_and_output_dirs_are_required(input_dir, output_dir, missing):
  with pytest.raises(ValueError, match=f"{missing} must be specified"):
    runner.Runner(input_dir_path=input_dir, output_dir_path=output_dir)


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


def test_a_run_writes_no_gcs_directory_markers(tmp_path, monkeypatch):
  """No process writes "dir/" marker objects: GCS has no directories.

  Workers marking directories for every file used to rewrite the same marker
  objects until GCS rejected them with 429 errors.
  """
  log_path = tmp_path / "marker_writes.txt"
  for method_name in ("upload_from_string", "upload_from_file"):
    _record_calls(monkeypatch, log_path, Blob, method_name,
                  lambda blob: blob.name if blob.name.endswith("/") else None)
  prefixes = _record_new_gcs_prefixes(monkeypatch)

  _import_two_csvs_in_one_worker()

  assert _read_calls(log_path) == []
  [root] = prefixes
  names = [blob.name for blob in _list_blobs(root)]
  assert any(name.endswith(".jsonld") for name in names), names
  assert [name for name in names if name.endswith("/")] == []


def test_the_handshake_and_report_are_written_as_json(monkeypatch):
  """Cloud Workflows parses the handshake it reads only if it is JSON."""
  prefixes = _record_new_gcs_prefixes(monkeypatch)

  _import_two_csvs_in_one_worker()

  [root] = prefixes
  content_types = {
      blob.name.rsplit("/", 1)[1]: blob.content_type
      for blob in _list_blobs(root)
      if blob.name.endswith(".json") and "/input/" not in blob.name
  }
  assert content_types == {
      f"{runlib.WORKFLOW_ID}.json": "application/json",
      "ingestion_report.json": "application/json",
  }


def _record_new_gcs_prefixes(monkeypatch) -> list[str]:
  """Records the gs:// prefix of each run that the test harness starts."""
  prefixes = []
  real_new_prefix = fake_gcs.new_prefix

  def recording_new_prefix() -> str:
    prefixes.append(real_new_prefix())
    return prefixes[-1]

  monkeypatch.setattr(fake_gcs, "new_prefix", recording_new_prefix)
  return prefixes


def _list_blobs(gcs_dir: str) -> list[Blob]:
  bucket_prefix = f"gs://{fake_gcs.BUCKET}/"
  return list(storage.Client().bucket(fake_gcs.BUCKET).list_blobs(
      prefix=gcs_dir.removeprefix(bucket_prefix) + "/"))


def test_import_workers_create_one_gcs_client_each(tmp_path, monkeypatch):
  """A worker reuses one GCS client for every read and upload."""
  log_path = tmp_path / "clients.txt"
  _record_calls(monkeypatch, log_path, storage.Client, "__init__",
                lambda client: "client")

  _import_two_csvs_in_one_worker()

  worker_clients = [c for by_worker, c in _read_calls(log_path) if by_worker]
  assert worker_clients == ["client"]


# Local paths are compared as absolute paths: "." is the current dir.
@pytest.mark.parametrize("input_dir, output_dir, overlaps", [
    (".", "out", True),
    ("", "out", True),
    ("data", "{cwd}/data/out", True),
    ("data", "data/out", True),
    ("~/in", "{home}/in/out", True),
    (".", "{tmp}/elsewhere", False),
    ("in", "out", False),
    ("gs://b/in", "gs://b/in/out", True),
    ("gs://b/in", "gs://b/out", False),
])
def test_overlapping_input_and_output_dirs_are_rejected(input_dir, output_dir,
                                                        overlaps, tmp_path,
                                                        monkeypatch):
  cwd = tmp_path / "cwd"
  cwd.mkdir()
  monkeypatch.chdir(cwd)
  monkeypatch.setenv("HOME", str(tmp_path / "home"))
  output_dir = output_dir.format(cwd=cwd, tmp=tmp_path, home=tmp_path / "home")
  input_store, output_store = open_dir(input_dir), open_dir(output_dir)
  if overlaps:
    with pytest.raises(ValueError, match="overlaps"):
      runner._check_not_overlapping(input_store, output_store)
  else:
    runner._check_not_overlapping(input_store, output_store)


def _done_future(result=None, error=None) -> concurrent.futures.Future:
  future = concurrent.futures.Future()
  if error:
    future.set_exception(error)
  else:
    future.set_result(result)
  return future


def test_a_file_that_fails_to_merge_is_reported_with_the_other_failures():
  files = [types.SimpleNamespace(path=p) for p in ("a.csv", "b.csv", "c.csv")]
  futures = [
      [_done_future(error=ValueError("cannot import a"))],
      [_done_future("b")],
      [_done_future("c")],
  ]
  merged = []

  def merge(file, results):
    if results == ["b"]:
      raise RuntimeError("cannot merge b")
    merged.append(file.path)

  failures = runner._finish_imports(files, futures, merge, _no_import_whole)
  assert [(f.file_path, str(f.error)) for f in failures] == [
      ("a.csv", "cannot import a"),
      ("b.csv", "cannot merge b"),
  ]
  assert merged == ["c.csv"]


def _no_import_whole(file, results):
  raise AssertionError(f"{file.path} imported whole: {results}")


def test_a_split_file_is_merged_once_with_the_results_of_all_its_parts(caplog):
  files = [types.SimpleNamespace(path=p) for p in ("a.csv", "b.csv")]
  futures = [[_done_future("a0"), _done_future("a1")], [_done_future("b")]]
  merged = []

  with caplog.at_level(logging.INFO):
    failures = runner._finish_imports(
        files, futures, lambda file, results: merged.append(
            (file.path, results)), _no_import_whole)
  assert not failures
  assert merged == [("a.csv", ["a0", "a1"]), ("b.csv", ["b"])]
  # Logged once per file, not per part.
  assert [r.getMessage() for r in caplog.records if "Imported" in r.message
         ] == ["[1/2] Imported file: a.csv", "[2/2] Imported file: b.csv"]


def test_tasks_are_submitted_largest_first_and_returned_in_order():
  submitted = []

  def submit(i):
    submitted.append(i)
    return f"future {i}"

  assert runner._submit_largest_first([5, 20, 5, 10], submit) == [
      "future 0", "future 1", "future 2", "future 3"
  ]
  assert submitted == [1, 3, 0, 2]  # Ties in task order.


_SLOW_IMPORT_SECONDS = 1
_real_run_single_import_proc = runner._run_single_import_proc


def _slow_import_proc(*args):
  """A worker's import that takes at least _SLOW_IMPORT_SECONDS.

  Module-level so that the worker processes can unpickle it.
  """
  time.sleep(_SLOW_IMPORT_SECONDS)
  return _real_run_single_import_proc(*args)


def test_an_interrupted_run_fails_promptly_and_reports_the_failure(monkeypatch):
  """
  Ctrl-C cancels the files not yet started and reports the failure.

  The interrupt is raised while the first result is merged, as Ctrl-C would
  be while the coordinator waits. Waiting for every file would take
  `all_files_seconds`.
  """
  monkeypatch.setattr(runner, "_run_single_import_proc", _slow_import_proc)

  def interrupt(generator, summary):
    raise KeyboardInterrupt

  monkeypatch.setattr(SummaryGenerator, "add_file_summary", interrupt)
  prefixes = _record_new_gcs_prefixes(monkeypatch)
  files = {"provenance.mcf": b.PROVENANCE_MCF}
  for i in range(29):
    files[f"case_{i:02d}.csv"] = b.csv_file(b.obs_row(f"country/C{i}"))
  workers = 2
  all_files_seconds = len(files) / workers * _SLOW_IMPORT_SECONDS

  start = time.monotonic()
  with pytest.raises(KeyboardInterrupt):
    b.run_dir(files,
              b.obs_config(b.OBS_MAPPINGS),
              runner_kwargs={"max_workers": workers})
  assert time.monotonic() - start < all_files_seconds / 2

  [root] = prefixes
  report = fake_gcs.read_text(
      f"{root}/output/{constants.INGESTION_REPORTS_DIR_NAME}/"
      f"{runlib.WORKFLOW_ID}/{constants.INGESTION_REPORT_FILE_NAME}")
  assert json.loads(report)["run"]["status"] == "FAILURE"
  handshake = fake_gcs.read_text(runlib.handshake_path(root))
  assert json.loads(handshake) == {
      "status":
          "FAILURE",
      "errors": [{
          "file": "",
          "errorType": "GENERIC_ERROR",
          "problemColumns": [],
          "errorMessage": "KeyboardInterrupt",
      }],
  }
