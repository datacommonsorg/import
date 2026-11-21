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
"""GCS request budgets of a run whose input is on GCS (see lib/gcs_requests.py).

On Cloud Batch every request is a round trip of tens of milliseconds, so the
number of requests per input file, not the bytes, sets the run time of small
files. Budgets are written so that a per-file request makes them fail.
"""

import collections
from urllib.parse import parse_qs
from urllib.parse import urlparse

from testsuite.lib import builders as b
from testsuite.lib import gcs_requests

# Files per import: enough that a request per file would exceed the budgets.
_CSVS_PER_IMPORT = 8


def _imports(*names: str) -> dict[str, str]:
  """Input dir with import subdirectories `names`, each with several CSVs."""
  files = {}
  for i, name in enumerate(names):
    files.update(
        b.import_subdir(name, provenance=f"Provenance{i}", source=f"Source{i}"))
    for n in range(_CSVS_PER_IMPORT):
      files[f"{name}/case{n}.csv"] = b.csv_file(b.obs_row(date=f"20{n:02d}"))
  return files


def _run(files: dict[str, str],
         import_names: list[str],
         mode: str = "gcs_input",
         **runner_kwargs):
  with gcs_requests.record() as requests:
    result = b.run_dir(files,
                       mode=mode,
                       runner_kwargs={
                           "import_names": import_names,
                           **runner_kwargs
                       })
  assert result.error is None, result.error
  return requests


def _listed_prefix(request: gcs_requests.Request) -> str:
  """The prefix that a list request lists, relative to the input dir."""
  [prefix] = parse_qs(urlparse(request.url).query)["prefix"]
  return prefix.split("/input/", 1)[1]


def test_opening_the_output_dir_lists_nothing_under_it():
  requests = _run(_imports("alpha"), ["alpha"])

  # GCS has no directories to make or mark. Listing the dir would also list
  # the shards of every earlier run into it.
  assert not requests.of("init")


def test_each_requested_import_is_listed_once():
  requests = _run(_imports("alpha", "beta", "gamma"), ["alpha", "beta"])

  # Loading the config lists each import; finding input files reuses that.
  lists = requests.of("load_config", "list")
  assert sorted(_listed_prefix(r) for r in lists) == ["alpha/", "beta/"]
  assert not requests.of("find_input_files")
  # Nothing writes directory markers into the input.
  assert not requests.of("load_config", "upload")


def test_a_bulk_load_lists_the_input_dir_once():
  requests = _run(_imports("alpha", "beta", "gamma"), ["ALL_IMPORTS"])

  assert not requests.of("find_input_files")
  # One listing, then each config.json is read in one download.
  kinds = sorted(r.kind for r in requests.of("load_config"))
  assert kinds == ["download"] * 3 + ["list"], kinds


def test_a_csv_header_check_is_one_ranged_read():
  requests = _run(_imports("alpha"), ["alpha"])

  kinds = [r.kind for r in requests.of("validate_headers")]
  # case.csv and the case<n>.csv files.
  assert kinds == ["range"] * (1 + _CSVS_PER_IMPORT), kinds


def test_a_worker_reads_each_input_file_once_and_writes_only_shards():
  requests = _run(_imports("alpha"), ["alpha"])

  kinds = collections.Counter(r.kind for r in requests.of(gcs_requests.WORKER))
  # Per CSV: one ranged read and one observation shard upload. The MCF file
  # is read whole, in one download.
  csvs = 1 + _CSVS_PER_IMPORT
  assert kinds == {"range": csvs, "upload": csvs, "download": 1}


def test_a_csv_larger_than_a_header_read_is_read_in_one_request(monkeypatch):
  # Header reads shrunk to 1 KiB, so that a small CSV spans several of them
  # but still fits in one data chunk (8 MiB).
  header_read_bytes = 1024
  monkeypatch.setattr("stats.importer._HEADER_READ_CHUNK_BYTES",
                      header_read_bytes)
  files = _imports("alpha")
  rows = [b.obs_row(date=str(1000 + n)) for n in range(100)]
  files["alpha/case.csv"] = b.csv_file(*rows)
  assert len(files["alpha/case.csv"]) > 3 * header_read_bytes
  requests = _run(files, ["alpha"])

  case_reads = [
      r for r in requests.of(gcs_requests.WORKER, "range")
      if urlparse(r.url).path.endswith("/case.csv")
  ]
  assert len(case_reads) == 1, case_reads


def test_larger_files_are_imported_first():
  files = _imports("alpha")
  for n in range(_CSVS_PER_IMPORT):
    rows = [b.obs_row(date=str(2000 + i)) for i in range((n * 5) % 8 + 1)]
    files[f"alpha/case{n}.csv"] = b.csv_file(*rows)
  # One worker imports the files in the order they are submitted.
  requests = _run(files, ["alpha"], max_workers=1)

  read = [
      urlparse(r.url).path.rsplit("/", 1)[1]
      for r in requests.of(gcs_requests.WORKER)
      if r.kind in ("range", "download")
  ]
  sizes = {name.split("/", 1)[1]: len(text) for name, text in files.items()}
  assert read == sorted(read, key=lambda name: -sizes[name])
  assert len(read) == len(sizes) - 1  # Every file but config.json.


def test_a_split_csv_is_planned_and_read_in_a_ranged_read_per_part():
  files = _imports("alpha")
  rows = [b.obs_row(date=str(1000 + n % 1000)) for n in range(40_000)]
  files["alpha/case.csv"] = b.csv_file(*rows)
  num_parts = 4
  requests = _run(files, ["alpha"],
                  max_workers=num_parts,
                  split_csv_bytes=len(files["alpha/case.csv"]) // 4)

  # The header, then a window at the start of each part but the first.
  assert [r.kind for r in requests.of("plan_splits")] == ["range"] * num_parts
  reads = [
      r for r in requests.of(gcs_requests.WORKER, "range")
      if urlparse(r.url).path.endswith("/case.csv")
  ]
  # A part reads the header, then its rows: one read for the first part.
  assert len(reads) == 2 * num_parts - 1
