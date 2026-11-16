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


def _run(files: dict[str, str], import_names: list[str]):
  with gcs_requests.record() as requests:
    result = b.run_dir(files,
                       mode="gcs_input",
                       runner_kwargs={"import_names": import_names})
  assert result.error is None, result.error
  return requests


def _listed_prefix(request: gcs_requests.Request) -> str:
  """The prefix that a list request lists, relative to the input dir."""
  [prefix] = parse_qs(urlparse(request.url).query)["prefix"]
  return prefix.split("/input/", 1)[1]


def test_finding_input_files_lists_each_requested_import_once():
  requests = _run(_imports("alpha", "beta", "gamma"), ["alpha", "beta"])

  found = requests.of("find_input_files")
  assert [r.kind for r in found] == ["list", "list"], found
  assert sorted(_listed_prefix(r) for r in found) == ["alpha/", "beta/"]
  # Opening the input store used to write its missing directory markers.
  assert not requests.of("load_config", "upload")


def test_a_bulk_load_lists_the_input_dir_once():
  requests = _run(_imports("alpha", "beta", "gamma"), ["ALL_IMPORTS"])

  assert not requests.of("find_input_files")
  # One listing, then each config.json is read: a metadata get and a download.
  kinds = sorted(r.kind for r in requests.of("load_config"))
  assert kinds == ["download"] * 3 + ["get"] * 3 + ["list"], kinds
