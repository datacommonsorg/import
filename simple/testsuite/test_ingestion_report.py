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
"""ingestion_report.json: written for every run, successful or not."""

import json

from testsuite.lib import builders as b
from testsuite.lib import run as runlib


def _expected_report(status: str, folder_names: list[str]) -> dict:
  """The report, without the timestamps that change every run."""
  return {
      "run": {
          "workflowExecutionId": runlib.WORKFLOW_ID,
          "mode": "INGEST",
          "status": status,
          "folderNames": folder_names,
          "outputPath": "<output>",
      },
      "folders": [{
          "folderName": name,
          "provenances": []
      } for name in folder_names],
  }


def _import_subdir(name: str) -> dict[str, str]:
  return {
      f"{name}/config.json": json.dumps(b.MCF_CONFIG),
      f"{name}/provenance.mcf": b.PROVENANCE_MCF,
  }


def test_successful_run_writes_the_report():
  result = b.run_mcf(b.mcf_node("dcid:Thing1"))
  assert result.error is None
  assert result.output["ingestion_report"] == _expected_report("SUCCESS", [])


def test_failed_run_still_writes_the_report():
  result = b.run_mcf(b.mcf_node("NoDcid"))
  assert result.error is not None
  assert result.output["ingestion_report"] == _expected_report("FAILURE", [])


def test_report_lists_the_selected_import_folders():
  result = b.run_dir(
      {
          **_import_subdir("oecd"),
          **_import_subdir("ilo"),
          **_import_subdir("ignored"),
      },
      runner_kwargs={"import_names": ["oecd", "ilo"]})
  assert result.output["ingestion_report"] == _expected_report(
      "SUCCESS", ["oecd", "ilo"])


def test_report_lists_every_folder_for_all_imports():
  result = b.run_dir({
      **_import_subdir("oecd"),
      **_import_subdir("ilo"),
  },
                     runner_kwargs={"import_names": ["ALL_IMPORTS"]})
  assert result.output["ingestion_report"] == _expected_report(
      "SUCCESS", ["ilo", "oecd"])
