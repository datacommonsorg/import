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
"""Tests for stats.report.summary_generator."""

import os
import tempfile
import unittest
from unittest import mock

from stats.report.model.summary import ExecutionMetadata
from stats.report.model.summary import Folder
from stats.report.model.summary import IngestionReport
from stats.report.model.summary import RunMode
from stats.report.model.summary import RunStatus
from stats.report.summary_generator import SummaryGenerator
from util.filesystem import create_store
from util.filesystem import Dir

_START = "2026-10-29T00:00:00Z"
_END = "2026-10-29T00:05:00Z"


def _generator(workflow_execution_id: str | None,
               output_dir: Dir) -> SummaryGenerator:
  """A generator with execution metadata for two folders."""
  generator = SummaryGenerator(workflow_execution_id)
  generator.set_execution_metadata(start_time=_START,
                                   folder_names=["oecd", "ilo"],
                                   output_dir=output_dir)
  return generator


class TestSummaryGenerator(unittest.TestCase):
  """Tests for SummaryGenerator."""

  def test_write_to_json_never_raises(self):
    """A failure while writing the report is logged, not raised."""
    output_dir = mock.MagicMock()
    output_dir.full_path.return_value = "/out/preprocessor_reports/wf-123"
    # Subdirectories resolve to the same mock, so open_file can be checked.
    output_dir.open_dir.return_value = output_dir
    output_dir.open_file.side_effect = OSError("disk full")
    generator = _generator("wf-123", output_dir)

    with self.assertLogs(level="ERROR"):
      result = generator.write_to_json(RunStatus.FAILURE, _END)

    self.assertIsNone(result)
    output_dir.open_file.assert_called_once()

  def test_build_report_fills_run_metadata(self):
    """The recorded metadata becomes the run block and one entry per folder."""
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      report = _generator("wf-123",
                          store.as_dir()).build_report(RunStatus.SUCCESS, _END)

    self.assertEqual(
        report,
        IngestionReport(
            run=ExecutionMetadata(workflowExecutionId="wf-123",
                                  mode=RunMode.INGEST,
                                  status=RunStatus.SUCCESS,
                                  startTime=_START,
                                  endTime=_END,
                                  folderNames=["oecd", "ilo"],
                                  outputPath=os.path.join(
                                      tmp, "preprocessor_reports", "wf-123")),
            folders=[Folder(folderName="oecd"),
                     Folder(folderName="ilo")]))

  def test_write_to_json_without_workflow_id_skips_id_folder(self):
    """Runs outside the workflow write to preprocessor_reports/ directly."""
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      _generator(None, store.as_dir()).write_to_json(RunStatus.SUCCESS, _END)

      self.assertTrue(
          os.path.exists(
              os.path.join(tmp, "preprocessor_reports",
                           "ingestion_report.json")))


if __name__ == "__main__":
  unittest.main()
