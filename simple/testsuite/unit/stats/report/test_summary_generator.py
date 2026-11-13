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
from stats.report.model.summary import FileCounts
from stats.report.model.summary import Folder
from stats.report.model.summary import FolderSummary
from stats.report.model.summary import IngestionReport
from stats.report.model.summary import Provenance
from stats.report.model.summary import RunMode
from stats.report.model.summary import RunStatus
from stats.report.summary_generator import SummaryGenerator
from util.filesystem import create_store
from util.filesystem import Dir

_START = "2026-10-29T00:00:00Z"
_END = "2026-10-29T00:05:00Z"


def _generator(workflow_execution_id: str | None,
               output_dir: Dir) -> SummaryGenerator:
  """A generator with execution metadata."""
  generator = SummaryGenerator(workflow_execution_id)
  generator.set_execution_metadata(start_time=_START, output_dir=output_dir)
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

  def test_build_report_fills_run_metadata_and_folders(self):
    """Recorded files become per-folder and per-provenance counts, sorted."""
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      generator = _generator("wf-123", store.as_dir())
      generator.record_input_file("oecd", ["oecdProv"], "oecd/a.csv")
      generator.record_input_file("ilo", ["iloProv"], "ilo/b.csv")
      generator.record_input_file("oecd", ["oecdProv"], "oecd/c.CSV")
      generator.record_input_file("oecd", ["oecdSchema"], "oecd/schema.mcf")
      generator.set_provenance_data("oecdProv", "oecdSource")
      # Requested folders are listed even without input files.
      generator.add_folders(["ilo", "oecd", "wto"])
      report = generator.build_report(RunStatus.SUCCESS, _END)

    self.assertEqual(
        report,
        IngestionReport(
            run=ExecutionMetadata(workflowExecutionId="wf-123",
                                  mode=RunMode.INGEST,
                                  status=RunStatus.SUCCESS,
                                  startTime=_START,
                                  endTime=_END,
                                  folderNames=["ilo", "oecd", "wto"],
                                  outputPath=os.path.join(
                                      tmp, "preprocessor_reports", "wf-123")),
            folders=[
                Folder(folderName="ilo",
                       summary=FolderSummary(files=FileCounts(count=1,
                                                              byType={"csv": 1
                                                                     }),
                                             provenanceCount=1),
                       provenances=[Provenance(id="iloProv", fileCount=1)]),
                Folder(folderName="oecd",
                       summary=FolderSummary(files=FileCounts(count=3,
                                                              byType={
                                                                  "csv": 2,
                                                                  "mcf": 1
                                                              }),
                                             provenanceCount=2),
                       provenances=[
                           Provenance(id="oecdProv",
                                      sourceId="oecdSource",
                                      fileCount=2),
                           Provenance(id="oecdSchema", fileCount=1),
                       ]),
                Folder(folderName="wto"),
            ]))

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
