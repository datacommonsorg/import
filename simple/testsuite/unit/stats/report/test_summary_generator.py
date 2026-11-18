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

from stats.report.model.summary import DateRange
from stats.report.model.summary import ExecutionMetadata
from stats.report.model.summary import FileCounts
from stats.report.model.summary import FileSummary
from stats.report.model.summary import Folder
from stats.report.model.summary import FolderSummary
from stats.report.model.summary import IngestionReport
from stats.report.model.summary import NodeCounts
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
      # A nested import folder keeps its full name.
      generator.record_input_file("unitednations/ilo", ["iloProv"],
                                  "unitednations/ilo/b.csv")
      generator.record_input_file("oecd", ["oecdProv"], "oecd/a.csv")
      generator.record_input_file("oecd", ["oecdProv"], "oecd/c.CSV")
      generator.record_input_file("oecd", ["oecdSchema"], "oecd/schema.mcf")
      generator.set_provenance_data("oecdProv", "oecdSource")
      # Requested folders are listed even without input files.
      generator.add_folders(["unitednations/ilo", "oecd", "wto"])
      report = generator.build_report(RunStatus.SUCCESS, _END)

    self.assertEqual(
        report,
        IngestionReport(
            run=ExecutionMetadata(
                workflowExecutionId="wf-123",
                mode=RunMode.INGEST,
                status=RunStatus.SUCCESS,
                startTime=_START,
                endTime=_END,
                folderNames=["oecd", "unitednations/ilo", "wto"],
                outputPath=os.path.join(tmp, "preprocessor_reports", "wf-123")),
            folders=[
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
                Folder(folderName="unitednations/ilo",
                       summary=FolderSummary(files=FileCounts(count=1,
                                                              byType={"csv": 1
                                                                     }),
                                             provenanceCount=1),
                       provenances=[Provenance(id="iloProv", fileCount=1)]),
                Folder(folderName="wto"),
            ]))

  def test_file_summary_adds_observations_and_date_range(self):
    """A file's observations and dates are reported under its provenance."""
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      generator = _generator("wf-123", store.as_dir())
      generator.record_input_file("oecd", ["oecdProv"], "oecd/a.csv")
      generator.add_file_summary(
          FileSummary(filePath="oecd/a.csv",
                      folder="oecd",
                      provenanceId="oecdProv",
                      observations=100,
                      dateRange=DateRange(min="2005", max="2010")))
      report = generator.build_report(RunStatus.SUCCESS, _END)

    self.assertEqual(report.folders, [
        Folder(folderName="oecd",
               summary=FolderSummary(files=FileCounts(count=1,
                                                      byType={"csv": 1}),
                                     provenanceCount=1,
                                     observations=100),
               provenances=[
                   Provenance(id="oecdProv",
                              fileCount=1,
                              observations=100,
                              dateRange=DateRange(min="2005", max="2010"))
               ])
    ])

  def test_node_counts_add_up_per_provenance_and_folder(self):
    """File and Runner node counts are summed per provenance, then per folder.

    Runner nodes go to every folder whose files use their provenance.
    """
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      generator = _generator("wf-123", store.as_dir())
      generator.record_input_file("oecd", ["oecdProv"], "oecd/a.mcf")
      generator.record_input_file("oecd", ["oecdOther"], "oecd/b.mcf")
      generator.record_input_file("ilo", ["iloProv"], "ilo/c.mcf")
      for path, provenance_id, by_type in [
          ("oecd/a.mcf", "oecdProv", {
              "StatisticalVariable": 2
          }),
          ("oecd/b.mcf", "oecdOther", {
              "StatisticalVariable": 1,
              "Property": 1
          }),
      ]:
        generator.add_file_summary(
            FileSummary(filePath=path,
                        folder="oecd",
                        provenanceId=provenance_id,
                        nodes=sum(by_type.values()),
                        nodesByType=by_type))
      generator.add_node_counts("oecdProv", 2, {"Provenance": 1, "Source": 1})
      folders = generator.build_report(RunStatus.SUCCESS, _END).folders

    self.assertEqual(
        [(f.folderName, f.summary.nodes, [(p.id, p.nodes)
                                          for p in f.provenances])
         for f in folders], [
             ("ilo", NodeCounts(), [("iloProv", NodeCounts())]),
             ("oecd",
              NodeCounts(count=6,
                         byType={
                             "Provenance": 1,
                             "Source": 1,
                             "StatisticalVariable": 3,
                             "Property": 1
                         }), [
                             ("oecdOther",
                              NodeCounts(count=2,
                                         byType={
                                             "StatisticalVariable": 1,
                                             "Property": 1
                                         })),
                             ("oecdProv",
                              NodeCounts(count=4,
                                         byType={
                                             "Provenance": 1,
                                             "Source": 1,
                                             "StatisticalVariable": 2
                                         })),
                         ]),
         ])

  def test_runner_nodes_go_to_every_folder_using_their_provenance(self):
    """Runner nodes count in every folder whose files use their provenance."""
    with tempfile.TemporaryDirectory() as tmp, \
         create_store(tmp) as store:
      generator = _generator("wf-123", store.as_dir())
      generator.record_input_file("oecd", ["sharedProv"], "oecd/a.mcf")
      generator.record_input_file("ilo", ["sharedProv"], "ilo/b.mcf")
      generator.add_node_counts("sharedProv", 2, {"Provenance": 1, "Source": 1})
      folders = generator.build_report(RunStatus.SUCCESS, _END).folders

    expected = NodeCounts(count=2, byType={"Provenance": 1, "Source": 1})
    self.assertEqual([(f.folderName, f.provenances[0].nodes) for f in folders],
                     [("ilo", expected), ("oecd", expected)])

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
