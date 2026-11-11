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
"""Generates ingestion_report.json for a preprocessor run.

Report code must never change the outcome of a run: errors are logged, not
raised.
"""

from collections import Counter
from dataclasses import dataclass
from dataclasses import field
import logging

from stats import constants
from stats.data import FileValidationError
from stats.report.model.summary import ExecutionMetadata
from stats.report.model.summary import FileCounts
from stats.report.model.summary import FileSummary
from stats.report.model.summary import Folder
from stats.report.model.summary import FolderSummary
from stats.report.model.summary import IngestionReport
from stats.report.model.summary import Provenance
from stats.report.model.summary import RunMode
from stats.report.model.summary import RunStatus
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import join_path


@dataclass
class _FolderStats:
  """Counts recorded for one import folder."""

  # Number of input files per lowercase extension, e.g. {"csv": 3, "mcf": 1}.
  files_by_type: Counter = field(default_factory=Counter)
  # Number of input files per provenance ID.
  files_by_provenance: Counter = field(default_factory=Counter)


class SummaryGenerator:
  """Collects stats during a preprocessor run and writes the ingestion report.

  Runner calls, in order: set_execution_metadata(), record_input_file() per
  matched file, add_file_summary() per imported file, set_provenance_data()
  and populate_node_info() after worker results are merged, record_failure()
  on failure, and write_to_json() at the end of the run.

  Worker processes can't update this object, so each returns a FileSummary
  to the parent and Runner passes it to add_file_summary().

  """

  def __init__(self, workflow_execution_id: str | None = None) -> None:
    """Creates a generator for one preprocessor run.

    Args:
      workflow_execution_id: Ingestion workflow execution ID. Empty for
        runs outside the workflow (local runs and tests).
    """
    self._workflow_execution_id = workflow_execution_id or ""
    # The report's run block, created by set_execution_metadata() and
    # completed by build_report().
    self._execution_metadata: ExecutionMetadata | None = None
    # Where the report is written. Not part of the report itself.
    self._output_dir: Dir | None = None
    # Provenance metadata, keyed by provenance DCID.
    self._provenance_data: dict[str, dict] = {}
    # Counts per import folder, keyed by folder name.
    self._folders: dict[str, _FolderStats] = {}
    # TODO: Add containers for merged FileSummary counts, node counts and
    # errors.

  def set_execution_metadata(self, start_time: str, output_dir: Dir) -> None:
    """Sets run metadata known at the start of the run.

    Status and end time are only known at the end, so they are passed to
    write_to_json() instead.

    Args:
      start_time: ISO-8601 UTC time the run started.
      output_dir: The run's output directory containing the JSON-LD shards
        and summary report.
    """
    self._execution_metadata = ExecutionMetadata(
        workflowExecutionId=self._workflow_execution_id,
        mode=RunMode.INGEST,
        startTime=start_time,
    )
    self._output_dir = output_dir

  def record_input_file(self, folder: str, provenance: list[str],
                        file_type: str) -> None:
    """Records one matched input file, before it is imported.

    Args:
      folder: Import folder the file belongs to.
      provenance: list of provenances configured for the file.
      file_type: Lowercase file extension, e.g. "csv" or "mcf".
    """
    if folder not in self._folders:
      self._folders[folder] = _FolderStats()
    stats = self._folders[folder]
    stats.files_by_type[file_type] += 1
    for provenance_id in provenance:
      stats.files_by_provenance[provenance_id] += 1

  def set_provenance_data(self, provenance_id: str,
                          source_id: str | None) -> None:
    """Sets metadata for one provenance, once its source is known.

    Args:
      provenance_id: Provenance DCID.
      source_id: DCID of the provenance's source, if known.
    """
    self._provenance_data[provenance_id] = {"source_id": source_id}

  def add_file_summary(self, summary: FileSummary) -> None:
    """Merges one file's counts, returned by the worker that imported it.

    Args:
      summary: Observations, date range and nodes written for one file.
    """
    # TODO: Add to per-folder and per-provenance totals: sum counts, keep the
    # min and max date, and union node types.

  def populate_node_info(self, folder: str, provenance: list[str],
                         counts_by_type: dict[str, int]) -> None:
    """Adds counts for nodes written by the parent process.

    Nodes from user files are counted in the workers and arrive through
    add_file_summary(). This function covers nodes the parent writes after
    merging worker results: generated provenance, source and StatVarGroup
    nodes. Counts are stored per file and per provenance.

    Args:
      folder: Import folder the nodes belong to.
      provenance: list of provenances the nodes belong to.
      counts_by_type: Number of nodes per typeOf value.
    """
    # TODO: Add to the node counts for each provenance in each folder.

  def record_failure(self, error: FileValidationError) -> None:
    """Records a run-level error.

    Args:
      error: The error, in the same shape as the handshake's errors.
    """
    # TODO: Keep the error for the report.

  def build_report(self, status: RunStatus, end_time: str) -> IngestionReport:
    """Builds the report models from the recorded facts.

    Args:
      status: Outcome of the run.
      end_time: ISO-8601 UTC time the run ended.

    Returns:
      The ingestion report.
    """
    if self._execution_metadata is None:
      raise ValueError("set_execution_metadata() was not called.")
    metadata = self._execution_metadata
    metadata.status = status
    metadata.endTime = end_time
    # Folder the report is written to, e.g.
    # gs://<bucket>/.../datacommons/preprocessor_reports/<workflowExecutionId>.
    metadata.outputPath = self._output_dir.full_path(self._report_dir_name())
    # Folders and provenances are sorted so the report is the same however
    # the files were found.
    folder_names = sorted(self._folders)
    metadata.folderNames = folder_names

    folders = []
    for name in folder_names:
      stats = self._folders[name]
      folders.append(self._build_folder(name, stats))

    # TODO: Add observation and node counts once they are recorded.
    return IngestionReport(run=metadata, folders=folders)

  def _build_folder(self, name: str, stats: _FolderStats) -> Folder:
    """Returns the report entry for one folder."""
    provenances = []
    for provenance_id in sorted(stats.files_by_provenance):
      source_id = ""
      if provenance_id in self._provenance_data:
        source_id = self._provenance_data[provenance_id]["source_id"] or ""
      provenances.append(
          Provenance(id=provenance_id,
                     sourceId=source_id,
                     fileCount=stats.files_by_provenance[provenance_id]))

    files = FileCounts(count=sum(stats.files_by_type.values()),
                       byType=dict(sorted(stats.files_by_type.items())))
    summary = FolderSummary(files=files, provenanceCount=len(provenances))
    return Folder(folderName=name, summary=summary, provenances=provenances)

  def _report_dir_name(self) -> str:
    """Returns the report folder, relative to the output dir.

    preprocessor_reports/<workflowExecutionId>, or preprocessor_reports for
    runs outside the workflow.
    """
    if self._workflow_execution_id:
      return join_path(constants.INGESTION_REPORTS_DIR_NAME,
                       self._workflow_execution_id)
    return constants.INGESTION_REPORTS_DIR_NAME

  def write_to_json(self, status: RunStatus, end_time: str) -> File | None:
    """Builds the report and writes it as ingestion_report.json.

    The report is written under the output dir passed to
    set_execution_metadata(), in preprocessor_reports/<workflowExecutionId>/.
    Never raises: the report must never change the outcome of a run.

    Args:
      status: Outcome of the run.
      end_time: ISO-8601 UTC time the run ended.

    Returns:
      The written report file, or None if building or writing it failed.
    """
    try:
      report = self.build_report(status, end_time)
      report_dir = self._output_dir.open_dir(self._report_dir_name())
      report_file = report_dir.open_file(constants.INGESTION_REPORT_FILE_NAME)
      report_file.write(report.model_dump_json(indent=2))
      return report_file
    except Exception:  # The report must never fail the run.
      logging.exception("Failed to write the ingestion report.")
      return None
