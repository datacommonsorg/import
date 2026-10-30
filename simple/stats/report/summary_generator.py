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
"""Generates the ingestion report for a preprocessor run.

This module holds all of the report's logic:
  - Recording facts as the run progresses: input files, observations, nodes.
  - Building the report models defined in model/summary.py and writing
    ingestion_report.json once per run, on success and on failure.

Report code must never change the outcome of a run, so errors while
collecting or writing the report are logged rather than raised.
"""

import logging

from stats import constants
from stats.data import FileValidationError
from stats.report.model.summary import FileSummary
from stats.report.model.summary import IngestionReport
from stats.report.model.summary import RunStatus
from util.filesystem import Dir
from util.filesystem import File

# Workflow execution ID used outside the ingestion workflow (local runs and
# tests), which have no WORKFLOW_EXECUTION_ID.
LOCAL_WORKFLOW_EXECUTION_ID = "local"


class SummaryGenerator:
  """Collects facts during a preprocessor run and writes the ingestion report.

  Runner creates one instance at the start of run() and calls it as follows:
    1. set_execution_metadata(): run metadata known at the start.
    2. record_input_file(): each matched input file, before header validation,
       so failed runs still report the files they found.
    3. add_file_summary(): per-file counts from the import workers (below).
    4. set_provenance_data() and populate_node_info(): provenance sources and
       nodes the parent process writes, once worker results are merged.
    5. record_failure(): run-level errors, if the run fails.
    6. write_to_json(): once at the end of the run, on success and on failure.

  Counts from worker processes: in production, files are imported in separate
  processes, which can't update this object. Each worker instead fills a
  FileSummary (observations, date range, nodes by type) from what its
  JsonLdStreamWriter writes, and returns it in ImportProcResult. Runner passes
  each one to add_file_summary(), which merges it in. Merging gives the same
  result whatever order the workers finish in. In thread mode, the shared
  writer keeps one FileSummary per file and Runner passes them the same way.

  TODO: This report replaces ImportReporter's process/report.json, which
  records per-file status and is overwritten on every run. Drop report.json
  once this report is written.

  Method signatures are a first draft and may change as the logic lands.
  """

  def __init__(self, workflow_execution_id: str | None = None) -> None:
    """Creates a generator for one preprocessor run.

    Args:
      workflow_execution_id: Ingestion workflow execution ID, which also keys
        the report's destination. Defaults to LOCAL_WORKFLOW_EXECUTION_ID.
    """
    self._workflow_execution_id = (workflow_execution_id or
                                   LOCAL_WORKFLOW_EXECUTION_ID)
    self._execution_metadata: dict | None = None
    # Provenance metadata, keyed by provenance DCID.
    self._provenance_data: dict[str, dict] = {}
    # TODO: Add containers for input files, merged FileSummary counts per
    # folder and provenance, node counts and errors.

  def set_execution_metadata(self, start_time: str, folder_names: list[str],
                             output_path: str) -> None:
    """Sets run metadata known at the start of the run.

    Status and end time are only known at the end, so they are passed to
    write_to_json() instead.

    Args:
      start_time: ISO-8601 UTC time the run started.
      folder_names: Import folders requested for the run.
      output_path: Where the run's JSON-LD is written.
    """
    self._execution_metadata = {
        "start_time": start_time,
        "folder_names": list(folder_names),
        "output_path": output_path,
    }

  def record_input_file(self, folder: str, provenance: str | None,
                        file_type: str) -> None:
    """Records one matched input file, before it is imported.

    Args:
      folder: Import folder the file belongs to.
      provenance: Provenance configured for the file, if any.
      file_type: Lowercase file extension, e.g. "csv" or "mcf".
    """
    # TODO: Count files by folder, type and provenance.

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

  def populate_node_info(self, folder: str, provenance: str,
                         counts_by_type: dict[str, int]) -> None:
    """Adds counts for nodes written by the parent process.

    Nodes from user files are counted in the workers and arrive through
    add_file_summary(). This covers nodes the parent writes after merging
    worker results: generated provenance, source and StatVar nodes, and
    StatVarGroups.

    Args:
      folder: Import folder the nodes belong to.
      provenance: Provenance the nodes belong to.
      counts_by_type: Number of nodes per typeOf value.
    """
    # TODO: Add to the node counts for this folder and provenance.

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
    raise NotImplementedError("TODO: Build the report from recorded facts.")

  def write_to_json(self, status: RunStatus, end_time: str,
                    report_dir: Dir) -> File | None:
    """Builds the report and writes it as ingestion_report.json.

    Never raises: the report must never change the outcome of a run.

    Args:
      status: Outcome of the run.
      end_time: ISO-8601 UTC time the run ended.
      report_dir: Where to write the report:
        ingestion/internal/reports/<workflowExecutionId>/ in the instance's
        artifacts bucket for workflow runs, and <output_dir>/reports/local/
        for local runs. Runner decides the directory.

    Returns:
      The written report file, or None if building or writing it failed.
    """
    try:
      report = self.build_report(status, end_time)
      report_file = report_dir.open_file(constants.INGESTION_REPORT_FILE_NAME)
      report_file.write(report.model_dump_json(indent=2))
      return report_file
    except Exception:  # The report must never fail the run.
      logging.exception("Failed to write the ingestion report.")
      return None
