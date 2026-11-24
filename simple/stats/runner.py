# Copyright 2026 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import concurrent.futures
from datetime import datetime
from datetime import timezone
import logging
import os
from typing import Optional

from stats import worker_pool
from stats.config.config import Config
from stats.config.discovery import InputDiscovery
from stats.graph.data import FileValidationError
from stats.graph.data import ImportCounts
from stats.graph.nodes import Nodes
from stats.importers import file_split
from stats.importers.factory import create_importer_for_file
from stats.importers.importer import count_nodes_from_triples
from stats.report.model.summary import FileSummary
from stats.report.model.summary import NodeCounts
from stats.report.model.summary import RunMode
from stats.report.model.summary import RunStatus
from stats.report.model.summary import ValidationErrorType
from stats.report.summary_generator import SummaryGenerator
from stats.validation.validation import MetadataValidator
from stats.writer.jsonld_stream_writer import JsonLdStreamWriter
from stats.writer.workflow_handoff import failure_error
from stats.writer.workflow_handoff import write_workflow_handoff
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import is_gcs_path
from util.filesystem import open_dir

# Upper bound on the threads that validate CSV headers. Header validation reads
# only the first rows of each file and spends its time waiting on storage (often
# GCS), so it is bounded independently of the CPU count; 32 matches the GCS
# client's connection pool (util/filesystem/gcs.py).
_MAX_HEADER_VALIDATION_THREADS = 32


class Runner:
  """Runs and coordinates all imports."""

  def __init__(
      self,
      input_dir_path: str,
      output_dir_path: str,
      import_names: Optional[list[str]] = None,
      max_workers: Optional[int] = None,
      split_file_bytes: Optional[int] = None,
  ) -> None:
    """Sets up the output dir; run() reads the config and the input dir.

    Args:
      max_workers: The maximum number of worker processes that import files
        concurrently. Defaults to `worker_pool.default_max_workers()`. Lower it
        to reduce peak memory, since each worker holds the data of the file it
        imports.
      split_file_bytes: Files larger than this are imported in parts, in
        parallel (see `file_split.num_parts`). Defaults to
        `file_split.SPLIT_FILE_BYTES`.
    """
    if max_workers is not None and max_workers < 1:
      raise ValueError(f"max_workers must be at least 1, got {max_workers}")
    if split_file_bytes is not None and split_file_bytes < 1:
      raise ValueError(
          f"split_file_bytes must be at least 1, got {split_file_bytes}")

    if not input_dir_path:
      raise ValueError("input_dir must be specified")
    if not output_dir_path:
      raise ValueError("output_dir must be specified")

    self.input_dir_path = input_dir_path
    self.import_names = import_names
    self._discovery = InputDiscovery(input_dir_path, import_names)

    # Output directories. Set up before the config is read so that a bad
    # config is reported like any other failure.
    self.output_dir = open_dir(output_dir_path, create=True)

    # If None, _load_config() sets the default, which depends on whether any
    # input or output dir is on GCS.
    self.max_workers: int | None = max_workers
    self.split_file_bytes = split_file_bytes
    # Set by _load_config().
    self.input_dir: Dir | None = None
    self.config: Config | None = None
    self.nodes: Nodes | None = None
    # Set while importing.
    self.graph_writer: JsonLdStreamWriter | None = None
    # The errors for the FAILURE handshake, set when the run fails.
    self._failure_errors: list[FileValidationError] | None = None
    # Errors in input files, each with its file, so the report can put them
    # under the file's folder and provenance.
    self._file_errors: list[tuple[File, FileValidationError]] = []

    self.summary_generator = SummaryGenerator(
        workflow_execution_id=os.getenv("WORKFLOW_EXECUTION_ID"),
        mode=(RunMode.DRY_RUN if os.getenv("INGESTION_MODE") == RunMode.DRY_RUN
              else RunMode.INGEST))

  def _load_config(self) -> None:
    """Opens the input dir, and reads and validates the config."""
    self.input_dir, self.config = self._discovery.load_config(self.output_dir)
    self.import_names = self._discovery.import_names
    self.nodes = Nodes(self.config)
    if self.max_workers is None:
      self.max_workers = worker_pool.default_max_workers(uses_gcs=any(
          is_gcs_path(d.store_path())
          for d in (self.input_dir, self.output_dir)))

  def run(self):
    self.summary_generator.set_execution_metadata(start_time=_utc_now(),
                                                  output_dir=self.output_dir)
    try:
      self._load_config()

      self._run_imports_and_export_jsonld()

      self.graph_writer.commit_and_close()

      self._write_report(RunStatus.SUCCESS)

      # Hand off processed import metadata to workflow or GCS
      write_workflow_handoff(self.graph_writer)

    except (Exception, KeyboardInterrupt) as e:  # A Ctrl-C fails the run too.
      logging.exception("Error updating stats")

      if self._failure_errors is None:
        self._failure_errors = [failure_error(e)]

      self._record_report_errors()
      self._write_report(RunStatus.FAILURE)

      handshake_path = write_workflow_handoff(self.graph_writer,
                                              self._failure_errors)
      if handshake_path:
        logging.error(
            "Preprocessor execution failed! Diagnostic error details were"
            " written to the GCS handshake path: %s", handshake_path)
      else:
        logging.error("Preprocessor execution failed!")
      raise
    finally:
      if self.graph_writer:  # Collisions are logged even if the run failed.
        self.graph_writer.log_collision_summary()

  def _record_input_files(self, files: list[File]) -> None:
    """Records each matched input file in the ingestion report."""
    for file in files:
      folder, provenance_id = self._report_folder_and_provenance(file)
      self.summary_generator.record_input_file(folder=folder,
                                               provenance=[provenance_id],
                                               file_path=file.path)

  def _report_folder_and_provenance(self, file: File) -> tuple[str, str]:
    """Returns the import folder and provenance ID that `file` belongs to."""
    return self._discovery.input_folder_name(file), self.nodes.provenance_id(
        file)

  def _record_provenance_sources(self) -> None:
    """Records each known provenance's source in the ingestion report."""
    for provenance in self.nodes.provenances.values():
      self.summary_generator.set_provenance_data(provenance.id,
                                                 provenance.source_id)

  def _write_report(self, status: RunStatus) -> None:
    """Writes the ingestion report with the run's final status."""
    # Requested folders are listed even if no input files were found in them.
    self.summary_generator.add_folders(self._discovery.report_folder_names())
    if self.nodes:  # No provenances if the config failed to load.
      self._record_provenance_sources()
    self.summary_generator.write_to_json(status, _utc_now())

  def _validate_single_file_headers(self,
                                    file: File) -> list[FileValidationError]:
    """Validates headers for a single CSV file and returns any errors found."""
    try:
      return create_importer_for_file(self.config, file, self.graph_writer,
                                      self.nodes).validate_headers()
    except Exception as e:
      return [
          FileValidationError(
              file=file.path,
              error_type=ValidationErrorType.GENERIC_ERROR,
              error_message=f"Failed to validate headers: {str(e)}")
      ]

  def _validate_all_headers(self, csv_files: list[File]) -> None:
    """Validates every CSV's headers before any file is imported.

    Raises ValueError if any fail. Importers don't check headers again.
    """
    if not csv_files:
      return

    # Pre-populate the per-file config cache on the main thread before spawning
    # worker threads so concurrent lookups do not race on mutating the cache dict.
    # The threads only build importers and read headers: they write nothing to
    # Nodes or the graph writer, which are not thread-safe.
    for file in csv_files:
      self.config.for_file(file)

    all_errors = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(
        _MAX_HEADER_VALIDATION_THREADS, len(csv_files))) as executor:
      for file, errors in zip(
          csv_files, executor.map(self._validate_single_file_headers,
                                  csv_files)):
        all_errors.extend(errors)
        self._file_errors.extend((file, error) for error in errors)

    if all_errors:
      self._failure_errors = all_errors
      formatted_errors = [
          f"File '{err.file}': {err.error_message}" for err in all_errors
      ]
      consolidated_msg = (
          "CSV Header Validation Failed! The following errors were found:\n" +
          "\n".join(formatted_errors))
      raise ValueError(consolidated_msg)

  def _discard_part_outputs(
      self, file: File,
      part_results: list[worker_pool.ImportProcResult]) -> None:
    """Deletes the shards and observation ids that `file`'s parts wrote."""
    self.graph_writer.delete_part_shards(file)
    for res in part_results:
      if res.obs_ids_path:
        os.remove(res.obs_ids_path)

  def _find_and_filter_input_files(self) -> tuple[list[File], list[File]]:
    """Discovers, filters, sorts, and returns matched CSV and MCF files."""
    return self._discovery.find_input_files()

  def _run_all_data_imports(self):
    """Finds the input files, validates CSV headers, and imports every file."""
    csv_files, mcf_files = self._find_and_filter_input_files()
    # Before header validation, so failed runs still report their files.
    self._record_input_files(csv_files + mcf_files)

    logging.info("Found %d CSV and %d MCF files to import", len(csv_files),
                 len(mcf_files))

    # Validate all CSV headers upfront
    self._validate_all_headers(csv_files)

    # MCF and CSV files are imported together: a CSV needs nothing from MCF.
    files = mcf_files + csv_files
    if not files:
      return
    parts_by_path = file_split.plan_splits(files, self.config, self.max_workers,
                                           self.split_file_bytes)
    collisions, failures = worker_pool.run_multiprocess_imports(
        files,
        parts_by_path,
        max_workers=self.max_workers,
        output_dir_path=self.output_dir.full_path(),
        import_names=self.import_names,
        config_model=self.config.model,
        jsonld_dir_name=self.graph_writer.jsonld_dir.name(),
        merge_file=self._merge_file_results,
        discard_part_outputs=self._discard_part_outputs,
    )
    if failures:
      # The handshake has an error per failed file, with its path.
      self._failure_errors = [
          failure_error(f.error, f.file_path) for f in failures
      ]
      files_by_path = {file.path: file for file in files}
      self._file_errors.extend(
          (files_by_path[f.file_path], error)
          for f, error in zip(failures, self._failure_errors))
      worker_pool.raise_for_failures(failures)
    if collisions:
      self.graph_writer.merge_collisions(collisions.file_counts,
                                         collisions.file_samples)

  def _record_report_errors(self) -> None:
    """Passes the errors that failed the run to the report.

    Input file errors go under their file's provenance; any other error, e.g.
    a config or metadata error, is reported for the whole run.
    """
    if not self._file_errors:
      for error in self._failure_errors:
        self.summary_generator.record_failure(error)
      return
    for file, error in self._file_errors:
      folder, provenance_id = self._report_folder_and_provenance(file)
      self.summary_generator.record_failure(error, folder, provenance_id)

  def _merge_file_results(self, file: File,
                          results: list[worker_pool.ImportProcResult]) -> None:
    """Merges the results of `file`'s tasks: the whole file, or its parts."""
    for res in results:
      self.nodes.merge_metadata(res.provenances, res.sources)
      self.graph_writer.merge_worker(res.processed_imports,
                                     res.file_collision_counts,
                                     res.file_sample_collisions)
    # One summary for the file: its parts' counts, added up.
    counts = results[0].counts
    for res in results[1:]:
      counts = counts.add(res.counts)
    self._record_file_summary(file, counts)

  def _record_file_summary(self, file: File, counts: ImportCounts) -> None:
    """Passes the observation counts of `file` to the report."""
    # Folders come only from --imports, as in _record_input_files.
    if not self.import_names:
      return
    folder, provenance_id = self._report_folder_and_provenance(file)
    self.summary_generator.add_file_summary(
        FileSummary(filePath=file.path,
                    folder=folder,
                    provenanceId=provenance_id,
                    observations=counts.observations,
                    dateRange=counts.date_range,
                    nodes=NodeCounts(count=counts.nodes,
                                     byType=dict(counts.nodes_by_type))))

  def _run_imports_and_export_jsonld(self):
    self.graph_writer = JsonLdStreamWriter(self.output_dir, self.import_names,
                                           self.config)

    # Run data imports (CSV and MCF)
    self._run_all_data_imports()

    # Generate triples from nodes grouped by provenance directory and write directly
    for prov_dir, triples in self.nodes.triples_by_provenance_dir().items():
      # Every triple is counted. "" (no import: the generated StatVarGroups) is
      # no folder's provenance, so its counts don't appear in the report.
      counts = count_nodes_from_triples(triples)
      self.summary_generator.add_runner_node_counts(prov_dir, counts.nodes,
                                                    counts.nodes_by_type)
      self.graph_writer.write_triples(triples, provenance_dir=prov_dir)

    # Perform strict metadata validation before committing and closing
    MetadataValidator(self.config, self.nodes).validate()


def _utc_now() -> str:
  """Returns the current UTC time in ISO-8601, e.g. 2026-11-02T18:45:00Z."""
  return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
