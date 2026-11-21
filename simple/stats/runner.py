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
from dataclasses import dataclass
from datetime import datetime
from datetime import timezone
import json
import logging
import os
import posixpath
import re
import tempfile
import traceback
from typing import Callable, Optional

from stats import constants
from stats import csv_split
from stats.config import Config
from stats.config import ImportConfig
from stats.config import InputFileConfig
from stats.config import parse_config
from stats.csv_split import FilePart
from stats.data import FileValidationError
from stats.data import ImportCounts
from stats.data import ImportType
from stats.data import invalid_configuration
from stats.data import PreprocessorError
from stats.data import Provenance
from stats.data import Source
from stats.entities_importer import EntitiesImporter
from stats.graph_writer import GraphWriter
from stats.importer import count_nodes_from_triples
from stats.importer import Importer
from stats.jsonld_stream_writer import JsonLdStreamWriter
from stats.jsonld_stream_writer import observation_shard_stem
from stats.mcf_importer import McfImporter
from stats.nodes import Nodes
from stats.obs_id_collisions import find_cross_file_collisions
from stats.obs_id_collisions import save_obs_ids
from stats.report.model.summary import FileSummary
from stats.report.model.summary import NodeCounts
from stats.report.model.summary import RunStatus
from stats.report.model.summary import ValidationErrorType
from stats.report.summary_generator import SummaryGenerator
from stats.validation import MetadataValidator
from stats.variable_per_row_importer import VariablePerRowImporter
from util.file_match import match
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import GcsObject
from util.filesystem import is_gcs_path
from util.filesystem import is_within
from util.filesystem import join_path
from util.filesystem import open_dir
from util.filesystem import write_file

_ARCHIVES_DIR_NAME = "archives"
# Every run names its imports: each is a subdirectory of the input dir with its
# own config.json.
_NO_IMPORTS_ERROR = (
    "No imports to run. Pass --imports with the import subdirectories of the"
    " input directory to import, e.g. --imports=oecd or --imports=oecd,ilo, or"
    f" --imports={constants.ALL_IMPORTS} for all of them.")
# Upper bound on the threads that validate CSV headers. Header validation reads
# only the first rows of each file and spends its time waiting on storage (often
# GCS), so it is bounded independently of the CPU count; 32 matches the GCS
# client's connection pool (util/filesystem/gcs.py).
_MAX_HEADER_VALIDATION_THREADS = 32
# Default worker processes per CPU for runs that read or write GCS, and the
# count above which no extra workers are added (see default_max_workers).
_GCS_WORKERS_PER_CPU = 2
_MAX_GCS_WORKERS = 32
# Upper bound on the threads that plan how to split large CSVs. Each reads a
# few small windows of a file, so it is bounded as header validation is.
_MAX_SPLIT_PLANNING_THREADS = 32


def _import_names_or_raise(names: list[str] | None) -> list[str]:
  """Returns the run's imports, stripped and without blanks.

  Each must name a subdirectory of the input dir, so names that resolve to the
  input dir itself or outside it (".", "..", "../x", "/x") are rejected.
  """
  imports = [name.strip() for name in names or [] if name.strip()]
  if not imports:
    raise invalid_configuration(_NO_IMPORTS_ERROR)
  for name in imports:
    path = posixpath.normpath(name)
    if path in (".", "..") or path.startswith("../") or posixpath.isabs(path):
      raise invalid_configuration(
          f"Invalid import {name!r}: an import is a subdirectory of the input"
          " directory.")
  return imports


def default_max_workers(uses_gcs: bool) -> int:
  """Returns the default number of worker processes that import files.

  Each worker is a separate process that holds its own copy of the parsed data
  for the file it imports, so for local runs the default is the number of CPUs
  this process may run on: more workers add memory without adding throughput.
  Workers of a run that reads or writes GCS (`uses_gcs`) also wait on it, so
  that default is `_GCS_WORKERS_PER_CPU` per CPU, up to `_MAX_GCS_WORKERS`, and
  never fewer than the CPUs.

  `os.sched_getaffinity` is preferred over `os.cpu_count` because it honors the
  CPU affinity mask and cpusets (e.g. a container or Cloud Batch task pinned to
  a subset of the host's CPUs), whereas `os.cpu_count` reports every CPU on the
  host. It is not available on every platform (e.g. macOS), hence the fallback.
  """
  try:
    cpus = len(os.sched_getaffinity(0))
  except AttributeError:
    cpus = os.cpu_count() or 1
  if not uses_gcs:
    return cpus
  return max(cpus, min(_GCS_WORKERS_PER_CPU * cpus, _MAX_GCS_WORKERS))


def num_import_workers(max_workers: int, num_tasks: int) -> int:
  """Returns how many worker processes to start: never more than the tasks."""
  return min(max_workers, num_tasks)


def _create_importer_for_file(
    config: Config,
    input_file: File,
    graph_writer: GraphWriter,
    nodes: Nodes,
    part: FilePart | None = None,
) -> Importer:
  """Returns the importer of `input_file`, or of its `part` (observations)."""
  if input_file.path.lower().endswith(".mcf"):
    return McfImporter(
        input_file=input_file,
        graph_writer=graph_writer,
        nodes=nodes,
    )

  import_type = config.import_type(input_file)

  match import_type:

    case ImportType.OBSERVATIONS:
      return VariablePerRowImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          nodes=nodes,
          part=part,
      )

    case ImportType.ENTITIES:
      return EntitiesImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          nodes=nodes,
      )

    case _:
      raise ValueError(
          f"Unsupported import type: {import_type} ({input_file.full_path()})")


@dataclass
class ImportProcResult:
  """What a worker process sends back to the coordinator for one task.

  A task is a file, or a part of a split file (see `_ImportTask`).
  `sources` and `provenances` are all of the worker's nodes, keyed as in
  `Nodes`; the coordinator merges them with `Nodes.merge_metadata`. The
  collision fields mirror the worker's `JsonLdStreamWriter` stats, which cover
  collisions within the task only. `counts` are the importer's counts of the
  nodes it wrote, for the ingestion report. `obs_ids_path` is where the
  task's observation ids were saved, or None if it had none.
  """
  file_collision_counts: dict[str, int]
  file_sample_collisions: dict[str, list[str]]
  sources: dict[str, Source]
  provenances: dict[str, Provenance]
  processed_imports: set[str]
  counts: ImportCounts
  obs_ids_path: str | None


@dataclass(frozen=True)
class _WorkerArgs:
  """Arguments that are the same for every worker process in a run."""
  output_dir_path: str
  import_names: list[str]
  config: ImportConfig
  jsonld_dir_name: str
  # The local dir where workers save observation ids for the coordinator.
  obs_ids_dir: str


@dataclass(frozen=True)
class _ImportTask:
  """What a worker process imports: a file, or one part of a split file.

  `size` is the bytes the task reads.
  """
  file_rel_path: str
  gcs_object: GcsObject | None
  input_dir_path: str
  size: int
  part: FilePart | None = None  # None for the whole file.

  @classmethod
  def of(cls, file: File, part: FilePart | None = None) -> "_ImportTask":
    size = part.size if part else (file.size() or 0)
    return cls(file.path, file.gcs_object(), file.store_path(), size, part)


@dataclass
class _FileFailure:
  """A file whose import or merge raised, with the traceback text.

  `traceback` includes the worker process's remote traceback.
  """
  file_path: str
  error: Exception
  traceback: str


class FileImportsFailedError(Exception):
  """Raised once every file has run if several of them failed to import."""

  def __init__(self, failures: list[_FileFailure]) -> None:
    self.failures = failures
    details = "\n".join(f"  {f.file_path}: {type(f.error).__name__}: {f.error}"
                        for f in failures)
    super().__init__(f"{len(failures)} files failed to import:\n{details}")


def _submit_largest_first(
    sizes: list[int], submit: Callable[[int], concurrent.futures.Future]
) -> list[concurrent.futures.Future]:
  """Submits task i with `submit(i)`, largest `sizes[i]` first.

  A large task submitted last would run alone at the end of the run. Returns
  the futures in task order, so that results are still merged in that order.
  """
  futures: list[concurrent.futures.Future | None] = [None] * len(sizes)
  for i in sorted(range(len(sizes)), key=lambda i: sizes[i], reverse=True):
    futures[i] = submit(i)
  return futures


# Imports a split file whole, given the results of its parts that succeeded.
_ImportWhole = Callable[[File, list[ImportProcResult]],
                        concurrent.futures.Future]


def _finish_imports(files: list[File],
                    futures_by_file: list[list[concurrent.futures.Future]],
                    merge: Callable[[File, list[ImportProcResult]], None],
                    import_whole: _ImportWhole) -> list[_FileFailure]:
  """Waits for every file, and returns those that failed.

  `futures_by_file[i]` are the tasks of `files[i]`: the whole file, or each of
  its parts. Every file runs to completion even if others fail. Files are
  awaited in order, so passing each successful one to `merge`, with the
  results of its tasks, is deterministic; each merged one is logged as
  "[i/N] Imported file", once whatever its parts. A file whose import or merge
  raises is a failure, once whatever its parts.

  A split file with a failed part is imported whole instead (see
  `_file_results`).
  """
  failures: list[_FileFailure] = []
  imported = 0
  for file, futures in zip(files, futures_by_file, strict=True):
    try:
      merge(file, _file_results(file, futures, import_whole))
      imported += 1
      logging.info("[%d/%d] Imported file: %s", imported, len(files), file.path)
    except Exception as e:
      failures.append(
          _FileFailure(file.path, e, "".join(traceback.format_exception(e))))
  return failures


def _file_results(file: File, futures: list[concurrent.futures.Future],
                  import_whole: _ImportWhole) -> list[ImportProcResult]:
  """Returns the results of a file's tasks; raises the file's error, if any.

  If a part of a split file fails, the file is imported whole with
  `import_whole` once all its parts are done (see `stats.csv_split` for why a
  part can fail where the whole file doesn't), so that its output, or its
  error, is that of the whole file.
  """
  if len(futures) == 1:
    return [futures[0].result()]
  results, errors = [], []
  for future in futures:
    try:
      results.append(future.result())
    except Exception as e:  # pylint: disable=broad-except
      errors.append(e)
  if not errors:
    return results
  logging.warning(
      "%d of the %d parts of %s failed to import, so the file is imported"
      " whole. The first error: %s: %s", len(errors), len(futures), file.path,
      type(errors[0]).__name__, errors[0])
  return [import_whole(file, results).result()]


def _raise_for_failures(failures: list[_FileFailure]) -> None:
  """Logs each failure's traceback, then raises.

  A single failure is re-raised as is, so its type and message reach the
  caller unchanged. Several are raised together as one `FileImportsFailedError`
  that names every failed file.
  """
  for failure in failures:
    logging.error("Failed to import %s:\n%s", failure.file_path,
                  failure.traceback)
  if len(failures) == 1:
    raise failures[0].error
  raise FileImportsFailedError(failures)


def _run_single_import_proc(task: _ImportTask,
                            args: _WorkerArgs) -> ImportProcResult:
  """Imports one file, or part of one, in a worker process; returns its results.

  The worker starts from an empty `Nodes`: a CSV needs only its provenance id,
  which comes from config. The Provenance and Source nodes that an MCF file
  defines are returned for the coordinator to merge. The task's observation
  ids, if any, are saved to a new file in `args.obs_ids_dir` (see
  `stats.obs_id_collisions`). The coordinator found the file, and its GCS
  object if any, so neither is looked up again.
  """
  input_file = open_dir(task.input_dir_path).open_file(
      task.file_rel_path, gcs_object=task.gcs_object)

  config = Config(args.config)
  nodes = Nodes(config=config)
  graph_writer = JsonLdStreamWriter(
      open_dir(args.output_dir_path),
      args.import_names,
      config,
      jsonld_dir_name=args.jsonld_dir_name,
      obs_shard_tag=f"p{task.part.index:03d}" if task.part else "")
  importer = _create_importer_for_file(config, input_file, graph_writer, nodes,
                                       task.part)
  try:
    importer.do_import()
  except BaseException:
    graph_writer.close_after_error()
    raise
  graph_writer.commit_and_close()
  obs_ids_path = None
  if len(graph_writer.obs_ids):
    fd, obs_ids_path = tempfile.mkstemp(suffix=".npy", dir=args.obs_ids_dir)
    os.close(fd)
    save_obs_ids(graph_writer.obs_ids.sorted(), obs_ids_path)

  return ImportProcResult(
      file_collision_counts=dict(graph_writer.file_collision_counts),
      file_sample_collisions=dict(graph_writer.file_sample_collisions),
      sources=dict(nodes.sources),
      provenances=dict(nodes.provenances),
      processed_imports=graph_writer.processed_imports(),
      counts=importer.counts,
      obs_ids_path=obs_ids_path,
  )


def _read_config_file(file: File) -> ImportConfig:
  """Reads and validates a `config.json` file.

  Invalid JSON (e.g. an empty file) and invalid config are reported as
  INVALID_CONFIGURATION, prefixed with the file's path.
  """
  text = file.read()
  try:
    return parse_config(json.loads(text), file.full_path())
  except ValueError as e:  # Including json.JSONDecodeError.
    raise invalid_configuration(f"{file.full_path()}: {e}") from e


def _failure_error(e: BaseException,
                   file_path: str = "") -> FileValidationError:
  """The handshake error for `e`: GENERIC_ERROR unless a PreprocessorError.

  `file_path` is the file whose import raised `e`, if known.
  """
  if isinstance(e, PreprocessorError):
    return FileValidationError(file=e.file_path or file_path,
                               error_type=e.error_type,
                               error_message=str(e))
  return FileValidationError(file=file_path,
                             error_type=ValidationErrorType.GENERIC_ERROR,
                             error_message=str(e) or type(e).__name__)


def _is_archived(file: File) -> bool:
  """Whether `file` is under an archives dir, whose files aren't imported."""
  return _ARCHIVES_DIR_NAME in file.path.split("/")


def _scope_input_file(entry: InputFileConfig, rel_dir: str) -> InputFileConfig:
  """Prefixes the entry's `pattern` with `rel_dir`."""
  return entry.model_copy(update={"pattern": join_path(rel_dir, entry.pattern)})


class Runner:
  """Runs and coordinates all imports."""

  def __init__(
      self,
      input_dir_path: str,
      output_dir_path: str,
      import_names: Optional[list[str]] = None,
      max_workers: Optional[int] = None,
      split_csv_bytes: Optional[int] = None,
  ) -> None:
    """Sets up the output dir; run() reads the config and the input dir.

    Args:
      max_workers: The maximum number of worker processes that import files
        concurrently. Defaults to `default_max_workers()`. Lower it to reduce
        peak memory, since each worker holds the data of the file it imports.
      split_csv_bytes: Observation CSVs larger than this are imported in
        parts, in parallel (see `csv_split.num_parts`). Defaults to
        `csv_split.SPLIT_CSV_BYTES`.
    """
    if max_workers is not None and max_workers < 1:
      raise ValueError(f"max_workers must be at least 1, got {max_workers}")
    if split_csv_bytes is not None and split_csv_bytes < 1:
      raise ValueError(
          f"split_csv_bytes must be at least 1, got {split_csv_bytes}")

    if not input_dir_path:
      raise ValueError("input_dir must be specified")
    if not output_dir_path:
      raise ValueError("output_dir must be specified")

    self.input_dir_path = input_dir_path
    self.import_names = import_names
    self.active_import_prefixes = None

    # Output directories. Set up before the config is read so that a bad
    # config is reported like any other failure.
    self.output_dir = open_dir(output_dir_path, create=True)

    # If None, _load_config() sets the default, which depends on whether any
    # input or output dir is on GCS.
    self.max_workers: int | None = max_workers
    self.split_csv_bytes = split_csv_bytes
    # Set by _load_config().
    self.input_dir: Dir | None = None
    self.config: Config | None = None
    self.nodes: Nodes | None = None
    # Every file under an input dir, by the dir's path relative to the input
    # root ("" for the root), filled as _list_input_dir() lists each dir.
    self._input_dir_files: dict[str, list[File]] = {}
    # Set while importing.
    self.graph_writer = None
    # The errors for the FAILURE handshake, set when the run fails.
    self._failure_errors: list[FileValidationError] | None = None
    # Errors in input files, each with its file, so the report can put them
    # under the file's folder and provenance.
    self._file_errors: list[tuple[File, FileValidationError]] = []

    self.summary_generator = SummaryGenerator(
        os.getenv("WORKFLOW_EXECUTION_ID"))

  def _open_input_dir(self, path: str) -> Dir:
    self.input_dir = open_dir(path)
    return self.input_dir

  def _load_config(self) -> None:
    """Opens the input dir, and reads and validates the config."""
    imports = _import_names_or_raise(self.import_names)
    # Reports and the writer name the run's folders after the cleaned names.
    self.import_names = imports

    # Case A: Bulk Load (ALL_IMPORTS)
    if imports == [constants.ALL_IMPORTS]:
      logging.info("Running bulk load for all imports under: %s",
                   self.input_dir_path)
      input_dir = self._open_input_dir(self.input_dir_path)
      configs = self._read_configs_from_subdirs(input_dir)
      self.active_import_prefixes = set(
          f"{posixpath.dirname(c.path)}/" for c in configs)

    # Case B: Combined Load for specific imports (len > 1)
    elif len(imports) > 1:
      logging.info("Running combined load for specific imports: %s", imports)
      self._open_input_dir(self.input_dir_path)
      configs = self._read_configs_from_list(imports)
      self.active_import_prefixes = set(
          f"{posixpath.dirname(c.path)}/" for c in configs)

    # Case C: Single Import
    else:
      import_dir = join_path(self.input_dir_path, imports[0])
      logging.info("Using import specific directory: %s", import_dir)
      config_file = self._open_input_dir(import_dir).open_file(
          constants.CONFIG_JSON_FILE_NAME)
      self.config = Config(_read_config_file(config_file))

    # The input dir is searched recursively, so it must not hold the output.
    _check_not_overlapping(self.input_dir, self.output_dir)

    self.nodes = Nodes(self.config)
    if self.max_workers is None:
      self.max_workers = default_max_workers(uses_gcs=any(
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
      self._handle_workflow_handoff()

    except (Exception, KeyboardInterrupt) as e:  # A Ctrl-C fails the run too.
      logging.exception("Error updating stats")

      if self._failure_errors is None:
        self._failure_errors = [_failure_error(e)]

      self._record_report_errors()
      self._write_report(RunStatus.FAILURE)

      handshake_path = self._handle_workflow_handoff()
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
    return self._input_folder_name(file), self.nodes.provenance_id(file)

  def _input_folder_name(self, file: File) -> str:
    """Returns the import an input file belongs to, for the report.

    With several imports or ALL_IMPORTS, the import folder the file is under,
    e.g. "health" for "health/data.csv" (with nested imports "a/" and "a/b/",
    the longest matching one); otherwise the one import --imports named.
    """
    prefixes = [
        prefix for prefix in self.active_import_prefixes or []
        if file.path.startswith(prefix)
    ]
    if not prefixes:
      return self.import_names[0]
    return max(prefixes, key=len).rstrip("/")

  def _report_folder_names(self) -> list[str]:
    """Returns the import folders this run was asked to import."""
    if self.import_names == [constants.ALL_IMPORTS]:
      # The folders found under the input dir, e.g. "folderA/" -> "folderA".
      return sorted(
          prefix.rstrip("/") for prefix in self.active_import_prefixes or [])
    return list(self.import_names or [])

  def _record_provenance_sources(self) -> None:
    """Records each known provenance's source in the ingestion report."""
    for provenance in self.nodes.provenances.values():
      self.summary_generator.set_provenance_data(provenance.id,
                                                 provenance.source_id)

  def _write_report(self, status: RunStatus) -> None:
    """Writes the ingestion report with the run's final status."""
    # Requested folders are listed even if no input files were found in them.
    self.summary_generator.add_folders(self._report_folder_names())
    if self.nodes:  # No provenances if the config failed to load.
      self._record_provenance_sources()
    self.summary_generator.write_to_json(status, _utc_now())

  def _handle_workflow_handoff(self) -> str | None:
    """Writes processed import metadata to a GCS handshake file for the ingestion workflow.

    Returns the handshake's path, or None if none was written.
    """
    workflow_id = os.getenv("WORKFLOW_EXECUTION_ID")
    if self._failure_errors is not None:
      errors_dict = [err.to_dict() for err in self._failure_errors]
      handshake_payload = {"status": "FAILURE", "errors": errors_dict}
    elif workflow_id and is_gcs_path(self.graph_writer.jsonld_dir.full_path()):
      handshake_payload = {"importList": json.dumps(self._import_list())}
    else:
      return None  # A successful run outside the workflow, or to local disk.

    temp_location = os.getenv("TEMP_LOCATION", "")

    if not workflow_id:
      logging.warning("WORKFLOW_EXECUTION_ID not set. Skipping GCS handshake.")
      return None

    if not temp_location.startswith("gs://"):
      logging.warning(
          "TEMP_LOCATION (%s) is not a GCS path. Skipping GCS handshake.",
          temp_location)
      return None

    output_json_path = f"{temp_location.rstrip('/')}/datacommons/ingestion_records/{workflow_id}.json"
    logging.info(
        "WORKFLOW_EXECUTION_ID set. Writing preprocessor result to GCS handshake path: %s",
        output_json_path)
    write_file(output_json_path, json.dumps(handshake_payload, indent=2))

    if self._failure_errors is None:
      logging.info("Successfully wrote preprocessor result to GCS.")
    return output_json_path

  def _import_list(self) -> list[dict[str, str]]:
    """The imports and their JSON-LD shards, for the ingestion workflow."""
    jsonld_path = self.graph_writer.jsonld_dir.full_path().rstrip("/")
    return [{
        "importName": name,
        "graphPath": f"{jsonld_path}/{name.replace('/', '_')}/**/*.jsonld"
    } for name in self.graph_writer.import_folders()]

  def _merge_configs(self, configs: list):
    """Merges multiple config.json files into a single configuration.

    Args:
      configs: A list of File objects representing the config.json files to
        merge. Each one's patterns are scoped to its dir.
    """
    input_files = []
    for file in configs:
      config = _read_config_file(file)
      rel_dir = posixpath.dirname(file.path)
      logging.info("Merging config from import directory: %s", rel_dir)
      input_files.extend(
          _scope_input_file(entry, rel_dir) for entry in config.input_files)

    self.config = Config(ImportConfig(input_files=input_files))

  def _list_input_dir(self, path: str) -> list[File]:
    """Returns every file under input dir `path` ("" for the input root).

    Each dir is listed once per run, and its listing reused.
    """
    directory = self.input_dir.open_dir(path, create=False)
    if directory.path not in self._input_dir_files:
      self._input_dir_files[directory.path] = directory.all_files(
          include_subdirs=True)
    return self._input_dir_files[directory.path]

  def _find_configs_in_dir(self, path: str) -> list[File]:
    """Finds all config.json files under input dir `path`, excluding archives.

    Shallower configs come first, then by path, so that their patterns are
    matched first; e.g. "a/config.json" before "a/b/config.json".
    """
    configs = [
        file for file in self._list_input_dir(path)
        if file.name() == constants.CONFIG_JSON_FILE_NAME and
        not _is_archived(file)
    ]
    return sorted(configs, key=lambda f: (f.path.count("/"), f.path))

  def _read_configs_from_subdirs(self, base_dir: Dir) -> list:
    """Scans subdirectories for config.json files and merges them.
    
    Args:
      base_dir: The base directory to scan.
      
    Raises:
      FileNotFoundError: If no config.json files are found.
    """
    configs = self._find_configs_in_dir(base_dir.path)
    logging.info("Found %s config files in subdirectories.", len(configs))
    if not configs:
      raise FileNotFoundError(
          f"No config.json files found in subdirectories of {base_dir.full_path()}"
      )
    self._merge_configs(configs)
    return configs

  def _read_configs_from_list(self, import_names: list[str]) -> list:
    """Reads and merges the configs of the imports in `import_names`.

    An import's config.json is the one at its root, or else those in its
    subdirectories. Each import dir is listed once; finding input files reuses
    the listing.

    Raises:
      FileNotFoundError: If an import dir is missing or empty, or has no
        config.json.
    """
    configs = []
    for name in import_names:
      files = self._list_input_dir(name)
      if not files:
        raise FileNotFoundError(f"Import directory not found or empty: {name}")
      root_config = join_path(name, constants.CONFIG_JSON_FILE_NAME)
      found = [f for f in files if f.path == root_config]
      if not found:
        logging.info(
            "Config file not found at root of %s. Scanning subdirectories.",
            name)
        found = self._find_configs_in_dir(name)
      if not found:
        raise FileNotFoundError(f"No config files found for {name}")
      configs.extend(found)

    logging.info("Found %s config files from list.", len(configs))
    self._merge_configs(configs)
    return configs

  def _is_active_import_file(self, file: File) -> bool:
    """Returns True if `file` is not archived and belongs to an active import."""
    if _is_archived(file):
      return False
    if not self.active_import_prefixes:
      return True
    return any(
        file.path.startswith(prefix) for prefix in self.active_import_prefixes)

  def _matches_config_pattern(self, file: File) -> bool:
    """Returns True if `file` matches at least one `inputFiles` pattern."""
    return any(match(file, pattern) for pattern in self.config.patterns())

  def _list_input_files(self) -> list[File]:
    """Returns the files under the run's imports.

    With several imports, only their dirs are listed, not the whole input dir.
    """
    if len(self.import_names) == 1:  # One import, or ALL_IMPORTS.
      return self._list_input_dir("")
    files_by_path = {}
    for name in self.import_names:
      # Nested imports, e.g. "a" and "a/b", list some files twice.
      for file in self._list_input_dir(name):
        files_by_path[file.path] = file
    return list(files_by_path.values())

  def _find_and_filter_input_files(self) -> tuple[list[File], list[File]]:
    """Discovers, filters, sorts, and returns matched CSV and MCF files."""
    input_files = self._list_input_files()
    csv_files: list[File] = []
    mcf_files: list[File] = []

    for file in input_files:
      if not self._is_active_import_file(file):
        continue
      if not self._matches_config_pattern(file):
        continue
      if match(file, "*.csv"):
        csv_files.append(file)
      elif match(file, "*.mcf"):
        mcf_files.append(file)

    # Sort alphabetically to guarantee consistent order
    csv_files.sort(key=lambda f: f.full_path())
    mcf_files.sort(key=lambda f: f.full_path())
    return csv_files, mcf_files

  def _validate_single_file_headers(self,
                                    file: File) -> list[FileValidationError]:
    """Validates headers for a single CSV file and returns any errors found."""
    try:
      return _create_importer_for_file(self.config, file, self.graph_writer,
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

  def _run_multiprocess_imports(
      self, files: list[File], parts_by_path: dict[str,
                                                   list[FilePart]]) -> None:
    """Imports `files` in worker processes and merges each result.

    A file in `parts_by_path` is imported in those parts, in parallel. Tasks
    are submitted largest first. See `_finish_imports` for the merge order and
    for failures. Once every file is merged, looks for observation ids that
    several files produced.
    """
    tasks_by_file = [[
        _ImportTask.of(file, part) for part in parts_by_path.get(file.path, [])
    ] or [_ImportTask.of(file)] for file in files]
    tasks = [task for file_tasks in tasks_by_file for task in file_tasks]
    num_workers = num_import_workers(self.max_workers, len(tasks))
    logging.info("Importing %d files in %d tasks with %d worker processes",
                 len(files), len(tasks), num_workers)
    # Workers run on this machine, so they can save ids to a local directory.
    # A failed run doesn't wait for the files in progress, which may still save
    # ids while the directory is removed.
    with tempfile.TemporaryDirectory(prefix="obs-ids-",
                                     ignore_cleanup_errors=True) as obs_ids_dir:
      worker_args = _WorkerArgs(
          output_dir_path=self.output_dir.full_path(),
          import_names=self.import_names,
          config=self.config.model,
          jsonld_dir_name=self.graph_writer.jsonld_dir.name(),
          obs_ids_dir=obs_ids_dir,
      )
      # (input file path, ids path) of every task with observations, in file
      # and then part order.
      obs_ids_files: list[tuple[str, str]] = []

      def merge(file: File, results: list[ImportProcResult]) -> None:
        self._merge_file_results(file, results)
        obs_ids_files.extend((file.path, res.obs_ids_path)
                             for res in results
                             if res.obs_ids_path)

      executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)

      def import_whole(
          file: File,
          part_results: list[ImportProcResult]) -> concurrent.futures.Future:
        self._discard_part_outputs(file, part_results)
        return executor.submit(_run_single_import_proc, _ImportTask.of(file),
                               worker_args)

      try:
        futures = iter(
            _submit_largest_first(
                [task.size for task in tasks], lambda i: executor.submit(
                    _run_single_import_proc, tasks[i], worker_args)))
        futures_by_file = [
            [next(futures) for _ in file_tasks] for file_tasks in tasks_by_file
        ]
        failures = _finish_imports(files,
                                   futures_by_file,
                                   merge=merge,
                                   import_whole=import_whole)
      except BaseException:
        # E.g. Ctrl-C: files not yet started are cancelled, and the run fails
        # without waiting for the ones in progress. (Not a `with` block: its
        # exit would wait for them, and reset cancel_futures.)
        executor.shutdown(wait=False, cancel_futures=True)
        raise
      executor.shutdown()
      if failures:
        # The handshake has an error per failed file, with its path.
        self._failure_errors = [
            _failure_error(f.error, f.file_path) for f in failures
        ]
        files_by_path = {file.path: file for file in files}
        self._file_errors.extend(
            (files_by_path[f.file_path], error)
            for f, error in zip(failures, self._failure_errors))
        _raise_for_failures(failures)
      self._merge_cross_file_collisions(obs_ids_files)

  def _discard_part_outputs(self, file: File,
                            part_results: list[ImportProcResult]) -> None:
    """Deletes the shards and observation ids that `file`'s parts wrote."""
    self.graph_writer.delete_part_shards(file)
    for res in part_results:
      if res.obs_ids_path:
        os.remove(res.obs_ids_path)

  def _merge_cross_file_collisions(
      self, obs_ids_files: list[tuple[str, str]]) -> None:
    """Adds the collisions between `obs_ids_files` to the writer's summary.

    `obs_ids_files` are (input file path, ids path) of every file, or part of
    a split file, with observations, in import order. Collisions between the
    parts of a file are the file's own, as if it were imported whole.
    """
    if len(obs_ids_files) < 2:
      return
    logging.info("Checking observation ids of %d files for collisions...",
                 len({path for path, _ in obs_ids_files}))
    collisions = find_cross_file_collisions(obs_ids_files)
    if collisions.count:
      logging.warning(
          "Found %d observation @id collisions between files, or between parts"
          " of a split file: they produce identical observation ids.",
          collisions.count)
    self.graph_writer.merge_collisions(collisions.file_counts,
                                       collisions.file_samples)

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
    self._run_multiprocess_imports(files, self._plan_split_files(csv_files))

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

  def _plan_split_files(self,
                        csv_files: list[File]) -> dict[str, list[FilePart]]:
    """Returns the parts to import large observation CSVs in, by file path.

    Observation CSVs larger than `split_csv_bytes` are split into about one
    part per worker (see `csv_split.num_parts`). A file that can't be split,
    or whose plan fails, is imported whole.
    """
    split_bytes = self.split_csv_bytes or csv_split.SPLIT_CSV_BYTES
    stems = {observation_shard_stem(file.path) for file in csv_files}
    large_files = []
    for file in csv_files:
      parts = csv_split.num_parts(file.size() or 0, self.max_workers,
                                  split_bytes)
      if (parts > 1 and
          self.config.import_type(file) == ImportType.OBSERVATIONS and
          not _part_shards_could_collide(observation_shard_stem(file.path),
                                         stems)):
        large_files.append((file, parts))
    if not large_files:
      return {}
    with concurrent.futures.ThreadPoolExecutor(max_workers=min(
        _MAX_SPLIT_PLANNING_THREADS, len(large_files))) as executor:
      plans = list(
          executor.map(lambda f: _plan_file_parts_or_none(*f), large_files))
    parts_by_path = {
        file.path: parts
        for (file, _), parts in zip(large_files, plans)
        if parts is not None
    }
    if parts_by_path:
      logging.info("Splitting %d large files into %d parts", len(parts_by_path),
                   sum(len(parts) for parts in parts_by_path.values()))
    return parts_by_path

  def _merge_file_results(self, file: File,
                          results: list[ImportProcResult]) -> None:
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


def _part_shards_could_collide(stem: str, stems: set[str]) -> bool:
  """Whether the shards of a split file could have another file's names.

  A part's shards are named "observation-<stem>-p001-00000.jsonld", which a
  file named "<stem>-p001.csv" also writes; such a file isn't split.
  """
  part_stem = re.compile(re.escape(stem) + r"-p\d{3,}")
  return any(part_stem.fullmatch(other) for other in stems)


def _plan_file_parts_or_none(file: File, parts: int) -> list[FilePart] | None:
  """Plans `parts` parts of `file`; None to import it whole, e.g. if that fails."""
  try:
    return csv_split.plan_file_parts(file, parts)
  except Exception:  # pylint: disable=broad-except
    logging.exception("Failed to plan parts of %s; importing it whole",
                      file.path)
    return None


def _utc_now() -> str:
  """Returns the current UTC time in ISO-8601, e.g. 2026-11-02T18:45:00Z."""
  return datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")


def _comparable_path(directory: Dir) -> str:
  """The dir's URL on GCS; its absolute path, with ~ and $VARS expanded, on
  local disk, where the paths shown keep them as given."""
  path = directory.full_path()
  if is_gcs_path(path):
    return path
  return os.path.abspath(os.path.expanduser(os.path.expandvars(path or ".")))


def _check_not_overlapping(input_dir: Dir, output_dir: Dir):
  input_path = _comparable_path(input_dir)
  output_path = _comparable_path(output_dir)

  # Check if paths are the same or if one is a parent of the other.
  if is_within(input_path, output_path) or is_within(output_path, input_path):
    raise ValueError(f"Input path ({input_dir.full_path()}) overlaps with"
                     f" output dir ({output_dir.full_path()})")
