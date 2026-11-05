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
import json
import logging
import os
import traceback
from typing import Callable, Optional

import fs.path as fspath
from stats import constants
from stats.config import Config
from stats.data import FileValidationError
from stats.data import ImportType
from stats.data import Provenance
from stats.data import Source
from stats.data import ValidationErrorType
from stats.entities_importer import EntitiesImporter
from stats.graph_writer import GraphWriter
from stats.importer import Importer
from stats.jsonld_stream_writer import JsonLdStreamWriter
from stats.mcf_importer import McfImporter
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from stats.validation import MetadataValidator
from stats.variable_per_row_importer import VariablePerRowImporter
from util.file_match import match
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import join_path
from util.filesystem import Store

_ARCHIVES_DIR_NAME = "archives"
# Upper bound on the worker processes that import files, and on the threads
# that validate CSV headers.
_MAX_WORKERS = 32


def _create_importer_for_file(
    config: Config,
    input_file: File,
    graph_writer: GraphWriter,
    reporter: FileImportReporter,
    nodes: Nodes,
) -> Importer:
  if input_file.path.lower().endswith(".mcf"):
    return McfImporter(
        input_file=input_file,
        graph_writer=graph_writer,
        reporter=reporter,
        nodes=nodes,
    )

  import_type = config.import_type(input_file)

  match import_type:

    case ImportType.OBSERVATIONS:
      return VariablePerRowImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          reporter=reporter,
          nodes=nodes,
      )

    case ImportType.ENTITIES:
      return EntitiesImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          reporter=reporter,
          nodes=nodes,
      )

    case _:
      raise ValueError(
          f"Unsupported import type: {import_type} ({input_file.full_path()})")


@dataclass
class ImportProcResult:
  """What a worker process sends back to the coordinator for one file.

  `sources` and `provenances` are all of the worker's nodes, keyed as in
  `Nodes`; the coordinator merges them with `Nodes.merge_metadata`. The
  collision fields mirror the worker's `JsonLdStreamWriter` stats.
  """
  file_rel_path: str
  obs_collision_count: int
  file_collision_counts: dict[str, int]
  file_sample_collisions: dict[str, list[str]]
  sources: dict[str, Source]
  provenances: dict[str, Provenance]
  processed_imports: set[str]


@dataclass(frozen=True)
class _WorkerArgs:
  """Arguments that are the same for every worker process in a run."""
  output_dir_path: str
  process_dir_path: str
  import_names: list[str] | None
  config_json_str: str
  jsonld_dir_name: str


@dataclass
class _FileFailure:
  """A file whose import raised, with the traceback text.

  With multiprocessing, `traceback` includes the worker's remote traceback.
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


def _finish_imports(files: list[File], futures: list[concurrent.futures.Future],
                    merge: Callable[[ImportProcResult], None]) -> None:
  """Waits for every file, then raises if any of them failed.

  Every file runs to completion even if others fail. Results are awaited in
  submission order, so passing each successful one to `merge` is
  deterministic. See `_raise_for_failures` for what is raised.
  """
  failures: list[_FileFailure] = []
  for file, future in zip(files, futures, strict=True):
    try:
      result = future.result()
    except Exception as e:
      failures.append(
          _FileFailure(file.path, e, "".join(traceback.format_exception(e))))
      continue
    merge(result)
  _raise_for_failures(failures)


def _raise_for_failures(failures: list[_FileFailure]) -> None:
  """Logs each failure's traceback, then raises.

  A single failure is re-raised as is, so its type and message reach the
  report and handshake unchanged. Several are raised together as one
  `FileImportsFailedError` that names every failed file.
  """
  if not failures:
    return
  for failure in failures:
    logging.error("Failed to import %s:\n%s", failure.file_path,
                  failure.traceback)
  if len(failures) == 1:
    raise failures[0].error
  raise FileImportsFailedError(failures)


def _run_single_import_proc(file_rel_path: str, input_dir_path: str,
                            args: _WorkerArgs) -> ImportProcResult:
  """Imports one file in a worker process and returns its results.

  The worker starts from an empty `Nodes`: a CSV needs only its provenance id,
  which comes from config. The Provenance and Source nodes that an MCF file
  defines are returned for the coordinator to merge.
  """
  with create_store(input_dir_path) as input_store_obj, \
       create_store(args.output_dir_path) as output_store_obj, \
       create_store(args.process_dir_path) as process_store_obj:
    input_dir = input_store_obj.as_dir()
    output_store = output_store_obj.as_dir()
    process_store = process_store_obj.as_dir()
    input_store = input_dir.open_file(file_rel_path)

    config = Config(json.loads(args.config_json_str))
    nodes = Nodes(config=config)
    graph_writer = JsonLdStreamWriter(output_store,
                                      args.import_names,
                                      nodes,
                                      jsonld_dir_name=args.jsonld_dir_name)

    sanitized_path = input_store.full_path().replace("://",
                                                     "_").replace("/", "_")
    report_file = process_store.open_file(f"report_{sanitized_path}.json")
    reporter = ImportReporter(report_file).get_file_reporter(input_store)

    importer = _create_importer_for_file(config, input_store, graph_writer,
                                         reporter, nodes)
    importer.do_import()
    graph_writer.commit_and_close()

    return ImportProcResult(
        file_rel_path=file_rel_path,
        obs_collision_count=graph_writer.obs_collision_count,
        file_collision_counts=dict(graph_writer.file_collision_counts),
        file_sample_collisions=dict(graph_writer.file_sample_collisions),
        sources=dict(nodes.sources),
        provenances=dict(nodes.provenances),
        processed_imports=set(graph_writer._processed_imports),
    )


def _read_config_dict(file: File) -> dict:
  """Reads and parses a `config.json` file, validating its structure and keys."""
  try:
    config_data = json.loads(file.read())
  except json.JSONDecodeError as e:
    logging.error("Failed to parse JSON from %s: %s", file.full_path(), e)
    raise
  if not isinstance(config_data, dict):
    raise ValueError(f"Expected JSON object in {file.full_path()}, "
                     f"got {type(config_data).__name__}")
  try:
    Config(config_data)
  except ValueError as e:
    raise ValueError(f"{file.full_path()}: {e}") from e
  return config_data


def _scope_input_file_entry(entry: object, rel_dir: str, import_name: str,
                            config_path: str) -> dict:
  """Prefixes `pattern`/`filename` with `rel_dir` and attaches `_import_name`."""
  if not isinstance(entry, dict):
    raise ValueError(f"Invalid entry in 'inputFiles' in {config_path}: "
                     f"must be a JSON object. Got: {entry}")
  scoped = dict(entry, _import_name=import_name)
  for key_field in ("pattern", "filename"):
    if key_field in scoped:
      scoped[key_field] = fspath.join(rel_dir, scoped[key_field])
  return scoped


class Runner:
  """Runs and coordinates all imports."""

  def __init__(
      self,
      config_file_path: str,
      input_dir_path: str,
      output_dir_path: str,
      import_names: Optional[list[str]] = None,
  ) -> None:
    assert (config_file_path or
            input_dir_path), "One of config_file or input_dir must be specified"
    assert output_dir_path, "output_dir must be specified"

    self.import_names = import_names
    self.active_import_prefixes = None

    # File systems, both input and output. Must be closed when run finishes.
    self.all_stores: list[Store] = []
    # Input-only stores
    self.input_stores: list[Store] = []

    # Config file driven (input paths pulled from config)
    if config_file_path:
      self._read_config_from_file(config_file_path)

      input_urls = self.config.data_download_urls()
      if not input_urls:
        raise ValueError("Data Download URLs not found in config.")
      for input_url in input_urls:
        input_store = create_store(input_url)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)

    # Input dir driven (config file found in input dir)
    else:
      effective_input_dir = input_dir_path
      imports = self.import_names or []
      self.active_import_prefixes = None

      # Case A: Bulk Load (ALL_IMPORTS)
      if imports == [constants.ALL_IMPORTS]:
        logging.info("Running bulk load for all imports under: %s",
                     effective_input_dir)
        input_store = create_store(effective_input_dir)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)
        configs = self._read_configs_from_subdirs(input_store.as_dir())
        self.active_import_prefixes = set(
            f"{fspath.dirname(c.path)}/" for c in configs)

      # Case B: Combined Load for specific imports (len > 1)
      elif len(imports) > 1:
        logging.info("Running combined load for specific imports: %s", imports)
        input_store = create_store(effective_input_dir)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)
        configs = self._read_configs_from_list(input_store.as_dir(), imports)
        self.active_import_prefixes = set(
            f"{fspath.dirname(c.path)}/" for c in configs)

      # Case C: Single Import
      elif imports:
        effective_input_dir = join_path(input_dir_path, imports[0])
        logging.info("Using import specific directory: %s", effective_input_dir)
        input_store = create_store(effective_input_dir)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)
        self._read_config_from_file(
            config_file_path=constants.CONFIG_JSON_FILE_NAME,
            config_file_dir=input_store.as_dir(),
        )

      # Case D: Default action (config at root)
      else:
        input_store = create_store(effective_input_dir)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)
        self._read_config_from_file(
            config_file_path=constants.CONFIG_JSON_FILE_NAME,
            config_file_dir=input_store.as_dir(),
        )

    # New option to traverse subdirs of input dir(s). Defaults to false.
    self.include_input_subdirs = self.config.include_input_subdirs()

    # Output directories
    output_store = create_store(output_dir_path, create_if_missing=True)
    if self.include_input_subdirs:
      for input_store in self.input_stores:
        _check_not_overlapping(input_store, output_store)
    self.all_stores.append(output_store)
    self.output_dir = output_store.as_dir()
    self.process_dir = self.output_dir.open_dir(constants.PROCESS_DIR_NAME)

    # Reporter.
    self.reporter = ImportReporter(
        report_file=self.process_dir.open_file(constants.REPORT_JSON_FILE_NAME))

    self.nodes = Nodes(self.config)
    self.graph_writer = None
    self.trigger_workflow_info = None

  def run(self):
    try:
      self._run_imports_and_export_jsonld()

      self.graph_writer.commit_and_close()

      # Report done.
      self.reporter.report_done()

      # Close all file storage.
      for store in self.all_stores:
        store.close()
      logging.info("File storage closed.")

      # Hand off processed import metadata to workflow or GCS
      self._handle_workflow_handoff()

    except Exception as e:
      logging.exception("Error updating stats")
      self.reporter.report_failure(error=str(e))

      if not hasattr(self, "_failure_errors"):
        file_path = getattr(e, "file_path", "")
        self._failure_errors = [
            FileValidationError(file=file_path,
                                error_type=ValidationErrorType.GENERIC_ERROR,
                                error_message=str(e))
        ]

      for store in self.all_stores:
        try:
          store.close()
        except Exception:
          pass

      self._handle_workflow_handoff()
      logging.error(
          "Preprocessor execution failed! Diagnostic error details were written to the GCS handshake path."
      )
      raise

  def _handle_workflow_handoff(self) -> None:
    """Writes processed import metadata to a GCS handshake file for the ingestion workflow."""
    if not self.trigger_workflow_info and not hasattr(self, "_failure_errors"):
      return

    workflow_id = os.getenv("WORKFLOW_EXECUTION_ID")
    temp_location = os.getenv("TEMP_LOCATION", "")

    if not workflow_id:
      logging.warning("WORKFLOW_EXECUTION_ID not set. Skipping GCS handshake.")
      return

    if not temp_location.startswith("gs://"):
      logging.warning(
          "TEMP_LOCATION (%s) is not a GCS path. Skipping GCS handshake.",
          temp_location)
      return

    if hasattr(self, "_failure_errors"):
      errors_dict = [
          err.to_dict() if hasattr(err, "to_dict") else err
          for err in self._failure_errors
      ]
      handshake_payload = {"status": "FAILURE", "errors": errors_dict}
    else:
      handshake_payload = {
          "importList": json.dumps(self.trigger_workflow_info),
          "generateStatVarGroups": bool(self.config.generate_hierarchy()),
      }

    output_json_path = f"{temp_location.rstrip('/')}/datacommons/ingestion_records/{workflow_id}.json"
    logging.info(
        "WORKFLOW_EXECUTION_ID set. Writing preprocessor result to GCS handshake path: %s",
        output_json_path)
    with create_store(output_json_path,
                      create_if_missing=True,
                      treat_as_file=True) as store:
      store.as_file().write(json.dumps(handshake_payload, indent=2))

    if hasattr(self, "_failure_errors"):
      logging.info(
          "Successfully wrote preprocessor error report to GCS handshake path: %s. "
          "Structured error details are available in this file for diagnostic parsing.",
          output_json_path)
    else:
      logging.info("Successfully wrote preprocessor result to GCS.")

  def _read_config_from_file(self,
                             config_file_path: str,
                             config_file_dir: Optional[Dir] = None) -> Config:
    if config_file_dir:
      raw_config = config_file_dir.open_file(config_file_path,
                                             create_if_missing=False).read()
    else:
      with create_store(config_file_path) as config_store:
        raw_config = config_store.as_file().read()

    config_data = json.loads(raw_config) if raw_config else {}
    self.config = Config(data=config_data)

  def _merge_configs(self, configs: list, base_dir: Dir):
    """Merges multiple config.json files into a single configuration.

    Args:
      configs: A list of File objects representing the config.json files to merge.
      base_dir: The base directory used to calculate relative paths for input files.
    """
    merged_data = {
        "includeInputSubdirs": True,
        "inputFiles": [],
        "_dir_import_names": {}
    }

    for file in configs:
      config_data = _read_config_dict(file)
      rel_dir = fspath.relativefrom(base_dir.path, fspath.dirname(file.path))
      import_name = config_data.get("importName") or rel_dir
      logging.info("Merging config from import directory: %s", rel_dir)
      merged_data["_dir_import_names"][rel_dir] = import_name
      merged_data["inputFiles"].extend(
          _scope_input_file_entry(entry, rel_dir, import_name, file.full_path())
          for entry in config_data.get("inputFiles", []))

    self.config = Config(data=merged_data)

  def _find_configs_in_dir(self, directory: Dir) -> list:
    """Finds all config.json files in a directory, excluding archives."""
    configs = []
    for file in directory.all_files(include_subdirs=True):
      if _ARCHIVES_DIR_NAME in file.path.split("/"):
        continue
      if file.name() == constants.CONFIG_JSON_FILE_NAME:
        configs.append(file)
    return configs

  def _read_configs_from_subdirs(self, base_dir: Dir) -> list:
    """Scans subdirectories for config.json files and merges them.
    
    Args:
      base_dir: The base directory to scan.
      
    Raises:
      FileNotFoundError: If no config.json files are found.
    """
    configs = self._find_configs_in_dir(base_dir)
    logging.info("Found %s config files in subdirectories.", len(configs))
    if not configs:
      raise FileNotFoundError(
          f"No config.json files found in subdirectories of {base_dir.full_path()}"
      )
    self._merge_configs(configs, base_dir)
    return configs

  def _read_configs_from_list(self, base_dir: Dir,
                              import_names: list[str]) -> list:
    """Reads configs for specific imports specified in a list and merges them.
    
    Args:
      base_dir: The base directory containing the import subdirectories.
      import_names: A list of subdirectory names to look for configs in.
      
    Raises:
      FileNotFoundError: If no config files are found for a requested import.
    """
    configs = []
    for name in import_names:
      name = name.strip()
      target_path = fspath.join(base_dir.path, name)
      if not base_dir.fs().isdir(target_path):
        raise FileNotFoundError(f"Import directory not found: {name}")
      try:
        imp_dir = base_dir.open_dir(name)
        file = imp_dir.open_file(constants.CONFIG_JSON_FILE_NAME,
                                 create_if_missing=False)
        configs.append(file)
      except FileNotFoundError:
        logging.info(
            "Config file not found at root of %s. Scanning subdirectories.",
            name)
        sub_configs = self._find_configs_in_dir(imp_dir)
        if not sub_configs:
          raise FileNotFoundError(f"No config files found for {name}")
        configs.extend(sub_configs)
      except ValueError as e:
        logging.error("Invalid directory for import %s: %s", name, e)
        raise e

    logging.info("Found %s config files from list.", len(configs))
    self._merge_configs(configs, base_dir)
    return configs

  def _is_active_import_file(self, file: File) -> bool:
    """Returns True if `file` is not archived and belongs to an active import."""
    if _ARCHIVES_DIR_NAME in file.path.split("/"):
      return False
    if not self.active_import_prefixes:
      return True
    return any(
        file.path.startswith(prefix) for prefix in self.active_import_prefixes)

  def _matches_config_pattern(self, file: File) -> bool:
    """Returns True if `file` matches at least one `inputFiles` pattern."""
    return any(
        match(file, pattern)
        for pattern in self.config._input_files_config.keys())

  def _find_and_filter_input_files(self) -> tuple[list[File], list[File]]:
    """Discovers, filters, sorts, and returns matched CSV and MCF files."""
    input_files: list[File] = []
    for input_store in self.input_stores:
      if input_store.isdir():
        input_files.extend(input_store.as_dir().all_files(
            self.include_input_subdirs))
      else:
        input_files.append(input_store.as_file())

    csv_files: list[File] = []
    mcf_files: list[File] = []

    for file in input_files:
      if not self._is_active_import_file(file):
        continue
      if not self._matches_config_pattern(file):
        logging.info(
            "Ignoring file '%s' as it does not match any pattern in config.json",
            file.path)
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
      return self._create_importer(file).validate_headers()
    except Exception as e:
      return [
          FileValidationError(
              file=file.path,
              error_type=ValidationErrorType.GENERIC_ERROR,
              error_message=f"Failed to validate headers: {str(e)}")
      ]

  def _validate_all_headers(self, csv_files: list[File]) -> None:
    """Validates all CSV headers upfront and raises ValueError if any validation fails."""
    if not csv_files:
      return

    # Pre-populate the per-file config cache on the main thread before spawning
    # worker threads so concurrent lookups do not race on mutating the cache dict.
    for file in csv_files:
      self.config._per_file_config(file)

    all_errors = []
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(_MAX_WORKERS, len(csv_files))) as executor:
      for errors in executor.map(self._validate_single_file_headers, csv_files):
        all_errors.extend(errors)

    if all_errors:
      self._failure_errors = all_errors
      formatted_errors = [
          f"File '{err.file}': {err.error_message}" for err in all_errors
      ]
      consolidated_msg = (
          "CSV Header Validation Failed! The following errors were found:\n" +
          "\n".join(formatted_errors))
      raise ValueError(consolidated_msg)

  def _run_multiprocess_imports(self, files: list[File]) -> None:
    """Imports `files` in worker processes and merges each result.

    See `_finish_imports` for the merge order and for failures.
    """
    worker_args = _WorkerArgs(
        output_dir_path=self.output_dir.full_path(),
        process_dir_path=self.process_dir.full_path(),
        import_names=self.import_names,
        config_json_str=json.dumps(self.config.data),
        jsonld_dir_name=self.graph_writer.jsonld_dir.name(),
    )
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=min(_MAX_WORKERS, len(files))) as executor:
      futures = [
          executor.submit(_run_single_import_proc, file.path,
                          file._store.root_path, worker_args) for file in files
      ]
      _finish_imports(files, futures, merge=self._merge_worker_result)

  def _run_all_data_imports(self):
    """Finds the input files, validates CSV headers, and imports every file."""
    csv_files, mcf_files = self._find_and_filter_input_files()

    logging.info("Found %d CSV files to import", len(csv_files))
    logging.info("Found %d MCF files to import", len(mcf_files))
    logging.info("Matched files to process: %s",
                 [f.full_path() for f in csv_files + mcf_files])

    # Validate all CSV headers upfront
    self._validate_all_headers(csv_files)

    self.reporter.report_started(import_files=list(csv_files + mcf_files))

    self._completed_files_count = 0
    self._total_files_count = len(csv_files) + len(mcf_files)

    if not self._total_files_count:
      return
    logging.info("Importing %d files (%d MCF, %d CSV)...",
                 self._total_files_count, len(mcf_files), len(csv_files))
    # MCF and CSV files are imported together: a CSV needs nothing from MCF.
    files = mcf_files + csv_files
    self._run_multiprocess_imports(files)

  def _merge_worker_result(self, res: ImportProcResult) -> None:
    """Merges a worker process's `ImportProcResult` into the coordinator."""
    self._log_file_progress("Imported file", res.file_rel_path)
    self.nodes.merge_metadata(res.provenances, res.sources)
    self._merge_worker_writer_state(res)

  def _merge_worker_writer_state(self, res: ImportProcResult) -> None:
    """Merges a worker's processed imports and collision stats into the writer."""
    self.graph_writer._processed_imports.update(res.processed_imports)
    if not res.obs_collision_count:
      return
    self.graph_writer.obs_collision_count += res.obs_collision_count
    for f_name, count in res.file_collision_counts.items():
      self.graph_writer.file_collision_counts[f_name] += count
      self.graph_writer.file_sample_collisions[f_name].extend(
          res.file_sample_collisions.get(f_name, []))

  def _log_file_progress(self, file_prefix: str, file_path: str):
    """Increments file progress counter and logs standard progress line."""
    self._completed_files_count += 1
    logging.info("[%d/%d] %s: %s", self._completed_files_count,
                 self._total_files_count, file_prefix, file_path)

  def _create_importer(self, input_file: File) -> Importer:
    reporter = self.reporter.get_file_reporter(input_file)
    return _create_importer_for_file(
        self.config,
        input_file,
        self.graph_writer,
        reporter,
        self.nodes,
    )

  def _run_imports_and_export_jsonld(self):
    logging.info(
        "Initializing JsonLdStreamWriter to stream JSON-LD directly to GCS/Disk"
    )
    self.graph_writer = JsonLdStreamWriter(self.output_dir, self.import_names,
                                           self.nodes)

    # Run data imports (CSV and MCF)
    self._run_all_data_imports()

    # Generate triples from nodes grouped by provenance directory and write directly
    for prov_dir, triples in self.nodes.triples_by_provenance_dir().items():
      if prov_dir == "_global":
        self.graph_writer.write_triples(triples)
      else:
        self.graph_writer.write_triples(triples, provenance_dir=prov_dir)

    # Perform strict metadata validation before committing and closing
    MetadataValidator(self.config, self.graph_writer).validate()

    # Populate trigger workflow info if running under ingestion workflow and output is GCS
    output_path = self.graph_writer.jsonld_dir.full_path()
    import_name = self.graph_writer.import_name
    if os.getenv("WORKFLOW_EXECUTION_ID") and output_path.startswith("gs://"):
      processed_imports = sorted(list(self.graph_writer._processed_imports))
      if not processed_imports:
        processed_imports = [import_name]
      import_list = []
      for imp in processed_imports:
        sanitized_imp = imp.replace("/", "_")
        gcs_pattern = f"{output_path.rstrip('/')}/{sanitized_imp}/**/*.jsonld"
        import_list.append({"importName": imp, "graphPath": gcs_pattern})
      self.trigger_workflow_info = import_list
    else:
      logging.info(
          "Not running under ingestion workflow or output is local. Skipping handshake info."
      )


def _check_not_overlapping(input_store: Store, output_store: Store):
  input_path = input_store.full_path()
  output_path = output_store.full_path()

  # Check if paths are the same or if one is a parent of the other.
  if (fspath.normpath(input_path) == fspath.normpath(output_path) or
      fspath.isparent(input_path, output_path) or
      fspath.isparent(output_path, input_path)):
    raise ValueError(
        f"Input path ({input_path}) overlaps with output dir ({output_path})")
