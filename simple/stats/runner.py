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
from dataclasses import field
from datetime import datetime
from datetime import timezone
import json
import logging
import math
import os
import threading
from typing import Optional
import unittest.mock as mock_module

import fs.path as fspath
import numpy as np
from stats import constants
from stats.config import Config
from stats.data import FileValidationError
from stats.data import ImportType
from stats.data import Triple
from stats.data import ValidationErrorType
from stats.entities_importer import EntitiesImporter
from stats.events_importer import EventsImporter
from stats.graph_writer import GraphWriter
from stats.importer import EntityResolutionError
from stats.importer import Importer
from stats.jsonld_stream_writer import JsonLdStreamWriter
from stats.mcf_importer import McfImporter
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
import stats.schema_constants as sc
from stats.validation import MetadataValidator
from stats.variable_per_row_importer import add_proxy_entity_nodes
from stats.variable_per_row_importer import VariablePerRowImporter
from util.file_match import match
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import fixed_gcs_roots
from util.filesystem import join_path
from util.filesystem import mark_gcs_roots_fixed
from util.filesystem import Store

from util import dc_client

_ARCHIVES_DIR_NAME = "archives"

# Observation CSVs bigger than this are split into parts that run in separate
# processes. Every part reads its own byte-range share of the rows in parallel,
# avoiding straggler cores on large multi-megabyte files.
_SPLIT_TARGET_BYTES = int(
    os.getenv("SIMPLE_IMPORT_SPLIT_TARGET_BYTES", 8 * 1024 * 1024))
_MAX_PARTS_PER_FILE = int(os.getenv("SIMPLE_IMPORT_MAX_PARTS_PER_FILE", 64))

# Overrides the number of import processes (defaults to the usable CPU count).
_MAX_PROCESSES_ENV = "SIMPLE_IMPORT_MAX_PROCESSES"


def _max_import_processes() -> int:
  """Determines max worker processes based on environment or available CPU cores."""
  override = os.getenv(_MAX_PROCESSES_ENV)
  if override:
    return max(1, int(override))
  if hasattr(os, "sched_getaffinity"):
    return max(1, len(os.sched_getaffinity(0)))  # Respects container limits.
  return max(1, os.cpu_count() or 1)


@dataclass
class _ImportTask:
  """Represents a planned import task unit (full file or byte chunk)."""
  file: File
  chunk_index: int = 0
  num_chunks: int = 1
  # Rough work estimate, used to start the biggest tasks first.
  est_bytes: int = 0


def _create_importer_for_file(
    config: Config,
    input_file: File,
    process_dir: Dir,
    graph_writer: GraphWriter,
    reporter: FileImportReporter,
    nodes: Nodes,
    chunk_index: int = 0,
    num_chunks: int = 1,
) -> Importer:
  """Instantiates the appropriate Importer implementation for a given file."""
  if ".mcf" in input_file.path.lower():
    return McfImporter(
        input_file=input_file,
        graph_writer=graph_writer,
        reporter=reporter,
        nodes=nodes,
    )

  import_type = config.import_type(input_file)

  match import_type:

    case ImportType.OBSERVATIONS:
      mappings = config.column_mappings(input_file)
      if not mappings:
        raise ValueError(
            f"Missing column mappings for file '{input_file.path}' in config.json"
        )
      return VariablePerRowImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          reporter=reporter,
          nodes=nodes,
          chunk_index=chunk_index,
          num_chunks=num_chunks,
      )

    case ImportType.EVENTS:
      sanitized_path = input_file.full_path().replace("://",
                                                      "_").replace("/", "_")
      debug_resolve_file = process_dir.open_file(
          f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{sanitized_path}")
      return EventsImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          debug_resolve_file=debug_resolve_file,
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
  """Aggregated execution result returned from an isolated worker process."""
  file_rel_path: str
  obs_collision_count: int
  file_collision_counts: dict
  file_sample_collisions: dict
  resolved_entities: dict
  event_types: dict
  entity_types: dict
  variables: dict
  sources: dict
  provenances: dict
  groups: dict
  properties: dict
  processed_imports: set
  # Set only for parts of a split file (num_chunks > 1):
  num_chunks: int = 1
  # Entities the part saw; the parent looks up their types once per file.
  entity_dcids: list = field(default_factory=list)
  # Sorted unique observation hashes, for duplicate checks across parts.
  obs_hashes: Optional[np.ndarray] = None
  # Final status and unresolved entities reported by worker
  status: str = "SUCCESS"
  unresolved_entities: list[str] = field(default_factory=list)


_WORKER_STORES: dict[str, Store] = {}


def _get_worker_store(root_path: str) -> Store:
  """Retrieves or initializes a cached Store for the given root path."""
  if root_path not in _WORKER_STORES:
    _WORKER_STORES[root_path] = create_store(root_path)
  return _WORKER_STORES[root_path]


def _init_import_worker(gcs_roots: list[str]) -> None:
  """Initializes worker processes, clearing cached stores and executors."""
  # The parent already fixed directory markers under these roots.
  mark_gcs_roots_fixed(gcs_roots)
  _WORKER_STORES.clear()
  import stats.jsonld_stream_writer as jsw
  jsw._process_upload_executor = None
  jsw._process_gcs_buckets.clear()


def _run_single_csv_import_proc(
    file_rel_path: str,
    input_dir_path: str,
    output_dir_path: str,
    process_dir_path: str,
    import_names: dict,
    config_json_str: str,
    jsonld_dir_name: str,
    chunk_index: int = 0,
    num_chunks: int = 1,
) -> ImportProcResult:
  """Executes an isolated import task within a child worker process."""
  is_split = num_chunks > 1
  part_tag = f"part{chunk_index:03d}" if is_split else ""
  input_store_obj = _get_worker_store(input_dir_path)
  output_store_obj = _get_worker_store(output_dir_path)

  input_dir = input_store_obj.as_dir()
  output_store = output_store_obj.as_dir()
  input_store = input_dir.open_file(file_rel_path)

  config = Config(json.loads(config_json_str))
  nodes = Nodes(config=config)
  graph_writer = JsonLdStreamWriter(output_store,
                                    import_names,
                                    nodes,
                                    jsonld_dir_name=jsonld_dir_name,
                                    shard_tag=part_tag)

  # In-memory reporter for worker processes to eliminate thousands of concurrent
  # redundant report file writes to GCS. The master process aggregates progress and
  # writes the final single process/report.json.
  reporter = ImportReporter().get_file_reporter(input_store)

  importer = _create_importer_for_file(config,
                                       input_store,
                                       output_store,
                                       graph_writer,
                                       reporter,
                                       nodes,
                                       chunk_index=chunk_index,
                                       num_chunks=num_chunks)
  importer.do_import()
  graph_writer.commit_and_close()
  return _collect_proc_result(file_rel_path, nodes, graph_writer, importer,
                              reporter, is_split, num_chunks)


def _collect_proc_result(
    file_rel_path: str,
    nodes: Nodes,
    graph_writer: JsonLdStreamWriter,
    importer,
    reporter,
    is_split: bool,
    num_chunks: int,
) -> ImportProcResult:
  """Extracts and aggregates in-memory state from a completed worker process."""
  entity_dcids = []
  obs_hashes = None
  if is_split:
    entity_dcids = list(getattr(importer, "entity_dcids", {}))
    # uint64 array: ~8 bytes per observation instead of ~70 for a set of ints.
    obs_hashes = np.fromiter(graph_writer.obs_hash_set,
                             dtype=np.uint64,
                             count=len(graph_writer.obs_hash_set))

  return ImportProcResult(
      file_rel_path=file_rel_path,
      obs_collision_count=graph_writer.obs_collision_count,
      file_collision_counts=dict(graph_writer.file_collision_counts),
      file_sample_collisions=dict(graph_writer.file_sample_collisions),
      resolved_entities={
          e.entity_dcid: (e.entity_type, getattr(e, "provenance_ids", set()))
          for e in nodes.entities.values()
      },
      event_types=dict(nodes.event_types),
      entity_types=dict(nodes.entity_types),
      variables=dict(nodes.variables),
      sources=dict(nodes.sources),
      provenances=dict(nodes.provenances),
      groups=dict(nodes.groups),
      properties=dict(nodes.properties),
      processed_imports=set(graph_writer._processed_imports),
      num_chunks=num_chunks,
      entity_dcids=entity_dcids,
      obs_hashes=obs_hashes,
      status=reporter.status.name,
      unresolved_entities=list(reporter.data.get("unresolvedEntities", [])),
  )


class Runner:
  """Runs and coordinates all imports."""

  def __init__(
      self,
      config_file_path: str,
      input_dir_path: str,
      output_dir_path: str,
      import_names: Optional[list[str]] = None,
      use_multiprocessing: bool = True,
      import_proxy_entities: Optional[bool] = None,
  ) -> None:
    assert (config_file_path or
            input_dir_path), "One of config_file or input_dir must be specified"
    assert output_dir_path, "output_dir must be specified"

    self.import_names = import_names
    self.use_multiprocessing = use_multiprocessing
    self.active_import_prefixes = None

    # File systems, both input and output. Must be closed when run finishes.
    self.all_stores: list[Store] = []
    # Input-only stores
    self.input_stores: list[Store] = []

    # "Special" file handlers.
    # i.e. if files of these types are present, they are handled in specific ways.
    self.special_files: dict[str, File] = {}

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

    # Special file handlers and flags
    if import_proxy_entities is not None:
      self.config.data["importProxyEntities"] = import_proxy_entities

    # Get dict of special file type string to special file name.
    # Example entry: verticalSpecsFile -> vertical_specs.json
    self.special_file_names_by_type = self.config.special_files()

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
        if isinstance(e, EntityResolutionError):
          self._failure_errors = [
              FileValidationError(
                  file=e.file_path,
                  error_type=ValidationErrorType.UNRESOLVED_ENTITY,
                  problem_columns=["entity"],
                  error_message=str(e))
          ]
        else:
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
        "variables": {},
        "sources": {},
        "_dir_import_names": {}
    }

    for file in configs:
      raw_config = file.read()
      try:
        config_data = json.loads(raw_config)
      except json.JSONDecodeError as e:
        logging.error("Failed to parse JSON from %s: %s", file.full_path(), e)
        raise e

      dir_path = fspath.dirname(file.path)
      rel_dir = fspath.relativefrom(base_dir.path, dir_path)

      logging.info("Merging config from import directory: %s", rel_dir)
      merged_data["_dir_import_names"][rel_dir] = config_data.get(
          "importName") or rel_dir

      input_files = config_data.get("inputFiles", [])
      if not isinstance(input_files, list):
        raise ValueError(
            f"'inputFiles' in {file.full_path()} must be a list of objects, "
            f"each with a 'pattern' and a 'provenance'. "
            f"Got: {type(input_files).__name__}")

      # Patterns are relative to the config's own directory, so prefix them
      # with that directory to make them resolvable from the merged base dir.
      import_name = config_data.get("importName") or rel_dir
      for entry in input_files:
        if not isinstance(entry, dict):
          raise ValueError(
              f"Invalid entry in 'inputFiles' in {file.full_path()}: "
              f"must be a JSON object. Got: {entry}")
        new_entry = dict(entry)
        for key_field in ["pattern", "filename"]:
          if key_field in new_entry:
            new_entry[key_field] = fspath.join(rel_dir, new_entry[key_field])
        new_entry["_import_name"] = import_name
        merged_data["inputFiles"].append(new_entry)

      # Merge variables
      variables = config_data.get("variables", {})
      for k, v in variables.items():
        merged_data["variables"][k] = v

      # Merge sources
      sources = config_data.get("sources", {})
      for k, v in sources.items():
        merged_data["sources"][k] = v

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

  def _check_if_special_file(self, file: File) -> bool:
    for file_type in self.special_file_names_by_type.keys():
      if file_type in self.special_files:
        # Already found this special file.
        continue
      file_name = self.special_file_names_by_type[file_type]
      if match(file, file_name):
        self.special_files[file_type] = file
        return True
    return False

  def _is_candidate_input_file(self, file: File) -> bool:
    """Checks whether a discovered file is eligible for import consideration."""
    if _ARCHIVES_DIR_NAME in file.path.split("/"):
      return False
    if self._check_if_special_file(file):
      return False
    if self.active_import_prefixes and not any(
        file.path.startswith(prefix) for prefix in self.active_import_prefixes):
      return False
    return True

  def _matches_config(self, file: File) -> bool:
    """Checks whether the file matches any inputFiles pattern in config.json."""
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
      if not self._is_candidate_input_file(file):
        continue

      if not self._matches_config(file):
        logging.info(
            "Ignoring file '%s' as it does not match any pattern in config.json",
            file.path)
        continue

      if match(file, "*.csv"):
        csv_files.append(file)
      elif match(file, "*.mcf"):
        mcf_files.append(file)

    # Sort alphabetically to guarantee consistent deterministic processing order
    csv_files.sort(key=lambda f: f.full_path())
    mcf_files.sort(key=lambda f: f.full_path())
    return csv_files, mcf_files

  def _validate_all_headers(self, csv_files: list[File]) -> None:
    """Validates all CSV headers upfront and raises ValueError if any validation fails."""
    if not csv_files:
      return

    all_errors = []

    def validate_single_file(file: File) -> list[FileValidationError]:
      try:
        importer = self._create_importer(file)
        return importer.validate_headers()
      except Exception as e:
        return [
            FileValidationError(
                file=file.path,
                error_type=ValidationErrorType.GENERIC_ERROR,
                error_message=f"Failed to validate headers: {str(e)}")
        ]

    try:
      validation_workers = int(os.getenv("HEADER_VALIDATION_MAX_WORKERS", "8"))
    except (ValueError, TypeError):
      validation_workers = 8
    validation_workers = min(validation_workers, len(csv_files) or 1)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=validation_workers) as executor:
      results = executor.map(validate_single_file, csv_files)
      for errors in results:
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

  def _run_all_data_imports(self):
    """Orchestrates file scanning, thread-pool configuration, and file ingestion."""
    csv_files, mcf_files = self._find_and_filter_input_files()

    logging.info("Found %d CSV files to import", len(csv_files))
    logging.info("Found %d MCF files to import", len(mcf_files))
    logging.debug("Matched files to process: %s",
                  [f.full_path() for f in csv_files + mcf_files])

    # Validate all CSV headers upfront
    self._validate_all_headers(csv_files)

    self.reporter.report_started(import_files=list(csv_files + mcf_files))

    self._completed_files_count = 0
    self._total_files_count = len(csv_files) + len(mcf_files)
    self._counter_lock = threading.Lock()

    all_files = list(mcf_files) + list(csv_files)
    if all_files:
      logging.info("Importing %d files (%d MCF, %d CSV)...", len(all_files),
                   len(mcf_files), len(csv_files))
      if not self.use_multiprocessing:
        # Import the MCF group to completion before starting the CSV group.
        # MCF declares the provenance and source nodes that CSV importers then
        # look up by name; if a CSV gets there first, Nodes.provenance()
        # auto-creates a provenance with no source and the source link is lost.
        # Files within each group still run concurrently.
        #
        # The multiprocessing branch below does not preserve this ordering.
        for file_group in (mcf_files, csv_files):
          if not file_group:
            continue
          num_threads = min(32, len(file_group))
          with concurrent.futures.ThreadPoolExecutor(
              max_workers=num_threads) as executor:
            futures = [
                executor.submit(self._run_single_import, file)
                for file in file_group
            ]
            for future in concurrent.futures.as_completed(futures):
              future.result()
      else:
        self._run_imports_in_processes(all_files)

  def _calculate_file_split_chunks(self, file: File, size: int,
                                   num_processes: int) -> int:
    """Calculates number of parallel chunks to split a large observation CSV into."""
    if (size <= _SPLIT_TARGET_BYTES or num_processes <= 1 or
        ".mcf" in file.path.lower()):
      return 1
    try:
      if self.config.import_type(file) != ImportType.OBSERVATIONS:
        return 1
    except ValueError:
      return 1
    return min(math.ceil(size / _SPLIT_TARGET_BYTES), _MAX_PARTS_PER_FILE,
               num_processes)

  def _plan_import_tasks(self, files: list[File],
                         num_processes: int) -> list[_ImportTask]:
    """Generates import tasks sorted by estimated cost, splitting large observation CSVs."""
    tasks: list[_ImportTask] = []
    for file in files:
      try:
        size = file.size()
      except Exception:
        size = 0
      num_chunks = self._calculate_file_split_chunks(file, size, num_processes)
      if num_chunks > 1:
        logging.info("Splitting %s (%d bytes) into %d parts.", file.path, size,
                     num_chunks)
      chunk_bytes = size // num_chunks
      for chunk_index in range(num_chunks):
        tasks.append(
            _ImportTask(file=file,
                        chunk_index=chunk_index,
                        num_chunks=num_chunks,
                        est_bytes=chunk_bytes))
    # MCF files must run first to declare sources and provenances, followed by largest CSVs.
    tasks.sort(key=lambda t: (not ".mcf" in t.file.path.lower(), -t.est_bytes))
    return tasks

  def _run_imports_in_processes(self, files: list[File]) -> None:
    num_processes = _max_import_processes()
    tasks = self._plan_import_tasks(files, num_processes)
    num_processes = min(num_processes, len(tasks))
    logging.info("Running %d import tasks in %d processes.", len(tasks),
                 num_processes)
    config_json_str = json.dumps(self.config.data)
    jsonld_dir_name = self.graph_writer.jsonld_dir.name()
    # Merge state for split files, keyed by file path.
    self._split_parts_left: dict[str, int] = {}
    self._split_entity_dcids: dict[str, dict[str, bool]] = {}
    self._split_obs_hashes: dict[str, list[np.ndarray]] = {}
    self._split_unresolved_entities: dict[str, list[str]] = {}
    with concurrent.futures.ProcessPoolExecutor(
        max_workers=num_processes,
        initializer=_init_import_worker,
        initargs=(fixed_gcs_roots(),)) as executor:
      futures = [
          executor.submit(
              _run_single_csv_import_proc,
              task.file.path,
              task.file._store.root_path,
              self.output_dir.full_path(),
              self.process_dir.full_path(),
              self.import_names,
              config_json_str,
              jsonld_dir_name,
              task.chunk_index,
              task.num_chunks,
          ) for task in tasks
      ]
      file_map = {f.path: f for f in files}
      for future in concurrent.futures.as_completed(futures):
        res = future.result()
        self._merge_proc_result(res)
        file_obj = file_map.get(res.file_rel_path)
        if res.num_chunks > 1:
          if self._merge_split_part(res):
            self._log_file_progress("Imported file", res.file_rel_path)
            if file_obj:
              file_reporter = self.reporter.get_file_reporter(file_obj)
              file_reporter.report_started()
              unresolved = sorted(
                  list(
                      set(
                          self._split_unresolved_entities.pop(
                              res.file_rel_path, []))))
              if unresolved:
                file_reporter.report_unresolved_entities(unresolved)
              file_reporter.report_success()
        else:
          self._log_file_progress("Imported file", res.file_rel_path)
          if file_obj:
            file_reporter = self.reporter.get_file_reporter(file_obj)
            file_reporter.report_started()
            if res.unresolved_entities:
              file_reporter.report_unresolved_entities(res.unresolved_entities)
            file_reporter.report_success()

  def _record_cross_part_collisions(self, path: str) -> None:
    """Computes and records duplicate observations across split parts in one vectorized pass."""
    part_hash_list = self._split_obs_hashes.pop(path, None)
    if not part_hash_list or len(part_hash_list) <= 1:
      return

    all_hashes = np.concatenate(part_hash_list)
    cross_part = len(all_hashes) - len(np.unique(all_hashes))
    if cross_part > 0 and hasattr(self.graph_writer, "obs_collision_count"):
      file_key = os.path.basename(path)
      logging.warning(
          "Observation @id collision in '%s': %d duplicates across split parts.",
          file_key, cross_part)
      self.graph_writer.obs_collision_count += cross_part
      self.graph_writer.file_collision_counts[file_key] += cross_part

  def _merge_split_part(self, res: ImportProcResult) -> bool:
    """Combines one part of a split file. Returns True once all parts have arrived.

    Each part only detects duplicates within its own rows; cross-part duplicates
    are computed upon final part completion so collision counts match an unsplit run.
    """
    path = res.file_rel_path
    parts_left = self._split_parts_left.get(path, res.num_chunks) - 1
    self._split_parts_left[path] = parts_left

    if res.obs_hashes is not None and len(res.obs_hashes):
      self._split_obs_hashes.setdefault(path, []).append(res.obs_hashes)

    if res.unresolved_entities:
      self._split_unresolved_entities.setdefault(path, []).extend(
          res.unresolved_entities)

    entity_dcids = self._split_entity_dcids.setdefault(path, {})
    for dcid in res.entity_dcids:
      entity_dcids[dcid] = True

    if parts_left > 0:
      return False

    self._record_cross_part_collisions(path)
    add_proxy_entity_nodes(self.nodes, self._split_entity_dcids.pop(path, {}))
    self._split_parts_left.pop(path, None)
    return True

  def _merge_proc_result(self, res: ImportProcResult) -> None:
    """Merges nodes, metadata, and collision metrics returned by a worker process."""
    self._merge_resolved_entities(res.resolved_entities)
    self._merge_metadata_nodes(res)
    self._merge_collision_stats(res)

  def _merge_resolved_entities(self, resolved_entities: dict) -> None:
    """Merges resolved entity DCIDs, types, and provenances into the main graph."""
    if not resolved_entities:
      return
    for dcid, val in resolved_entities.items():
      t, p_ids = val if isinstance(val, tuple) else (val, set())
      if isinstance(p_ids, set):
        for p_id in (p_ids or [""]):
          self.nodes.entity_with_type(dcid, t, provenance_id=p_id)
      else:
        self.nodes.entity_with_type(dcid, t, provenance_id=p_ids)

  def _merge_metadata_nodes(self, res: ImportProcResult) -> None:
    """Merges types, variables, sources, provenances, and groups from a worker."""
    if res.event_types:
      self.nodes.event_types.update(res.event_types)
    if res.entity_types:
      self.nodes.entity_types.update(res.entity_types)
    if res.variables:
      self.nodes.variables.update(res.variables)
    if res.sources:
      for src in res.sources.values():
        self.nodes.register_source(id=src.id,
                                   name=src.name,
                                   url=src.url,
                                   provenance_id=getattr(
                                       src, "provenance_id", ""))
    if res.provenances:
      for prov in res.provenances.values():
        self.nodes.register_provenance(id=prov.id,
                                       name=prov.name,
                                       url=prov.url,
                                       source_id=prov.source_id,
                                       properties=prov.properties)
    if res.groups:
      self.nodes.groups.update(res.groups)
      for svg in res.groups.values():
        self.nodes.ids_to_groups[svg.id] = svg
    if res.properties:
      for prop_name, prop_obj in res.properties.items():
        if prop_name not in self.nodes.properties:
          self.nodes.properties[prop_name] = prop_obj
        else:
          self.nodes.properties[prop_name].provenance_ids.update(
              getattr(prop_obj, "provenance_ids", set()))
    if res.processed_imports:
      self.graph_writer._processed_imports.update(res.processed_imports)

  def _merge_collision_stats(self, res: ImportProcResult) -> None:
    """Merges intra-file collision metrics reported by a worker."""
    if not (res.obs_collision_count and
            hasattr(self.graph_writer, "obs_collision_count")):
      return
    self.graph_writer.obs_collision_count += res.obs_collision_count
    for f_name, count in res.file_collision_counts.items():
      self.graph_writer.file_collision_counts[f_name] += count
      self.graph_writer.file_sample_collisions[f_name].extend(
          res.file_sample_collisions.get(f_name, []))

  def _log_file_progress(self, file_prefix: str, file: File):
    """Increments file progress counter thread-safely and logs standard progress line."""
    with self._counter_lock:
      self._completed_files_count += 1
      current_count = self._completed_files_count
    logging.info("[%d/%d] %s: %s", current_count, self._total_files_count,
                 file_prefix, file)

  def _run_single_import(self, input_file: File):
    self._log_file_progress("Importing CSV file", input_file)
    self._create_importer(input_file).do_import()

  def _create_importer(self, input_file: File) -> Importer:
    reporter = self.reporter.get_file_reporter(input_file)
    return _create_importer_for_file(
        self.config,
        input_file,
        self.process_dir,
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
        f"Input path (${input_path}) overlaps with output dir ({output_path})")
