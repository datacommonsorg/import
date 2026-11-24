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
"""Input folder discovery, config.json merging, and CSV/MCF file filtering."""

import json
import logging
import os
import posixpath

from stats import constants
from stats.config.config import Config
from stats.config.config import ImportConfig
from stats.config.config import InputFileConfig
from stats.config.config import parse_config
from stats.graph.data import invalid_configuration
from util.file_match import match
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import is_gcs_path
from util.filesystem import is_within
from util.filesystem import join_path
from util.filesystem import open_dir

_ARCHIVES_DIR_NAME = "archives"
# Every run names its imports: each is a subdirectory of the input dir with its
# own config.json.
_NO_IMPORTS_ERROR = (
    "No imports to run. Pass --imports with the import subdirectories of the"
    " input directory to import, e.g. --imports=oecd or --imports=oecd,ilo, or"
    f" --imports={constants.ALL_IMPORTS} for all of them.")


def import_names_or_raise(names: list[str] | None) -> list[str]:
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


def read_config_file(file: File) -> ImportConfig:
  """Reads and validates a `config.json` file.

  Invalid JSON (e.g. an empty file) and invalid config are reported as
  INVALID_CONFIGURATION, prefixed with the file's path.
  """
  text = file.read()
  try:
    return parse_config(json.loads(text), file.full_path())
  except ValueError as e:  # Including json.JSONDecodeError.
    raise invalid_configuration(f"{file.full_path()}: {e}") from e


def _is_archived(file: File) -> bool:
  """Whether `file` is under an archives dir, whose files aren't imported."""
  return _ARCHIVES_DIR_NAME in file.path.split("/")


def _scope_input_file(entry: InputFileConfig, rel_dir: str) -> InputFileConfig:
  """Prefixes the entry's `pattern` with `rel_dir`."""
  return entry.model_copy(update={"pattern": join_path(rel_dir, entry.pattern)})


def _comparable_path(directory: Dir) -> str:
  """The dir's URL on GCS; its absolute path, with ~ and $VARS expanded, on
  local disk, where the paths shown keep them as given."""
  path = directory.full_path()
  if is_gcs_path(path):
    return path
  return os.path.abspath(os.path.expanduser(os.path.expandvars(path or ".")))


def check_not_overlapping(input_dir: Dir, output_dir: Dir) -> None:
  """Raises ValueError if `input_dir` and `output_dir` overlap."""
  input_path = _comparable_path(input_dir)
  output_path = _comparable_path(output_dir)

  # Check if paths are the same or if one is a parent of the other.
  if is_within(input_path, output_path) or is_within(output_path, input_path):
    raise ValueError(f"Input path ({input_dir.full_path()}) overlaps with"
                     f" output dir ({output_dir.full_path()})")


class InputDiscovery:
  """Discovers and merges config.json files and lists matched input files.

  Directory listings are cached for the duration of a run so config discovery
  and input file filtering do not re-list GCS buckets.
  """

  def __init__(self, input_dir_path: str,
               import_names: list[str] | None) -> None:
    self.input_dir_path = input_dir_path
    self.import_names: list[str] | None = import_names
    self.input_dir: Dir | None = None
    self.config: Config | None = None
    self.active_import_prefixes: set[str] | None = None
    # Every file under an input dir, by the dir's path relative to the input
    # root ("" for the root), filled as _list_input_dir() lists each dir.
    self._input_dir_files: dict[str, list[File]] = {}

  def _open_input_dir(self, path: str) -> Dir:
    self.input_dir = open_dir(path)
    return self.input_dir

  def load_config(self, output_dir: Dir) -> tuple[Dir, Config]:
    """Opens the input dir, reads and validates configs, and checks overlap."""
    imports = import_names_or_raise(self.import_names)
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
      self.config = Config(read_config_file(config_file))

    # The input dir is searched recursively, so it must not hold the output.
    check_not_overlapping(self.input_dir, output_dir)
    return self.input_dir, self.config

  def input_folder_name(self, file: File) -> str:
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

  def report_folder_names(self) -> list[str]:
    """Returns the import folders this run was asked to import."""
    if self.import_names == [constants.ALL_IMPORTS]:
      # The folders found under the input dir, e.g. "folderA/" -> "folderA".
      return sorted(
          prefix.rstrip("/") for prefix in self.active_import_prefixes or [])
    return list(self.import_names or [])

  def find_input_files(self) -> tuple[list[File], list[File]]:
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

  def _merge_configs(self, configs: list[File]) -> None:
    """Merges multiple config.json files into a single configuration."""
    input_files = []
    for file in configs:
      config = read_config_file(file)
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

  def _read_configs_from_subdirs(self, base_dir: Dir) -> list[File]:
    """Scans subdirectories for config.json files and merges them."""
    configs = self._find_configs_in_dir(base_dir.path)
    logging.info("Found %s config files in subdirectories.", len(configs))
    if not configs:
      raise FileNotFoundError(
          f"No config.json files found in subdirectories of {base_dir.full_path()}"
      )
    self._merge_configs(configs)
    return configs

  def _read_configs_from_list(self, import_names: list[str]) -> list[File]:
    """Reads and merges the configs of the imports in `import_names`."""
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
