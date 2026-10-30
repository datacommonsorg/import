# Copyright 2023 Google Inc.
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

from stats.data import EntityType
from stats.data import ImportType
from stats.data import strip_namespace
from util.file_match import match
from util.filesystem import File

_INPUT_FILES_FIELD = "inputFiles"
_IMPORT_TYPE_FIELD = "importType"
_IGNORE_COLUMNS_FIELD = "ignoreColumns"
_NAME_FIELD = "name"
_DESCRIPTION_FIELD = "description"
_PROVENANCE_FIELD = "provenance"
_ID_COLUMN_FIELD = "idColumn"
_DATA_DOWNLOAD_URL_FIELD = "dataDownloadUrl"
_FORMAT_FIELD = "format"
# Default and only accepted variable import format
_VARIABLE_PER_ROW_FORMAT = "variablePerRow"
_COLUMN_MAPPINGS_FIELD = "columnMappings"
_ROW_ENTITY_TYPE_FIELD = "rowEntityType"
_ENTITY_COLUMNS = "columnsToResolve"
_ENTITIES_FIELD = "entities"
_GROUP_STAT_VARS_BY_PROPERTY = "groupStatVarsByProperty"
_OBSERVATION_PROPERTIES = "observationProperties"
_INCLUDE_INPUT_SUBDIRS_PROPERTY = "includeInputSubdirs"


class Config:
  """A wrapper around the import config specified via config.json.

    It provides convenience methods for accessing parameters from the config.
    """

  def __init__(self, data: dict) -> None:
    self.data = data
    self._input_files_config: dict[str, dict] = {}
    input_files = self.data.get(_INPUT_FILES_FIELD, [])

    if not isinstance(input_files, list):
      raise ValueError(
          f"'{_INPUT_FILES_FIELD}' must be a list of objects, each with a "
          f"'pattern' and a 'provenance'. Got: {type(input_files).__name__}")
    self._parse_input_files_config(input_files)

    # If input file paths are specified with wildcards - e.g. "gs://bucket/foo*.csv",
    # this dict maintains a mapping from actual file path to the wildcard key
    # for fast lookup.
    # e.g. "foo1.csv" -> "foo*.csv", "foo2.csv" -> "foo*.csv",
    # "path/to/foo.csv" -> "**/foo.csv, etc.
    self._config_key_by_full_path: dict[str, str] = {}

  def _parse_input_files_config(self, input_files: list) -> None:
    """Parses the list-of-objects format for inputFiles."""
    for entry in input_files:
      if not isinstance(entry, dict):
        raise ValueError(
            f"Invalid entry in '{_INPUT_FILES_FIELD}': must be a JSON object. Got: {entry}"
        )
      key = entry.get("pattern") or entry.get("filename")
      if not key:
        raise ValueError(
            f"Invalid entry in '{_INPUT_FILES_FIELD}': must specify 'pattern' or 'filename'. Got: {entry}"
        )
      self._validate_format(key, entry)
      self._input_files_config[key] = entry

  def _validate_format(self, key: str, entry: dict) -> None:
    """Rejects any `format` other than the one supported CSV layout.

    `format` is a no-op today because every observations CSV is variable-per-row.
    It is still validated so that a config carrying a stale value fails loudly
    instead of having its CSVs silently misread.
    """
    format_str = entry.get(_FORMAT_FIELD)
    if format_str and format_str != _VARIABLE_PER_ROW_FORMAT:
      raise ValueError(
          f"Unsupported format '{format_str}' for '{key}'. The only supported "
          f"format is '{_VARIABLE_PER_ROW_FORMAT}', which is also the default.")

  def data_download_urls(self) -> list[str]:
    cfg = self.data.get(_DATA_DOWNLOAD_URL_FIELD)
    if not cfg:
      return []
    if isinstance(cfg, list):
      return cfg
    raise ValueError(
        f"{_DATA_DOWNLOAD_URL_FIELD} can only be a list, found: {cfg}")

  def import_type(self, input_file: File) -> ImportType:
    import_type_str = self._per_file_config(input_file).get(_IMPORT_TYPE_FIELD)
    if not import_type_str:
      return ImportType.OBSERVATIONS
    if import_type_str not in iter(ImportType):
      raise ValueError(
          f"Unsupported import type: {import_type_str} ({input_file.full_path()})"
      )
    return ImportType(import_type_str)

  def column_mappings(self, input_file: File) -> dict[str, str]:
    return self._per_file_config(input_file).get(_COLUMN_MAPPINGS_FIELD, {})

  def entity(self, entity_type_name: str) -> EntityType:
    entity_type_cfg = self.data.get(_ENTITIES_FIELD,
                                    {}).get(entity_type_name, {})
    return EntityType("",
                      entity_type_cfg.get(_NAME_FIELD, entity_type_name),
                      description=entity_type_cfg.get(_DESCRIPTION_FIELD, ""))

  def id_column(self, input_file: File) -> str:
    return self._per_file_config(input_file).get(_ID_COLUMN_FIELD, "")

  def ignore_columns(self, input_file: File) -> list[str]:
    return self._per_file_config(input_file).get(_IGNORE_COLUMNS_FIELD, [])

  def provenance_name(self, input_file: File) -> str:
    return self._per_file_config(input_file).get(_PROVENANCE_FIELD,
                                                 input_file.path)

  def row_entity_type(self, input_file: File) -> str:
    return self._per_file_config(input_file).get(_ROW_ENTITY_TYPE_FIELD, "")

  def entity_columns(self, input_file: File) -> list[str]:
    return self._per_file_config(input_file).get(_ENTITY_COLUMNS, [])

  def observation_properties(self, input_file: File) -> dict[str, str]:
    return self._per_file_config(input_file).get(_OBSERVATION_PROPERTIES, {})

  def generate_hierarchy(self) -> bool:
    val = self.data.get(_GROUP_STAT_VARS_BY_PROPERTY)
    if isinstance(val, str):
      return val.lower() in ("true", "1", "yes")
    return bool(val)

  def include_input_subdirs(self) -> bool:
    return self.data.get(_INCLUDE_INPUT_SUBDIRS_PROPERTY) or False

  def import_name(self, input_file: File) -> str:
    """Returns the normalized import name associated with a given input file."""
    prov_id = self._per_file_config(input_file).get("provenance")
    if prov_id:
      return strip_namespace(prov_id)
    raise ValueError(
        f"Could not determine import name: missing 'provenance' configuration for file '{input_file.path}'."
    )

  def _per_file_config(self, input_file: File) -> dict:
    """ Looks up the config for a given file.

    The lookup process is as follows:
    - If the file name exactly matches a config key, the config for that key
      is returned.
    - Else if the file's path relative to the input directory exactly matches
      a config key, the config for that key is returned.
    - Else, we attempt to match the file with each config key in order,
      returning the first matching result.
      Matches are checked with the match function in simple/util/file_match.py.
    """
    for_exact_name = self._input_files_config.get(input_file.name(), {})
    if for_exact_name:
      return for_exact_name

    for_exact_full_path = self._input_files_config.get(input_file.full_path(),
                                                       {})
    if for_exact_full_path:
      return for_exact_full_path

    if input_file.full_path() not in self._config_key_by_full_path:
      self._config_key_by_full_path[input_file.full_path(
      )] = self._find_first_matching_config_key(input_file)

    return self._input_files_config.get(
        self._config_key_by_full_path[input_file.full_path()], {})

  def _find_first_matching_config_key(self, input_file: File) -> str | None:
    for input_file_pattern in self._input_files_config.keys():
      if match(input_file, input_file_pattern):
        return input_file_pattern
    return None
