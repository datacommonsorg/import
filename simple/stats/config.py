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
"""The import config specified via config.json: its schema and accessors."""

import collections
import logging
from typing import Any

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import field_validator
from pydantic import model_validator
from pydantic import ValidationError
from pydantic.alias_generators import to_camel
from stats.data import ImportType
from stats.data import invalid_configuration
from stats.data import strip_namespace
from util.file_match import match
from util.file_match import validate_pattern
from util.filesystem import File

# Deprecated top-level key: accepted, with a warning (see parse_config), but
# ignored. Subdirectories of the input directory are always searched.
_INCLUDE_INPUT_SUBDIRS_FIELD = "includeInputSubdirs"
_REFERENCE_HINT = ("Write each reference as a cell value with a 'dcid:' prefix"
                   " instead, e.g. 'dcid:country/USA'.")
# Removed `inputFiles` entry keys, rejected with how to migrate rather than
# the generic unknown-key error.
_REMOVED_ENTRY_KEYS = {
    "columnsToResolve": _REFERENCE_HINT,
    "referenceColumns": _REFERENCE_HINT,
    "rowEntityType": "Map a 'dcid:typeOf' column in 'columnMappings' instead.",
    "idColumn": "Map a 'dcid:dcid' column in 'columnMappings' instead.",
}

# Our wording, in JSON terms, for pydantic messages in Python terms or naming
# our classes, by error type.
_PYDANTIC_MESSAGE_OVERRIDES = {
    "model_type": "Input should be a JSON object",
    "dict_type": "Input should be a JSON object",
    "list_type": "Input should be a JSON array",
}


class _ConfigModel(BaseModel):
  """A config.json object.

  Its keys are the camelCase names of the fields, e.g. `columnMappings` for
  `column_mappings`. Unknown keys are rejected, and validated models are
  immutable.
  """
  model_config = ConfigDict(alias_generator=to_camel,
                            extra="forbid",
                            frozen=True,
                            populate_by_name=True)


class InputFileConfig(_ConfigModel):
  """One `inputFiles` entry: the files it applies to and their parameters."""

  pattern: str = ""
  provenance: str = ""
  import_type: ImportType = ImportType.OBSERVATIONS
  column_mappings: dict[str, str] = {}
  ignore_columns: list[str] = []
  observation_properties: dict[str, str | int | float] = {}

  @model_validator(mode="after")
  def _check_pattern_is_set(self) -> "InputFileConfig":
    if not self.pattern:
      got = self.model_dump(mode="json", by_alias=True, exclude_unset=True)
      raise ValueError("Invalid entry in 'inputFiles': must specify a"
                       f" 'pattern'. Got: {got}")
    return self

  @field_validator("pattern")
  @classmethod
  def _check_pattern(cls, pattern: str) -> str:
    validate_pattern(pattern)
    return pattern

  @field_validator("import_type", mode="before")
  @classmethod
  def _default_empty_import_type(cls, value: Any) -> Any:
    """An `importType` of "" or null means the default."""
    if value is None or value == "":
      return ImportType.OBSERVATIONS
    return value


# The entry of a file that no `inputFiles` entry matches: every field defaults.
_NO_ENTRY = InputFileConfig.model_construct()


class ImportConfig(_ConfigModel):
  """A whole config.json."""

  input_files: list[InputFileConfig] = []

  @model_validator(mode="before")
  @classmethod
  def _drop_include_input_subdirs(cls, data: Any) -> Any:
    """Drops the deprecated `includeInputSubdirs`, if set."""
    if not isinstance(data, dict) or _INCLUDE_INPUT_SUBDIRS_FIELD not in data:
      return data
    return {k: v for k, v in data.items() if k != _INCLUDE_INPUT_SUBDIRS_FIELD}

  @model_validator(mode="after")
  def _check_unique_patterns(self) -> "ImportConfig":
    counts = collections.Counter(f.pattern for f in self.input_files)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if duplicates:
      raise ValueError(
          f"Duplicate pattern(s) in 'inputFiles': {duplicates}. Each"
          " pattern can only have one entry.")
    return self


def _warn_if_include_input_subdirs(data: Any, path: str) -> None:
  """Logs that `includeInputSubdirs`, if set in `data`, is ignored."""
  if not isinstance(data, dict) or _INCLUDE_INPUT_SUBDIRS_FIELD not in data:
    return
  logging.warning(
      "%s: '%s' is deprecated and ignored: subdirectories of the input"
      " directory are always searched, and patterns decide which files are"
      " imported. Remove it from this config.json.", path,
      _INCLUDE_INPUT_SUBDIRS_FIELD)


def parse_config(data: Any, path: str = "config.json") -> ImportConfig:
  """Validates `data`, the parsed config.json at `path`.

  Raises:
    ValueError: Listing every problem found. Runner reports it as
      INVALID_CONFIGURATION.
  """
  _warn_if_include_input_subdirs(data, path)
  try:
    return ImportConfig.model_validate(data)
  except ValidationError as e:
    raise invalid_configuration(_render_errors(e, data)) from None


def _render_errors(error: ValidationError, data: Any) -> str:
  """Renders a ValidationError as one message, one line per problem.

  Unknown and removed keys are grouped per object, first. Errors raised by our
  validators keep their own message; pydantic's are prefixed with where they
  are.
  """
  unknown_keys: dict[tuple, list[str]] = collections.defaultdict(list)
  removed_keys: dict[tuple, list[str]] = collections.defaultdict(list)
  lines = []
  for e in error.errors(include_url=False):
    loc = e["loc"]
    if e["type"] == "extra_forbidden":
      parent, key = loc[:-1], str(loc[-1])
      # Removed keys are entry keys: at the top level, a key is just unknown.
      if parent and key in _REMOVED_ENTRY_KEYS:
        removed_keys[parent].append(key)
      else:
        unknown_keys[parent].append(key)
    elif e["type"] == "value_error" and "error" in (e.get("ctx") or {}):
      lines.append(str(e["ctx"]["error"]))
    else:
      message = _PYDANTIC_MESSAGE_OVERRIDES.get(e["type"], e["msg"])
      # A file that isn't an object: Runner may prefix the file's own path.
      where = _describe(loc, data) if loc else "top-level value"
      lines.append(f"Invalid {where}: {message}")
  # Top-level keys first, then each inputFiles entry in order.
  key_lines = []
  for parent, keys in sorted(unknown_keys.items()):
    key_lines.append(_unknown_keys_line(parent, keys, data))
  for parent, keys in sorted(removed_keys.items()):
    key_lines.append(_removed_keys_line(parent, keys, data))
  lines = key_lines + lines
  if len(lines) == 1:
    return lines[0]
  return (f"Found {len(lines)} problems in config.json:\n" +
          "\n".join(f"- {line}" for line in lines))


def _unknown_keys_line(parent: tuple, keys: list[str], data: Any) -> str:
  model = InputFileConfig if parent else ImportConfig
  allowed = sorted(f.alias or name for name, f in model.model_fields.items())
  return (f"Unknown key(s) in {_describe(parent, data)}: {sorted(keys)}."
          f" Allowed keys are: {allowed}")


def _removed_keys_line(parent: tuple, keys: list[str], data: Any) -> str:
  # Each hint once, in key order: both reference keys share one.
  hints = dict.fromkeys(_REMOVED_ENTRY_KEYS[key] for key in sorted(keys))
  return (f"Removed key(s) in {_describe(parent, data)}: {sorted(keys)}. " +
          " ".join(hints))


def _describe(loc: tuple, data: Any) -> str:
  """Names the value at `loc`, e.g. 'ignoreColumns' in an inputFiles entry.

  `inputFiles` is the only top-level field, so a longer `loc` is inside an
  entry.
  """
  if not loc:
    return "config.json"
  if len(loc) == 1:
    return f"'{loc[0]}' in config.json"
  # loc[0] is the key as written: "input_files" is also accepted.
  entry = _describe_entry(data[loc[0]], loc[1])
  if len(loc) == 2:
    return entry
  field = ".".join(str(part) for part in loc[2:])
  return f"'{field}' in {entry}"


def _describe_entry(input_files: list, index: int) -> str:
  """Names an `inputFiles` entry by its pattern, else by its index."""
  entry = input_files[index]
  pattern = entry.get("pattern") if isinstance(entry, dict) else None
  if isinstance(pattern, str) and pattern:
    return f"'inputFiles' entry for '{pattern}'"
  return f"'inputFiles' entry at index {index}"


class Config:
  """A wrapper around the import config specified via config.json.

  It provides convenience methods for accessing parameters from the config.
  """

  def __init__(self, data: dict | ImportConfig) -> None:
    """Validates `data`, the parsed config.json, unless already a model.

    Raises:
      ValueError: If `data` is not a valid config. Runner reports it as
        INVALID_CONFIGURATION.
    """
    self.model = data if isinstance(data, ImportConfig) else parse_config(data)
    # Patterns are unique, and dicts keep the config's order.
    self._input_files_config: dict[str, InputFileConfig] = {
        f.pattern: f for f in self.model.input_files
    }
    # Cache of a file's full path to the pattern of the first entry that
    # matches it (or None), e.g. "path/to/foo1.csv" -> "foo*.csv".
    self._config_key_by_full_path: dict[str, str | None] = {}

  def patterns(self) -> list[str]:
    """The `inputFiles` patterns, in config order."""
    return list(self._input_files_config)

  def import_type(self, input_file: File) -> ImportType:
    return self._entry(input_file).import_type

  def column_mappings(self, input_file: File) -> dict[str, str]:
    return self._entry(input_file).column_mappings

  def ignore_columns(self, input_file: File) -> list[str]:
    return self._entry(input_file).ignore_columns

  def provenance_name(self, input_file: File) -> str:
    return self._entry(input_file).provenance

  def observation_properties(self,
                             input_file: File) -> dict[str, str | int | float]:
    return self._entry(input_file).observation_properties

  def import_name(self, input_file: File) -> str:
    """Returns the normalized import name associated with a given input file."""
    prov_id = self.provenance_name(input_file)
    if prov_id:
      return strip_namespace(prov_id)
    raise ValueError(
        f"Could not determine import name: missing 'provenance' configuration for file '{input_file.path}'."
    )

  def for_file(self, input_file: File) -> InputFileConfig | None:
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
    for_exact_name = self._input_files_config.get(input_file.name())
    if for_exact_name:
      return for_exact_name

    for_exact_full_path = self._input_files_config.get(input_file.full_path())
    if for_exact_full_path:
      return for_exact_full_path

    if input_file.full_path() not in self._config_key_by_full_path:
      self._config_key_by_full_path[input_file.full_path(
      )] = self._find_first_matching_config_key(input_file)

    key = self._config_key_by_full_path[input_file.full_path()]
    return self._input_files_config.get(key) if key else None

  def _entry(self, input_file: File) -> InputFileConfig:
    """The entry for `input_file`, or an empty one (all defaults) if none."""
    return self.for_file(input_file) or _NO_ENTRY

  def _find_first_matching_config_key(self, input_file: File) -> str | None:
    for input_file_pattern in self._input_files_config.keys():
      if match(input_file, input_file_pattern):
        return input_file_pattern
    return None
