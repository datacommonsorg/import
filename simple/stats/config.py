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

# Default and only accepted variable import format
_VARIABLE_PER_ROW_FORMAT = "variablePerRow"
# Removed top-level block that used to define Class node metadata. Rejected
# with a pointer to MCF rather than the generic unknown-key error.
_REMOVED_ENTITIES_FIELD = "entities"
_REFERENCE_COLUMN_FIELDS = ("columnsToResolve", "referenceColumns")


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
  # Undocumented alias of `pattern`, used when `pattern` is not set.
  filename: str = ""
  provenance: str = ""
  import_type: ImportType = ImportType.OBSERVATIONS
  format: str | None = None
  column_mappings: dict[str, str] = {}
  ignore_columns: list[str] = []
  observation_properties: dict[str, str | int | float] = {}
  row_entity_type: str = ""
  id_column: str = ""
  columns_to_resolve: list[str] = []
  reference_columns: list[str] = []

  @property
  def key(self) -> str:
    """The pattern that selects this entry's files."""
    return self.pattern or self.filename

  @model_validator(mode="before")
  @classmethod
  def _check_entry(cls, data: Any) -> Any:
    if not isinstance(data, dict):
      raise ValueError("Invalid entry in 'inputFiles': must be a"
                       f" JSON object. Got: {data}")
    if not (data.get("pattern") or data.get("filename")):
      raise ValueError("Invalid entry in 'inputFiles': must specify"
                       f" 'pattern' or 'filename'. Got: {data}")
    return data

  @field_validator("pattern", "filename")
  @classmethod
  def _check_pattern(cls, pattern: str) -> str:
    if pattern:
      validate_pattern(pattern)
    return pattern

  @field_validator("import_type", mode="before")
  @classmethod
  def _check_import_type(cls, value: Any) -> Any:
    if not value:
      return ImportType.OBSERVATIONS
    if value not in {t.value for t in ImportType}:
      raise ValueError(f"Unsupported import type: {value}")
    return value

  @model_validator(mode="after")
  def _check_fields(self) -> "InputFileConfig":
    # `format` is a no-op because every observations CSV is variable-per-row.
    # It is still validated so that a config carrying a stale value fails
    # loudly instead of having its CSVs silently misread.
    if self.format and self.format != _VARIABLE_PER_ROW_FORMAT:
      raise ValueError(
          f"Unsupported format '{self.format}' for '{self.key}'. The only"
          f" supported format is '{_VARIABLE_PER_ROW_FORMAT}', which is also"
          " the default.")
    is_entities = self.import_type == ImportType.ENTITIES
    reference_fields = sorted(_keys_set(self) & set(_REFERENCE_COLUMN_FIELDS))
    if reference_fields and not is_entities:
      raise ValueError(
          f"'{reference_fields[0]}' is only supported for 'importType':"
          f" '{ImportType.ENTITIES}', but entry '{self.key}' has"
          f" importType '{self.import_type}'.")
    if is_entities and not self.row_entity_type:
      raise ValueError(
          f"'rowEntityType' is required for 'importType':"
          f" '{ImportType.ENTITIES}', but entry '{self.key}' has none.")
    return self


# The entry of a file that no `inputFiles` entry matches: every field defaults.
_NO_ENTRY = InputFileConfig.model_construct()


def _keys_set(model: BaseModel) -> set[str]:
  """The config.json keys set in the object `model` was validated from."""
  fields = type(model).model_fields
  return {fields[name].alias or name for name in model.model_fields_set}


class ImportConfig(_ConfigModel):
  """A whole config.json."""

  input_files: list[InputFileConfig] = []
  # TODO(gmechali): groupStatVarsByProperty is slated for deprecation. Until
  # then it keeps its historical leniency: any value, with "true", "1" and
  # "yes" as the only true strings.
  group_stat_vars_by_property: bool = False
  include_input_subdirs: bool = False
  import_name: str = ""

  @model_validator(mode="before")
  @classmethod
  def _check_top_level(cls, data: Any) -> Any:
    if not isinstance(data, dict):
      raise ValueError(
          f"config.json must be a JSON object. Got: {type(data).__name__}")
    _reject_entities_block(data)
    input_files = data.get("inputFiles", [])
    if not isinstance(input_files, list):
      raise ValueError(
          "'inputFiles' must be a list of objects, each with a "
          f"'pattern' and a 'provenance'. Got: {type(input_files).__name__}")
    return data

  @field_validator("group_stat_vars_by_property", mode="before")
  @classmethod
  def _lenient_bool(cls, value: Any) -> bool:
    if isinstance(value, str):
      return value.lower() in ("true", "1", "yes")
    return bool(value)

  @model_validator(mode="after")
  def _check_unique_patterns(self) -> "ImportConfig":
    counts = collections.Counter(f.key for f in self.input_files)
    duplicates = sorted(key for key, count in counts.items() if count > 1)
    if duplicates:
      raise ValueError(
          f"Duplicate pattern(s) in 'inputFiles': {duplicates}. Each"
          " pattern can only have one entry.")
    return self


def _reject_entities_block(data: dict) -> None:
  """Raises ValueError if `data` has the removed top-level `entities` block.

  Class nodes for entity types used to be defined in config.json. They must now
  be defined in an MCF file like any other schema node.
  """
  # TODO(gmechali): Delete this migration error after a few releases; the
  # generic unknown-key validation will then reject the block.
  if _REMOVED_ENTITIES_FIELD not in data:
    return
  entities = data[_REMOVED_ENTITIES_FIELD]
  found = (f" Found entries for: {sorted(entities)}."
           if isinstance(entities, dict) and entities else "")
  raise ValueError(
      f"The top-level '{_REMOVED_ENTITIES_FIELD}' block in config.json is no "
      f"longer supported.{found} Define each entity type that is not already in "
      "the Data Commons schema as a Class node in an MCF file instead, e.g.:\n"
      "  Node: dcid:Author\n"
      "  typeOf: dcs:Class\n"
      '  name: "Author"\n'
      '  description: "A person who wrote an article"')


def parse_config(data: Any) -> ImportConfig:
  """Validates `data`, the parsed config.json.

  Raises:
    ValueError: Listing every problem found. Runner reports it as
      INVALID_CONFIGURATION.
  """
  try:
    return ImportConfig.model_validate(data)
  except ValidationError as e:
    raise invalid_configuration(_render_errors(e, data)) from None


def _render_errors(error: ValidationError, data: Any) -> str:
  """Renders a ValidationError as one message, one line per problem.

  Unknown keys are grouped per object, as "Unknown key(s) in ...", with the
  allowed keys. Errors raised by our validators keep their own message.
  """
  unknown_keys: dict[tuple, list[str]] = collections.defaultdict(list)
  lines = []
  for e in error.errors(include_url=False):
    loc = e["loc"]
    if e["type"] == "extra_forbidden":
      unknown_keys[loc[:-1]].append(str(loc[-1]))
    elif e["type"] == "value_error" and "error" in (e.get("ctx") or {}):
      lines.append(str(e["ctx"]["error"]))
    else:
      lines.append(f"Invalid {_describe_location(loc, data)}: {e['msg']}")
  unknown_key_lines = []
  # Top-level keys first, then each inputFiles entry in order.
  for parent, keys in sorted(unknown_keys.items(), key=lambda kv: kv[0]):
    model = InputFileConfig if parent else ImportConfig
    allowed = sorted(f.alias or name for name, f in model.model_fields.items())
    unknown_key_lines.append(
        f"Unknown key(s) in {_describe_object(parent, data)}:"
        f" {sorted(keys)}. Allowed keys are: {allowed}")
  lines = unknown_key_lines + lines
  if len(lines) == 1:
    return lines[0]
  return (f"Found {len(lines)} problems in config.json:\n" +
          "\n".join(f"- {line}" for line in lines))


def _describe_object(loc: tuple, data: Any) -> str:
  """Names the object at `loc`: config.json or an `inputFiles` entry.

  Errors inside an entry only occur once `_check_entry` accepted it, so the
  entry is an object with a pattern or filename.
  """
  if len(loc) >= 2 and loc[0] == "inputFiles":
    entry = data["inputFiles"][loc[1]]
    key = entry.get("pattern") or entry.get("filename")
    return f"'inputFiles' entry for '{key}'"
  return "config.json"


def _describe_location(loc: tuple, data: Any) -> str:
  """Names the value at `loc`, e.g. 'ignoreColumns' in an inputFiles entry."""
  if len(loc) >= 3 and loc[0] == "inputFiles":
    field = ".".join(str(part) for part in loc[2:])
    return f"'{field}' in {_describe_object(loc[:2], data)}"
  field = ".".join(str(part) for part in loc)
  return f"'{field}' in config.json"


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
        f.key: f for f in self.model.input_files
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

  def id_column(self, input_file: File) -> str:
    return self._entry(input_file).id_column

  def ignore_columns(self, input_file: File) -> list[str]:
    return self._entry(input_file).ignore_columns

  def provenance_name(self, input_file: File) -> str:
    return self._entry(input_file).provenance

  def row_entity_type(self, input_file: File) -> str:
    return self._entry(input_file).row_entity_type

  def entity_columns(self, input_file: File) -> list[str]:
    entry = self._entry(input_file)
    return entry.reference_columns or entry.columns_to_resolve

  def observation_properties(self,
                             input_file: File) -> dict[str, str | int | float]:
    return self._entry(input_file).observation_properties

  def generate_hierarchy(self) -> bool:
    return self.model.group_stat_vars_by_property

  def include_input_subdirs(self) -> bool:
    return self.model.include_input_subdirs

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
