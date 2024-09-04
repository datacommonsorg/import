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

import re

from stats import constants
from stats.data import AggregationConfig
from stats.data import EntityType
from stats.data import EventType
from stats.data import ImportType
from stats.data import InputFileFormat
from stats.data import ObservationProperties
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVar
from stats.data import to_observation_properties

_INPUT_FILES_FIELD = "inputFiles"
_IMPORT_TYPE_FIELD = "importType"
_ENTITY_TYPE_FIELD = "entityType"
_IGNORE_COLUMNS_FIELD = "ignoreColumns"
_VARIABLES_FIELD = "variables"
_NAME_FIELD = "name"
_DESCRIPTION_FIELD = "description"
_SEARCH_DESCRIPTIONS_FIELD = "searchDescriptions"
# DEPRECATED: Use searchDescriptions instead.
_NL_SENTENCES_FIELD = "nlSentences"
_GROUP_FIELD = "group"
_SOURCES_FIELD = "sources"
_PROVENANCES_FIELD = "provenances"
_URL_FIELD = "url"
_PROVENANCE_FIELD = "provenance"
_DATABASE_FIELD = "database"
_EVENT_TYPE_FIELD = "eventType"
_ID_COLUMN_FIELD = "idColumn"
_EVENTS_FIELD = "events"
_COMPUTED_VARIABLES_FIELD = "computedVariables"
_AGGREGATION_FIELD = "aggregation"
_PROPERTIES_FIELD = "properties"
_DATA_DOWNLOAD_URL_FIELD = "dataDownloadUrl"
_FORMAT_FIELD = "format"
_COLUMN_MAPPINGS_FIELD = "columnMappings"
_ROW_ENTITY_TYPE_FIELD = "rowEntityType"
_ENTITY_COLUMNS = "entityColumns"
_ENTITIES_FIELD = "entities"
_GROUP_STAT_VARS_BY_PROPERTY = "groupStatVarsByProperty"
_GENERATE_TOPICS = "generateTopics"
_OBSERVATION_PROPERTIES = "observationProperties"


class Config:
  """A wrapper around the import config specified via config.json.

    It provides convenience methods for accessing parameters from the config.
    """

  def __init__(self, data: dict) -> None:
    self.data = data
    self._input_files_config: dict[str, dict] = self.data.get(
        _INPUT_FILES_FIELD, {})
    # If input file names are specified with wildcards - e.g. "foo*.csv",
    # this dict maintains a mapping from actual file name to the wildcard key
    # for fast lookup.
    # e.g. "foo1.csv" -> "foo*.csv", "foo2.csv" -> "foo*.csv", etc.
    self._input_file_name_keys: dict[str, str] = {}
    # dict from provenance name to Provenance
    self.provenances: dict[str, Provenance] = {}
    # dict from provenance name to Source
    self.provenance_sources: dict[str, Source] = {}
    self._parse_provenances_and_sources()

  def data_download_urls(self) -> list[str]:
    cfg = self.data.get(_DATA_DOWNLOAD_URL_FIELD)
    if not cfg:
      return []
    if isinstance(cfg, list):
      return cfg
    raise ValueError(
        f"{_DATA_DOWNLOAD_URL_FIELD} can only be a list, found: {cfg}")

  def import_type(self, input_file_name: str) -> ImportType:
    import_type_str = self._input_file(input_file_name).get(_IMPORT_TYPE_FIELD)
    if not import_type_str:
      return ImportType.OBSERVATIONS
    if import_type_str not in iter(ImportType):
      raise ValueError(
          f"Unsupported import type: {import_type_str} ({input_file_name})")
    return ImportType(import_type_str)

  def format(self, input_file_name: str) -> ImportType | None:
    format_str = self._input_file(input_file_name).get(_FORMAT_FIELD)
    if not format_str:
      return None
    if format_str not in iter(InputFileFormat):
      raise ValueError(f"Unsupported format: {format_str} ({input_file_name})")
    return InputFileFormat(format_str)

  def column_mappings(self, input_file_name: str) -> dict[str, str]:
    return self._input_file(input_file_name).get(_COLUMN_MAPPINGS_FIELD, {})

  def computed_variables(self, input_file_name: str) -> list[str]:
    return self._input_file(input_file_name).get(_COMPUTED_VARIABLES_FIELD, [])

  def variable(self, variable_name: str) -> StatVar:
    var_cfg = self.data.get(_VARIABLES_FIELD, {}).get(variable_name, {})
    # Combine search descriptions and the deprecated NL sentences until the latter is removed.
    search_descriptions = var_cfg.get(_SEARCH_DESCRIPTIONS_FIELD,
                                      []) + var_cfg.get(_NL_SENTENCES_FIELD, [])
    return StatVar(
        "",
        var_cfg.get(_NAME_FIELD, variable_name),
        description=var_cfg.get(_DESCRIPTION_FIELD, ""),
        search_descriptions=search_descriptions,
        group_path=var_cfg.get(_GROUP_FIELD, ""),
        properties=var_cfg.get(_PROPERTIES_FIELD, {}),
    )

  def aggregation(self, variable_name: str) -> AggregationConfig:
    aggregation_cfg = self.data.get(_VARIABLES_FIELD, {}) \
      .get(variable_name, {}) \
      .get(_AGGREGATION_FIELD, {})
    return AggregationConfig(**aggregation_cfg)

  def event_type(self, input_file_name: str) -> str:
    return self._input_file(input_file_name).get(_EVENT_TYPE_FIELD, "")

  def event(self, event_type_name: str) -> EventType:
    event_type_cfg = self.data.get(_EVENTS_FIELD, {}).get(event_type_name, {})
    return EventType("",
                     event_type_cfg.get(_NAME_FIELD, event_type_name),
                     description=event_type_cfg.get(_DESCRIPTION_FIELD, ""))

  def entity(self, entity_type_name: str) -> EntityType:
    entity_type_cfg = self.data.get(_ENTITIES_FIELD,
                                    {}).get(entity_type_name, {})
    return EntityType("",
                      entity_type_cfg.get(_NAME_FIELD, entity_type_name),
                      description=entity_type_cfg.get(_DESCRIPTION_FIELD, ""))

  def id_column(self, input_file_name: str) -> str:
    return self._input_file(input_file_name).get(_ID_COLUMN_FIELD, "")

  def entity_type(self, input_file_name: str) -> str:
    return self._input_file(input_file_name).get(_ENTITY_TYPE_FIELD, "")

  def ignore_columns(self, input_file_name: str) -> list[str]:
    return self._input_file(input_file_name).get(_IGNORE_COLUMNS_FIELD, [])

  def provenance_name(self, input_file_name: str) -> str:
    return self._input_file(input_file_name).get(_PROVENANCE_FIELD,
                                                 input_file_name)

  def row_entity_type(self, input_file_name: str) -> str:
    return self._input_file(input_file_name).get(_ROW_ENTITY_TYPE_FIELD, "")

  def entity_columns(self, input_file_name: str) -> list[str]:
    return self._input_file(input_file_name).get(_ENTITY_COLUMNS, [])

  def database(self) -> dict:
    return self.data.get(_DATABASE_FIELD)

  def generate_hierarchy(self) -> bool:
    return self.data.get(_GROUP_STAT_VARS_BY_PROPERTY) or False

  def special_files(self) -> dict[str, str]:
    special_files: dict[str, str] = {}
    for special_file_type in constants.SPECIAL_FILE_TYPES:
      special_file = self.data.get(special_file_type, "")
      if special_file:
        special_files[special_file] = special_file_type
    return special_files

  def generate_topics(self) -> bool:
    return self.data.get(_GENERATE_TOPICS) or False

  def observation_properties(self,
                             input_file_name: str) -> ObservationProperties:
    return to_observation_properties(
        self._input_file(input_file_name).get(_OBSERVATION_PROPERTIES, {}))

  def _input_file(self, input_file_name: str) -> dict:
    # Exact match.
    input_file_config = self._input_files_config.get(input_file_name, {})
    if input_file_config:
      return input_file_config

    # Wildcard match
    if input_file_name not in self._input_file_name_keys.keys():
      self._input_file_name_keys[input_file_name] = self._input_file_name_match(
          input_file_name)
    return self._input_files_config.get(
        self._input_file_name_keys[input_file_name], {})

  def _input_file_name_match(self, input_file_name: str) -> str | None:
    for input_file_pattern in self._input_files_config.keys():
      if "*" not in input_file_pattern:
        continue
      regex = self._input_file_pattern_to_regex(input_file_pattern)
      if re.match(regex, input_file_name) is not None:
        return input_file_pattern
    return None

  def _input_file_pattern_to_regex(self, input_file_pattern: str) -> str:
    r"""
    Transforms a string of the form "a*b.c" to the regex "a.*b\.c".
    """
    return input_file_pattern.replace(".", r"\.").replace("*", ".*")

  def _parse_provenances_and_sources(self):
    sources_cfg = self.data.get(_SOURCES_FIELD, {})
    for source_name, source_cfg in sources_cfg.items():
      source = Source(id="",
                      name=source_name,
                      url=source_cfg.get(_URL_FIELD, ""))
      provenances = source_cfg.get(_PROVENANCES_FIELD, {})
      for prov_name, prov_url in provenances.items():
        provenance = Provenance(id="",
                                source_id="",
                                name=prov_name,
                                url=prov_url)
        self.provenances[prov_name] = provenance
        self.provenance_sources[prov_name] = source
