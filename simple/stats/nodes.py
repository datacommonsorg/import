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

from collections import defaultdict
from functools import wraps
import re
import threading

from stats.config import Config
from stats.data import EntityType
from stats.data import Property
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVarGroup
from stats.data import strip_namespace
from stats.data import Triple
import stats.schema_constants as sc
from util.filesystem import File

_CUSTOM_PROVENANCE_ID_PREFIX = "c/p/"
_CUSTOM_SOURCE_ID_PREFIX = "c/s/"
_CUSTOM_PROPERTY_ID_PREFIX = "c/prop/"
_CUSTOM_ENTITY_TYPE_ID_PREFIX = "c/n/"
# Pattern to check if a string conforms to that of a valid DCID.
_DCID_PATTERN = r"^(?:[A-Za-z0-9_/]+:)?[A-Za-z0-9_/]+$"

_DEFAULT_SOURCE = Source(f"{_CUSTOM_SOURCE_ID_PREFIX}default",
                         "Custom Data Commons")
_DEFAULT_PROVENANCE = Provenance(id=f"{_CUSTOM_PROVENANCE_ID_PREFIX}default",
                                 source_id=_DEFAULT_SOURCE.id,
                                 name="Custom Import",
                                 url="custom-import")


def thread_safe(func):
  """Decorator to make a method thread-safe using the object's reentrant lock."""

  @wraps(func)
  def wrapper(self, *args, **kwargs):
    with self.lock:
      return func(self, *args, **kwargs)

  return wrapper


class Nodes:

  def __init__(self, config: Config) -> None:
    self.lock = threading.RLock()
    self.config = config
    # Dictionary of SVGs from SVG id to SVG
    self.groups: dict[str, StatVarGroup] = {}
    # dict from provenance ID (and name) to Provenance
    self.provenances: dict[str, Provenance] = {}
    # dict from source ID (and name) to Source
    self.sources: dict[str, Source] = {}
    # dict from property column name to Property
    self.properties: dict[str, Property] = {}
    # dict from entity type name to EntityType
    self.entity_types: dict[str, EntityType] = {}
    self._used_provenance_ids = set()
    self._used_source_ids = set()
    self.has_custom_mcf_nodes = False
    self.sources[_DEFAULT_SOURCE.id] = _DEFAULT_SOURCE
    self.provenances[_DEFAULT_PROVENANCE.id] = _DEFAULT_PROVENANCE
    # Used to generate property IDs
    self._property_generated_id_count = 0
    # Used to generate entity type IDs
    self._entity_type_generated_id_count = 0
    # Always ensure the global root StatVarGroup (dc/g/Root) exists so manually
    # defined StatVarGroup hierarchies in MCF have a root Node in Spanner.
    self._ensure_root_group()
    # If generating SV hierarchy, create default custom dc group at the outset.
    if config.generate_hierarchy():
      self._default_custom_group()

  @thread_safe
  def register_provenance(
      self,
      id: str,
      name: str = "",
      url: str = "",
      source_id: str = "",
      properties: dict[str, str] | None = None) -> Provenance:
    self.has_custom_mcf_nodes = True
    clean_id = _clean_metadata_id(id)
    clean_source_id = _clean_metadata_id(source_id) if source_id else ""

    prov = self.provenances.get(clean_id)
    if not prov:
      prov = Provenance(id=clean_id, source_id="", name="")
      self.provenances[clean_id] = prov
    if name:
      prov.name = prov.name or name
      self.provenances[name] = prov
    prov.name = prov.name or clean_id
    prov.url = prov.url or url
    prov.source_id = prov.source_id or clean_source_id
    prov.provenance_id = prov.provenance_id or clean_id
    if properties:
      prov.properties.update(properties)
    return prov

  @thread_safe
  def register_source(self,
                      id: str,
                      name: str = "",
                      url: str = "",
                      properties: dict[str, str] | None = None,
                      provenance_id: str = "") -> Source:
    self.has_custom_mcf_nodes = True
    clean_id = _clean_metadata_id(id)

    src = self.sources.get(clean_id)
    if not src:
      src = Source(id=clean_id, name=name or clean_id, url=url)
      self.sources[clean_id] = src
    if name:
      if src.name == clean_id:
        src.name = name
      self.sources[name] = src
    if url and not src.url:
      src.url = url
      src.domain = src.domain or Source(id="", name="", url=url).domain
    if properties:
      src.properties.update(properties)
    src.provenance_id = src.provenance_id or provenance_id
    return src

  @thread_safe
  def provenance(self, input_file: File) -> Provenance:
    prov_name = self.config.provenance_name(input_file)
    if not prov_name:
      raise ValueError(
          f"A provenance is absolutely required for file '{input_file.path}'. "
          f"Please specify the 'provenance' property in your config.json.")

    prov = self.provenances.get(prov_name)
    if not prov:
      prov = self.register_provenance(prov_name)

    self._used_provenance_ids.add(prov.id)
    if prov.source_id:
      self._used_source_ids.add(prov.source_id)
    return prov

  @thread_safe
  def property(self,
               property_column_name: str,
               provenance_id: str = "") -> Property:
    if not property_column_name in self.properties:
      self.properties[property_column_name] = Property(
          self._property_id(property_column_name), property_column_name)
    if provenance_id:
      self.properties[property_column_name].provenance_ids.add(provenance_id)

    return self.properties[property_column_name]

  @thread_safe
  def entity_type(self, entity_type_name: str, input_file: File) -> EntityType:
    if not entity_type_name in self.entity_types:
      entity_type_cfg = self.config.entity(entity_type_name)
      self.entity_types[entity_type_name] = EntityType(
          self._entity_type_id(entity_type_name),
          entity_type_cfg.name,
          description=entity_type_cfg.description)

    return self.entity_types[entity_type_name].add_provenance(
        self.provenance(input_file))

  def _property_id(self, property_column_name: str) -> str:
    dcid = _sanitize_dcid(property_column_name)
    if dcid:
      return dcid
    self._property_generated_id_count += 1
    return f"{_CUSTOM_PROPERTY_ID_PREFIX}{self._property_generated_id_count}"

  def _entity_type_id(self, entity_type_name: str) -> str:
    dcid = _sanitize_dcid(entity_type_name)
    if dcid:
      return dcid
    self._entity_type_generated_id_count += 1
    return f"{_CUSTOM_ENTITY_TYPE_ID_PREFIX}{self._entity_type_generated_id_count}"

  def _ensure_root_group(self) -> StatVarGroup:
    if sc.ROOT_SVG_ID not in self.groups:
      self.groups[sc.ROOT_SVG_ID] = StatVarGroup(sc.ROOT_SVG_ID,
                                                 sc.ROOT_SVG_NAME, "")
    return self.groups[sc.ROOT_SVG_ID]

  def _default_custom_group(self) -> StatVarGroup:
    self._ensure_root_group()
    if sc.DEFAULT_CUSTOM_ROOT_SVG_ID not in self.groups:
      self.groups[sc.DEFAULT_CUSTOM_ROOT_SVG_ID] = StatVarGroup(
          sc.DEFAULT_CUSTOM_ROOT_SVG_ID, sc.DEFAULT_CUSTOM_ROOT_SVG_NAME,
          sc.ROOT_SVG_ID)
    return self.groups[sc.DEFAULT_CUSTOM_ROOT_SVG_ID]

  @thread_safe
  def get_provenance_urls(self) -> dict[str, str]:
    prov_urls = {}
    for prov in self.provenances.values():
      prov_id = strip_namespace(prov.id)
      prov_urls[prov_id] = prov.url
      prov_urls[prov.id] = prov.url
      if prov.name:
        prov_urls[prov.name] = prov.url
    return prov_urls

  def _is_unused_default_source(self, source: Source) -> bool:
    return (self.has_custom_mcf_nodes and source.id == _DEFAULT_SOURCE.id and
            _DEFAULT_SOURCE.id not in self._used_source_ids)

  def _is_unused_default_provenance(self, provenance: Provenance) -> bool:
    return (self.has_custom_mcf_nodes and
            provenance.id == _DEFAULT_PROVENANCE.id and
            _DEFAULT_PROVENANCE.id not in self._used_provenance_ids)

  @thread_safe
  def triples_by_provenance_dir(self) -> dict[str, list[Triple]]:
    result: dict[str, list[Triple]] = defaultdict(list)
    for source in self.sources.values():
      if self._is_unused_default_source(source):
        continue
      result[_provenance_dir(source.provenance_id)].extend(source.triples())
    for provenance in self.provenances.values():
      if self._is_unused_default_provenance(provenance):
        continue
      prov_id = provenance.provenance_id or provenance.id
      result[_provenance_dir(prov_id)].extend(provenance.triples())
    for group in self.groups.values():
      result["_global"].extend(group.triples())
    for node in [*self.entity_types.values(), *self.properties.values()]:
      for dir_name in _provenance_dirs_for_node(node):
        result[dir_name].extend(node.triples())
    return {k: _dedupe_triples(triples) for k, triples in result.items()}


def _sanitize_dcid(raw: str) -> str | None:
  """Returns `raw` (or `raw` with spaces/dashes replaced by `_`) if it is a valid DCID."""
  if re.fullmatch(_DCID_PATTERN, raw):
    return raw
  candidate = re.sub(r"[ -]", "_", raw)
  if re.fullmatch(_DCID_PATTERN, candidate):
    return candidate
  return None


def _provenance_dir(raw_id: str) -> str:
  if not raw_id or raw_id == "_global":
    return "_global"
  return strip_namespace(raw_id)


def _provenance_dirs_for_node(node: EntityType | Property) -> set[str]:
  if not node.provenance_ids:
    return {"_global"}
  return {strip_namespace(p) for p in node.provenance_ids}


def _dedupe_triples(triples: list[Triple]) -> list[Triple]:
  seen = set()
  deduped = []
  for t in triples:
    key = (t.subject_id, t.predicate, t.object_id, t.object_value)
    if key not in seen:
      seen.add(key)
      deduped.append(t)
  return deduped


def _clean_metadata_id(id: str) -> str:
  """Only strips standard Data Commons system prefixes, preserving custom namespaces."""
  if id.startswith("dcid:"):
    return id[5:]
  if id.startswith("dcs:"):
    return id[4:]
  return id
