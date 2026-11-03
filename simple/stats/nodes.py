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
import threading

from stats.config import Config
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVarGroup
from stats.data import strip_namespace
from stats.data import Triple
import stats.schema_constants as sc
from util.filesystem import File


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
  def merge_metadata(self, provenances: dict[str, Provenance],
                     sources: dict[str, Source]) -> None:
    """Registers Provenance and Source nodes held by another `Nodes`.

    Worker processes each build their own `Nodes`, so this is how metadata
    crosses process boundaries in both directions: the coordinator's nodes
    seed each worker, and each worker's nodes are merged back into the
    coordinator.
    """
    for src in sources.values():
      self.register_source(id=src.id,
                           name=src.name,
                           url=src.url,
                           provenance_id=src.provenance_id)
    for prov in provenances.values():
      self.register_provenance(id=prov.id,
                               name=prov.name,
                               url=prov.url,
                               source_id=prov.source_id,
                               properties=prov.properties)

  @thread_safe
  def find_provenance(self, prov_name: str) -> Provenance | None:
    """Returns the registered provenance named or identified by `prov_name`.

    `prov_name` is a config `provenance` value, such as `dcid:MyProvenance`.
    """
    return (self.provenances.get(prov_name) or
            self.provenances.get(_clean_metadata_id(prov_name)))

  @thread_safe
  def provenance(self, input_file: File) -> Provenance:
    """Returns the provenance that config names for `input_file`.

    Raises:
      ValueError: If config names no provenance, or MCF does not define it.
    """
    prov_name = self.config.provenance_name(input_file)
    if not prov_name:
      raise ValueError(
          f"A provenance is absolutely required for file '{input_file.path}'. "
          f"Please specify the 'provenance' property in your config.json.")

    prov = self.find_provenance(prov_name)
    if not prov:
      raise ValueError(
          undefined_provenances_message({prov_name: [input_file.path]}))
    return prov

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

  @thread_safe
  def triples_by_provenance_dir(self) -> dict[str, list[Triple]]:
    result: dict[str, list[Triple]] = defaultdict(list)
    for source in self.sources.values():
      result[_provenance_dir(source.provenance_id)].extend(source.triples())
    for provenance in self.provenances.values():
      prov_id = provenance.provenance_id or provenance.id
      result[_provenance_dir(prov_id)].extend(provenance.triples())
    for group in self.groups.values():
      result["_global"].extend(group.triples())
    return {k: _dedupe_triples(triples) for k, triples in result.items()}


def undefined_provenances_message(
    files_by_provenance: dict[str, list[str]]) -> str:
  """The error for provenances that config references but MCF doesn't define.

  Args:
    files_by_provenance: Each undefined provenance, as written in config, mapped
      to the paths of the files that reference it.
  """
  lines = [
      f"  - '{prov_name}', used by {', '.join(paths)}"
      for prov_name, paths in sorted(files_by_provenance.items())
  ]
  return ("Provenances referenced in config.json are not defined in any MCF"
          " file:\n" + "\n".join(lines) +
          "\nDefine each one as a Provenance node, with a 'source' linking to"
          " a Source node, in an MCF file matched by 'inputFiles'.")


def _provenance_dir(raw_id: str) -> str:
  if not raw_id or raw_id == "_global":
    return "_global"
  return strip_namespace(raw_id)


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
