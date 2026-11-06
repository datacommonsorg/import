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
from dataclasses import replace

from stats.config import Config
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVarGroup
from stats.data import strip_namespace
from stats.data import Triple
import stats.schema_constants as sc
from util.filesystem import File


class Nodes:
  """Provenance, Source and StatVarGroup nodes of a run.

  Not thread-safe: the coordinator and each worker process have their own;
  workers' nodes are merged into the coordinator's with `merge_metadata`.
  """

  def __init__(self, config: Config) -> None:
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

  def register_provenance(
      self,
      id: str,
      name: str = "",
      url: str = "",
      source_id: str = "",
      custom_triples: list[Triple] | None = None) -> Provenance:
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
    _extend_unique(prov.custom_triples, _about(clean_id, custom_triples))
    return prov

  def register_source(self,
                      id: str,
                      name: str = "",
                      url: str = "",
                      custom_triples: list[Triple] | None = None,
                      import_folder: str = "") -> Source:
    """Registers a Source, or fills in the missing fields of a known one.

    A Source is written to one import folder. When several imports define it,
    that is the alphabetically first `import_folder`, so the choice does not
    depend on registration order.
    """
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
    _extend_unique(src.custom_triples, _about(clean_id, custom_triples))
    if import_folder:
      src.import_folder = min(src.import_folder or import_folder, import_folder)
    return src

  def merge_metadata(self, provenances: dict[str, Provenance],
                     sources: dict[str, Source]) -> None:
    """Registers Provenance and Source nodes held by another `Nodes`.

    Worker processes each build their own `Nodes`, so this is how metadata
    crosses process boundaries: the nodes a worker adds are merged back into
    the coordinator. Each node goes through `register_source` or
    `register_provenance`, so:
    - `name`, `url` and `source_id` keep the first non-empty value; a
      non-empty `name` is also (re)pointed at the node as a lookup key.
    - a Source's `import_folder` becomes the
      alphabetically first of the folders that define it.
    - `custom_triples`, of Sources and Provenances alike, are unioned.
    """
    for src in sources.values():
      self.register_source(id=src.id,
                           name=src.name,
                           url=src.url,
                           custom_triples=src.custom_triples,
                           import_folder=src.import_folder)
    for prov in provenances.values():
      self.register_provenance(id=prov.id,
                               name=prov.name,
                               url=prov.url,
                               source_id=prov.source_id,
                               custom_triples=prov.custom_triples)

  def provenance_id(self, input_file: File) -> str:
    """Returns the id of the provenance that config names for `input_file`.

    The id comes from config alone, without registering or looking up a
    Provenance node: MCF files define those, possibly in another worker
    process, and `MetadataValidator` checks at the end of the run that every
    referenced provenance is defined.

    Raises:
      ValueError: If config names no provenance.
    """
    prov_name = self.config.provenance_name(input_file)
    if not prov_name:
      raise ValueError(
          f"A provenance is absolutely required for file '{input_file.path}'. "
          f"Please specify the 'provenance' property in your config.json.")
    return _clean_metadata_id(prov_name)

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

  def triples_by_provenance_dir(self) -> dict[str, list[Triple]]:
    result: dict[str, list[Triple]] = defaultdict(list)
    for source in self.sources.values():
      result[_provenance_dir(source.import_folder)].extend(source.triples())
    for provenance in self.provenances.values():
      prov_id = provenance.provenance_id or provenance.id
      result[_provenance_dir(prov_id)].extend(provenance.triples())
    for group in self.groups.values():
      result["_global"].extend(group.triples())
    return {k: _dedupe_triples(triples) for k, triples in result.items()}


def _provenance_dir(raw_id: str) -> str:
  if not raw_id or raw_id == "_global":
    return "_global"
  return strip_namespace(raw_id)


def _about(subject_id: str, triples: list[Triple] | None) -> list[Triple]:
  """Returns `triples` with `subject_id` as their subject.

  MCF files may spell a node's id differently (e.g. "dcid:X" or "X"), so the
  custom triples of a node are stored under its one clean id.
  """
  return [replace(t, subject_id=subject_id) for t in triples or []]


def _extend_unique(triples: list[Triple], new_triples: list[Triple]) -> None:
  """Appends the `new_triples` that `triples` does not hold yet."""
  for triple in new_triples:
    if triple not in triples:
      triples.append(triple)


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
