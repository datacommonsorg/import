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
"""Loader-view facts: what the Data Commons loader stores for preprocessor output.

The ingestion pipeline parses every JSON-LD shard with
util/src/main/java/org/datacommons/util/parser/jsonld/JsonLdParser.java into an
McfGraph. This module re-implements that parser's observable behavior so tests
assert on what actually lands in the graph, not on how the preprocessor happened
to serialize it:

1. Each shard is JSON-LD *expanded* with its own @context (as JsonLdParser calls
   JsonLdProcessor.expand). Keys that don't expand to IRIs are dropped, exactly
   as the loader drops them.
2. Ids, types and resolved references lose the two bases JsonLdParser strips
   (https://datacommons.org/browser/, https://schema.org/) and then the
   "dcid:", "dcs:" and "schema:" namespaces GraphReader strips
   (McfUtil.stripNamespace). Any other prefix (e.g. "undata:") survives, as it
   does in the graph.
3. Property keys are reduced to their last path / fragment segment (cleanKey).
4. @type values become typeOf references.
5. observationProperties nested inside a reference object are lifted onto the
   parent node. Any other nested key is ignored by the loader; it is recorded
   here as an anomaly so tests can flag it.
6. Literals keep their JSON type (100 vs 100.0 vs "100"). The loader stores
   value.toString(), so these are distinct stored values.

Facts are a *set* (what the graph contains). Records keep one entry per
top-level node object so tests can also detect duplicated records, which a set
would hide.
"""

import collections
import dataclasses
import json
import pathlib
import re
from typing import Any, Iterable

from pyld import jsonld

DCID_BASE = "https://datacommons.org/browser/"
SCHEMA_BASE = "https://schema.org/"

KIND_REF = "ref"  # McfGraph RESOLVED_REF
KIND_UREF = "uref"  # McfGraph UNRESOLVED_REF (local "l:" ids)
KIND_LIT = "lit"  # McfGraph TEXT; object holds the JSON encoding of the value

OBSERVATION_TYPE = "StatVarObservation"

# Observation ids are "obs_" + a sha256 hex digest. Goldens abbreviate them to
# keep lines readable; 16 hex chars (64 bits) cannot collide in test fixtures.
_OBS_ID_RE = re.compile(r"^obs_([0-9a-f]{64})$")
_OBS_ID_KEEP_HEX = 16

FACTS_HEADER = (
    "# Loader-view facts (see simple/tests/stats/scenario_lib/facts.py).\n"
    "# Columns: folder, subject, predicate, kind (ref|uref|lit), object.\n"
    "# Generated in 'threaded' mode. Regenerate with TEST_MODE=write; never edit"
    " by hand.\n")


def strip_prefix(value: str) -> str:
  """Port of JsonLdParser.stripPrefix."""
  if value.startswith(DCID_BASE):
    return value[len(DCID_BASE):]
  if value.startswith(SCHEMA_BASE):
    return value[len(SCHEMA_BASE):]
  return value


# Port of McfUtil.stripNamespace (Vocabulary DCID_PREFIX, DC_SCHEMA_PREFIX,
# SCHEMA_ORG_PREFIX). GraphReader applies it to node ids (via
# GraphUtils.getPropVal), types (GraphUtils.getPropertyValues) and reference
# objects (graphToEdges), so these prefixes never reach the graph.
_MCF_NAMESPACES = ("dcid:", "dcs:", "schema:")


def strip_namespace(value: str) -> str:
  if value.startswith(_MCF_NAMESPACES):
    return value[value.index(":") + 1:]
  return value


def loader_id(value: str) -> str:
  """The id the graph stores for a JSON-LD @id / reference / type."""
  return strip_namespace(strip_prefix(value))


def clean_key(key: str) -> str:
  """Port of JsonLdParser.cleanKey."""
  while key.endswith("/") or key.endswith("#"):
    if len(key) <= 1:
      return ""
    key = key[:-1]
  if "/" in key:
    key = key[key.rindex("/") + 1:]
  if "#" in key:
    key = key[key.rindex("#") + 1:]
  return key


def encode_literal(value: Any) -> str:
  """Encodes a literal so its JSON type stays visible (100 vs "100")."""
  return json.dumps(value, ensure_ascii=False, sort_keys=True)


def decode_literal(encoded: str) -> Any:
  return json.loads(encoded)


def _escape(text: str) -> str:
  """Makes control characters visible so one fact is always one line."""
  return (text.replace("\\", "\\\\").replace("\t", "\\t").replace(
      "\n", "\\n").replace("\r", "\\r"))


def abbreviate(token: str) -> str:
  """Shortens observation ids for display; leaves every other token alone."""
  match = _OBS_ID_RE.match(token)
  if match:
    return f"obs_{match.group(1)[:_OBS_ID_KEEP_HEX]}"
  return token


@dataclasses.dataclass(frozen=True, order=True)
class Fact:
  folder: str
  subject: str
  predicate: str
  kind: str
  obj: str

  def line(self) -> str:
    obj = self.obj if self.kind == KIND_LIT else abbreviate(self.obj)
    return "\t".join(
        _escape(part) for part in
        [self.folder,
         abbreviate(self.subject), self.predicate, self.kind, obj])


@dataclasses.dataclass
class Record:
  """One top-level node object, i.e. one McfGraph node built by the loader."""
  folder: str
  shard: str
  subject: str
  facts: list[Fact]

  def objects(self, predicate: str) -> list[Fact]:
    return [f for f in self.facts if f.predicate == predicate]

  def first(self, predicate: str) -> Fact | None:
    values = sorted(self.objects(predicate))
    return values[0] if values else None

  @property
  def types(self) -> set[str]:
    return {f.obj for f in self.objects("typeOf")}

  @property
  def is_observation(self) -> bool:
    return OBSERVATION_TYPE in self.types


@dataclasses.dataclass
class LoaderView:
  """Everything the loader would ingest from one preprocessor run."""
  records: list[Record]
  anomalies: list[str]
  null_values: list[str]
  shard_count: int

  @property
  def facts(self) -> set[Fact]:
    return {fact for record in self.records for fact in record.facts}

  @property
  def observation_records(self) -> list[Record]:
    return [r for r in self.records if r.is_observation]

  def observation_record_counts(self) -> collections.Counter:
    return collections.Counter(
        (r.folder, r.subject) for r in self.observation_records)

  def render(self) -> str:
    return render_facts(self.facts)


def load_loader_view(jsonld_root: pathlib.Path) -> LoaderView:
  """Builds the loader view for every shard under `<output>/jsonld`.

  The preprocessor writes `<output>/jsonld/<run dir>/<import folder>/<shard>`.
  The import folder matters (the ingestion handshake selects shards per
  folder), so it is part of every fact; the run dir and shard name are not.
  """
  records: list[Record] = []
  anomalies: list[str] = []
  null_values: list[str] = []
  shard_paths = sorted(
      jsonld_root.rglob("*.jsonld")) if jsonld_root.exists() else []
  for path in shard_paths:
    rel_parts = path.relative_to(jsonld_root).parts
    shard = "/".join(rel_parts)
    if len(rel_parts) == 3:
      folder = rel_parts[1]
    else:
      folder = "/".join(rel_parts[1:-1]) or "<root>"
      anomalies.append(f"{shard}: shard is not at <run dir>/<import folder>/")
    doc = json.loads(path.read_text(encoding="utf-8"))
    null_values.extend(_find_nulls(doc, shard))
    for node in jsonld.expand(doc):
      record = _to_record(node, folder, shard, anomalies)
      if record is not None:
        records.append(record)
  return LoaderView(records=records,
                    anomalies=anomalies,
                    null_values=null_values,
                    shard_count=len(shard_paths))


def render_facts(facts: Iterable[Fact]) -> str:
  """Renders facts grouped by node, with observations ordered readably.

  Non-observation nodes come first, sorted by id. Observations follow, sorted
  by (observationAbout, variableMeasured, observationDate) so a reviewer can
  scan them like a table.
  """
  by_node: dict[tuple[str, str], list[Fact]] = collections.defaultdict(list)
  for fact in facts:
    by_node[(fact.folder, fact.subject)].append(fact)

  def node_sort_key(item):
    (folder, subject), node_facts = item
    types = {f.obj for f in node_facts if f.predicate == "typeOf"}
    if OBSERVATION_TYPE not in types:
      return (folder, 0, subject, "", "", "")

    def first(predicate: str) -> str:
      values = sorted(f.obj for f in node_facts if f.predicate == predicate)
      return values[0] if values else ""

    return (folder, 1, first("observationAbout"), first("variableMeasured"),
            first("observationDate"), subject)

  lines = []
  for _, node_facts in sorted(by_node.items(), key=node_sort_key):
    lines.extend(fact.line() for fact in sorted(node_facts))
  return FACTS_HEADER + "".join(f"{line}\n" for line in lines)


def parse_rendered(text: str) -> list[str]:
  """Returns the fact lines of a rendered facts file (comments removed)."""
  return [
      line for line in text.splitlines() if line and not line.startswith("#")
  ]


def _as_list(value: Any) -> list:
  return value if isinstance(value, list) else [value]


def _find_nulls(value: Any, where: str, path: str = "$") -> list[str]:
  """JSON nulls are silently dropped by expansion, so find them up front."""
  found = []
  if value is None:
    found.append(f"{where}: null at {path}")
  elif isinstance(value, dict):
    for key, child in value.items():
      found.extend(_find_nulls(child, where, f"{path}.{key}"))
  elif isinstance(value, list):
    for index, child in enumerate(value):
      found.extend(_find_nulls(child, where, f"{path}[{index}]"))
  return found


def _to_record(node: dict, folder: str, shard: str,
               anomalies: list[str]) -> Record | None:
  """Port of JsonLdParser.parseNode for one expanded node object."""
  raw_id = node.get("@id")
  if raw_id is None:
    anomalies.append(f"{shard}: node without @id is skipped by the loader")
    return None
  subject = loader_id(str(raw_id))
  facts: list[Fact] = []
  for key, value in node.items():
    predicate = clean_key(key)
    if not predicate or predicate == "@id":
      continue
    if predicate == "@type":
      for type_value in _as_list(value):
        facts.append(
            Fact(folder, subject, "typeOf", KIND_REF,
                 loader_id(str(type_value))))
      continue
    for item in _as_list(value):
      facts.extend(
          _value_facts(folder, subject, predicate, item, shard, anomalies))
  return Record(folder=folder, shard=shard, subject=subject, facts=facts)


def _value_facts(folder: str, subject: str, predicate: str, item: Any,
                 shard: str, anomalies: list[str]) -> list[Fact]:
  """Port of JsonLdParser.processValueItem."""
  if isinstance(item, dict):
    if "@value" in item:
      return [
          Fact(folder, subject, predicate, KIND_LIT,
               encode_literal(item["@value"]))
      ]
    if "@id" in item:
      ref = strip_prefix(str(item["@id"]))
      kind = KIND_UREF if ref.startswith("l:") else KIND_REF
      if kind == KIND_REF:
        ref = strip_namespace(ref)
      facts = [Fact(folder, subject, predicate, kind, ref)]
      for nested_key, nested_value in item.items():
        if nested_key == "@id":
          continue
        nested_predicate = clean_key(nested_key)
        if not nested_predicate:
          continue
        if nested_predicate == "observationProperties":
          for inner in _as_list(nested_value):
            facts.extend(
                _value_facts(folder, subject, nested_predicate, inner, shard,
                             anomalies))
        else:
          anomalies.append(
              f"{shard}: nested key '{nested_key}' under {abbreviate(subject)}"
              f" -> {predicate} is ignored by the loader")
      return facts
    anomalies.append(f"{shard}: unsupported value for {abbreviate(subject)} ->"
                     f" {predicate} is ignored by the loader: {item}")
    return []
  if item is None:
    return []
  return [Fact(folder, subject, predicate, KIND_LIT, encode_literal(item))]
