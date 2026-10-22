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
"""Invariants: correctness rules every successful run must satisfy.

Goldens catch *change*; invariants catch *wrongness*, including wrongness that
was already present when a golden was generated. Each rule is tied to a
concrete loader requirement or data-integrity property:

- jsonld_anomalies: nothing the loader would silently skip or ignore.
- no_null_values: JSON nulls are dropped by expansion (silent data loss).
- observation_required_props: GraphReader throws if an observation has no
  entity, and a series needs a variable, a date and a value.
- unique_observation_records: one record per observation id. Duplicates make
  the stored value depend on parse order.
- observation_identity: observations that are the same data point (same
  entity, variable, date, provenance, unit, ...) must share one id. Otherwise
  the graph gets two points for one key and duplicate detection is bypassed.
- loader_strippable_ids: ids, references and property names must not keep a
  prefix the loader doesn't strip (only https://datacommons.org/browser/ and
  https://schema.org/ are stripped). "dcs:Foo" would be stored verbatim.
- dcid_charset: no whitespace or control characters in ids and references.
- node_records_typed: the loader types a node record without typeOf as
  "Thing". A node split across shards can get its type overwritten.
- provenance_url: observations carry their provenance node's url
  (provenanceUrl is part of the loader's time-series key).
- numeric_values: values are finite JSON numbers (non-numeric values are
  rejected before writing, so a string means a conversion fell through).
"""

import collections
import dataclasses
import json
import math
import re

from tests.stats.scenario_lib import facts as facts_lib

_LEFTOVER_PREFIX_RE = re.compile(r"^[A-Za-z][A-Za-z0-9+.\-]*:")
_BAD_CHARS_RE = re.compile(r"[\s\x00-\x1f\x7f]")
_IDENTITY_EXCLUDED_PREDICATES = {
    "value", "scalingFactor", "provenanceUrl", "typeOf"
}
_REQUIRED_OBSERVATION_PREDICATES = ("variableMeasured", "observationDate",
                                    "value")


@dataclasses.dataclass(frozen=True)
class Violation:
  check: str
  detail: str


def _is_url(value: str) -> bool:
  return value.startswith("http://") or value.startswith("https://")


def _strip_any_prefix(value: str) -> str:
  match = _LEFTOVER_PREFIX_RE.match(value)
  return value[match.end():] if match and not _is_url(value) else value


def _listed_observation_properties(record: facts_lib.Record) -> list[str]:
  """Entity properties named by observationProperties, prefixes removed."""
  listed = []
  for fact in record.objects("observationProperties"):
    value = fact.obj
    if fact.kind == facts_lib.KIND_LIT:
      decoded = facts_lib.decode_literal(fact.obj)
      value = decoded if isinstance(decoded, str) else str(decoded)
    listed.append(_strip_any_prefix(value))
  return listed


def check_jsonld_anomalies(view: facts_lib.LoaderView) -> list[Violation]:
  return [Violation("jsonld_anomalies", a) for a in view.anomalies]


def check_no_null_values(view: facts_lib.LoaderView) -> list[Violation]:
  return [Violation("no_null_values", n) for n in view.null_values]


def check_observation_required_props(
    view: facts_lib.LoaderView) -> list[Violation]:
  violations = []
  for record in view.observation_records:
    present = {f.predicate for f in record.facts}
    missing = [p for p in _REQUIRED_OBSERVATION_PREDICATES if p not in present]
    listed = _listed_observation_properties(record)
    if listed:
      missing.extend(f"{p} (listed in observationProperties)" for p in listed
                     if p not in present)
    elif "observationAbout" not in present:
      missing.append("observationAbout")
    if missing:
      violations.append(
          Violation(
              "observation_required_props",
              f"{record.shard}: {facts_lib.abbreviate(record.subject)} is"
              f" missing {missing}; has {_summarize(record)}"))
  return violations


def check_unique_observation_records(
    view: facts_lib.LoaderView) -> list[Violation]:
  violations = []
  by_subject = collections.defaultdict(list)
  for record in view.observation_records:
    by_subject[(record.folder, record.subject)].append(record)
  for (folder, subject), records in sorted(by_subject.items()):
    if len(records) > 1:
      violations.append(
          Violation(
              "unique_observation_records",
              f"{folder}/{facts_lib.abbreviate(subject)} appears in"
              f" {len(records)} records: " +
              " | ".join(_summarize(r) for r in records)))
  return violations


def check_observation_identity(view: facts_lib.LoaderView) -> list[Violation]:
  subjects_by_key = collections.defaultdict(set)
  example = {}
  for record in view.observation_records:
    key = (record.folder,
           frozenset((f.predicate, f.kind, f.obj)
                     for f in record.facts
                     if f.predicate not in _IDENTITY_EXCLUDED_PREDICATES))
    subjects_by_key[key].add(record.subject)
    example.setdefault(key, record)
  violations = []
  for key, subjects in subjects_by_key.items():
    if len(subjects) > 1:
      ids = ", ".join(sorted(facts_lib.abbreviate(s) for s in subjects))
      violations.append(
          Violation(
              "observation_identity",
              f"{len(subjects)} different ids ({ids}) for the same data point:"
              f" {_summarize(example[key], skip_value=True)}"))
  return violations


def check_loader_strippable_ids(view: facts_lib.LoaderView) -> list[Violation]:
  violations = set()
  for record in view.records:
    if _LEFTOVER_PREFIX_RE.match(
        record.subject) and not _is_url(record.subject):
      violations.add(f"subject '{record.subject}' keeps a prefix")
    for fact in record.facts:
      if ":" in fact.predicate:
        violations.add(f"property '{fact.predicate}' (on"
                       f" {facts_lib.abbreviate(record.subject)}) keeps a"
                       " prefix")
      if (fact.kind == facts_lib.KIND_REF and
          _LEFTOVER_PREFIX_RE.match(fact.obj) and not _is_url(fact.obj)):
        violations.add(f"reference '{fact.obj}' (from"
                       f" {facts_lib.abbreviate(record.subject)} ->"
                       f" {fact.predicate}) keeps a prefix")
  return [Violation("loader_strippable_ids", v) for v in sorted(violations)]


def check_dcid_charset(view: facts_lib.LoaderView) -> list[Violation]:
  violations = set()
  for record in view.records:
    if not record.subject or _BAD_CHARS_RE.search(record.subject):
      violations.add(f"subject {record.subject!r}")
    for fact in record.facts:
      if fact.kind in (facts_lib.KIND_REF, facts_lib.KIND_UREF) and (
          not fact.obj or _BAD_CHARS_RE.search(fact.obj)):
        violations.add(f"reference {fact.obj!r} from"
                       f" {facts_lib.abbreviate(record.subject)} ->"
                       f" {fact.predicate}")
  return [Violation("dcid_charset", v) for v in sorted(violations)]


def check_node_records_typed(view: facts_lib.LoaderView) -> list[Violation]:
  violations = []
  for record in view.records:
    if not record.is_observation and not record.types:
      violations.append(
          Violation(
              "node_records_typed",
              f"{record.shard}: record for '{record.subject}' has no typeOf"
              f" (loader types it Thing): {_summarize(record)}"))
  return violations


def check_provenance_url(view: facts_lib.LoaderView) -> list[Violation]:
  """Observations must carry their provenance node's url as provenanceUrl.

  GraphReader reads provenanceUrl into the time-series key, so a missing or
  mismatched value loses (or corrupts) source attribution in the graph.
  """
  urls: dict[str, set[str]] = collections.defaultdict(set)
  for record in view.records:
    if any(t.split(":")[-1] == "Provenance" for t in record.types):
      for fact in record.objects("url"):
        if fact.kind == facts_lib.KIND_LIT:
          urls[record.subject].add(str(facts_lib.decode_literal(fact.obj)))
  violations = []
  for record in view.observation_records:
    provenance = record.first("provenance")
    expected = urls.get(provenance.obj, set()) if provenance else set()
    actual = {
        str(facts_lib.decode_literal(f.obj))
        for f in record.objects("provenanceUrl")
        if f.kind == facts_lib.KIND_LIT
    }
    if expected and not actual:
      violations.append(
          f"{facts_lib.abbreviate(record.subject)} has no provenanceUrl, but"
          f" its provenance '{provenance.obj}' has url {sorted(expected)}")
    elif actual and not actual <= expected:
      violations.append(
          f"{facts_lib.abbreviate(record.subject)} has provenanceUrl"
          f" {sorted(actual)}, but its provenance"
          f" '{provenance.obj if provenance else None}' has url"
          f" {sorted(expected)}")
  # One line per distinct problem keeps reports short for big scenarios.
  counted = collections.Counter(
      re.sub(r"obs_[0-9a-f]+", "<obs>", v) for v in violations)
  return [
      Violation("provenance_url", f"{n}x {text}")
      for text, n in sorted(counted.items())
  ]


def check_numeric_values(view: facts_lib.LoaderView) -> list[Violation]:
  """Observation values must be emitted as finite JSON numbers.

  Non-numeric values are rejected before writing (validate_numeric_values),
  and the writer converts values with _parse_numeric, so a string value
  ("1e5") or a non-finite one ("inf") means a conversion fell through.
  """
  violations = []
  for record in view.observation_records:
    for fact in record.objects("value"):
      value = (facts_lib.decode_literal(fact.obj)
               if fact.kind == facts_lib.KIND_LIT else fact.obj)
      is_number = isinstance(value,
                             (int, float)) and not isinstance(value, bool)
      if not is_number or not math.isfinite(value):
        violations.append(
            Violation(
                "numeric_values",
                f"{facts_lib.abbreviate(record.subject)} value {fact.obj} is"
                f" not a finite JSON number: {_summarize(record)}"))
  return violations


CHECKS = {
    "jsonld_anomalies": check_jsonld_anomalies,
    "no_null_values": check_no_null_values,
    "observation_required_props": check_observation_required_props,
    "unique_observation_records": check_unique_observation_records,
    "observation_identity": check_observation_identity,
    "loader_strippable_ids": check_loader_strippable_ids,
    "dcid_charset": check_dcid_charset,
    "node_records_typed": check_node_records_typed,
    "provenance_url": check_provenance_url,
    "numeric_values": check_numeric_values,
}


def check_all(view: facts_lib.LoaderView) -> list[Violation]:
  violations = []
  for check in CHECKS.values():
    violations.extend(check(view))
  return violations


def _summarize(record: facts_lib.Record, skip_value: bool = False) -> str:
  parts = []
  for fact in sorted(record.facts):
    if skip_value and fact.predicate == "value":
      continue
    obj = fact.obj if fact.kind == facts_lib.KIND_LIT else json.dumps(fact.obj)
    parts.append(f"{fact.predicate}={obj}")
  return "{" + ", ".join(parts) + "}"
