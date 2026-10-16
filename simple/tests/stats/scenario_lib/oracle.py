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
"""Row oracle: an independent expectation for variable-per-row observations.

A golden only proves the output didn't change. The oracle proves the content
is right. It reads the input CSVs with Python's csv module (not pandas) and
derives, from the config alone, the observation every row must produce. It
then compares that multiset with the observation *records* in the output, so
lost, duplicated and altered rows are all caught.

Intended contract encoded here (deliberately not copied from the
implementation):
- Exactly one observation per data row whose value cell is non-empty and not a
  missing-value marker.
- Values compare numerically. Representation (100 vs 100.0) is pinned by the
  golden and the value spec, not here.
- Entity, variable and date cells are kept literally: "NA" is Namibia, not a
  missing value. Namespace prefixes are stripped and custom namespaces are
  rewritten to dcid.
- Rows with an empty entity, variable or date must not produce an
  observation (they are invalid).
- columnsToResolve maps names to dcids through the scenario's [dc_api.resolve]
  fixture. Already-prefixed values are pre-resolved.
- observationProperties from config.json apply when a row has no value of its
  own.

Representation details are checked by goldens; this oracle is about content.
"""

import collections
import csv
import json
import pathlib
import re
from typing import Any

from stats import constants
from tests.stats.scenario_lib import facts as facts_lib
from tests.stats.scenario_lib import invariants
from tests.stats.scenario_lib import spec

# Cells that mean "no value" in the value column. This covers the documented
# STANDARD_NA_VALUES plus the standard spreadsheet / pandas missing markers.
_MISSING_VALUE_MARKERS = {
    "", "-", "nil", "n/a", "na", "none", "null", "<na>", "nan", "#n/a",
    "#n/a n/a", "#na", "-nan", "1.#ind", "1.#qnan", "-1.#ind", "-1.#qnan"
}
_PREFIX_RE = re.compile(r"^([A-Za-z0-9_\-]+):")

_STANDARD_KEYS = {
    "dcid:observationAbout": "about",
    "dcid:variableMeasured": "variable",
    "dcid:observationDate": "date",
    "dcid:value": "value",
    "dcid:unit": "unit",
    "dcid:scalingFactor": "scalingFactor",
    "dcid:measurementMethod": "measurementMethod",
    "dcid:observationPeriod": "observationPeriod",
}
_PROPERTY_FIELDS = ("unit", "scalingFactor", "measurementMethod",
                    "observationPeriod")
_NUMERIC_FIELDS = ("value", "scalingFactor")


def normalize_id(text: str) -> str:
  """Intended id normalization: trim, drop a namespace prefix, keep URLs."""
  text = text.strip()
  if text.startswith(("http://", "https://")):
    return text
  match = _PREFIX_RE.match(text)
  if match:
    text = text[match.end():]
  return text.lstrip("/")


def _numeric(value: Any) -> str:
  try:
    # "+ 0.0" folds -0.0 into 0.0: numerically equal values must compare equal.
    return repr(float(value) + 0.0)
  except (TypeError, ValueError):
    return str(value).strip()


def _field_value(field: str, raw: str) -> str:
  if field in _NUMERIC_FIELDS:
    return _numeric(raw)
  if field in ("date", "observationPeriod"):
    return raw.strip()
  return normalize_id(raw)


def expected_observations(scenario: spec.Scenario) -> collections.Counter:
  """Every observation the input must produce, as a multiset of field tuples."""
  config = json.loads((scenario.input_dir / "config.json").read_text())
  resolve_table = scenario.dc_api.get("resolve", {})
  expected = collections.Counter()
  for entry in config.get("inputFiles", []):
    pattern = entry.get("pattern", "")
    if pattern.lower().endswith(".mcf"):
      continue
    if entry.get("importType", "observations") != "observations":
      continue
    mappings = entry.get("columnMappings", {})
    to_resolve = set(entry.get("columnsToResolve", []))
    defaults = {
        k: str(v) for k, v in entry.get("observationProperties", {}).items()
    }
    for path in sorted(
        p for p in scenario.input_dir.glob(pattern) if p.is_file()):
      for row in _read_rows(path):
        observation = _expected_for_row(row, mappings, to_resolve,
                                        resolve_table, defaults)
        if observation is not None:
          expected[observation] += 1
  return expected


def observed_observations(view: facts_lib.LoaderView) -> collections.Counter:
  """The same projection over output records (one entry per record)."""
  observed = collections.Counter()
  for record in view.observation_records:
    fields = {}
    for field in ("about", "variable", "date", "value") + _PROPERTY_FIELDS:
      predicate = {
          "about": "observationAbout",
          "variable": "variableMeasured",
          "date": "observationDate",
      }.get(field, field)
      fact = record.first(predicate)
      if fact is not None:
        fields[field] = _field_value(field, _plain(fact))
    for listed in invariants._listed_observation_properties(record):  # pylint: disable=protected-access
      if listed in ("observationAbout",):
        continue
      fact = record.first(listed)
      if fact is not None:
        fields[f"prop:{listed}"] = normalize_id(_plain(fact))
    observed[_freeze(fields)] += 1
  return observed


def check_rows(scenario: spec.Scenario,
               view: facts_lib.LoaderView) -> list[invariants.Violation]:
  expected = expected_observations(scenario)
  observed = observed_observations(view)
  missing = expected - observed
  unexpected = observed - expected
  violations = []
  for fields, count in sorted(missing.items(), key=str)[:10]:
    violations.append(
        invariants.Violation(
            "row_oracle", f"missing {count}x observation {_describe(fields)}"))
  for fields, count in sorted(unexpected.items(), key=str)[:10]:
    violations.append(
        invariants.Violation(
            "row_oracle",
            f"unexpected {count}x observation {_describe(fields)}"))
  return violations


def _read_rows(path: pathlib.Path) -> list[dict[str, str]]:
  with open(path, newline="", encoding="utf-8-sig") as f:
    return list(csv.DictReader(f))


def _resolve(value: str, column: str, to_resolve: set[str],
             resolve_table: dict[str, str]) -> str:
  if column not in to_resolve:
    return value
  if _PREFIX_RE.match(value.strip()):
    return value  # Pre-resolved, e.g. dcid:country/USA.
  prefix = constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES.get(column.lower())
  if prefix is not None:
    return f"{prefix}{value.strip()}"
  return resolve_table.get(value, resolve_table.get(value.strip(), value))


def _expected_for_row(row: dict[str, str], mappings: dict[str, str],
                      to_resolve: set[str], resolve_table: dict[str, str],
                      defaults: dict[str, str]) -> frozenset | None:
  value_column = mappings.get("dcid:value", "value")
  raw_value = (row.get(value_column) or "").strip()
  if raw_value.lower() in _MISSING_VALUE_MARKERS:
    return None
  fields: dict[str, str] = {}
  for key, column in mappings.items():
    raw = row.get(column)
    if raw is None:
      continue
    raw = _resolve(raw, column, to_resolve, resolve_table)
    field = _STANDARD_KEYS.get(key)
    if field is None:
      if raw.strip():
        fields[f"prop:{normalize_id(key)}"] = normalize_id(raw)
      continue
    if field in _PROPERTY_FIELDS and not raw.strip():
      continue  # Falls back to the config default below.
    fields[field] = _field_value(field, raw)
  for key, default in defaults.items():
    field = key if key in _PROPERTY_FIELDS else f"prop:{normalize_id(key)}"
    fields.setdefault(
        field,
        _field_value(field, default)
        if field in _PROPERTY_FIELDS else normalize_id(default))
  for required in ("variable", "date"):
    if not fields.get(required):
      return None
  has_entity = fields.get("about") or any(k.startswith("prop:") for k in fields)
  if not has_entity:
    return None
  if "dcid:observationAbout" in mappings and not fields.get("about"):
    return None
  return _freeze(fields)


def _plain(fact: facts_lib.Fact) -> str:
  if fact.kind == facts_lib.KIND_LIT:
    value = facts_lib.decode_literal(fact.obj)
    return value if isinstance(value, str) else json.dumps(value)
  return fact.obj


def _freeze(fields: dict[str, str]) -> frozenset:
  return frozenset(fields.items())


def _describe(fields: frozenset) -> str:
  return "{" + ", ".join(f"{k}={v!r}" for k, v in sorted(fields)) + "}"
