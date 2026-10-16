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
"""Loads and validates scenario.toml files and the suite registries.

The schema is strict on purpose: an unknown or misspelled key is an error, so
a typo can never silently disable an expectation.
"""

import dataclasses
import json
import pathlib
import tomllib
from typing import Any

from tests.stats.scenario_lib import facts as facts_lib

SCENARIOS_DIR = pathlib.Path(__file__).resolve().parent.parent / "scenarios"
FEATURES_FILE = SCENARIOS_DIR / "features.toml"
KNOWN_ISSUES_FILE = SCENARIOS_DIR / "known_issues.toml"

ALL_MODES = ("threaded", "tiny_chunks", "multiprocess", "rdflib_nodes")
GOLDEN_MODE = "threaded"

# Checks a known issue may waive. "golden" = facts differ from the golden,
# "error" = the run raised although the scenario expects success.
INVARIANT_CHECKS = (
    "jsonld_anomalies",
    "no_null_values",
    "observation_required_props",
    "unique_observation_records",
    "observation_identity",
    "loader_strippable_ids",
    "dcid_charset",
    "node_records_typed",
    "provenance_url",
    "numeric_values",
)
WAIVABLE_CHECKS = INVARIANT_CHECKS + ("row_oracle", "golden", "error",
                                      "must_contain")

_TOP_KEYS = {
    "description", "features", "modes", "expect", "oracle", "runner", "dc_api",
    "error", "must_contain", "must_not_contain", "must_contain_observation",
    "known_issues"
}
_RUNNER_KEYS = {"config_file", "import_names", "import_proxy_entities"}
_ERROR_KEYS = {"type", "message_contains", "handshake_error_types"}
_FACT_KEYS = {"folder", "subject", "predicate", "ref", "uref", "lit"}
_OBS_KEYS = {
    "folder", "about", "variable", "date", "value", "unit", "scalingFactor",
    "measurementMethod", "observationPeriod", "provenance", "provenanceUrl"
}
_KNOWN_ISSUE_KEYS = {"id", "check", "modes", "note"}


class ScenarioError(ValueError):
  """A scenario.toml (or registry) is malformed."""


@dataclasses.dataclass(frozen=True)
class FactPattern:
  """A hand-written fact. Unset fields match anything."""
  folder: str | None
  subject: str | None
  predicate: str | None
  kind: str | None
  obj: str | None

  def matches(self, fact: facts_lib.Fact) -> bool:
    return ((self.folder is None or self.folder == fact.folder) and
            (self.subject is None or self.subject == fact.subject) and
            (self.predicate is None or self.predicate == fact.predicate) and
            (self.kind is None or self.kind == fact.kind) and
            (self.obj is None or self.obj == fact.obj))

  def describe(self) -> str:
    parts = [
        f"{name}={value!r}" for name, value in dataclasses.asdict(self).items()
        if value is not None
    ]
    return ", ".join(parts)


@dataclasses.dataclass(frozen=True)
class ObservationPattern:
  """A hand-written observation: fields are compared on loader-view values."""
  fields: dict[str, Any]

  def describe(self) -> str:
    return ", ".join(f"{k}={v!r}" for k, v in sorted(self.fields.items()))


@dataclasses.dataclass(frozen=True)
class KnownIssue:
  id: str
  check: str
  modes: tuple[str, ...]
  note: str

  def applies(self, check: str, mode: str) -> bool:
    return self.check == check and mode in self.modes


@dataclasses.dataclass(frozen=True)
class ExpectedError:
  type: str
  message_contains: tuple[str, ...]
  handshake_error_types: tuple[str, ...]


@dataclasses.dataclass(frozen=True)
class Scenario:
  name: str
  path: pathlib.Path
  description: str
  features: tuple[str, ...]
  modes: tuple[str, ...]
  expect: str
  oracle: str
  runner: dict[str, Any]
  dc_api: dict[str, Any]
  error: ExpectedError | None
  must_contain: tuple[FactPattern, ...]
  must_not_contain: tuple[FactPattern, ...]
  must_contain_observation: tuple[ObservationPattern, ...]
  known_issues: tuple[KnownIssue, ...]

  @property
  def input_dir(self) -> pathlib.Path:
    return self.path / "input"

  @property
  def expected_dir(self) -> pathlib.Path:
    return self.path / "expected"

  @property
  def facts_golden(self) -> pathlib.Path:
    return self.expected_dir / "facts.tsv"

  def known_issues_for(self, check: str, mode: str) -> list[KnownIssue]:
    return [k for k in self.known_issues if k.applies(check, mode)]


def _check_keys(where: str, table: dict, allowed: set[str]) -> None:
  unknown = set(table) - allowed
  if unknown:
    raise ScenarioError(f"{where}: unknown keys {sorted(unknown)}")


def _modes(where: str, value: Any) -> tuple[str, ...]:
  if value in (None, "all"):
    return ALL_MODES
  if not isinstance(value, list) or not value:
    raise ScenarioError(f"{where}: modes must be 'all' or a non-empty list")
  unknown = set(value) - set(ALL_MODES)
  if unknown:
    raise ScenarioError(f"{where}: unknown modes {sorted(unknown)}")
  return tuple(m for m in ALL_MODES if m in value)


def _fact_pattern(where: str, table: dict) -> FactPattern:
  _check_keys(where, table, _FACT_KEYS)
  object_keys = [k for k in ("ref", "uref", "lit") if k in table]
  if len(object_keys) > 1:
    raise ScenarioError(f"{where}: use only one of ref / uref / lit")
  kind = obj = None
  if object_keys:
    kind = object_keys[0]
    raw = table[kind]
    obj = facts_lib.encode_literal(raw) if kind == "lit" else str(raw)
  return FactPattern(folder=table.get("folder"),
                     subject=table.get("subject"),
                     predicate=table.get("predicate"),
                     kind=kind,
                     obj=obj)


def load_scenario(path: pathlib.Path) -> Scenario:
  where = f"{path.name}/scenario.toml"
  with open(path / "scenario.toml", "rb") as f:
    data = tomllib.load(f)
  _check_keys(where, data, _TOP_KEYS)

  description = data.get("description", "").strip()
  if not description:
    raise ScenarioError(f"{where}: description is required")
  features = tuple(data.get("features", []))
  if not features:
    raise ScenarioError(f"{where}: at least one feature is required")

  expect = data.get("expect", "success")
  if expect not in ("success", "error"):
    raise ScenarioError(f"{where}: expect must be 'success' or 'error'")
  oracle = data.get("oracle", "none")
  if oracle not in ("vpr", "none"):
    raise ScenarioError(f"{where}: oracle must be 'vpr' or 'none'")

  runner = dict(data.get("runner", {}))
  _check_keys(f"{where} [runner]", runner, _RUNNER_KEYS)

  error = None
  if expect == "error":
    error_table = data.get("error")
    if not error_table:
      raise ScenarioError(f"{where}: expect='error' requires an [error] table")
    _check_keys(f"{where} [error]", error_table, _ERROR_KEYS)
    if "type" not in error_table:
      raise ScenarioError(f"{where} [error]: type is required")
    error = ExpectedError(
        type=error_table["type"],
        message_contains=tuple(error_table.get("message_contains", [])),
        handshake_error_types=tuple(error_table.get("handshake_error_types",
                                                    [])))
  elif "error" in data:
    raise ScenarioError(f"{where}: [error] is only valid with expect='error'")

  must_contain = tuple(
      _fact_pattern(f"{where} must_contain[{i}]", t)
      for i, t in enumerate(data.get("must_contain", [])))
  must_not_contain = tuple(
      _fact_pattern(f"{where} must_not_contain[{i}]", t)
      for i, t in enumerate(data.get("must_not_contain", [])))
  observations = []
  for i, table in enumerate(data.get("must_contain_observation", [])):
    _check_keys(f"{where} must_contain_observation[{i}]", table, _OBS_KEYS)
    observations.append(ObservationPattern(fields=dict(table)))

  known_issues = []
  for i, table in enumerate(data.get("known_issues", [])):
    issue_where = f"{where} known_issues[{i}]"
    _check_keys(issue_where, table, _KNOWN_ISSUE_KEYS)
    for required in ("id", "check"):
      if required not in table:
        raise ScenarioError(f"{issue_where}: '{required}' is required")
    if table["check"] not in WAIVABLE_CHECKS:
      raise ScenarioError(f"{issue_where}: unknown check '{table['check']}'")
    known_issues.append(
        KnownIssue(id=table["id"],
                   check=table["check"],
                   modes=_modes(issue_where, table.get("modes")),
                   note=table.get("note", "")))

  return Scenario(name=path.name,
                  path=path,
                  description=description,
                  features=features,
                  modes=_modes(where, data.get("modes")),
                  expect=expect,
                  oracle=oracle,
                  runner=runner,
                  dc_api=dict(data.get("dc_api", {})),
                  error=error,
                  must_contain=must_contain,
                  must_not_contain=must_not_contain,
                  must_contain_observation=tuple(observations),
                  known_issues=tuple(known_issues))


def discover_scenarios() -> list[Scenario]:
  return [
      load_scenario(p.parent)
      for p in sorted(SCENARIOS_DIR.glob("*/scenario.toml"))
  ]


def load_feature_registry() -> dict[str, str]:
  with open(FEATURES_FILE, "rb") as f:
    return dict(tomllib.load(f).get("features", {}))


def load_known_issue_registry() -> dict[str, dict[str, Any]]:
  with open(KNOWN_ISSUES_FILE, "rb") as f:
    return dict(tomllib.load(f))


def describe(value: Any) -> str:
  return json.dumps(value, sort_keys=True, default=str)
