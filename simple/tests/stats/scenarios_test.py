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
"""Scenario suite: every scenario x every execution mode.

Each scenario under tests/stats/scenarios/ is a folder of inputs plus
expectations (see scenarios/README.md). Every (scenario, mode) pair becomes
one test, so a failure names exactly which feature broke in which mode.

Regenerate goldens (from 'threaded' mode) with:
  TEST_MODE=write python3 -m pytest tests/stats/scenarios_test.py
"""

import pytest
from stats import config as config_module
from tests.stats.scenario_lib import harness
from tests.stats.scenario_lib import modes as modes_lib
from tests.stats.scenario_lib import spec
from tests.stats.test_util import is_write_mode

_SCENARIOS = spec.discover_scenarios()

# Module-level string constants in stats/config.py that are values, not keys.
_CONFIG_CONSTANTS_THAT_ARE_NOT_KEYS = {"_VARIABLE_PER_ROW_FORMAT"}


def _cases():
  for scenario in _SCENARIOS:
    for mode in scenario.modes:
      yield pytest.param(scenario,
                         modes_lib.MODES[mode],
                         id=f"{scenario.name}-{mode}")


@pytest.mark.parametrize("scenario,mode", list(_cases()))
def test_scenario(scenario: spec.Scenario, mode: modes_lib.Mode, tmp_path):
  write_golden = (is_write_mode() and mode.name == spec.GOLDEN_MODE and
                  scenario.expect == "success")
  result = harness.run_scenario(scenario, mode, tmp_path)
  problems = harness.evaluate(result, write_golden=write_golden)
  if problems:
    pytest.fail(f"Scenario '{scenario.name}' failed in mode '{mode.name}'"
                f" ({mode.description})\n{scenario.description}\n\n" +
                "\n\n".join(problems),
                pytrace=False)


def test_at_least_one_scenario_exists():
  assert _SCENARIOS, f"no scenarios found under {spec.SCENARIOS_DIR}"


def test_every_scenario_feature_is_registered():
  registry = spec.load_feature_registry()
  unknown = sorted(
      (s.name, f) for s in _SCENARIOS for f in s.features if f not in registry)
  assert not unknown, (
      f"scenarios cite features missing from {spec.FEATURES_FILE.name}:"
      f" {unknown}")


def test_every_config_key_is_tracked():
  """Every config.json key the preprocessor reads must be accounted for.

  A new key in stats/config.py fails this test until it is added to
  [config_keys] in features.toml, either mapped to the feature that covers it
  or left empty as explicitly untested.
  """
  config_keys = spec.load_config_key_registry()
  read_keys = {
      value for name, value in vars(config_module).items()
      if name.startswith("_") and name.isupper() and isinstance(value, str) and
      name not in _CONFIG_CONSTANTS_THAT_ARE_NOT_KEYS
  }
  untracked = sorted(read_keys - set(config_keys))
  assert not untracked, (
      f"config.json keys read by stats/config.py but missing from"
      f" [config_keys] in {spec.FEATURES_FILE.name}: {untracked}")
  stale = sorted(set(config_keys) - read_keys)
  assert not stale, (f"[config_keys] in {spec.FEATURES_FILE.name} lists keys"
                     f" stats/config.py no longer reads: {stale}")
  registry = spec.load_feature_registry()
  unknown = sorted(
      f"{k} -> {f}" for k, f in config_keys.items() if f and f not in registry)
  assert not unknown, f"[config_keys] cites unregistered features: {unknown}"


def test_every_registered_feature_has_a_scenario():
  covered = {f for s in _SCENARIOS for f in s.features}
  uncovered = sorted(set(spec.load_feature_registry()) - covered)
  assert not uncovered, (
      f"features in {spec.FEATURES_FILE.name} with no scenario: {uncovered}")


def test_known_issues_are_registered_and_referenced():
  registry = spec.load_known_issue_registry()
  cited = {k.id for s in _SCENARIOS for k in s.known_issues}
  unregistered = sorted(cited - set(registry))
  stale = sorted(set(registry) - cited)
  assert not unregistered, (
      f"known issues missing from {spec.KNOWN_ISSUES_FILE.name}:"
      f" {unregistered}")
  assert not stale, (
      f"{spec.KNOWN_ISSUES_FILE.name} lists issues no scenario reproduces"
      f" (fixed? remove them): {stale}")
  for issue_id, entry in registry.items():
    assert entry.get("title"), f"known issue {issue_id} needs a title"


def test_every_scenario_has_a_golden_or_expects_an_error():
  missing = [
      s.name
      for s in _SCENARIOS
      if s.expect == "success" and not s.facts_golden.exists()
  ]
  assert not missing or is_write_mode(), (
      f"scenarios without expected/facts.tsv: {missing}; generate with"
      " TEST_MODE=write")
