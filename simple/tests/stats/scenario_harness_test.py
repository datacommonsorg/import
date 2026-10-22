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
"""Self-tests for the scenario harness.

A test suite is only trustworthy if it fails on bad output. These tests:
1. Seed known kinds of bugs into the real pipeline (from the test side only)
   and assert the right checks catch each one. A control run proves the clean
   pipeline passes.
2. Unit-test the loader-view port against JsonLdParser / GraphReader rules.
3. Unit-test the DC API stub against the real util/dc_client.py code.
4. Unit-test the strict scenario schema and known-issue semantics.
"""

import contextlib
import dataclasses
import json
import os
import pathlib
from unittest import mock

import pytest
from stats import variable_per_row_importer as vpr
from tests.stats.scenario_lib import facts as facts_lib
from tests.stats.scenario_lib import harness
from tests.stats.scenario_lib import invariants
from tests.stats.scenario_lib import modes as modes_lib
from tests.stats.scenario_lib import oracle
from tests.stats.scenario_lib import spec
from tests.stats.scenario_lib.dc_api_stub import DcApiStub

from util import dc_client

_THREADED = modes_lib.MODES["threaded"]


def _scenario(name: str) -> spec.Scenario:
  return spec.load_scenario(spec.SCENARIOS_DIR / name)


def _run(scenario: spec.Scenario, tmp_path: pathlib.Path, *patches):
  with contextlib.ExitStack() as stack:
    for patch in patches:
      stack.enter_context(patch)
    result = harness.run_scenario(scenario, _THREADED, tmp_path)
  return result, harness.evaluate(result)


def _failed_checks(problems: list[str]) -> set[str]:
  return {p[1:p.index("]")] for p in problems if p.startswith("[")}


def _mutate_observations(mutate):
  """Patches the VPR pipeline so `mutate` edits each observations chunk."""
  real_filter = vpr.filter_invalid_observation_values

  def patched(df):
    return mutate(real_filter(df))

  return mock.patch.object(vpr, "filter_invalid_observation_values", patched)


# --- 1. Seeded bugs: the harness must catch each one. -----------------------


def test_control_clean_pipeline_passes(tmp_path):
  _, problems = _run(_scenario("obs_basic"), tmp_path)
  assert problems == []


def test_dropped_row_is_caught(tmp_path):
  _, problems = _run(_scenario("obs_basic"), tmp_path,
                     _mutate_observations(lambda df: df.iloc[1:]))
  assert {"golden", "row_oracle"} <= _failed_checks(problems), problems


def test_altered_value_is_caught(tmp_path):

  def alter(df):
    df = df.copy()
    df.iloc[0, df.columns.get_loc("value")] = "999"
    return df

  _, problems = _run(_scenario("obs_basic"), tmp_path,
                     _mutate_observations(alter))
  assert {"golden", "row_oracle"} <= _failed_checks(problems), problems


def test_duplicated_record_is_caught_although_facts_are_unchanged(tmp_path):
  import pandas as pd

  _, problems = _run(
      _scenario("obs_basic"), tmp_path,
      _mutate_observations(lambda df: pd.concat([df, df.iloc[:1]])))
  checks = _failed_checks(problems)
  assert {"unique_observation_records", "row_oracle"} <= checks, problems
  # An exact duplicate adds no new fact, so the set-based golden cannot see
  # it. This is why record-level checks exist.
  assert "golden" not in checks


def test_dropped_provenance_url_is_caught(tmp_path):
  _, problems = _run(
      _scenario("obs_provenance_url_from_mcf"), tmp_path,
      mock.patch("stats.nodes.Nodes.get_provenance_urls", return_value={}))
  assert {"provenance_url", "golden",
          "must_contain"} <= _failed_checks(problems), problems


def test_waived_check_that_passes_is_reported_as_no_longer_reproducing(
    tmp_path):
  scenario = dataclasses.replace(_scenario("obs_basic"),
                                 known_issues=(spec.KnownIssue(
                                     id="EX",
                                     check="golden",
                                     modes=("threaded",),
                                     note=""),))
  _, problems = _run(scenario, tmp_path)
  assert any("EX (golden) no longer reproduces" in p for p in problems)


def test_waiver_only_covers_its_own_check(tmp_path):
  scenario = dataclasses.replace(_scenario("obs_basic"),
                                 known_issues=(spec.KnownIssue(
                                     id="EX",
                                     check="golden",
                                     modes=("threaded",),
                                     note=""),))
  _, problems = _run(scenario, tmp_path,
                     _mutate_observations(lambda df: df.iloc[1:]))
  checks = _failed_checks(problems)
  assert "golden" not in checks  # Waived (and reproduced).
  assert "row_oracle" in checks  # Not waived: still reported.


def test_error_scenario_fails_if_the_run_succeeds(tmp_path):
  scenario = dataclasses.replace(_scenario("obs_basic"),
                                 expect="error",
                                 error=spec.ExpectedError(
                                     type="ValueError",
                                     message_contains=(),
                                     handshake_error_types=()))
  _, problems = _run(scenario, tmp_path)
  assert any("but it succeeded" in p for p in problems)


# --- 2. Loader-view port. ----------------------------------------------------

_CONTEXT = {"dcid": facts_lib.DCID_BASE}


def _view(tmp_path: pathlib.Path,
          graph: list,
          folder: str = "Prov") -> facts_lib.LoaderView:
  shard = tmp_path / "jsonld" / "run_dir" / folder / "node-00000.jsonld"
  shard.parent.mkdir(parents=True)
  shard.write_text(json.dumps({"@context": _CONTEXT, "@graph": graph}))
  return facts_lib.load_loader_view(tmp_path / "jsonld")


def test_literal_json_types_stay_distinct(tmp_path):
  view = _view(tmp_path, [{
      "@id": "dcid:a",
      "dcid:i": 100,
      "dcid:f": 100.0,
      "dcid:s": "100"
  }])
  objects = {f.predicate: f.obj for f in view.facts}
  assert objects == {"i": "100", "f": "100.0", "s": '"100"'}


def test_ids_types_and_refs_follow_loader_stripping(tmp_path):
  view = _view(tmp_path, [{
      "@id": "dcid:geoId/06",
      "@type": ["dcs:State", "dcid:Place"],
      "dcid:containedIn": {
          "@id": "schema:Country"
      },
      "dcid:custom": {
          "@id": "undata:WHO"
      },
  }])
  lines = {(f.subject, f.predicate, f.obj) for f in view.facts}
  assert lines == {
      ("geoId/06", "typeOf", "State"),
      ("geoId/06", "typeOf", "Place"),
      ("geoId/06", "containedIn", "Country"),
      ("geoId/06", "custom", "undata:WHO"),
  }


def test_keys_outside_the_context_are_dropped_like_the_loader_does(tmp_path):
  view = _view(tmp_path, [{"@id": "dcid:a", "name": "x", "dcid:ok": 1}])
  assert {f.predicate for f in view.facts} == {"ok"}


def test_observation_properties_are_lifted_and_other_nested_keys_flagged(
    tmp_path):
  view = _view(tmp_path, [{
      "@id": "dcid:obs_1",
      "@type": "dcid:StatVarObservation",
      "dcid:variableMeasured": {
          "@id": "dcid:Count_Person",
          "dcid:observationProperties": ["dcid:observationAbout"],
          "dcid:unexpected": "x",
      },
  }])
  record = view.records[0]
  assert record.first("observationProperties").obj == '"dcid:observationAbout"'
  assert record.first("variableMeasured").obj == "Count_Person"
  assert len(view.anomalies) == 1 and "unexpected" in view.anomalies[0]


def test_nulls_are_reported_even_though_expansion_drops_them(tmp_path):
  view = _view(tmp_path, [{"@id": "dcid:a", "dcid:v": None}])
  assert view.null_values and "dcid:v" in view.null_values[0]


def test_folder_is_part_of_every_fact(tmp_path):
  view = _view(tmp_path, [{"@id": "dcid:a", "dcid:v": 1}], folder="Imp_A")
  assert {f.folder for f in view.facts} == {"Imp_A"}


def test_render_is_stable_and_comment_free_when_parsed(tmp_path):
  view = _view(tmp_path, [{"@id": "dcid:b", "dcid:v": 1}, {"@id": "dcid:a"}])
  rendered = view.render()
  assert rendered.startswith("# Loader-view facts")
  assert facts_lib.parse_rendered(rendered) == ["Prov\tb\tv\tlit\t1"]


def test_invariants_flag_missing_entity_and_unstripped_refs(tmp_path):
  view = _view(tmp_path, [{
      "@id": "dcid:obs_1",
      "@type": "dcid:StatVarObservation",
      "dcid:variableMeasured": {
          "@id": "undata:Count"
      },
      "dcid:observationDate": "2020",
      "dcid:value": 1,
  }])
  checks = {v.check for v in invariants.check_all(view)}
  assert {"observation_required_props", "loader_strippable_ids"} <= checks


# --- 3. DC API stub, exercised through the real dc_client. -------------------


@contextlib.contextmanager
def _stub_env(fixtures):
  with DcApiStub(fixtures) as stub, \
       mock.patch.dict(os.environ, {"DC_API_ROOT": stub.root,
                                    "DC_API_KEY": "",
                                    "NO_PROXY": "127.0.0.1,localhost",
                                    "no_proxy": "127.0.0.1,localhost"}), \
       harness._real_dc_client():  # pylint: disable=protected-access
    yield stub


def test_stub_serves_place_resolution():
  fixtures = {"resolve": {"California": "geoId/06"}, "types": {}}
  with _stub_env(fixtures):
    resolved = dc_client.resolve_place_entities(["California", "Atlantis"],
                                                entity_type="State")
  assert resolved == {"California": "geoId/06"}


def test_stub_serves_entity_types_and_omits_unknown_nodes():
  with _stub_env({"types": {"country/USA": "Country"}}):
    types = dc_client.get_property_of_entities(["country/USA", "country/XYZ"],
                                               "typeOf")
  assert types == {"country/USA": "Country"}


def test_stub_serves_entities_of_type():
  fixtures = {"entities_of_type": {"Author": {"Jane Doe": "author/jane"}}}
  with _stub_env(fixtures):
    entities, token = dc_client.get_entities_of_type("Author")
  assert entities == {"Jane Doe": "author/jane"} and token == ""


def test_stub_injects_faults_and_dc_client_retries():
  with _stub_env({"faults": {"/v2/node": [503, "ok"]}}) as stub, \
       mock.patch("time.sleep"):
    dc_client.resolve_entity_type(["country/USA"])
  assert [r["path"] for r in stub.requests] == ["/v2/node", "/v2/node"]


def test_stub_records_unexpected_requests():
  with _stub_env({}) as stub:
    with pytest.raises(Exception):
      dc_client.post("/v2/observation", {})
  assert stub.unexpected


# --- 4. Strict schema and the oracle. ---------------------------------------


def _write_scenario(tmp_path: pathlib.Path, toml_text: str) -> pathlib.Path:
  folder = tmp_path / "s"
  (folder / "input").mkdir(parents=True)
  (folder / "scenario.toml").write_text(toml_text)
  return folder


def test_unknown_scenario_keys_are_rejected(tmp_path):
  folder = _write_scenario(
      tmp_path, 'description = "d"\nfeatures = ["f"]\nmust_contian = []\n')
  with pytest.raises(spec.ScenarioError, match="must_contian"):
    spec.load_scenario(folder)


def test_unknown_known_issue_check_is_rejected(tmp_path):
  folder = _write_scenario(
      tmp_path, 'description = "d"\nfeatures = ["f"]\n'
      '[[known_issues]]\nid = "E1"\ncheck = "goldn"\n')
  with pytest.raises(spec.ScenarioError, match="goldn"):
    spec.load_scenario(folder)


def test_oracle_derives_rows_from_input_not_from_output():
  expected = oracle.expected_observations(_scenario("obs_basic"))
  assert sum(expected.values()) == 5
  fields = {dict(obs)["about"] for obs in expected}
  assert fields == {"country/USA", "country/FRA"}


def test_oracle_keeps_na_as_an_entity_but_skips_na_values(tmp_path):
  folder = _write_scenario(
      tmp_path, 'description = "d"\nfeatures = ["f"]\noracle = "vpr"\n')
  (folder / "input" / "config.json").write_text(
      json.dumps({
          "inputFiles": [{
              "pattern": "d.csv",
              "provenance": "dcid:P",
              "columnMappings": {
                  "dcid:observationAbout": "e",
                  "dcid:variableMeasured": "v",
                  "dcid:observationDate": "d",
                  "dcid:value": "x"
              }
          }]
      }))
  (folder / "input" /
   "d.csv").write_text("e,v,d,x\nNA,V,2020,1\nUSA,V,2020,NA\n")
  expected = oracle.expected_observations(spec.load_scenario(folder))
  assert [dict(o)["about"] for o in expected] == ["NA"]


def test_control_characters_never_break_a_golden_line(tmp_path):
  view = _view(tmp_path, [{"@id": "dcid:a", "dcid:v": {"@id": "dcid:x\ny"}}])
  rendered = facts_lib.parse_rendered(view.render())
  assert rendered == ["Prov\ta\tv\tref\tx\\ny"]
