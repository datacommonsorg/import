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
"""Generated (fuzz) tests: random valid inputs vs. their expected records.

Hypothesis draws inputs from lib/strategies.py, which also computes the exact
JSON-LD records each input must produce. When a check fails, Hypothesis
shrinks the input to a minimal example and prints it; turn that example into
a case in cases/*.md (with `> xfail:` if it is a bug to fix later).

Budget: HYPOTHESIS_PROFILE=ci (default, fixed seed), nightly or dev; see
conftest.py.
"""

import collections
import json

from hypothesis import given
from hypothesis import note
from testsuite.lib import canonical
from testsuite.lib import run
from testsuite.lib import strategies as gen

_MCF_CONFIG = {
    "inputFiles": [{
        "pattern": "*.mcf",
        "provenance": "dcid:TestProvenance"
    }],
    "importProxyEntities": False,
}


def _records(output: dict, kind: str | None = None) -> list[dict]:
  return [rec for _, _, _, rec in canonical.all_records(output, kind)]


def _key(record: dict) -> str:
  return json.dumps(canonical.canonical_record(record),
                    sort_keys=True,
                    ensure_ascii=False)


def _baseline_records(mode: str) -> collections.Counter:
  if mode not in _BASELINES:
    result = run.run_preprocessor(
        run.write_input_dir({"provenance.mcf": gen.PROVENANCE_MCF},
                            _MCF_CONFIG), mode)
    _BASELINES[mode] = collections.Counter(
        _key(r) for r in _records(result.output))
  return _BASELINES[mode]


_BASELINES: dict[str, collections.Counter] = {}


def _added(output: dict, mode: str) -> list[dict]:
  actual = collections.Counter(_key(r) for r in _records(output))
  return [json.loads(r) for r in (actual - _baseline_records(mode)).elements()]


def _spec_form(records: list[dict]) -> list[str]:
  """Records in the spec's comparison form (see lib/strategies.py)."""
  out = []
  for rec in records:
    rec = dict(rec)
    if rec.get("@type") == "dcid:StatVarObservation":
      rec.pop("@id", None)  # A content hash; checked separately below.
    if "dcid:observationDate" in rec:
      date = rec["dcid:observationDate"]
      if isinstance(date, float) and date.is_integer():
        date = int(date)
      rec["dcid:observationDate"] = str(date)
    out.append(
        json.dumps(gen.normalize(rec), sort_keys=True, ensure_ascii=False))
  return sorted(out)


@given(gen.mcf_file())
def test_mcf_nodes_match_model(mcf):
  """Every generated MCF node comes out as exactly its expected record."""
  text, expected = mcf
  note(f"MCF input:\n{text}")
  result = run.run_preprocessor(
      run.write_input_dir(
          {
              "provenance.mcf": gen.PROVENANCE_MCF,
              "nodes.mcf": text
          }, _MCF_CONFIG), "threaded")
  assert result.output["error"] is None, result.output.get("_traceback")
  added = _spec_form(_added(result.output, "threaded"))
  want = _spec_form(expected)
  assert added == want, ("records differ (- expected, + actual):\n" +
                         canonical.diff([json.loads(w) for w in want],
                                        [json.loads(a) for a in added]))


@given(gen.mcf_file())
def test_mcf_nodes_same_in_every_process_mode(mcf):
  """Process modes must agree on generated MCF.

  rdflib_nodes is left out: the legacy rdflib writer emits every literal as a
  string (see "legacy rdflib writer keeps numbers" in cases/mcf_nodes.md).
  """
  text, _ = mcf
  note(f"MCF input:\n{text}")
  input_dir = run.write_input_dir(
      {
          "provenance.mcf": gen.PROVENANCE_MCF,
          "nodes.mcf": text
      }, _MCF_CONFIG)
  outputs = {
      mode: run.run_preprocessor(input_dir, mode).output["folders"]
      for mode in ("threaded", "multiprocess")
  }
  for mode, folders in outputs.items():
    assert folders == outputs["threaded"], (
        f"{mode} differs from threaded:\n" +
        canonical.diff(outputs["threaded"], folders))


@given(gen.observation_csv())
def test_observation_rows_match_model(obs):
  """Every generated CSV row becomes exactly one expected observation.

  The CSV varies column names and order, quoting style (minimal/all), line
  endings (LF/CRLF), and an optional unit column and ignored notes column
  holding commas, quotes and newlines.
  """
  note(f"CSV input:\n{obs.text!r}\nmappings: {obs.column_mappings}")
  config = {
      "inputFiles": [
          {
              "pattern": "*.mcf",
              "provenance": "dcid:TestProvenance"
          },
          {
              "pattern": "data.csv",
              "provenance": "dcid:TestProvenance",
              "columnMappings": obs.column_mappings,
              "ignoreColumns": obs.ignore_columns,
          },
      ],
      "importProxyEntities": False,
  }
  result = run.run_preprocessor(
      run.write_input_dir(
          {
              "provenance.mcf": gen.PROVENANCE_MCF,
              "data.csv": obs.text
          }, config), "threaded")
  assert result.output["error"] is None, result.output.get("_traceback")
  observations = _records(result.output, canonical.OBSERVATIONS)
  actual = _spec_form(observations)
  want = _spec_form(obs.expected)
  assert actual == want, ("observations differ (- expected, + actual):\n" +
                          canonical.diff([json.loads(w) for w in want],
                                         [json.loads(a) for a in actual]))
  # Same key (entity, variable, date, unit) <=> same observation id.
  by_key = collections.defaultdict(set)
  for r in observations:
    key = (json.dumps(r["dcid:observationAbout"]),
           json.dumps(r["dcid:variableMeasured"]),
           str(r["dcid:observationDate"]), json.dumps(r.get("dcid:unit")))
    by_key[key].add(r["@id"])
  assert all(len(ids) == 1 for ids in by_key.values()), by_key
  assert len({i for ids in by_key.values() for i in ids}) == len(by_key)
