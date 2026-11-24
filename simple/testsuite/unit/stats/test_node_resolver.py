# Copyright 2026 Google Inc.
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

from unittest import mock

import pandas as pd
import pytest

from stats import node_resolver
from stats import pp_rules
from stats.data import Triple


class _FakeResponse:

  def __init__(self, status_code: int, payload: dict):
    self.status_code = status_code
    self._payload = payload
    self.text = str(payload)

  def raise_for_status(self):
    if self.status_code >= 400:
      raise RuntimeError(f"HTTP {self.status_code}")

  def json(self):
    return self._payload


def test_normalize_node_url():
  assert (node_resolver.normalize_node_url("https://34-102-209-210.sslip.io") ==
          "https://34-102-209-210.sslip.io/core/api/v2/node")
  assert (node_resolver.normalize_node_url(
      "https://34-102-209-210.sslip.io/core/api/v2/node") ==
          "https://34-102-209-210.sslip.io/core/api/v2/node")
  assert (node_resolver.normalize_node_url("https://api.datacommons.org") ==
          "https://api.datacommons.org/v2/node")
  assert (node_resolver.normalize_node_url(
      "https://api.datacommons.org/v2/node") ==
          "https://api.datacommons.org/v2/node")


def test_resolver_batching_cache_and_positive_only(tmp_path):
  existing = {"country/USA", "Count_Person", "geoId/06001"}
  calls = []

  def fake_post(url, json=None, timeout=None):  # pylint: disable=unused-argument
    calls.append(list(json["nodes"]))
    data = {}
    for d in json["nodes"]:
      if d in existing:
        data[d] = {"arcs": {"typeOf": {"nodes": [{"dcid": "Thing"}]}}}
      else:
        data[d] = {}
    return _FakeResponse(200, {"data": data})

  cache_file = str(tmp_path / "node_cache.txt")
  resolver = node_resolver.NodeExistenceResolver(
      mixer_url="https://example.org",
      api_key="test-key",
      cache_path=cache_file,
      batch_size=2,
      concurrency=2,
  )

  with mock.patch("requests.Session.post", side_effect=fake_post):
    # Cold run: 4 unresolved -> 2 batches of 2; 3 found, 1 missing.
    r1 = resolver.verify(
        ["country/USA", "Count_Person", "geoId/06001", "does/NotExist"])
    assert r1.total_unresolved == 4
    assert r1.cache_hits == 0
    assert r1.mixer_queried == 4
    assert r1.mixer_found == 3
    assert r1.mixer_missing == 1
    assert r1.missing_dcids == ["does/NotExist"]
    assert r1.mixer_calls == 2

    # Warm run: the 3 found DCIDs hit cache; missing DCID is re-checked
    # (positive-only cache). Now suppose "does/NotExist" was added to Mixer!
    calls.clear()
    existing.add("does/NotExist")
    r2 = resolver.verify(
        ["country/USA", "Count_Person", "geoId/06001", "does/NotExist"])
    assert r2.cache_hits == 3
    assert r2.mixer_queried == 1
    assert r2.mixer_found == 1
    assert r2.mixer_missing == 0
    assert calls == [["does/NotExist"]]

    # Third run: all 4 hit cache -> 0 Mixer calls!
    calls.clear()
    r3 = resolver.verify(
        ["country/USA", "Count_Person", "geoId/06001", "does/NotExist"])
    assert r3.cache_hits == 4
    assert r3.mixer_queried == 0
    assert r3.mixer_calls == 0
    assert calls == []


def test_resolver_retries_on_503():
  attempts = {"n": 0}

  def fake_post(url, json=None, timeout=None):  # pylint: disable=unused-argument
    attempts["n"] += 1
    if attempts["n"] == 1:
      return _FakeResponse(503, {"error": "unavailable"})
    return _FakeResponse(
        200, {
            "data": {
                "country/USA": {
                    "arcs": {
                        "typeOf": {
                            "nodes": [{
                                "dcid": "Country"
                            }]
                        }
                    }
                }
            }
        })

  resolver = node_resolver.NodeExistenceResolver(
      mixer_url="https://example.org", batch_size=10, concurrency=1)
  with mock.patch("requests.Session.post", side_effect=fake_post), mock.patch(
      "stats.node_resolver.time.sleep"):
    res = resolver.verify(["country/USA"])
  assert attempts["n"] == 2
  assert res.mixer_found == 1
  assert res.mixer_missing == 0
  assert res.mixer_errors == 0


def test_pp_rules_dc_ref_exists_integration():
  acc = pp_rules.RuleAccumulator()
  # Local MCF defines "LocalVar" (so it should NOT be queried in Mixer).
  acc.check_triples(
      [Triple("LocalVar", "typeOf", object_id="StatisticalVariable")],
      "vars.mcf",
  )
  df = pd.DataFrame(
      {
          "dcid:observationAbout": ["dcid:country/USA", "dcid:fake/Place"],
          "variable": ["dcid:LocalVar", "dcid:LocalVar"],
          "date": ["2020", "2020"],
          "value": ["1", "2"],
      },
      dtype=object,
  )
  acc.check_observations(df, "obs.csv", ["dcid:observationAbout"])

  def fake_post(url, json=None, timeout=None):  # pylint: disable=unused-argument
    data = {}
    for d in json["nodes"]:
      if d in ("country/USA", "StatisticalVariable"):
        data[d] = {"arcs": {"typeOf": {"nodes": [{"dcid": "Thing"}]}}}
      else:
        data[d] = {}
    return _FakeResponse(200, {"data": data})

  resolver = node_resolver.NodeExistenceResolver(
      mixer_url="https://example.org", batch_size=10, concurrency=1)
  with mock.patch("requests.Session.post", side_effect=fake_post):
    out = acc.finalize(resolver=resolver)

  ref_rule = out["rules"]["dc.ref.exists"]
  assert ref_rule["checked"] == 4  # country/USA, fake/Place, LocalVar, StatisticalVariable
  assert ref_rule["violations"] == 1
  assert ref_rule["samples"] == [{"dcid": "fake/Place", "file": "obs.csv"}]
  assert out["references"]["resolution"]["mixer_queried"] == 3  # LocalVar skipped!
  assert out["references"]["resolution"]["mixer_found"] == 2
  assert out["references"]["resolution"]["mixer_missing"] == 1
