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

import json
import numpy as np
import pandas as pd
import pytest

from stats import pp_rules
from stats.data import Triple


def _make_obs_df(**overrides) -> pd.DataFrame:
  base = {
      "dcid:observationAbout": ["dcid:country/USA", "dcid:country/CAN"],
      "variable": ["dcid:Count_Person", "dcid:Count_Person"],
      "date": ["2020", "2021-05-01"],
      "value": ["100", "42.5"],
      "observation_period": ["P1Y", ""],
      "scaling_factor": ["", "100"],
  }
  base.update(overrides)
  return pd.DataFrame(base, dtype=object)


class TestDcidFormatValidation:

  @pytest.mark.parametrize("dcid", [
      "country/USA",
      "dcid:geoId/06001",
      "dcs:Count_Person",
      "schema:Place",
      "foo/bar-1.2+3(a)&b%c:d",
  ])
  def test_valid_dcids(self, dcid):
    assert pp_rules.is_valid_dcid(dcid)

  @pytest.mark.parametrize("dcid", [
      "",
      "dcid:",
      "dcs:",
      "country/US A",
      "country/USA\n",
      "country/USA\t",
      "country/USA\r",
      "foo\x00bar",
      'bad"quote',
      " leading_space",
      None,
      123,
  ])
  def test_invalid_dcids(self, dcid):
    assert not pp_rules.is_valid_dcid(dcid)

  def test_observation_dcid_format_and_max_length(self):
    long_dcid = "dcid:" + "a" * 257
    df = _make_obs_df(
        **{
            "dcid:observationAbout": [
                "dcid:country/USA",
                "dcid:bad dcid",
                "dcid:country/USA\n",
                long_dcid,
            ],
            "variable": [
                "dcid:Count_Person",
                "dcid:",
                "dcid:Count\tBad",
                "dcid:" + "v" * 260,
            ],
            "date": ["2020", "2020", "2020", "2020"],
            "value": ["1", "2", "3", "4"],
            "observation_period": ["", "", "", ""],
            "scaling_factor": ["", "", "", ""],
        })
    acc = pp_rules.RuleAccumulator()
    acc.check_observations(df, "obs.csv", ["dcid:observationAbout"])
    out = acc.finalize()["rules"]

    assert out["dc.dcid.format.entity"]["checked"] == 4
    assert out["dc.dcid.format.entity"]["violations"] == 2
    assert [s["row"] for s in out["dc.dcid.format.entity"]["samples"]] == [3, 4]

    assert out["dc.dcid.max_length.entity"]["violations"] == 1
    assert out["dc.dcid.max_length.entity"]["samples"][0]["row"] == 5

    assert out["dc.dcid.format.variable"]["violations"] == 2
    assert [s["row"] for s in out["dc.dcid.format.variable"]["samples"]] == [
        3, 4
    ]
    assert out["dc.dcid.max_length.variable"]["violations"] == 1


class TestRequiredObservationFields:

  def test_missing_required_fields_including_pd_na(self):
    df = pd.DataFrame(
        {
            "dcid:observationAbout": ["dcid:country/USA", "", None, pd.NA],
            "variable": ["", "dcid:Count_Person", np.nan, "dcid:Count_Person"],
            "date": ["2020", "", "2020", pd.NA],
            "value": ["10", "20", "", np.nan],
        },
        dtype=object,
    )
    acc = pp_rules.RuleAccumulator()
    acc.check_observations(df, "obs.csv", ["dcid:observationAbout"])
    out = acc.finalize()["rules"]

    assert out["dc.obs.entity_required"]["checked"] == 4
    assert out["dc.obs.entity_required"]["violations"] == 3
    assert out["dc.obs.variable_required"]["violations"] == 2
    assert out["dc.obs.date_required"]["violations"] == 2
    assert out["dc.obs.value_required"]["violations"] == 2
    # Only the 2 present values are checked for numeric validity.
    assert out["dc.obs.value_numeric"]["checked"] == 2
    assert out["dc.obs.value_numeric"]["violations"] == 0

  def test_multi_entity_dimensions_require_at_least_one(self):
    df = pd.DataFrame(
        {
            "dcid:origin": ["dcid:country/USA", "", ""],
            "dcid:destination": ["", "dcid:country/CAN", ""],
            "variable": ["dcid:Flow", "dcid:Flow", "dcid:Flow"],
            "date": ["2020", "2020", "2020"],
            "value": ["1", "2", "3"],
        },
        dtype=object,
    )
    acc = pp_rules.RuleAccumulator()
    acc.check_observations(df, "flow.csv", ["dcid:origin", "dcid:destination"])
    out = acc.finalize()["rules"]

    # Row 1 has origin, row 2 has destination, row 3 has neither.
    assert out["dc.obs.entity_required"]["checked"] == 3
    assert out["dc.obs.entity_required"]["violations"] == 1
    assert out["dc.obs.entity_required"]["samples"][0]["row"] == 4


class TestNumericValidation:

  def test_rejects_nan_inf_and_non_numeric(self):
    df = pd.DataFrame(
        {
            "dcid:observationAbout": ["dcid:country/USA"] * 8,
            "variable": ["dcid:V"] * 8,
            "date": ["2020"] * 8,
            "value": [
                "42", "-3.14", "1e-5", "NaN", "Inf", "-infinity", "abc", ""
            ],
        },
        dtype=object,
    )
    acc = pp_rules.RuleAccumulator()
    acc.check_observations(df, "nums.csv", ["dcid:observationAbout"])
    out = acc.finalize()["rules"]

    assert out["dc.obs.value_required"]["violations"] == 1
    assert out["dc.obs.value_numeric"]["checked"] == 7
    assert out["dc.obs.value_numeric"]["violations"] == 4
    assert [s["value"] for s in out["dc.obs.value_numeric"]["samples"]] == [
        "NaN", "Inf", "-infinity", "abc"
    ]

  def test_all_valid_fast_path_and_inf_only_chunk(self):
    # All valid finite numbers -> C float64 fast path.
    df_ok = _make_obs_df(value=["1", "2.5e3"])
    acc_ok = pp_rules.RuleAccumulator()
    acc_ok.check_observations(df_ok, "ok.csv", ["dcid:observationAbout"])
    out_ok = acc_ok.finalize()["rules"]
    assert out_ok["dc.obs.value_numeric"]["checked"] == 2
    assert out_ok["dc.obs.value_numeric"]["violations"] == 0

    # Parseable by float64 but contains Inf -> caught by np.isfinite.
    df_inf = _make_obs_df(value=["1.0", "inf"])
    acc_inf = pp_rules.RuleAccumulator()
    acc_inf.check_observations(df_inf, "inf.csv", ["dcid:observationAbout"])
    out_inf = acc_inf.finalize()["rules"]
    assert out_inf["dc.obs.value_numeric"]["checked"] == 2
    assert out_inf["dc.obs.value_numeric"]["violations"] == 1


class TestAdditionalValidations:

  def test_iso_date_period_and_scaling_factor(self):
    df = pd.DataFrame(
        {
            "dcid:observationAbout": ["dcid:country/USA"] * 5,
            "variable": ["dcid:V"] * 5,
            "date": [
                "2020",
                "2020-05",
                "2020-05-15T14:30:00Z",
                "2020-13-01",
                "05/15/2020",
            ],
            "value": ["1", "2", "3", "4", "5"],
            "observation_period": ["P1Y", "P1Y6M", "PT1H", "1Y", "P"],
            "scaling_factor": ["", "100", "0.5", "not_a_num", "inf"],
        },
        dtype=object,
    )
    acc = pp_rules.RuleAccumulator()
    acc.check_observations(df, "extra.csv", ["dcid:observationAbout"])
    out = acc.finalize()["rules"]

    assert out["dc.obs.date_format"]["checked"] == 5
    assert out["dc.obs.date_format"]["violations"] == 2
    assert [s["value"] for s in out["dc.obs.date_format"]["samples"]] == [
        "2020-13-01", "05/15/2020"
    ]

    assert out["dc.obs.period_format"]["checked"] == 5
    assert out["dc.obs.period_format"]["violations"] == 2
    assert [s["value"] for s in out["dc.obs.period_format"]["samples"]] == [
        "1Y", "P"
    ]

    assert out["dc.obs.scaling_factor_numeric"]["checked"] == 4
    assert out["dc.obs.scaling_factor_numeric"]["violations"] == 2
    assert [
        s["value"] for s in out["dc.obs.scaling_factor_numeric"]["samples"]
    ] == ["not_a_num", "inf"]


class TestTriplesAndCrossWorkerMerge:

  def test_cross_worker_merge_typeof_stat_type_and_node_dcid(self):
    w1 = pp_rules.RuleAccumulator()
    w1.check_triples(
        [
            # Split across workers: typeOf arrives in w2.
            Triple("country/USA", "name", object_value="United States"),
            # Untyped everywhere.
            Triple("orphanNode", "name", object_value="Orphan"),
            # Bad node DCID.
            Triple("bad node\n", "typeOf", object_id="Place"),
            # SV with invalid statType.
            Triple("sv/Bad", "typeOf", object_id="StatisticalVariable"),
            Triple("sv/Bad", "statType", object_id="dcid:madeUpStat"),
        ],
        "a.mcf",
    )

    w2 = pp_rules.RuleAccumulator()
    w2.check_triples(
        [
            Triple("country/USA", "typeOf", object_id="Country"),
            Triple("sv/Good", "typeOf", object_id="StatisticalVariable"),
            Triple("sv/Good", "statType", object_id="dcs:measuredValue"),
        ],
        "b.mcf",
    )

    coord = pp_rules.RuleAccumulator()
    coord.merge(w1.partial())
    coord.merge(w2.partial())
    out = coord.finalize()
    rules = out["rules"]

    assert rules["dc.node.typeof_required"]["checked"] == 5
    assert rules["dc.node.typeof_required"]["violations"] == 1
    assert rules["dc.node.typeof_required"]["samples"][0][
        "subject"] == "orphanNode"

    assert rules["dc.sv.stat_type_valid"]["checked"] == 2
    assert rules["dc.sv.stat_type_valid"]["violations"] == 1
    assert rules["dc.sv.stat_type_valid"]["samples"][0]["subject"] == "sv/Bad"

    assert rules["dc.dcid.format.node"]["violations"] == 1
    assert rules["dc.dcid.format.node"]["samples"][0]["subject"] == "bad node\n"

  def test_row_offset_part_index_and_sample_cap(self, tmp_path, monkeypatch):
    acc = pp_rules.RuleAccumulator()
    bad_df = pd.DataFrame(
        {
            "dcid:observationAbout": ["dcid:country/USA"] * 25,
            "variable": ["dcid:V"] * 25,
            "date": ["2020"] * 25,
            "value": ["bad"] * 25,
        },
        dtype=object,
    )
    acc.check_observations(bad_df,
                           "chunked.csv", ["dcid:observationAbout"],
                           row_offset=100,
                           part_index=3)
    report_path = tmp_path / "pp_rules.json"
    monkeypatch.setenv("PP_RULES_REPORT", str(report_path))
    out = pp_rules.report(acc)

    st = out["rules"]["dc.obs.value_numeric"]
    assert st["violations"] == 25
    assert len(st["samples"]) == pp_rules.K
    assert st["samples"][0] == {
        "file": "chunked.csv",
        "row": 102,
        "value": "bad",
        "part": 3,
    }
    written = json.loads(report_path.read_text())
    assert written["total_violations"] == 25
