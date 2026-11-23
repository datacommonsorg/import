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
"""Unit tests for JsonLdStreamWriter local shard writing and observation JSON-LD."""

import json
import os

import pandas as pd
from stats import constants
from stats.config import Config
from stats.data import Triple
from stats.jsonld_stream_writer import JsonLdStreamWriter
from util.filesystem import open_dir


def test_shards_go_into_a_folder_that_another_worker_just_made(
    tmp_path, monkeypatch):
  """Workers make the folders of their shards, maybe at the same time."""
  real_makedirs = os.makedirs

  def makedirs_after_another_worker(name, mode=0o777, exist_ok=False):
    if str(name).rstrip("/").endswith("/prov"):
      real_makedirs(name, exist_ok=True)
    return real_makedirs(name, mode, exist_ok)

  writer = JsonLdStreamWriter(open_dir(str(tmp_path)), ["run"], Config({}))
  monkeypatch.setattr(os, "makedirs", makedirs_after_another_worker)
  writer.write_triples([Triple("a", "typeOf", object_id="Thing")],
                       provenance_dir="prov")
  writer.commit_and_close()

  [shard] = tmp_path.glob("jsonld/run_*/prov/node-*.jsonld")
  assert '"@id":"dcid:a"' in shard.read_text()


def _obs_df(rows: list[dict]) -> pd.DataFrame:
  defaults = {
      constants.COLUMN_ENTITY: None,
      constants.COLUMN_VARIABLE: "Count_Person",
      constants.COLUMN_DATE: "2020",
      constants.COLUMN_VALUE: "100",
      constants.COLUMN_PROVENANCE: "dc/base/Test",
      constants.COLUMN_UNIT: "",
      constants.COLUMN_SCALING_FACTOR: "",
      constants.COLUMN_MEASUREMENT_METHOD: "",
      constants.COLUMN_OBSERVATION_PERIOD: "",
      constants.COLUMN_PROPERTIES: "",
  }
  return pd.DataFrame([{
      **defaults,
      **r
  } for r in rows])[constants.OBSERVATION_COLUMNS]


def _write_and_load_obs(
    tmp_path, rows: list[dict]) -> tuple[list[dict], JsonLdStreamWriter]:
  out = tmp_path / "out"
  inp = tmp_path / "in"
  inp.mkdir(exist_ok=True)
  csv_file = inp / "obs.csv"
  csv_file.write_text("")
  cfg = Config({
      "inputFiles": [{
          "pattern": "obs.csv",
          "provenance": "dcid:dc/base/Test",
      }]
  })
  writer = JsonLdStreamWriter(open_dir(str(out)), ["run"], cfg)
  writer.write_observations(_obs_df(rows),
                            open_dir(str(inp)).open_file("obs.csv"))
  writer.commit_and_close()
  nodes = []
  for shard in sorted(out.glob("jsonld/run_*/*/observation-*.jsonld")):
    doc = json.loads(shard.read_text())
    nodes.extend(doc["@graph"])
  return nodes, writer


def test_observation_json_escaping_and_custom_namespaces(tmp_path):
  nodes, _ = _write_and_load_obs(
      tmp_path,
      [{
          constants.COLUMN_ENTITY:
              "acme:place/1",
          constants.COLUMN_DATE:
              '2020 "Q1"',
          constants.COLUMN_VALUE:
              "42.50",
          constants.COLUMN_UNIT:
              "acme:USD",
          constants.COLUMN_OBSERVATION_PERIOD:
              "P1Y",
          constants.COLUMN_PROPERTIES:
              json.dumps({
                  "customNote": 'line1\n"quoted"\\path',
                  "emptyDim": None,
              }),
      }],
  )
  assert len(nodes) == 1
  obs = nodes[0]
  assert obs["dcid:observationAbout"] == {"@id": "dcid:place/1"}
  assert obs["dcid:emptyDim"] is None
  assert obs["dcid:unit"] == {"@id": "dcid:USD"}
  assert obs["dcid:observationDate"] == '2020 "Q1"'
  assert obs["dcid:value"] == "42.50"
  assert obs["dcid:customNote"] == 'line1\n"quoted"\\path'
  assert obs["dcid:variableMeasured"] == {
      "@id": "dcid:Count_Person",
      "dcid:observationProperties": ["dcid:customNote", "dcid:emptyDim"],
  }


def test_observation_custom_property_overrides_standard_key(tmp_path):
  nodes, _ = _write_and_load_obs(
      tmp_path,
      [{
          constants.COLUMN_ENTITY:
              "country/USA",
          constants.COLUMN_UNIT:
              "USD",
          constants.COLUMN_PROPERTIES:
              json.dumps({
                  "dcid:observationAbout": "country/FRA",
                  "unit": "dcid:EUR",
              }),
      }],
  )
  assert len(nodes) == 1
  obs = nodes[0]
  assert obs["dcid:observationAbout"] == {"@id": "dcid:country/FRA"}
  assert obs["dcid:unit"] == {"@id": "dcid:EUR"}


def test_observation_invalid_properties_json_falls_back(tmp_path, caplog):
  nodes, _ = _write_and_load_obs(
      tmp_path,
      [{
          constants.COLUMN_ENTITY: "country/USA",
          constants.COLUMN_PROPERTIES: "{not-json",
      }],
  )
  assert len(nodes) == 1
  assert nodes[0]["dcid:observationAbout"] == {"@id": "dcid:country/USA"}
  assert "Failed to decode properties JSON" in caplog.text
