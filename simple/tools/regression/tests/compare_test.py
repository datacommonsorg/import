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
"""The comparator on small planted outputs: each difference is detected.

Every test writes a baseline output, plants one change in a copy of it, and
checks which categories the comparison reports.
"""

import copy
import json
import os
from typing import Any, Callable

import pytest
from tools.regression.categories import CATEGORIES
from tools.regression.categories import CATEGORIES_BY_NAME
from tools.regression.categories import Kind
from tools.regression.compare import compare_outputs
from tools.regression.compare import SideOutput
from tools.regression.shards import load_output_tree

CONTEXT = {"dcid": "https://datacommons.org/browser/"}
INPUT_ROOT = "/data/input"


def obs(about: str, date: int, value: float, **extra: Any) -> dict:
  return {
      "@id": f"dcid:obs_{about}_{date}",
      "@type": "dcid:StatVarObservation",
      "dcid:variableMeasured": {
          "@id": "dcid:Count_Person"
      },
      "dcid:observationAbout": {
          "@id": f"dcid:{about}"
      },
      "dcid:observationDate": date,
      "dcid:provenance": {
          "@id": "dcid:ProvA"
      },
      "dcid:value": value,
      **extra
  }


def node(dcid: str, type_: str = "dcid:Thing", **props: Any) -> dict:
  return {"@id": f"dcid:{dcid}", "@type": type_, **props}


# An output, as {folder: {shard file name: [records]}}. Enough observations
# to spread over many spill buckets.
BASE_SHARDS = {
    "ProvA": {
        "observation-data-00000.jsonld": [
            obs(f"geo{i}", 2020, i) for i in range(500)
        ],
        "node-00000-aaaaaaaaaaaa.jsonld": [
            node("ProvA", "dcid:Provenance", **{"dcid:name": "Prov A"}),
            node("Count_Person", "dcid:StatisticalVariable"),
        ],
    },
    "ProvB": {
        "observation-data-00000.jsonld": [obs("geoB", 2021, 7)],
        "node-00000-bbbbbbbbbbbb.jsonld": [node("Thing1", **{"dcid:x": 1})],
    },
}
BASE_REPORT = {
    "status": "SUCCESS",
    "startTime": "2026-01-01 00:00:00",
    "importFiles": {
        f"{INPUT_ROOT}/data.csv": {
            "status": "SUCCESS",
            "lastUpdate": "2026-01-01 00:00:01"
        }
    },
}


def write_output(root: str,
                 shards: dict,
                 report: dict | None = None,
                 run_dir: str = "default_import_name_20260101_000000_000000",
                 context: dict | None = None) -> str:
  """Writes a preprocessor-shaped output directory and returns it."""
  for folder, files in shards.items():
    os.makedirs(os.path.join(root, "jsonld", run_dir, folder))
    for name, records in files.items():
      path = os.path.join(root, "jsonld", run_dir, folder, name)
      with open(path, "w") as f:
        json.dump({"@context": context or CONTEXT, "@graph": records}, f)
  os.makedirs(os.path.join(root, "process"))
  report = BASE_REPORT if report is None else report
  with open(os.path.join(root, "process", "report.json"), "w") as f:
    json.dump(report, f)
  with open(os.path.join(root, "process", "report_data.csv.json"), "w") as f:
    json.dump({"importFiles": report["importFiles"]}, f)
  return root


def compare(tmp_path,
            change: Callable[[dict], None] = lambda shards: None,
            **candidate_kwargs: Any) -> dict[str, int]:
  """Compares BASE_SHARDS with a changed copy; returns non-zero counts."""
  shards = copy.deepcopy(BASE_SHARDS)
  change(shards)
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  cand = write_output(str(tmp_path / "cand"), shards, **candidate_kwargs)
  result = compare_outputs(SideOutput(base, INPUT_ROOT),
                           SideOutput(cand, INPUT_ROOT),
                           scratch_dir=str(tmp_path),
                           workers=2)
  for name, count in result.findings.counts.items():
    if count:
      assert result.findings.samples[name], f"{name} has no samples"
  return {k: v for k, v in result.findings.counts.items() if v}


def obs_shard(shards: dict, folder: str = "ProvA") -> list[dict]:
  return shards[folder]["observation-data-00000.jsonld"]


def failing(counts: dict[str, int]) -> dict[str, int]:
  return {
      k: v
      for k, v in counts.items()
      if CATEGORIES_BY_NAME[k].kind == Kind.FAILING
  }


def test_identical_outputs_report_no_difference(tmp_path):
  assert compare(tmp_path) == {}


def test_volatile_fields_are_ignored(tmp_path):
  """Timestamps, the run dir timestamp, node shard uids and input roots."""

  def rename_node_shard(shards):
    folder = shards["ProvB"]
    folder["node-00000-cccccccccccc.jsonld"] = folder.pop(
        "node-00000-bbbbbbbbbbbb.jsonld")

  report = copy.deepcopy(BASE_REPORT)
  report["startTime"] = "2027-05-05 12:00:00"
  shards = copy.deepcopy(BASE_SHARDS)
  rename_node_shard(shards)
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  moved_report = json.loads(
      json.dumps(report).replace(INPUT_ROOT, "/elsewhere/input"))
  cand = write_output(str(tmp_path / "cand"),
                      shards,
                      report=moved_report,
                      run_dir="default_import_name_20270505_120000_123456")
  result = compare_outputs(SideOutput(base, INPUT_ROOT),
                           SideOutput(cand, "/elsewhere/input"),
                           scratch_dir=str(tmp_path),
                           workers=2)
  assert {k: v for k, v in result.findings.counts.items() if v} == {}


def test_obs_value_changed(tmp_path):
  counts = compare(tmp_path,
                   lambda s: obs_shard(s)[3].update({"dcid:value": 99}))
  assert failing(counts) == {"obs_value_changed": 1}
  assert counts["shard_bytes_changed"] == 1


def test_obs_only_baseline(tmp_path):
  counts = compare(tmp_path, lambda s: obs_shard(s).pop(10))
  assert failing(counts) == {"obs_only_baseline": 1}


def test_obs_only_candidate(tmp_path):
  counts = compare(tmp_path, lambda s: obs_shard(s).append(obs("new", 1, 1)))
  assert failing(counts) == {"obs_only_candidate": 1}


def test_obs_multiplicity_changed(tmp_path):
  counts = compare(tmp_path, lambda s: obs_shard(s).append(obs_shard(s)[0]))
  assert failing(counts) == {"obs_multiplicity_changed": 1}


def test_obs_properties_changed(tmp_path):
  counts = compare(tmp_path,
                   lambda s: obs_shard(s)[0].update({"@id": "dcid:obs_new"}))
  assert failing(counts) == {"obs_properties_changed": 1}


def test_obs_provenance_url_changed(tmp_path):
  counts = compare(
      tmp_path, lambda s: obs_shard(s)[0].update(
          {"dcid:provenanceUrl": "https://example.org"}))
  assert failing(counts) == {"obs_provenance_url_changed": 1}


def test_obs_dimension_is_part_of_the_key(tmp_path):
  counts = compare(tmp_path,
                   lambda s: obs_shard(s)[0].update({"dcid:sex": "dcid:Male"}))
  assert failing(counts) == {"obs_only_baseline": 1, "obs_only_candidate": 1}


def test_obs_folder_moved_is_informational(tmp_path):
  counts = compare(tmp_path,
                   lambda s: obs_shard(s, "ProvB").append(obs_shard(s).pop(0)))
  assert failing(counts) == {}
  assert counts["obs_folder_moved"] == 1


def test_node_only_baseline(tmp_path):
  counts = compare(tmp_path,
                   lambda s: s["ProvB"]["node-00000-bbbbbbbbbbbb.jsonld"].pop())
  assert failing(counts) == {"node_only_baseline": 1}


def test_node_only_candidate(tmp_path):
  counts = compare(
      tmp_path, lambda s: s["ProvB"]["node-00000-bbbbbbbbbbbb.jsonld"].append(
          node("Thing2")))
  assert failing(counts) == {"node_only_candidate": 1}


def test_node_properties_changed(tmp_path):
  counts = compare(
      tmp_path, lambda s: s["ProvB"]["node-00000-bbbbbbbbbbbb.jsonld"][0].
      update({"dcid:x": 2}))
  assert failing(counts) == {"node_properties_changed": 1}


def test_node_folder_moved_is_informational(tmp_path):

  def move(shards):
    thing = shards["ProvB"]["node-00000-bbbbbbbbbbbb.jsonld"].pop()
    shards["ProvA"]["node-00000-aaaaaaaaaaaa.jsonld"].append(thing)

  counts = compare(tmp_path, move)
  assert failing(counts) == {}
  assert counts["node_folder_moved"] == 1


def test_node_split_over_records_is_informational(tmp_path):

  def split(shards):
    shards["ProvB"]["node-00001-dddddddddddd.jsonld"] = [
        node("Thing1", **{"dcid:x": 1})
    ]

  counts = compare(tmp_path, split)
  assert failing(counts) == {}
  assert counts["node_records_changed"] == 1
  assert counts["shard_set_changed"] == 1


def test_obs_shard_renamed(tmp_path):

  def rename(shards):
    folder = shards["ProvB"]
    folder["observation-other-00000.jsonld"] = folder.pop(
        "observation-data-00000.jsonld")

  counts = compare(tmp_path, rename)
  assert failing(counts) == {}
  assert counts["shard_set_changed"] == 2


def test_report_json_changed(tmp_path):
  report = copy.deepcopy(BASE_REPORT)
  report["status"] = "FAILURE"
  report["importFiles"][f"{INPUT_ROOT}/data.csv"]["status"] = "FAILURE"
  counts = compare(tmp_path, report=report)
  assert failing(counts) == {
      "report_json_changed": 2,
      "worker_reports_changed": 1
  }


def test_run_dir_name_changed(tmp_path):
  counts = compare(tmp_path, run_dir="other_import_20260101_000000_000000")
  assert failing(counts) == {"run_dir_name_changed": 1}


def test_jsonld_context_changed(tmp_path):
  counts = compare(tmp_path, context={"dcid": "https://example.org/"})
  assert failing(counts) == {"jsonld_context_changed": 1}


def test_missing_output_on_one_side(tmp_path):
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  empty = str(tmp_path / "empty")
  os.makedirs(empty)
  result = compare_outputs(SideOutput(base, INPUT_ROOT),
                           SideOutput(empty, INPUT_ROOT),
                           scratch_dir=str(tmp_path),
                           workers=2)
  counts = result.findings.counts
  assert counts["obs_only_baseline"] == 501
  assert counts["node_only_baseline"] == 3
  assert counts["report_json_changed"] > 0


@pytest.mark.parametrize("strict", [False, True])
def test_strict_makes_informational_categories_fail(tmp_path, strict):
  shards = copy.deepcopy(BASE_SHARDS)
  obs_shard(shards, "ProvB").append(obs_shard(shards).pop(0))
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  cand = write_output(str(tmp_path / "cand"), shards)
  result = compare_outputs(SideOutput(base, INPUT_ROOT),
                           SideOutput(cand, INPUT_ROOT),
                           scratch_dir=str(tmp_path),
                           workers=2)
  assert bool(result.findings.failing(strict)) == strict


def test_every_category_is_documented():
  readme = os.path.join(os.path.dirname(__file__), "..", "README.md")
  with open(readme) as f:
    text = f.read()
  assert [c.name for c in CATEGORIES if f"`{c.name}`" not in text] == []


def test_hidden_files_next_to_the_run_dir_are_ignored(tmp_path):
  """Finder and similar tools drop files such as .DS_Store into output dirs."""
  root = write_output(str(tmp_path / "out"), BASE_SHARDS)
  open(os.path.join(root, "jsonld", ".DS_Store"), "w").close()
  tree = load_output_tree(root)
  assert tree.run_dir_name == "default_import_name_20260101_000000_000000"
