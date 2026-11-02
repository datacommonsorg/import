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
"""
The comparator on small planted outputs: each difference is detected.

Every test writes a baseline output, plants one change in a copy of it, and
checks which categories the comparison reports.
"""

import copy
import json
import os
from typing import Any, Callable

import pytest
from tools.regression.comparison.categories import CATEGORIES
from tools.regression.comparison.compare import compare_outputs
from tools.regression.comparison.compare import Comparison
from tools.regression.comparison.compare import SideOutput
from tools.regression.comparison.shards import load_output_tree

CONTEXT = {"dcid": "https://datacommons.org/browser/"}
INPUT_ROOT = "/data/input"
RUN_DIR = "default_import_name_20260101_000000_000000"
NODE_SHARD_A = "node-00000-aaaaaaaaaaaa.jsonld"
NODE_SHARD_B = "node-00000-bbbbbbbbbbbb.jsonld"
OBS_SHARD = "observation-data-00000.jsonld"


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
        OBS_SHARD: [obs(f"geo{i}", 2020, i) for i in range(500)],
        NODE_SHARD_A: [
            node("ProvA", "dcid:Provenance", **{"dcid:name": "Prov A"}),
            node("Count_Person", "dcid:StatisticalVariable"),
        ],
    },
    "ProvB": {
        OBS_SHARD: [obs("geoB", 2021, 7)],
        NODE_SHARD_B: [node("Thing1", **{"dcid:x": 1})],
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
                 run_dir: str = RUN_DIR,
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


def compare_dirs(tmp_path,
                 base: str,
                 cand: str,
                 cand_input_root: str = INPUT_ROOT) -> Comparison:
  return compare_outputs(SideOutput(base, INPUT_ROOT),
                         SideOutput(cand, cand_input_root),
                         scratch_dir=str(tmp_path),
                         workers=2)


def compare(tmp_path,
            change: Callable[[dict], Any] = lambda shards: None,
            **candidate_kwargs: Any) -> Comparison:
  """Compares BASE_SHARDS with a changed copy of it."""
  shards = copy.deepcopy(BASE_SHARDS)
  change(shards)
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  cand = write_output(str(tmp_path / "cand"), shards, **candidate_kwargs)
  return compare_dirs(tmp_path, base, cand)


def nonzero(result: Comparison) -> dict[str, int]:
  """The non-zero counts; every one of them must come with samples."""
  counts = {k: v for k, v in result.findings.counts.items() if v}
  assert [k for k in counts if not result.findings.samples[k]] == []
  return counts


def obs_shard(shards: dict, folder: str = "ProvA") -> list[dict]:
  return shards[folder][OBS_SHARD]


def move_obs_to_prov_b(shards: dict) -> None:
  obs_shard(shards, "ProvB").append(obs_shard(shards).pop(0))


def move_node_to_prov_a(shards: dict) -> None:
  shards["ProvA"][NODE_SHARD_A].append(shards["ProvB"][NODE_SHARD_B].pop())


def split_node(shards: dict) -> None:
  shards["ProvB"]["node-00001-dddddddddddd.jsonld"] = [
      node("Thing1", **{"dcid:x": 1})
  ]


def rename_obs_shard(shards: dict) -> None:
  folder = shards["ProvB"]
  folder["observation-other-00000.jsonld"] = folder.pop(OBS_SHARD)


# (planted change, expected non-zero counts). A changed shard also counts as
# shard_bytes_changed, which is informational.
PLANTED = {
    "obs_value_changed": (lambda s: obs_shard(s)[3].update({"dcid:value": 99}),
                          dict(obs_value_changed=1, shard_bytes_changed=1)),
    "obs_only_baseline": (lambda s: obs_shard(s).pop(10),
                          dict(obs_only_baseline=1, shard_bytes_changed=1)),
    "obs_only_candidate": (lambda s: obs_shard(s).append(obs("new", 1, 1)),
                           dict(obs_only_candidate=1, shard_bytes_changed=1)),
    "obs_multiplicity_changed": (lambda s: obs_shard(s).append(obs_shard(s)[0]),
                                 dict(obs_multiplicity_changed=1,
                                      shard_bytes_changed=1)),
    "obs_properties_changed":
        (lambda s: obs_shard(s)[0].update({"@id": "dcid:obs_new"}),
         dict(obs_properties_changed=1, shard_bytes_changed=1)),
    "obs_provenance_url_changed": (lambda s: obs_shard(s)[0].update(
        {"dcid:provenanceUrl": "https://example.org"}),
                                   dict(obs_provenance_url_changed=1,
                                        shard_bytes_changed=1)),
    "obs_dimension_is_part_of_the_key":
        (lambda s: obs_shard(s)[0].update({"dcid:sex": "dcid:Male"}),
         dict(obs_only_baseline=1, obs_only_candidate=1,
              shard_bytes_changed=1)),
    "obs_folder_moved": (move_obs_to_prov_b,
                         dict(obs_folder_moved=1, shard_bytes_changed=2)),
    "obs_shard_renamed": (rename_obs_shard, dict(shard_set_changed=2)),
    "node_only_baseline": (lambda s: s["ProvB"][NODE_SHARD_B].pop(),
                           dict(node_only_baseline=1, shard_bytes_changed=2)),
    "node_only_candidate":
        (lambda s: s["ProvB"][NODE_SHARD_B].append(node("Thing2")),
         dict(node_only_candidate=1, shard_bytes_changed=2)),
    "node_properties_changed":
        (lambda s: s["ProvB"][NODE_SHARD_B][0].update({"dcid:x": 2}),
         dict(node_properties_changed=1, shard_bytes_changed=2)),
    "node_folder_moved": (move_node_to_prov_a,
                          dict(node_folder_moved=1, shard_bytes_changed=4)),
    "node_records_changed": (split_node,
                             dict(node_records_changed=1,
                                  shard_set_changed=1,
                                  shard_bytes_changed=1)),
}


@pytest.mark.parametrize("name", PLANTED)
def test_planted_change_is_detected(tmp_path, name):
  change, expected = PLANTED[name]
  assert nonzero(compare(tmp_path, change)) == expected


def test_run_dir_name_changed(tmp_path):
  result = compare(tmp_path, run_dir="other_import_20260101_000000_000000")
  assert nonzero(result) == {"run_dir_name_changed": 1}


def test_jsonld_context_changed(tmp_path):
  result = compare(tmp_path, context={"dcid": "https://example.org/"})
  assert nonzero(result) == dict(jsonld_context_changed=1,
                                 shard_bytes_changed=6)


def test_identical_outputs_report_no_difference(tmp_path):
  assert nonzero(compare(tmp_path)) == {}


def test_volatile_fields_are_ignored(tmp_path):
  """Timestamps, the run dir timestamp, node shard uids and input roots."""
  shards = copy.deepcopy(BASE_SHARDS)
  shards["ProvB"]["node-00000-cccccccccccc.jsonld"] = shards["ProvB"].pop(
      NODE_SHARD_B)
  report = copy.deepcopy(BASE_REPORT)
  report["startTime"] = "2027-05-05 12:00:00"
  moved_report = json.loads(
      json.dumps(report).replace(INPUT_ROOT, "/elsewhere/input"))
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  cand = write_output(str(tmp_path / "cand"),
                      shards,
                      report=moved_report,
                      run_dir="default_import_name_20270505_120000_123456")
  result = compare_dirs(tmp_path, base, cand, "/elsewhere/input")
  assert nonzero(result) == {}


def test_report_json_changed(tmp_path):
  report = copy.deepcopy(BASE_REPORT)
  report["status"] = "FAILURE"
  report["importFiles"][f"{INPUT_ROOT}/data.csv"]["status"] = "FAILURE"
  assert nonzero(compare(tmp_path,
                         report=report)) == dict(report_json_changed=2,
                                                 worker_reports_changed=1)


def test_missing_output_on_one_side(tmp_path):
  base = write_output(str(tmp_path / "base"), BASE_SHARDS)
  empty = str(tmp_path / "empty")
  os.makedirs(empty)
  counts = compare_dirs(tmp_path, base, empty).findings.counts
  assert counts["obs_only_baseline"] == 501
  assert counts["node_only_baseline"] == 3
  assert counts["report_json_changed"] > 0


@pytest.mark.parametrize("strict", [False, True])
def test_strict_makes_informational_categories_fail(tmp_path, strict):
  findings = compare(tmp_path, move_obs_to_prov_b).findings
  assert bool(findings.failing(strict)) == strict


def test_every_category_is_documented():
  readme = os.path.join(os.path.dirname(__file__), "..", "..", "README.md")
  with open(readme) as f:
    text = f.read()
  assert [c.name for c in CATEGORIES if f"`{c.name}`" not in text] == []


def test_hidden_files_next_to_the_run_dir_are_ignored(tmp_path):
  """Finder and similar tools drop files such as .DS_Store into output dirs."""
  root = write_output(str(tmp_path / "out"), BASE_SHARDS)
  open(os.path.join(root, "jsonld", ".DS_Store"), "w").close()
  assert load_output_tree(root).run_dir_name == RUN_DIR
