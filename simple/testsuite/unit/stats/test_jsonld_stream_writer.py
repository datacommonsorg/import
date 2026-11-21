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
"""Writing shards to a local output dir (the testsuite writes to GCS)."""

import os
from unittest import mock

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


def _observations(n: int) -> pd.DataFrame:
  return pd.DataFrame([{
      **{
          column: "" for column in constants.OBSERVATION_COLUMNS
      },
      constants.COLUMN_ENTITY: f"country/C{i}",
      constants.COLUMN_VARIABLE: "Count_Person",
      constants.COLUMN_DATE: "2020",
      constants.COLUMN_VALUE: "1",
  } for i in range(n)])[constants.OBSERVATION_COLUMNS]


def test_the_shards_of_a_files_parts_are_tagged_and_deleted_together(tmp_path):
  """A part of a split file names its shards after the part, e.g. "p003".

  Deleting the parts' shards leaves the file's other shards, and those of a
  file whose name starts the same.
  """
  output = open_dir(str(tmp_path))
  JsonLdStreamWriter(output, ["run"], Config({}))  # Makes the run folder.
  [run_dir] = tmp_path.glob("jsonld/run_*")
  writer = JsonLdStreamWriter(output, ["run"],
                              Config({}),
                              jsonld_dir_name=run_dir.name,
                              obs_shard_tag="p003")
  writer._write_observation_shards(_observations(3), "prov", "a/data.csv")
  writer.commit_and_close()
  for name in ("observation-a_data-00000.jsonld",
               "observation-a_data-p003x-00000.jsonld",
               "observation-a_data2-p001-00000.jsonld"):
    (run_dir / "prov" / name).write_text("{}")
  assert (run_dir / "prov/observation-a_data-p003-00000.jsonld").exists()

  file = mock.Mock(path="a/data.csv")
  with mock.patch.object(Config, "import_name", return_value="prov"):
    writer.delete_part_shards(file)
  assert sorted(p.name for p in run_dir.glob("prov/*")) == [
      "observation-a_data-00000.jsonld",
      "observation-a_data-p003x-00000.jsonld",
      "observation-a_data2-p001-00000.jsonld",
  ]


def test_shards_of_a_whole_file_are_not_tagged(tmp_path):
  writer = JsonLdStreamWriter(open_dir(str(tmp_path)), ["run"], Config({}))
  writer._write_observation_shards(_observations(1), "prov", "a/data.csv")
  writer.commit_and_close()
  assert [p.name for p in tmp_path.glob("jsonld/run_*/prov/*")
         ] == ["observation-a_data-00000.jsonld"]
