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

from stats.config import Config
from stats.data import Triple
from stats.jsonld_stream_writer import JsonLdStreamWriter
from stats.nodes import Nodes
from util.filesystem import open_dir


def test_shards_go_into_a_folder_that_another_worker_just_made(
    tmp_path, monkeypatch):
  """Workers make the folders of their shards, maybe at the same time."""
  real_makedirs = os.makedirs

  def makedirs_after_another_worker(name, mode=0o777, exist_ok=False):
    if str(name).rstrip("/").endswith("/prov"):
      real_makedirs(name, exist_ok=True)
    return real_makedirs(name, mode, exist_ok)

  writer = JsonLdStreamWriter(open_dir(str(tmp_path)), ["run"],
                              Nodes(Config({})))
  monkeypatch.setattr(os, "makedirs", makedirs_after_another_worker)
  writer.write_triples([Triple("a", "typeOf", object_id="Thing")],
                       provenance_dir="prov")
  writer.commit_and_close()

  [shard] = tmp_path.glob("jsonld/run_*/prov/node-*.jsonld")
  assert '"@id":"dcid:a"' in shard.read_text()
