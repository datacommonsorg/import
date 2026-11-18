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

from fs.osfs import OSFS
from stats.config import Config
from stats.data import Triple
from stats.jsonld_stream_writer import JsonLdStreamWriter
from stats.nodes import Nodes
from util.filesystem import create_store


def test_shards_go_into_a_folder_that_another_worker_just_made(
    tmp_path, monkeypatch):
  """Workers make the folders of their shards, maybe at the same time."""
  real_makedir = OSFS.makedir

  def makedir_after_another_worker(fs, path, permissions=None, recreate=False):
    if path.rstrip("/").endswith("/prov"):
      os.makedirs(fs.getsyspath(path), exist_ok=True)
    return real_makedir(fs, path, permissions, recreate)

  monkeypatch.setattr(OSFS, "makedir", makedir_after_another_worker)
  with create_store(str(tmp_path)) as store:
    writer = JsonLdStreamWriter(store.as_dir(), ["run"], Nodes(Config({})))
    writer.write_triples([Triple("a", "typeOf", object_id="Thing")],
                         provenance_dir="prov")
    writer.commit_and_close()

  [shard] = tmp_path.glob("jsonld/run_*/prov/node-*.jsonld")
  assert '"@id":"dcid:a"' in shard.read_text()
