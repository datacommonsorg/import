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
"""Asynchronous local/GCS sink for writing and cleaning up JSON-LD shards."""

import concurrent.futures
import functools

from stats.writer.shard_naming import is_part_shard
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import gcs_client
from util.filesystem import GCS_RETRY
from util.filesystem import is_gcs_path

_UPLOAD_CONCURRENCY = 4
# Shards serialized but not yet written, per writer, at most: bounds memory
# while writes overlap with building the next shards.
_MAX_PENDING_SHARDS = 2 * _UPLOAD_CONCURRENCY
_SHARD_CONTENT_TYPE = "application/ld+json"


def _upload_gcs_shard(jsonld_path: str, rel_path: str, text: str) -> None:
  """Uploads shard `text` to `jsonld_path`/`rel_path`, a gs:// path."""
  bucket_name, _, blob_prefix = jsonld_path[len("gs://"):].partition("/")
  blob_prefix = blob_prefix.rstrip("/")
  blob_key = f"{blob_prefix}/{rel_path}" if blob_prefix else rel_path
  gcs_client().bucket(bucket_name).blob(blob_key).upload_from_string(
      text, content_type=_SHARD_CONTENT_TYPE, retry=GCS_RETRY)


class ShardSink:
  """Writes JSON-LD shards in the background to local disk or GCS.

  Not thread-safe: the coordinator and each worker process have their own.
  The background thread pool is started on first write so forked worker
  processes each start their own.
  """

  def __init__(self, jsonld_dir: Dir) -> None:
    self.jsonld_dir = jsonld_dir
    jsonld_path = jsonld_dir.full_path()
    self._gcs_jsonld_path = jsonld_path if is_gcs_path(jsonld_path) else None
    self._executor: concurrent.futures.ThreadPoolExecutor | None = None
    self._pending: list[concurrent.futures.Future] = []
    # Import folders made under a local jsonld_dir (GCS has no folders).
    self._local_folders: set[str] = set()

  def write(self, folder: str, name: str, text: str) -> None:
    """Writes shard `text` to jsonld_dir/folder/name in the background.

    At most `_MAX_PENDING_SHARDS` are in flight: a write error is raised here
    or by `finish`.
    """
    if self._executor is None:
      self._executor = concurrent.futures.ThreadPoolExecutor(
          max_workers=_UPLOAD_CONCURRENCY)
    while len(self._pending) >= _MAX_PENDING_SHARDS:
      self._pending.pop(0).result()
    rel_path = f"{folder}/{name}"
    if self._gcs_jsonld_path:
      task = functools.partial(_upload_gcs_shard, self._gcs_jsonld_path,
                               rel_path, text)
    else:
      if folder not in self._local_folders:
        # Another worker may make it at the same time, which open_dir allows.
        self.jsonld_dir.open_dir(folder)
        self._local_folders.add(folder)
      task = functools.partial(self._write_local_shard, rel_path, text)
    self._pending.append(self._executor.submit(task))

  def _write_local_shard(self, rel_path: str, text: str) -> None:
    self.jsonld_dir.open_file(rel_path).write(text)

  def finish(self, raise_errors: bool = True) -> None:
    """Waits for all shards in flight; raises the first error if asked to."""
    try:
      for future in self._pending:
        error = future.exception()
        if error and raise_errors:
          raise error
    finally:
      self._pending = []
      if self._executor is not None:
        self._executor.shutdown(wait=True)
        self._executor = None

  def delete_part_shards(self, folder: str, stem: str, *, is_obs: bool) -> None:
    """Deletes all part shards matching `stem` under `jsonld_dir/folder`."""
    shards = [
        shard
        for shard in self.jsonld_dir.open_dir(folder, create=False).all_files()
        if is_part_shard(shard.name(), stem, is_obs=is_obs)
    ]
    if not shards:
      return
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=min(_UPLOAD_CONCURRENCY, len(shards))) as pool:
      list(pool.map(File.delete, shards))
