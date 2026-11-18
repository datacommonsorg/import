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
"""Files under a root dir on local disk."""

import os
import posixpath

from util.filesystem.paths import join_path


class LocalStorage:
  """The files under a root dir on local disk."""

  def __init__(self, path: str):
    self.path = path
    # "~" and $VARS are expanded to access files, not in the paths shown.
    self._os_root = os.path.expanduser(os.path.expandvars(path))

  def _os_path(self, path: str) -> str:
    return os.path.join(self._os_root, path)

  def makedirs(self, path: str) -> None:
    # exist_ok: another worker process may make it at the same time.
    os.makedirs(self._os_path(path), exist_ok=True)

  def list(self, path: str, recursive: bool):
    top = self._os_path(path)
    if not os.path.isdir(top):
      return []
    files = []
    # Symlinked dirs are listed too, and an unreadable dir fails the listing.
    for dir_path, dir_names, names in os.walk(top,
                                              onerror=_raise,
                                              followlinks=True):
      rel_dir = os.path.relpath(dir_path, top)
      files.extend((join_path(rel_dir, name), None) for name in names)
      if not recursive:
        dir_names.clear()
    return sorted(files)

  def open(self, path: str, gcs_object, chunk_size):
    del gcs_object, chunk_size  # GCS only.
    return open(self._os_path(path), "rb")

  def write(self, path: str, data: bytes) -> None:
    self.makedirs(posixpath.dirname(path))
    with open(self._os_path(path), "wb") as f:
      f.write(data)


def _raise(error: OSError) -> None:
  raise error
