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
"""Dir and File: paths under a root dir on local disk or GCS."""

import io
import posixpath

from util.filesystem.gcs import GCS_STREAM_CHUNK_BYTES
from util.filesystem.gcs import GcsObject
from util.filesystem.gcs import GcsStorage
from util.filesystem.local import LocalStorage
from util.filesystem.paths import is_gcs_path
from util.filesystem.paths import join_path
from util.filesystem.paths import normpath


def open_dir(path: str, create: bool = False) -> "Dir":
  """Returns the dir at a local path or gs:// URL.

  Args:
    create: Whether to make the dir on local disk if it's missing. GCS has no
      dirs to make.
  """
  root = GcsStorage(path) if is_gcs_path(path) else LocalStorage(path)
  if create:
    root.makedirs("")
  return Dir(root, "")


def write_file(path: str, text: str) -> None:
  """Writes `text` to the file at a local path or gs:// URL."""
  parent, _, name = path.rpartition("/")
  open_dir(parent or ".", create=True).open_file(name).write(text)


class _Entry:
  """A dir or file, by its path relative to the root dir open_dir() opened."""

  def __init__(self, root: "LocalStorage | GcsStorage", path: str):
    self._root = root
    self.path = normpath(path).lstrip("/")  # "" for the root dir.

  def __str__(self) -> str:
    return self.full_path()

  def name(self) -> str:
    """The last part of the path, e.g. a file name with its extension."""
    return posixpath.basename(self.path)

  def store_path(self) -> str:
    """The path or URL that open_dir() opened the root with."""
    return self._root.path


class Dir(_Entry):
  """A dir under the root dir that open_dir() opened."""

  def full_path(self, sub_path: str = "") -> str:
    """The path or URL of this dir, or of `sub_path` under it if given."""
    return join_path(self._root.path, self.path, sub_path)

  def open_dir(self, path: str, create: bool = True) -> "Dir":
    """Returns the subdir at `path`; `create` makes it on local disk."""
    directory = Dir(self._root, join_path(self.path, path))
    if create:
      self._root.makedirs(directory.path)
    return directory

  def open_file(self,
                path: str,
                gcs_object: "GcsObject | None" = None) -> "File":
    """Returns the file at `path`; `gcs_object` is its GCS object as listed."""
    return File(self._root, join_path(self.path, path), gcs_object)

  def all_files(self, include_subdirs: bool = False) -> list["File"]:
    """Returns the files in this dir, and in its subdirs if `include_subdirs`.

    The order is unspecified, and a missing dir has none. On GCS, one listing
    of the dir's prefix finds them all, without a request per file or
    directory.
    """
    return [
        self.open_file(path, gcs_object)
        for path, gcs_object in self._root.list(self.path, include_subdirs)
    ]


class File(_Entry):
  """A file under the root dir that open_dir() opened.

  `gcs_object`, its GCS object as listed, lets open_stream() read it in
  ranges, and fail if the object was replaced since rather than mix versions.
  """

  def __init__(self,
               root: "LocalStorage | GcsStorage",
               path: str,
               gcs_object: "GcsObject | None" = None):
    super().__init__(root, path)
    self._gcs_object = gcs_object

  def full_path(self) -> str:
    return join_path(self._root.path, self.path)

  def gcs_object(self) -> "GcsObject | None":
    return self._gcs_object

  def read(self) -> str:
    """Returns the file's text; raises FileNotFoundError if it's missing."""
    with self._open(gcs_object=None) as stream:
      return stream.read()

  def read_string_io(self) -> io.StringIO:
    return io.StringIO(self.read())

  def open_stream(self, chunk_size: int = GCS_STREAM_CHUNK_BYTES):
    """Returns an open text stream of the file, e.g. to read it line by line.

    On GCS, a listed file is fetched as the stream is read, one ranged request
    per `chunk_size` bytes: reading only the header fetches only the first
    chunk.
    """
    return self._open(self._gcs_object, chunk_size)

  def _open(self,
            gcs_object: "GcsObject | None",
            chunk_size: int = GCS_STREAM_CHUNK_BYTES) -> io.TextIOWrapper:
    try:
      raw = self._root.open(self.path, gcs_object, chunk_size)
    except FileNotFoundError as e:
      raise FileNotFoundError(f"File not found: {self.full_path()}") from e
    # UTF-8, with line ends as they are.
    return io.TextIOWrapper(raw, encoding="utf-8", newline="")

  def write(self, content: str) -> None:
    """Writes the file's text, making its dir on local disk."""
    self._root.write(self.path, content.encode("utf-8"))
