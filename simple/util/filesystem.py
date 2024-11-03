# Copyright 2024 Google LLC
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
"""A thin wrapper on PyFilesystem for working with files and directories
in a platform-agnostic way.

Usage:
  - Use create_store() with the path or URL of a directory or file.
    - If the path doesn't exist yet, set create_if_missing=True
    - If the path is definitely to a file, use treat_as_file=True
      - When treat_as_file is not set, the type of the path defaults to dir
        and falls back to file. Check the result with isdir().
  - Use store.as_dir() or store.as_file() to work with its contents.
  - When done, call store.close().

In theory, this can support any file systems with an implementation available
in PyFilesystem. So far, OSFS (with system paths) and GCSFS (with gs:// paths)
have been tested. In-memory (mem://) and temp (temp://) paths are also built in.
"""

import io

from fs import open_fs
import fs.errors as fserrors
import fs.path as fspath
from fs_gcsfs import GCSFS

# Paths with this prefix refer to Google Cloud Storage.
_GCS_PATH_PREFIX = "gs://"


def create_store(path: str,
                 create_if_missing: bool = False,
                 treat_as_file: bool = False) -> "Store":
  """Initializes a file store from a path or URL.

  IMPORTANT: Call close() on the returned store when finished with it.
  To automatically close, use the store as a context manager,
  e.g. `with create_store(path) as store:`
  """
  return Store(path, create_if_missing, treat_as_file)


class Store:
  """File storage location. May be local or remote, directory or file."""

  def __init__(self, path: str, create_if_missing: bool, treat_as_file: bool):

    # Path of the associated PyFilesystem FS.
    # For single-file stores, this is updated below to be the parent directory of the file.
    self.root_path = path

    if not treat_as_file:
      try:
        self.fs = open_fs(self.root_path, create=create_if_missing)
        self._wrapper: _StoreWrapper = Dir(self, path="/")
        self._isdir = True
      except fserrors.CreateFailed:
        # Fall back to treating the path as a file path.
        treat_as_file = True

    if treat_as_file:
      self._isdir = False
      if "://" in path:
        # fspath.dirname normalizes "://" to ":/" so "://" must be handled separately.
        root_prefix, root_suffix = path.split("://", 1)
        self.root_path = f"{root_prefix}://{fspath.dirname(root_suffix)}"
      else:
        self.root_path = fspath.dirname(path)
      file_name = fspath.basename(path)
      self.fs = open_fs(fspath.forcedir(self.root_path),
                        create=create_if_missing)
      self._wrapper: _StoreWrapper = File(self,
                                          path=file_name,
                                          create_if_missing=create_if_missing)

    if self.root_path.startswith(_GCS_PATH_PREFIX):
      _fix_gcsfs_storage(self.fs)

  def __enter__(self):
    return self

  def __exit__(self, exc_type, exc_value, traceback):
    self.close()

  def close(self) -> None:
    self.fs.close()

  def isdir(self) -> bool:
    return self._isdir

  def as_dir(self) -> "Dir":
    if not self._isdir:
      raise TypeError(f"Not a directory: {self.full_path()}")
    return self._wrapper

  def as_file(self) -> "File":
    if self._isdir:
      raise TypeError(f"Not a file: {self.full_path()}")
    return self._wrapper

  def full_path(self) -> str:
    return self._wrapper.full_path()


class _StoreWrapper():

  def __init__(self, store: "Store", path: str):
    self._store = store

    self.path = fspath.relpath(path)

  def __str__(self) -> str:
    return self.full_path()

  def fs(self):
    return self._store.fs

  def full_path(self, sub_path="") -> str:
    """ Absolute path to this file or dir, or a sub file or dir if sub_path is given."""
    root_path = self._store.root_path
    if "://" not in root_path:
      return fspath.join(root_path, self.path, sub_path)
    else:
      # fspath.join normalizes "://" to ":/" so "://" must be handled separately.
      root_prefix, root_suffix = root_path.split("://", 1)
      return f"{root_prefix}://{fspath.join(root_suffix, self.path, sub_path)}"


class Dir(_StoreWrapper):

  def __init__(self, store: "Store", path: str):
    super().__init__(store, path)

  def open_dir(self, path: str) -> "Dir":
    # The new dir will use the same underlying store with a path relative to
    # the same root.
    new_path = fspath.join(self.path, path)
    if not self.fs().exists(new_path):
      self.fs().makedirs(new_path)
    if not self.fs().isdir(new_path):
      raise ValueError(f"{self.full_path(path)} exists and is not a directory")
    return Dir(self._store, new_path)

  def open_file(self, path: str, create_if_missing: bool = True) -> "File":
    new_path = fspath.join(self.path, path)
    if self.fs().isdir(new_path):
      raise ValueError(
          f"{self.full_path(path)} exists and is a directory, not a file")
    return File(self._store, new_path, create_if_missing)

  def all_files(self):
    files = []
    subfs = self.fs().opendir(self.path)
    for abspath in subfs.walk.files(self.path):
      files.append(self.open_file(abspath))
    return files


class File(_StoreWrapper):

  def __init__(self, store: "Store", path: str, create_if_missing: bool):
    super().__init__(store, path)
    if not self.fs().exists(self.path):
      if create_if_missing:
        # Make parent dir if needed.
        parent_dir_path = fspath.dirname(path)
        if not self.fs().isdir(parent_dir_path):
          self.fs().makedirs(parent_dir_path)
        # Make empty file.
        self.fs().touch(path)
      else:
        raise FileNotFoundError(f"File not found: {self.full_path()}")

  def name(self) -> str:
    """The file name, including any extension and without a leading slash."""
    return fspath.basename(self.path)

  def syspath(self) -> str | None:
    """The local system path of the file, or None if it has none."""
    if self.fs().hassyspath(self.path):
      return self.fs().getsyspath(self.path)
    return None

  def read(self) -> str:
    """Returns file contents as a string."""
    return self.fs().readtext(self.path)

  def write(self, content: str) -> None:
    """Writes file contents from a string."""
    self.fs().writetext(self.path, content)

  def read_bytes(self) -> bytes:
    return self.fs().readbytes(self.path)

  def write_bytes(self, content: bytes) -> None:
    self.fs().writebytes(self.path, content)

  def read_string_io(self) -> io.StringIO:
    return io.StringIO(self.read())

  def size(self) -> int:
    """Returns the size of the file in bytes."""
    return self.fs().getsize(self.path)

  def copy_to(self, dest: "File"):
    """Copies the contents of the file to the given destination file."""
    dest.write_bytes(self.read_bytes())


def _fix_gcsfs_storage(fs: GCSFS) -> None:
  """Utility function that walks the entire `root_path` and makes sure that all intermediate directories are correctly marked with empty blobs.

  As GCS is no real file system but only a key-value store, there is also no concept of folders. S3FS and GCSFS overcome this limitation by adding
  empty files with the name "<path>/" every time a directory is created, see https://fs-gcsfs.readthedocs.io/en/latest/#limitations.

  This is the same as GCSFS.fix_storage() but with a fix for an infinite loop when root_path does not end with a slash.
  """
  names = [
      blob.name
      for blob in fs.bucket.list_blobs(prefix=fspath.forcedir(fs.root_path))
  ]
  marked_dirs = set()
  all_dirs = set()

  for name in names:
    # If a blob ends with a slash, it's a directory marker
    if name.endswith("/"):
      marked_dirs.add(fspath.dirname(name))

    name = fspath.dirname(name)
    while name != fs.root_path:
      all_dirs.add(name)
      name = fspath.dirname(name)

  if fspath.forcedir(fs.root_path) != "/":
    all_dirs.add(fs.root_path)

  unmarked_dirs = all_dirs.difference(marked_dirs)

  if len(unmarked_dirs) > 0:
    for unmarked_dir in unmarked_dirs:
      dir_name = fspath.forcedir(unmarked_dir)
      blob = fs.bucket.blob(dir_name)
      blob.upload_from_string(b"")
