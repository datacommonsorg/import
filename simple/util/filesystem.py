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
import logging
import threading
import time
from typing import Iterable, Optional

from fs import open_fs
import fs.errors as fserrors
import fs.path as fspath
from fs_gcsfs import GCSFS
import requests

# Paths with this prefix refer to Google Cloud Storage.
_GCS_PATH_PREFIX = "gs://"

# Minimum bytes fetched per request when streaming a GCS object.
_GCS_STREAM_CHUNK_SIZE = 8 * 1024 * 1024
# Use with File.open_stream() when only the first lines (e.g. a CSV header)
# are needed, so a header check doesn't download megabytes.
HEADER_READ_CHUNK_SIZE = 256 * 1024

# HTTP connection pool size for GCS clients. The default (10) is smaller than
# the thread pools that read files concurrently, so connections get discarded
# and re-opened.
_GCS_HTTP_POOL_SIZE = 64

# GCS roots ("gs://bucket/root") whose directory markers were already fixed in
# this process. Fixing lists every object under the root, so it must not run
# every time a store is opened: import workers open three stores per file.
_FIXED_GCS_ROOTS: set[str] = set()
_FIXED_GCS_ROOTS_LOCK = threading.Lock()


def fixed_gcs_roots() -> list[str]:
  """GCS roots already fixed in this process, for handing to worker processes."""
  with _FIXED_GCS_ROOTS_LOCK:
    return sorted(_FIXED_GCS_ROOTS)


def mark_gcs_roots_fixed(roots: Iterable[str]) -> None:
  """Records GCS roots as fixed so stores opened on them skip the fix-up."""
  with _FIXED_GCS_ROOTS_LOCK:
    _FIXED_GCS_ROOTS.update(roots)


def _gcs_root_id(fs: GCSFS) -> str:
  return f"{_GCS_PATH_PREFIX}{fs.bucket.name}/{(fs.root_path or '').strip('/')}"


def _gcs_key(fs: GCSFS, path: str) -> str:
  """GCS object key for a path relative to the store root."""
  parts = [(fs.root_path or "").strip("/"), fspath.relpath(path).strip("/")]
  return "/".join(part for part in parts if part)


def _gcs_dir_prefix(fs: GCSFS, path: str) -> str:
  """GCS key prefix that matches everything under a directory path."""
  key = _gcs_key(fs, path)
  return f"{key}/" if key else ""


def _enlarge_gcs_http_pool(fs: GCSFS) -> None:
  try:
    adapter = requests.adapters.HTTPAdapter(pool_connections=_GCS_HTTP_POOL_SIZE,
                                            pool_maxsize=_GCS_HTTP_POOL_SIZE)
    fs.client._http.mount("https://", adapter)
  except Exception as e:  # Best effort: the default pool still works.
    logging.debug("Could not resize the GCS connection pool: %s", e)


def _fix_gcsfs_storage_once(fs: GCSFS) -> None:
  root_id = _gcs_root_id(fs)
  with _FIXED_GCS_ROOTS_LOCK:
    if root_id in _FIXED_GCS_ROOTS:
      return
  _fix_gcsfs_storage(fs)
  mark_gcs_roots_fixed([root_id])


def join_path(base: str, *args: str) -> str:
  """Safely joins path components, keeping GCS/URL schemes (like gs://) intact."""
  if "://" in base:
    scheme, rest = base.split("://", 1)
    joined_rest = fspath.join(rest, *args)
    return f"{scheme}://{joined_rest}"
  return fspath.join(base, *args)


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
        # If the path is a GCS path, set strict=False in case the root_path is not initialized with an empty blob.
        if self.root_path.startswith(_GCS_PATH_PREFIX):
          self.fs = open_fs(f"{self.root_path}?strict=False",
                            create=create_if_missing)
          _enlarge_gcs_http_pool(self.fs)
          # Fix GCS storage if needed.
          _fix_gcsfs_storage_once(self.fs)
        else:
          self.fs = open_fs(self.root_path, create=create_if_missing)

        self._wrapper: _StoreWrapper = Dir(self, path="/")
        self._isdir = True
      except fserrors.CreateFailed as e:
        logging.info(
            f"Failed to open file: {self.root_path}. Falling back to treating the path as a file path. CreateFailed exception: {repr(e)}"
        )
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

  def syspath(self) -> str | None:
    """The local system path of the file or dir, or None if it has none."""
    if self.fs().hassyspath(self.path):
      return self.fs().getsyspath(self.path)
    return None


class Dir(_StoreWrapper):

  def __init__(self, store: "Store", path: str):
    super().__init__(store, path)

  def name(self) -> str:
    return fspath.basename(self.path.rstrip("/"))

  def open_dir(self, path: str, create: bool = True) -> "Dir":
    # The new dir will use the same underlying store with a path relative to
    # the same root.
    new_path = fspath.join(self.path, path)
    if create:
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

  def all_files(self, include_subdirs: bool = False):
    if isinstance(self.fs(), GCSFS):
      return self._all_gcs_files(include_subdirs)
    files = []
    subfs = self.fs().opendir(self.path)
    max_depth = None if include_subdirs else 1
    for abspath in subfs.walk.files("/", max_depth=max_depth):
      files.append(self.open_file(abspath.lstrip("/")))
    return files

  def _all_gcs_files(self, include_subdirs: bool) -> list["File"]:
    """Lists files with one paginated GCS list call.

    Walking through PyFilesystem sends requests per directory and checks each
    file exists one by one, which took minutes for a few thousand inputs.
    """
    gcs_fs = self.fs()
    root_prefix = _gcs_dir_prefix(gcs_fs, "")
    blobs = gcs_fs.bucket.list_blobs(
        prefix=_gcs_dir_prefix(gcs_fs, self.path),
        delimiter=None if include_subdirs else "/")
    files = []
    for blob in blobs:
      if blob.name.endswith("/"):
        continue  # Directory marker, not a file.
      files.append(
          File(self._store,
               blob.name[len(root_prefix):],
               create_if_missing=False,
               known_size=blob.size))
    return files


class File(_StoreWrapper):

  def __init__(self,
               store: "Store",
               path: str,
               create_if_missing: bool,
               known_size: Optional[int] = None):
    """known_size: size from a directory listing. The file is then known to
    exist, so the existence check (a request per file on GCS) is skipped."""
    super().__init__(store, path)
    self._size = known_size
    if known_size is not None:
      return
    if not self.fs().exists(self.path):
      if create_if_missing:
        parent_dir_path = fspath.dirname(path)
        self.fs().makedirs(parent_dir_path, recreate=True)
        # Make empty file.
        self.fs().touch(path)
      else:
        raise FileNotFoundError(f"File not found: {self.full_path()}")

  def name(self) -> str:
    """The file name, including any extension and without a leading slash."""
    return fspath.basename(self.path)

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

  def open_stream(self,
                  max_retries: int = 3,
                  chunk_size: Optional[int] = None,
                  mode: str = "rt"):
    """Returns an open text or binary stream for streaming with retries for transient network drops.

    On GCS the object is streamed with ranged reads of at least chunk_size
    bytes. GCSFS.open() would download the whole object before returning, so
    reading just a header used to fetch the entire file. Pass
    HEADER_READ_CHUNK_SIZE when only the first lines are needed.
    """
    for attempt in range(max_retries):
      try:
        fs = self.fs()
        if isinstance(fs, GCSFS):
          blob = fs.bucket.blob(_gcs_key(fs, self.path))
          if "b" in mode:
            blob_stream = blob.open(mode,
                                    chunk_size=chunk_size or _GCS_STREAM_CHUNK_SIZE)
            return io.BufferedReader(blob_stream,
                                     buffer_size=chunk_size or _GCS_STREAM_CHUNK_SIZE)
          return blob.open(mode,
                           chunk_size=chunk_size or _GCS_STREAM_CHUNK_SIZE,
                           encoding="utf-8",
                           newline="")
        return fs.open(self.path, mode)
      except Exception as e:
        if attempt == max_retries - 1:
          raise e
        logging.warning(
            "Transient error opening stream for %s (attempt %d/%d): %s. Retrying...",
            self.path, attempt + 1, max_retries, e)
        time.sleep(1)

  def size(self) -> int:
    """Returns the size of the file in bytes."""
    if self._size is not None:
      return self._size  # From the listing that created this File.
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
