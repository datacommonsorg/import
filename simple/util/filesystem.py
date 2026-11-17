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

import dataclasses
import io
import logging
import os
import time

from fs import open_fs
import fs.errors as fserrors
import fs.path as fspath
from fs_gcsfs import GCSFS
from google.cloud import storage
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

# Paths with this prefix refer to Google Cloud Storage.
_GCS_PATH_PREFIX = "gs://"

# HTTP connections that the GCS client keeps open: enough for the threads that
# validate CSV headers (runner._MAX_HEADER_VALIDATION_THREADS).
_GCS_HTTP_POOL_SIZE = 32

# The Content-Encoding of objects stored gzip-compressed (`gsutil cp -Z`).
_GZIP = "gzip"
# Bytes fetched per request when a GCS file is streamed (see File.open_stream).
_GCS_STREAM_CHUNK_BYTES = 8 * 1024 * 1024

# This process's GCS client and the pid it was created in (see gcs_client()).
_gcs_client: storage.Client | None = None
_gcs_client_pid: int | None = None


def gcs_client() -> storage.Client:
  """Returns this process's GCS client, created on first use.

  Creating a client looks up credentials and opens new connections, so every
  GCS store and upload in a process shares one. A forked child creates its
  own, since the connections of its parent's client can't be shared.
  Requests that fail with 429 or 5xx are retried, as fs-gcsfs does by default.
  """
  global _gcs_client, _gcs_client_pid
  if _gcs_client is not None and _gcs_client_pid == os.getpid():
    return _gcs_client
  client = storage.Client()
  # allowed_methods=False retries every HTTP method, including uploads.
  retry = Retry(total=5,
                status_forcelist=[429, 502, 503, 504],
                backoff_factor=0.5,
                allowed_methods=False)
  adapter = HTTPAdapter(pool_connections=_GCS_HTTP_POOL_SIZE,
                        pool_maxsize=_GCS_HTTP_POOL_SIZE,
                        max_retries=retry)
  client._http.mount("https://", adapter)
  # Plain HTTP is only used by GCS emulators.
  client._http.mount("http://", adapter)
  _gcs_client, _gcs_client_pid = client, os.getpid()
  return client


def is_gcs_path(path: str) -> bool:
  """Whether `path` is on Google Cloud Storage (a gs:// URL)."""
  return path.startswith(_GCS_PATH_PREFIX)


def _open_fs(url: str, create: bool, strict: bool = True):
  """Opens a PyFilesystem FS for `url`, like `open_fs`.

  Args:
    url: A local path or URL. gs:// URLs are opened with `gcs_client()`.
    create: Whether to create the directory if it doesn't exist.
    strict: For gs:// URLs only: whether to check that the root directory's
      marker blob exists (see `_fix_gcsfs_storage`).
  """
  if not is_gcs_path(url):
    return open_fs(url, create=create)
  # Split the URL as fs-gcsfs's own opener does.
  bucket_name, *path_parts = fspath.iteratepath(
      url.removeprefix(_GCS_PATH_PREFIX))
  # retry=0: gcs_client() already retries, and fs-gcsfs would otherwise
  # replace its HTTP adapter.
  return GCSFS(bucket_name,
               root_path=fspath.join(*path_parts),
               create=create,
               client=gcs_client(),
               retry=0,
               strict=strict)


def join_path(base: str, *args: str) -> str:
  """Safely joins path components, keeping GCS/URL schemes (like gs://) intact."""
  if "://" in base:
    scheme, rest = base.split("://", 1)
    joined_rest = fspath.join(rest, *args)
    return f"{scheme}://{joined_rest}"
  return fspath.join(base, *args)


def create_store(path: str,
                 create_if_missing: bool = False,
                 treat_as_file: bool = False,
                 mark_gcs_dirs: bool = True) -> "Store":
  """Initializes a file store from a path or URL.

  IMPORTANT: Call close() on the returned store when finished with it.
  To automatically close, use the store as a context manager,
  e.g. `with create_store(path) as store:`

  Args:
    mark_gcs_dirs: For a GCS directory, lists everything under it and writes
      the directory markers that are missing (see `_fix_gcsfs_storage`). Set
      to False when the directory was already marked, or is only read: reads
      and `Dir.all_files` don't need markers.
  """
  return Store(path, create_if_missing, treat_as_file, mark_gcs_dirs)


class Store:
  """File storage location. May be local or remote, directory or file."""

  def __init__(self, path: str, create_if_missing: bool, treat_as_file: bool,
               mark_gcs_dirs: bool):
    # Path of the associated PyFilesystem FS.
    # For single-file stores, this is updated below to be the parent directory of the file.
    self.root_path = path

    if not treat_as_file:
      try:
        # If the path is a GCS path, set strict=False in case the root_path is not initialized with an empty blob.
        self.fs = _open_fs(self.root_path,
                           create=create_if_missing,
                           strict=False)
        if mark_gcs_dirs and isinstance(self.fs, GCSFS):
          _fix_gcsfs_storage(self.fs)

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
      self.fs = _open_fs(fspath.forcedir(self.root_path),
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

  def store_path(self) -> str:
    """The path or URL of the store that this file or dir is in."""
    return self._store.root_path

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
    """Returns the subdirectory at `path`, relative to this one.

    Args:
      create: Whether to create the directory if it's missing and check that
        it is one. Set to False for a directory that is only listed or known
        to exist: that doesn't access storage at all.
    """
    # The new dir will use the same underlying store with a path relative to
    # the same root.
    new_path = fspath.join(self.path, path)
    if not create:
      return Dir(self._store, new_path)
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

  def all_files(self, include_subdirs: bool = False) -> list["File"]:
    """Returns the files in this dir, and in its subdirs if `include_subdirs`.

    The order is unspecified, and a missing dir has none. On GCS, one listing
    of the dir's prefix finds them all, without a request per file or
    directory.
    """
    if isinstance(self.fs(), GCSFS):
      return self._list_gcs_files(include_subdirs)
    if not self.fs().isdir(self.path):
      return []
    files = []
    subfs = self.fs().opendir(self.path)
    max_depth = None if include_subdirs else 1
    for abspath in subfs.walk.files("/", max_depth=max_depth):
      files.append(self.open_file(abspath.lstrip("/")))
    return files

  def _list_gcs_files(self, include_subdirs: bool) -> list["File"]:
    """Lists this dir's files on GCS with one (paginated) listing.

    Directory marker objects (names ending with "/") are skipped. The listing
    shows that the files exist, so that isn't checked again.
    """
    prefix = _gcs_dir_prefix(self.fs(), self.path)
    delimiter = None if include_subdirs else "/"
    files = []
    for blob in self.fs().bucket.list_blobs(prefix=prefix, delimiter=delimiter):
      rel_path = blob.name[len(prefix):]
      if not rel_path or rel_path.endswith("/"):
        continue
      if any(part in ("", ".", "..") for part in rel_path.split("/")):
        # Its path would name another object, or none.
        logging.warning(
            "Skipping GCS object with an empty or dot path"
            " segment: %s", blob.name)
        continue
      files.append(
          File(self._store,
               fspath.join(self.path, rel_path),
               create_if_missing=False,
               exists=True,
               gcs_object=GcsObject.of(blob)))
    return files


class File(_StoreWrapper):

  def __init__(self,
               store: "Store",
               path: str,
               create_if_missing: bool,
               exists: bool = False,
               gcs_object: "GcsObject | None" = None):
    """Opens the file at `path`, relative to the store's root.

    Args:
      exists: Whether the caller knows that the file exists, e.g. from a
        listing, so that it isn't checked.
      gcs_object: The file's GCS object, as listed. `open_stream` needs it to
        stream the file in ranges, and fails if the object was replaced since
        rather than mix its versions.
    """
    super().__init__(store, path)
    self._gcs_object = gcs_object
    if exists:
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

  def gcs_object(self) -> "GcsObject | None":
    """The file's GCS object, as listed, if known (see __init__)."""
    return self._gcs_object

  def read(self) -> str:
    """Returns file contents as a string."""
    return self.fs().readtext(self.path)

  def write(self, content: str) -> None:
    """Writes file contents from a string."""
    self.fs().writetext(self.path, content)

  def read_string_io(self) -> io.StringIO:
    return io.StringIO(self.read())

  def open_stream(self,
                  chunk_size: int = _GCS_STREAM_CHUNK_BYTES,
                  max_retries: int = 3):
    """Returns an open text stream of the file, e.g. to read it line by line.

    On GCS, a listed file is fetched as the stream is read, one ranged request
    per `chunk_size` bytes, each retried by the GCS client: reading only the
    header fetches only the first chunk. Elsewhere, opening the file is
    retried up to `max_retries` times for transient errors.
    """
    if isinstance(self.fs(), GCSFS):
      return self._open_gcs_stream(chunk_size)
    for attempt in range(max_retries):
      try:
        return self.fs().open(self.path, "r")
      except Exception as e:
        if attempt == max_retries - 1:
          raise e
        logging.warning(
            "Transient error opening stream for %s (attempt %d/%d): %s. Retrying...",
            self.path, attempt + 1, max_retries, e)
        time.sleep(1)

  def _open_gcs_stream(self, chunk_size: int) -> io.TextIOWrapper:
    gcs_object = self._gcs_object
    if gcs_object is None or gcs_object.content_encoding == _GZIP:
      # Unlisted, there is no size or generation to read ranges of; and
      # ranges of a gzip-encoded object are of its compressed bytes. Either
      # way the object is downloaded whole, as before.
      return self.fs().open(self.path, "r")
    blob = self.fs().bucket.blob(_gcs_key(self.fs(), self.path),
                                 generation=gcs_object.generation)
    raw = _GcsRangeReader(blob, gcs_object.size, chunk_size)
    # Decoded as fs.open(path, "r") does, also without a buffered layer, so
    # that the decoder sees the same reads.
    return io.TextIOWrapper(raw, encoding="utf-8", newline="")


@dataclasses.dataclass(frozen=True)
class GcsObject:
  """What a GCS listing says about an object, for reading it."""
  size: int
  generation: int
  content_encoding: str | None

  @classmethod
  def of(cls, blob: storage.Blob) -> "GcsObject":
    return cls(size=blob.size,
               generation=blob.generation,
               content_encoding=blob.content_encoding)


class _GcsRangeReader(io.RawIOBase):
  """Reads a GCS object of known size in ranged requests of `chunk_size`.

  Reads return as many bytes as asked for until the end, like a local file's.
  Reading at the end sends no request.
  """

  def __init__(self, blob: storage.Blob, size: int, chunk_size: int) -> None:
    super().__init__()
    self._blob = blob
    self._size = size
    self._chunk_size = chunk_size
    self._chunk = b""  # The last bytes fetched, from offset _pos - len.
    self._chunk_pos = 0  # Bytes of _chunk already read.
    self._pos = 0  # Offset of the end of _chunk.

  def readable(self) -> bool:
    return True

  def readinto(self, buffer) -> int:
    view = memoryview(buffer).cast("B")
    filled = 0
    while filled < len(view):
      if self._chunk_pos == len(self._chunk) and not self._fetch_chunk():
        break
      n = min(len(view) - filled, len(self._chunk) - self._chunk_pos)
      view[filled:filled + n] = self._chunk[self._chunk_pos:self._chunk_pos + n]
      filled += n
      self._chunk_pos += n
    return filled

  def _fetch_chunk(self) -> bool:
    """Fetches the next chunk; returns False at the end of the object."""
    if self._pos >= self._size:
      return False
    end = min(self._pos + self._chunk_size, self._size)
    # Checksums are of whole objects, so they can't validate a range.
    data = self._blob.download_as_bytes(start=self._pos,
                                        end=end - 1,
                                        checksum=None)
    if len(data) != end - self._pos:
      raise OSError(f"Expected {end - self._pos} bytes at offset {self._pos} of"
                    f" gs://{self._blob.bucket.name}/{self._blob.name}, got"
                    f" {len(data)}")
    self._chunk, self._chunk_pos = data, 0
    self._pos += len(data)
    return True


def _gcs_key(fs: GCSFS, path: str) -> str:
  """Returns the GCS object name of `path`, relative to the root of `fs`."""
  parts = [fs.root_path.strip("/"), path.strip("/")]
  return "/".join(part for part in parts if part)


def _gcs_dir_prefix(fs: GCSFS, path: str) -> str:
  """Returns the GCS object name prefix of everything under dir `path`."""
  key = _gcs_key(fs, path)
  return f"{key}/" if key else ""


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
