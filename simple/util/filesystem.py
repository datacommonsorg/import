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
"""Files on local disk or Google Cloud Storage (GCS), by path or gs:// URL.

open_dir(path) returns a Dir, whose open_dir(), open_file() and all_files()
return the dirs and files under it. A Dir or File is only a path: making one
doesn't access storage, and there is nothing to close.

GCS has no directories, only object names: nothing here writes the "dir/"
marker objects that some tools use as directories, and listing skips them.
"""

import dataclasses
import io
import logging
import mimetypes
import os
import posixpath

from google.api_core.exceptions import NotFound
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
  GCS read and write in a process shares one. A forked child creates its
  own, since the connections of its parent's client can't be shared.
  Requests that fail with 429 or 5xx are retried.
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


def _normpath(path: str) -> str:
  """Normalizes a path as PyFilesystem did, as the paths shown, written into
  reports and matched against config patterns expect: "a//b/./" is "a/b",
  "//a" is "/a" and "" stays "".
  """
  path = posixpath.normpath(path) if path else ""
  if path == ".":
    return ""
  return path[1:] if path.startswith("//") else path


def join_path(base: str, *parts: str) -> str:
  """Joins and normalizes paths, keeping a scheme like gs:// intact.

  A part starting with "/" restarts the path, as in os.path.join.
  """
  scheme, sep, rest = base.partition("://")
  if not sep:
    scheme, rest = "", base
  return scheme + sep + _normpath(posixpath.join(rest, *parts))


def open_dir(path: str, create: bool = False) -> "Dir":
  """Returns the dir at a local path or gs:// URL.

  Args:
    create: Whether to make the dir on local disk if it's missing. GCS has no
      dirs to make.
  """
  root = _Gcs(path) if is_gcs_path(path) else _Local(path)
  if create:
    root.makedirs("")
  return Dir(root, "")


def write_file(path: str, text: str) -> None:
  """Writes `text` to the file at a local path or gs:// URL."""
  parent, _, name = path.rpartition("/")
  open_dir(parent or ".", create=True).open_file(name).write(text)


class Dir:
  """A dir, by its path relative to the root dir that open_dir() opened."""

  def __init__(self, root: "_Local | _Gcs", path: str):
    self._root = root
    self.path = _normpath(path).lstrip("/")  # "" for the root.

  def __str__(self) -> str:
    return self.full_path()

  def name(self) -> str:
    return posixpath.basename(self.path)

  def store_path(self) -> str:
    """The path or URL that open_dir() opened the root with."""
    return self._root.path

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


class File:
  """A file, by its path relative to the root dir that open_dir() opened.

  `gcs_object`, its GCS object as listed, lets open_stream() read it in
  ranges, and fail if the object was replaced since rather than mix versions.
  """

  def __init__(self,
               root: "_Local | _Gcs",
               path: str,
               gcs_object: "GcsObject | None" = None):
    self._root = root
    self.path = _normpath(path).lstrip("/")
    self._gcs_object = gcs_object

  def __str__(self) -> str:
    return self.full_path()

  def name(self) -> str:
    """The file name, including any extension and without a leading slash."""
    return posixpath.basename(self.path)

  def store_path(self) -> str:
    """The path or URL that open_dir() opened the root with."""
    return self._root.path

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

  def open_stream(self, chunk_size: int = _GCS_STREAM_CHUNK_BYTES):
    """Returns an open text stream of the file, e.g. to read it line by line.

    On GCS, a listed file is fetched as the stream is read, one ranged request
    per `chunk_size` bytes: reading only the header fetches only the first
    chunk.
    """
    return self._open(self._gcs_object, chunk_size)

  def _open(self, gcs_object, chunk_size=_GCS_STREAM_CHUNK_BYTES):
    try:
      raw = self._root.open(self.path, gcs_object, chunk_size)
    except FileNotFoundError as e:
      raise FileNotFoundError(f"File not found: {self.full_path()}") from e
    # UTF-8, with line ends as they are.
    return io.TextIOWrapper(raw, encoding="utf-8", newline="")

  def write(self, content: str) -> None:
    """Writes the file's text, making its dir on local disk."""
    self._root.write(self.path, content.encode("utf-8"))


class _Local:
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


class _Gcs:
  """The objects under a root name prefix in a GCS bucket."""

  def __init__(self, path: str):
    self.path = path
    self._bucket_name, _, self._prefix = _normpath(
        path.removeprefix(_GCS_PATH_PREFIX)).strip("/").partition("/")

  def _key(self, path: str) -> str:
    return f"{self._prefix}/{path}".strip("/")

  def _blob(self, path: str, generation: int | None = None) -> storage.Blob:
    return gcs_client().bucket(self._bucket_name).blob(self._key(path),
                                                       generation=generation)

  def makedirs(self, path: str) -> None:
    del path  # GCS has no dirs.

  def list(self, path: str, recursive: bool):
    key = self._key(path)
    prefix = f"{key}/" if key else ""
    files = []
    for blob in gcs_client().bucket(self._bucket_name).list_blobs(
        prefix=prefix, delimiter=None if recursive else "/"):
      rel_path = blob.name[len(prefix):]
      if not rel_path or rel_path.endswith("/"):
        continue  # A dir marker.
      if any(part in ("", ".", "..") for part in rel_path.split("/")):
        # Its path would name another object, or none.
        logging.warning(
            "Skipping GCS object with an empty or dot path"
            " segment: %s", blob.name)
        continue
      files.append((rel_path, GcsObject.of(blob)))
    return files

  def open(self, path: str, gcs_object: "GcsObject | None", chunk_size: int):
    if gcs_object and gcs_object.content_encoding != _GZIP:
      blob = self._blob(path, generation=gcs_object.generation)
      return _GcsRangeReader(blob, gcs_object.size, chunk_size)
    # Unlisted, there is no size or generation to read ranges of; and ranges
    # of a gzip-encoded object are of its compressed bytes. Either way the
    # object is downloaded whole (and decompressed).
    try:
      return io.BytesIO(self._blob(path).download_as_bytes())
    except NotFound as e:
      raise FileNotFoundError(path) from e

  def write(self, path: str, data: bytes) -> None:
    # Typed by name, as fs-gcsfs did: Cloud Workflows parses the JSON
    # handshake only if it is application/json.
    content_type, encoding = mimetypes.guess_type(path)
    if not content_type or encoding:
      content_type = "application/octet-stream"
    self._blob(path).upload_from_string(data, content_type=content_type)


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
