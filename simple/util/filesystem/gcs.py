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
"""Files under a name prefix in a Google Cloud Storage (GCS) bucket."""

import dataclasses
import io
import logging
import mimetypes
import os

from google.api_core.exceptions import GoogleAPICallError
from google.api_core.exceptions import NotFound
from google.api_core.exceptions import TooManyRequests
from google.api_core.retry import Retry as ApiRetry
from google.cloud import storage
import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from util.filesystem.paths import GCS_PATH_PREFIX
from util.filesystem.paths import normpath

# HTTP connections that the GCS client keeps open: enough for the threads that
# validate CSV headers (runner._MAX_HEADER_VALIDATION_THREADS).
_GCS_HTTP_POOL_SIZE = 32

# The Content-Encoding of objects stored gzip-compressed (`gsutil cp -Z`).
_GZIP = "gzip"
# Bytes fetched per request when a GCS file is streamed (see File.open_stream).
GCS_STREAM_CHUNK_BYTES = 8 * 1024 * 1024

# Retries of uploads that failed with 429, 5xx or a connection error, for up
# to 2 minutes. Downloads and listings get the library's default retries,
# which retry the same errors with the same deadline.
GCS_RETRY = ApiRetry(
    initial=1.0,
    maximum=10.0,
    multiplier=2.0,
    deadline=120.0,
    predicate=lambda e: isinstance(e, (TooManyRequests, GoogleAPICallError,
                                       requests.exceptions.RequestException)))

# This process's GCS client and the pid it was created in (see gcs_client()).
_gcs_client: storage.Client | None = None
_gcs_client_pid: int | None = None


def gcs_client() -> storage.Client:
  """Returns this process's GCS client, created on first use.

  Creating a client looks up credentials and opens new connections, so every
  GCS read and write in a process shares one. A forked child creates its
  own, since the connections of its parent's client can't be shared.
  Requests that fail with 429 or 5xx are retried here first. If they still
  fail, the last response is returned rather than raised, so that the
  library's own retries (with a deadline, see GCS_RETRY) see the 429 or 5xx.
  """
  global _gcs_client, _gcs_client_pid
  if _gcs_client is not None and _gcs_client_pid == os.getpid():
    return _gcs_client
  client = storage.Client()
  # allowed_methods=False retries every HTTP method, including uploads.
  retry = Retry(total=5,
                status_forcelist=[429, 502, 503, 504],
                backoff_factor=0.5,
                allowed_methods=False,
                raise_on_status=False)
  adapter = HTTPAdapter(pool_connections=_GCS_HTTP_POOL_SIZE,
                        pool_maxsize=_GCS_HTTP_POOL_SIZE,
                        max_retries=retry)
  client._http.mount("https://", adapter)
  # Plain HTTP is only used by GCS emulators.
  client._http.mount("http://", adapter)
  _gcs_client, _gcs_client_pid = client, os.getpid()
  return client


class GcsStorage:
  """The objects under a root name prefix in a GCS bucket."""

  def __init__(self, path: str):
    self.path = path
    self._bucket_name, _, self._prefix = normpath(
        path.removeprefix(GCS_PATH_PREFIX)).strip("/").partition("/")

  def _key(self, path: str) -> str:
    return f"{self._prefix}/{path}".strip("/")

  def _bucket(self) -> storage.Bucket:
    return gcs_client().bucket(self._bucket_name)

  def _blob(self, path: str, generation: int | None = None) -> storage.Blob:
    return self._bucket().blob(self._key(path), generation=generation)

  def makedirs(self, path: str) -> None:
    del path  # GCS has no dirs.

  def list(self, path: str, recursive: bool):
    key = self._key(path)
    prefix = f"{key}/" if key else ""
    files = []
    for blob in self._bucket().list_blobs(prefix=prefix,
                                          delimiter=None if recursive else "/"):
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
      return GcsRangeReader(blob, gcs_object.size, chunk_size)
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
    # Retried like shards: rewriting the same bytes is harmless.
    self._blob(path).upload_from_string(data,
                                        content_type=content_type,
                                        retry=GCS_RETRY)


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


class GcsRangeReader(io.RawIOBase):
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
