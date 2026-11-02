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
"""
An in-process fake GCS, so gs:// paths run the real GCS code paths.

Uses the gcp-storage-emulator package (an HTTP implementation of the GCS JSON
API) and points google-cloud-storage at it via STORAGE_EMULATOR_HOST. The
preprocessor's fs_gcsfs stores, google.cloud.storage uploads and handshake
writes therefore all go through the same client code as in production.

One emulator serves the whole test session; each run gets its own prefix.
"""

import os
import threading
import uuid

BUCKET = "testsuite-bucket"

_lock = threading.Lock()
_server = None
_host = ""


# Environment that makes google-cloud-storage talk to the emulator. Client
# certificates are disabled because the emulator speaks plain HTTP.
def env() -> dict[str, str]:
  _ensure_started()
  return {
      "STORAGE_EMULATOR_HOST": _host,
      "GOOGLE_CLOUD_PROJECT": "testsuite",
      "GOOGLE_API_USE_CLIENT_CERTIFICATE": "false",
      "CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE": "false",
  }


def new_prefix() -> str:
  """A fresh gs:// directory for one run."""
  return f"gs://{BUCKET}/run-{uuid.uuid4().hex[:12]}"


def _ensure_started() -> None:
  global _server, _host
  with _lock:
    if _server:
      return
    from gcp_storage_emulator.server import create_server  # pylint: disable=import-outside-toplevel

    # Port 0: the OS picks a free port while binding, so parallel workers
    # can't race for the same port. Read the port back once it's bound.
    _server = create_server("127.0.0.1",
                            0,
                            in_memory=True,
                            default_bucket=BUCKET)
    try:
      api = _server._api  # pylint: disable=protected-access
      _ = api.daemon  # Verify attribute exists before writing to it.
      # Daemon thread, so the emulator never keeps the test process alive.
      api.daemon = True
      _server.start()
      port = api._httpd.server_address[1]  # pylint: disable=protected-access
    except (AttributeError, KeyError, IndexError) as e:
      raise RuntimeError(
          "gcp-storage-emulator internals changed: failed to access internal"
          " server or api attributes") from e
    _host = f"http://127.0.0.1:{port}"
    # Set process-wide: google-cloud-storage reads these lazily (e.g. when a
    # client first opens its HTTP session), and forked workers inherit them.
    os.environ.update({
        "STORAGE_EMULATOR_HOST": _host,
        "GOOGLE_CLOUD_PROJECT": "testsuite",
        "GOOGLE_API_USE_CLIENT_CERTIFICATE": "false",
        "CLOUDSDK_CONTEXT_AWARE_USE_CLIENT_CERTIFICATE": "false",
    })
    # Local fakes must never be reached through an HTTP proxy.
    for var in ("NO_PROXY", "no_proxy"):
      hosts = [h for h in os.environ.get(var, "").split(",") if h]
      os.environ[var] = ",".join(hosts + ["127.0.0.1", "localhost"])


def _client():
  from google.cloud import storage  # pylint: disable=import-outside-toplevel
  _ensure_started()
  return storage.Client()


def upload_dir(local_dir: str, gcs_dir: str) -> None:
  """Copies a local directory tree to gs://BUCKET/<prefix>."""
  bucket = _client().bucket(BUCKET)
  prefix = gcs_dir.removeprefix(f"gs://{BUCKET}/").rstrip("/")
  for root, _, files in os.walk(local_dir):
    for name in files:
      path = os.path.join(root, name)
      rel = os.path.relpath(path, local_dir)
      bucket.blob(f"{prefix}/{rel}").upload_from_filename(path)


def download_dir(gcs_dir: str, local_dir: str) -> None:
  """Copies every object under a gs:// directory to a local directory."""
  bucket = _client().bucket(BUCKET)
  prefix = gcs_dir.removeprefix(f"gs://{BUCKET}/").rstrip("/") + "/"
  for blob in bucket.list_blobs(prefix=prefix):
    if blob.name.endswith("/"):
      continue  # Directory marker.
    path = os.path.join(local_dir, blob.name[len(prefix):])
    os.makedirs(os.path.dirname(path), exist_ok=True)
    blob.download_to_filename(path)


def read_text(gcs_path: str) -> str | None:
  bucket = _client().bucket(BUCKET)
  blob = bucket.blob(gcs_path.removeprefix(f"gs://{BUCKET}/"))
  return blob.download_as_text() if blob.exists() else None
