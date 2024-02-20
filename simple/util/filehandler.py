# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A generic FileHandler abstraction that allows clients to work seamlessly with
local and GCS files and directories.
"""

import io
import logging
import os

from google.cloud import storage

_GCS_PATH_PREFIX = "gs://"


class FileHandler:
  """(Abstract) base class that should be extended by concrete implementations."""

  def __init__(self, path: str, isdir: bool) -> None:
    self.path = path
    self.isdir = isdir

  def __str__(self) -> str:
    return self.path

  def read_string(self) -> str:
    pass

  def read_string_io(self) -> io.StringIO:
    return io.StringIO(self.read_string())

  def write_string(self, content: str) -> None:
    pass

  def read_bytes(self) -> bytes:
    pass

  def write_bytes(self, content: bytes) -> None:
    pass

  def make_file(self, file_name: str) -> "FileHandler":
    pass

  def make_dirs(self) -> None:
    pass

  def basename(self) -> str:
    pass

  def exists(self) -> bool:
    pass

  def list_files(self, extension: str = None) -> list[str]:
    pass


class LocalFileHandler(FileHandler):

  def __init__(self, path: str) -> None:
    isdir = os.path.isdir(path)
    super().__init__(path, isdir)

  def read_string(self) -> str:
    with open(self.path, "r") as f:
      return f.read()

  def write_string(self, content: str) -> None:
    with open(self.path, "w") as f:
      f.write(content)

  def read_bytes(self) -> bytes:
    with open(self.path, "rb") as f:
      return f.read()

  def write_bytes(self, content: bytes) -> None:
    with open(self.path, "wb") as f:
      f.write(content)

  def make_file(self, file_name: str) -> FileHandler:
    return LocalFileHandler(os.path.join(self.path, file_name))

  def make_dirs(self) -> None:
    return os.makedirs(self.path, exist_ok=True)

  def basename(self) -> str:
    path = self.path.rstrip(self.path[-1]) if self.path.endswith(
        os.sep) else self.path
    return path.split(os.sep)[-1]

  def exists(self) -> bool:
    return os.path.exists(self.path)

  def list_files(self, extension: str = None) -> list[str]:
    all_files = os.listdir(self.path)
    if not extension:
      return all_files
    return filter(lambda name: name.lower().endswith(extension.lower()),
                  all_files)


class GcsMeta(type):

  @property
  def gcs_client(cls) -> storage.Client:
    if getattr(cls, "_GCS_CLIENT", None) is None:
      gcs_client = storage.Client()
      logging.info("Using GCS project: %s", gcs_client.project)
      cls._GCS_CLIENT = gcs_client
    return cls._GCS_CLIENT


class GcsFileHandler(FileHandler, metaclass=GcsMeta):

  def __init__(self, path: str, is_dir: bool = None) -> None:
    if not path.startswith(_GCS_PATH_PREFIX):
      raise ValueError(f"Expected {_GCS_PATH_PREFIX} prefix, got {path}")

    # If is_dir is specified, use that to set the isdir property.
    if is_dir is not None:
      isdir = is_dir
      # If it is a dir, suffix with "/" if needed.
      if isdir:
        if not path.endswith("/"):
          path = f"{path}/"
    else:
      isdir = path.endswith("/")

    bucket_name, blob_name = path[len(_GCS_PATH_PREFIX):].split('/', 1)
    self.bucket = GcsFileHandler.gcs_client.bucket(bucket_name)
    self.blob = self.bucket.blob(blob_name)

    super().__init__(path, isdir)

  def read_string(self) -> str:
    return self.blob.download_as_string().decode("utf-8")

  def write_string(self, content: str) -> None:
    self.blob.upload_from_string(content)

  def read_bytes(self) -> bytes:
    return self.blob.download_as_bytes()

  def write_bytes(self, content: bytes) -> None:
    self.blob.upload_from_string(content,
                                 content_type="application/octet-stream")

  def make_file(self, file_name: str) -> FileHandler:
    return GcsFileHandler(f"{self.path}{'' if self.isdir else '/'}{file_name}")

  def basename(self) -> str:
    path = self.path.rstrip(
        self.path[-1]) if self.path.endswith("/") else self.path
    return path.split("/")[-1]

  def exists(self) -> bool:
    return self.blob.exists()

  def list_files(self, extension: str = None) -> list[str]:
    prefix = self.blob.name if self.path.endswith("/") else f"{self.blob.name}/"
    all_files = [
        blob.name[len(prefix):]
        for blob in self.bucket.list_blobs(prefix=prefix, delimiter="/")
    ]
    if not extension:
      return all_files
    return filter(lambda name: name.lower().endswith(extension.lower()),
                  all_files)


def is_gcs_path(path: str) -> bool:
  return path.startswith(_GCS_PATH_PREFIX)


def create_file_handler(path: str, is_dir: bool = None) -> FileHandler:
  if is_gcs_path(path):
    return GcsFileHandler(path, is_dir)
  return LocalFileHandler(path)
