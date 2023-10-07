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

import os
import io
import logging
from google.cloud import storage

_GCS_PATH_PREFIX = "gs://"


class FileHandler:

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

    def make_file(self, file_name: str) -> "FileHandler":
        pass

    def make_dirs(self) -> None:
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

    def make_file(self, file_name: str) -> FileHandler:
        return LocalFileHandler(os.path.join(self.path, file_name))

    def make_dirs(self) -> None:
        return os.makedirs(self.path, exist_ok=True)


class GcsFileHandler(FileHandler):
    gcs_client = storage.Client()
    logging.info("Using GCP Project: %s", gcs_client.project)

    def __init__(self, path: str) -> None:
        if not path.startswith(_GCS_PATH_PREFIX):
            raise ValueError(f"Expected {_GCS_PATH_PREFIX} prefix, got {path}")
        bucket_name, blob_name = path[len(_GCS_PATH_PREFIX):].split('/', 1)
        self.bucket = GcsFileHandler.gcs_client.bucket(bucket_name)
        self.blob = self.bucket.blob(blob_name)
        super().__init__(path, path.endswith("/"))

    def read_string(self) -> str:
        return self.blob.download_as_string().decode("utf-8")

    def write_string(self, content: str) -> None:
        self.blob.upload_from_string(content)

    def make_file(self, file_name: str) -> FileHandler:
        return GcsFileHandler(
            f"{self.path}{'' if self.isdir else '/'}{file_name}")


def create_file_handler(path: str) -> FileHandler:
    if path.startswith(_GCS_PATH_PREFIX):
        return GcsFileHandler(path)
    return LocalFileHandler(path)
