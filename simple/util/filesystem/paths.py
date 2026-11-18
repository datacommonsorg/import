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
"""Path strings: local paths and gs:// URLs, normalized as PyFilesystem did."""

import posixpath

# Paths with this prefix refer to Google Cloud Storage.
GCS_PATH_PREFIX = "gs://"


def is_gcs_path(path: str) -> bool:
  """Whether `path` is on Google Cloud Storage (a gs:// URL)."""
  return path.startswith(GCS_PATH_PREFIX)


def normpath(path: str) -> str:
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
  return scheme + sep + normpath(posixpath.join(rest, *parts))


def is_within(path: str, parent: str) -> bool:
  """Whether `path` is `parent` or under it; both normalized (see join_path)."""
  return path == parent or path.startswith(parent.rstrip("/") + "/")
