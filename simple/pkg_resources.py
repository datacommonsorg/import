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
"""Minimal stand-in for setuptools' removed pkg_resources module.

setuptools>=82 no longer ships pkg_resources, but fs (PyFilesystem2) and
fs-gcsfs, which are unmaintained, still import it. This module provides only
the four names they use, backed by the standard library's importlib.metadata.

Python finds this module because the preprocessor and its tests run from
simple/. Delete it once the preprocessor no longer depends on fs/fs-gcsfs.
"""

import importlib.metadata
import types


class DistributionNotFound(Exception):
  pass


def declare_namespace(name: str) -> None:
  """No-op: fs packages are regular packages under modern Python."""
  del name


def get_distribution(name: str) -> types.SimpleNamespace:
  try:
    return types.SimpleNamespace(version=importlib.metadata.version(name))
  except importlib.metadata.PackageNotFoundError as e:
    raise DistributionNotFound(name) from e


def iter_entry_points(group: str, name: str | None = None):
  return iter(ep for ep in importlib.metadata.entry_points(group=group)
              if name is None or ep.name == name)
