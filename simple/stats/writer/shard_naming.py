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
"""Naming conventions and collision checks for JSON-LD output shards."""

import re

_PART_TAG_RE = re.compile(r"p\d{3,}")
_OBS_PART_SUFFIX_RE = re.compile(r"p\d{3,}-\d{5,}\.jsonld")
_NODE_PART_SUFFIX_RE = re.compile(r"p\d{3,}-\d{5,}-[0-9a-f]+\.jsonld")


def file_shard_stem(file_path: str) -> str:
  """The part of an observation CSV's shard names that names the file.

  E.g. "a/data.csv" writes "observation-a_data-00000.jsonld": its stem is
  "a_data".
  """
  return file_path.replace("/", "_").rsplit(".", 1)[0]


def node_part_stem(file_path: str) -> str:
  """Shard name stem for a split node file, keeping its extension.

  E.g. "a/case.mcf" -> "a_case_mcf" and "a/case.csv" -> "a_case_csv", so a
  split MCF and a split entity CSV with the same base name both split without
  colliding on fallback cleanup.
  """
  return file_path.replace("/", "_").replace(".", "_")


def part_shard_tag(part_index: int) -> str:
  """Shard name tag for part `part_index` of a split file, e.g. 'p001'."""
  return f"p{part_index:03d}"


def part_shards_could_collide(file_path: str, stems: set[str]) -> bool:
  """Whether split shards of `file_path` could collide with another file's.

  A split file writes "observation-<stem>-p001-00000.jsonld", which another
  file named "<stem>-p001.csv" would also write; skip splitting such files.
  """
  prefix = f"{file_shard_stem(file_path)}-"
  return any(
      _PART_TAG_RE.fullmatch(other[len(prefix):])
      for other in stems
      if other.startswith(prefix))


def is_part_shard(name: str, stem: str, *, is_obs: bool) -> bool:
  """Whether `name` is a shard written for a part of `stem`."""
  if is_obs:
    prefix, suffix_re = f"observation-{stem}-", _OBS_PART_SUFFIX_RE
  else:
    prefix, suffix_re = f"node-{stem}-", _NODE_PART_SUFFIX_RE
  return name.startswith(prefix) and bool(
      suffix_re.fullmatch(name[len(prefix):]))
