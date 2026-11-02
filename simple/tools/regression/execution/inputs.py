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
"""Resolves the input directory both sides read: local, staged or on GCS."""

import enum
import logging
import os
import subprocess

_GCS_PREFIX = "gs://"


class GcsInput(enum.Enum):
  # Copy the input to local disk once (read-only rsync); both sides read it.
  STAGE = "stage"
  # Both sides read gs:// directly, as the ingestion workflow does.
  DIRECT = "direct"


def resolve_input_dir(input_dir: str | None, gcs_input: GcsInput,
                      stage_dir: str) -> str | None:
  """Returns the input directory to pass to the preprocessor.

  Args:
    input_dir: A local directory or a gs:// path, or None for config-driven
      runs whose files come from --config_file.
    gcs_input: What to do with a gs:// input_dir.
    stage_dir: Where a staged gs:// input is copied. An existing copy is
      updated incrementally, so the directory can be reused across runs.
  """
  if input_dir is None:
    return None
  if not input_dir.startswith(_GCS_PREFIX):
    path = os.path.abspath(input_dir)
    if not os.path.isdir(path):
      raise FileNotFoundError(f"--input_dir {path} is not a directory")
    return path
  if gcs_input == GcsInput.DIRECT:
    return input_dir
  os.makedirs(stage_dir, exist_ok=True)
  logging.info("Staging %s to %s", input_dir, stage_dir)
  subprocess.run(
      ["gcloud", "storage", "rsync", "--recursive", input_dir, stage_dir],
      check=True)
  return stage_dir
