# Copyright 2025 Google LLC
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

import os

import pytest
from stats.db import get_blue_green_config_from_env


def test_blue_green_disabled_by_default():
  """Test blue-green is disabled by default."""
  # Clear env vars
  if "ENABLE_BLUE_GREEN_IMPORT" in os.environ:
    del os.environ["ENABLE_BLUE_GREEN_IMPORT"]

  config = get_blue_green_config_from_env()
  assert config["enabled"] is False


def test_blue_green_enabled_via_env():
  """Test blue-green can be enabled."""
  os.environ["ENABLE_BLUE_GREEN_IMPORT"] = "true"

  try:
    config = get_blue_green_config_from_env()
    assert config["enabled"] is True
  finally:
    del os.environ["ENABLE_BLUE_GREEN_IMPORT"]


def test_blue_green_local_sqlite_path():
  """Test local SQLite path configuration."""
  os.environ["ENABLE_BLUE_GREEN_IMPORT"] = "true"
  os.environ["LOCAL_BUILD_SQLITE_PATH"] = "/tmp/test_build.db"

  try:
    config = get_blue_green_config_from_env()
    assert config["enabled"] is True
    assert config["local_sqlite_path"] == "/tmp/test_build.db"
  finally:
    del os.environ["ENABLE_BLUE_GREEN_IMPORT"]
    del os.environ["LOCAL_BUILD_SQLITE_PATH"]


def test_blue_green_default_sqlite_path():
  """Test default SQLite path when not specified."""
  os.environ["ENABLE_BLUE_GREEN_IMPORT"] = "true"

  try:
    config = get_blue_green_config_from_env()
    assert config["local_sqlite_path"] == "/tmp/datacommons_build.db"
  finally:
    del os.environ["ENABLE_BLUE_GREEN_IMPORT"]
