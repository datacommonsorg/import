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
"""Config: per-file lookup and how validation errors are rendered."""

from unittest import mock

import pytest
from stats.config import Config
from stats.config import parse_config


def _file(path: str) -> mock.Mock:
  f = mock.Mock()
  f.name.return_value = path.rsplit("/", 1)[-1]
  f.full_path.return_value = path
  f.path = path
  return f


def _config(*patterns: str) -> Config:
  return Config({
      "inputFiles": [{
          "pattern": p,
          "provenance": f"dcid:{i}"
      } for i, p in enumerate(patterns)]
  })


def test_for_file_prefers_an_exact_name_over_an_earlier_glob():
  config = _config("*.csv", "a.csv")
  assert config.for_file(_file("dir/a.csv")).pattern == "a.csv"
  assert config.for_file(_file("dir/b.csv")).pattern == "*.csv"


def test_for_file_prefers_an_exact_path_over_an_earlier_glob():
  config = _config("*.csv", "dir/a.csv")
  assert config.for_file(_file("dir/a.csv")).pattern == "dir/a.csv"


def test_for_file_returns_none_without_a_match():
  assert _config("*.csv").for_file(_file("a.mcf")) is None


def test_unknown_keys_are_grouped_per_object():
  with pytest.raises(ValueError) as e:
    parse_config({
        "x": 1,
        "y": 2,
        "inputFiles": [{
            "pattern": "a.csv",
            "p": 1,
            "q": 2
        }],
    })
  lines = str(e.value).split("\n")
  assert lines[0] == "Found 2 problems in config.json:"
  assert lines[1].startswith("- Unknown key(s) in config.json: ['x', 'y']."
                             " Allowed keys are: ['dataDownloadUrl',")
  assert lines[2].startswith("- Unknown key(s) in 'inputFiles' entry for"
                             " 'a.csv': ['p', 'q']. Allowed keys are: [")
  assert e.value.error_type == "INVALID_CONFIGURATION"


def test_nested_values_are_located():
  with pytest.raises(ValueError,
                     match=r"^Invalid 'columnMappings\.dcid:value' in"
                     r" 'inputFiles' entry for 'a\.csv': Input should be a"
                     r" valid string$"):
    parse_config({
        "inputFiles": [{
            "pattern": "a.csv",
            "columnMappings": {
                "dcid:value": 1
            }
        }]
    })
