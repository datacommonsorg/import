# Copyright 2024 Google Inc.
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

import json
import os
import shutil
import tempfile
import unittest

from parameterized import parameterized
from stats.cache import _generate_svg_cache_internal
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import read_triples_csv

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "cache")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _read_json(path: str) -> dict:
  if not os.path.exists(path):
    return {}
  with open(path, "r") as f:
    return json.load(f)


class TestCache(unittest.TestCase):

  @parameterized.expand([
      ("svg_cache_basic",),
      ("svg_cache_with_specialized_names",),
  ])
  def test_generate_svg_cache_internal(self, test_name: str):
    with tempfile.TemporaryDirectory() as temp_dir:
      input_dir = os.path.join(_INPUT_DIR, test_name)
      expected_dir = os.path.join(_EXPECTED_DIR, test_name)
      output_proto_path = os.path.join(temp_dir, "svg_cache.textproto")
      expected_proto_path = os.path.join(expected_dir, "svg_cache.textproto")

      svg_triples = read_triples_csv(os.path.join(input_dir, "svg_triples.csv"))
      sv_triples = read_triples_csv(os.path.join(input_dir, "sv_triples.csv"))
      specialized_names = _read_json(
          os.path.join(input_dir, "specialized_names.json"))

      svg_cache = _generate_svg_cache_internal(svg_triples, sv_triples,
                                               specialized_names)

      with open(output_proto_path, "w") as f:
        f.write(str(svg_cache))

      if is_write_mode():
        shutil.copy(output_proto_path, expected_proto_path)
        return

      compare_files(self, output_proto_path, expected_proto_path, test_name)
