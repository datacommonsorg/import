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
from proto.cache_data_pb2 import StatVarGroups
from stats.cache import _generate_svg_cache_internal
from stats.util import base64_decode_and_gunzip
from stats.util import gzip_and_base64_encode
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import read_triples_csv
from tests.stats.test_util import use_fake_gzip_time

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

  def test_encode_decode(self):
    use_fake_gzip_time()

    expected_proto = StatVarGroups()
    expected_proto.stat_var_groups.get_or_create("svg1").absolute_name = "SVG1"

    expected_serialized_proto = expected_proto.SerializeToString()
    expected_encoded_string = "H4sIAAAAAAAC/+Pi42IpLks3FGLjYgkOczcEAFH0/f4QAAAA"

    encoded_string = gzip_and_base64_encode(expected_serialized_proto)
    self.assertEqual(encoded_string, expected_encoded_string, "encoded string")

    serialized_proto = base64_decode_and_gunzip(encoded_string)
    self.assertEqual(serialized_proto, expected_serialized_proto,
                     "decoded bytes")

    proto = StatVarGroups()
    proto.ParseFromString(serialized_proto)
    self.assertEqual(proto, expected_proto, "proto")
