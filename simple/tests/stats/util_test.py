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

import unittest

from stats.util import base64_decode_and_gunzip_json
from stats.util import gzip_and_base64_encode_json
from tests.stats.test_util import use_fake_gzip_time


class TestUtil(unittest.TestCase):

  def test_encode_decode_json(self):
    use_fake_gzip_time()

    data = {"foo": "bar"}
    expected_encoded_string = "H4sIAAAAAAAC/6tWSsvPV7JSUEpKLFKqBQDfwKkADgAAAA=="

    encoded_string = gzip_and_base64_encode_json(data)
    self.assertEqual(encoded_string, expected_encoded_string, "encoded string")

    decoded_data = base64_decode_and_gunzip_json(encoded_string)
    self.assertDictEqual(decoded_data, data, "decoded json")
