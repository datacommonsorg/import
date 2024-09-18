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
"""
Includes generic utility functions.
"""

import base64
import gzip
import io
import json


def gzip_and_base64_encode(data: bytes) -> str:
  """Compresses bytes using GZIP, base64 encodes them and returns the encoded string."""
  compressed_buffer = io.BytesIO()

  with gzip.GzipFile(fileobj=compressed_buffer, mode="wb") as gz_file:
    gz_file.write(data)

  return base64.b64encode(compressed_buffer.getvalue()).decode('utf-8')


def base64_decode_and_gunzip(encoded_data: str) -> bytes:
  """Decodes a Base64 string, decompresses the GZIP data and returns the uncompressed bytes."""

  compressed_data = base64.b64decode(encoded_data)

  with gzip.GzipFile(fileobj=io.BytesIO(compressed_data), mode="rb") as gz_file:
    return gz_file.read()


def gzip_and_base64_encode_json(data: dict) -> str:
  return gzip_and_base64_encode(json.dumps(data).encode())


def base64_decode_and_gunzip_json(encoded_data: str) -> dict:
  return json.loads(base64_decode_and_gunzip(encoded_data))
