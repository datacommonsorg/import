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
"""Canonical serialization and fixed-size digests of JSON-LD values."""

import hashlib
import json
from typing import Any

_DIGEST64_BYTES = 8
_DIGEST128_BYTES = 16


def canonical_json(value: Any) -> str:
  """Serializes a JSON value with sorted keys and no whitespace."""
  return json.dumps(value, sort_keys=True, separators=(",", ":"))


def digest64(text: str) -> int:
  """Returns a 64-bit digest of text, as an unsigned int."""
  digest = hashlib.blake2b(text.encode("utf-8"),
                           digest_size=_DIGEST64_BYTES).digest()
  return int.from_bytes(digest, "little")


def digest128(text: str) -> tuple[int, int]:
  """Returns a 128-bit digest of text, as two unsigned 64-bit ints."""
  digest = hashlib.blake2b(text.encode("utf-8"),
                           digest_size=_DIGEST128_BYTES).digest()
  return (int.from_bytes(digest[:_DIGEST64_BYTES], "little"),
          int.from_bytes(digest[_DIGEST64_BYTES:], "little"))


def load_json(path: str) -> Any:
  """Reads a JSON file."""
  with open(path, encoding="utf-8") as f:
    return json.load(f)
