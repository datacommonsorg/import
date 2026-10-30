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

import re

# Pre-compiled regular expression for validating entity references (URIs, DCIDs, or namespaced IDs)
# - Branch 1: Matches standard HTTP/HTTPS URLs (e.g. https://schema.org/url)
# - Branch 2: Matches standard slashed or namespaced IDs (e.g. country/FRA, dcid:country/FRA, wikidata:Q30)
_ENTITY_REF_RE = re.compile(
    r'^https?://\S+$|^[a-zA-Z0-9_-]+[/:][a-zA-Z0-9_/.-]+$')

_NAMESPACE_PREFIX_RE = re.compile(r"^[a-zA-Z0-9_\-]+:")


def has_namespace_prefix(val: str) -> bool:
  """Returns True if string starts with a namespace prefix (e.g. 'dcid:', 'undata:', 'acme:')."""
  if not isinstance(val, str) or not val:
    return False
  return bool(_NAMESPACE_PREFIX_RE.match(val.strip()))


def is_uri_or_namespace(val: str) -> bool:
  """Returns True if the value is a full URL, standard DCID, or valid custom namespace."""
  if not isinstance(val, str):
    return False
  if not val:
    return False
  if val.startswith(("http://", "https://", "dcid:")):
    return True
  if ":" in val and " " not in val:
    prefix = val.split(":", 1)[0]
    return prefix.isalnum()
  return False


def is_entity_reference(val: any) -> bool:
  """Returns True if the value syntactically looks like an entity reference (DCID or URI)."""
  if not isinstance(val, str):
    return False
  return bool(_ENTITY_REF_RE.match(val.strip()))
