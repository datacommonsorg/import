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
"""util.filesystem.paths: path strings."""

import pytest
from util.filesystem import is_within
from util.filesystem import join_path


# As PyFilesystem's fs.path.join did, which merged config patterns rely on.
@pytest.mark.parametrize("parts, expected", [
    (("", ""), ""),
    (("a", "{b,c}/**/*.csv"), "a/{b,c}/**/*.csv"),
    (("a", "/x/*.csv"), "/x/*.csv"),
    (("a", "//x/*.csv"), "/x/*.csv"),
    (("a//b/", "./c/../d"), "a/b/d"),
    (("gs://b/out/", "", ""), "gs://b/out"),
])
def test_join_path(parts, expected):
  assert join_path(*parts) == expected


@pytest.mark.parametrize("path, parent, expected", [
    ("gs://b/out", "gs://b/out", True),
    ("gs://b/out/jsonld", "gs://b/out", True),
    ("gs://b/out/jsonld", "gs://b/out/", True),
    ("gs://b/outx", "gs://b/out", False),
    ("gs://b", "gs://b/out", False),
    ("/data/in/x", "/", True),
])
def test_is_within(path, parent, expected):
  assert is_within(path, parent) is expected
