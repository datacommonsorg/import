# Copyright 2024 Google LLC
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
"""util.filesystem on local disk."""

import os

from util.filesystem import open_dir


def test_a_local_dir_is_made_if_asked(tmp_path):
  path = str(tmp_path / "a" / "b")
  assert open_dir(path, create=True).full_path() == path
  assert os.path.isdir(path)
  open_dir(path, create=True)  # Already there.


def test_local_listing_follows_symlinked_dirs(tmp_path):
  (tmp_path / "real").mkdir()
  (tmp_path / "real" / "a.csv").write_text("1")
  (tmp_path / "input").mkdir()
  (tmp_path / "input" / "linked").symlink_to(tmp_path / "real")
  files = open_dir(str(tmp_path / "input")).all_files(include_subdirs=True)
  assert [(f.path, f.read()) for f in files] == [("linked/a.csv", "1")]


def test_local_paths_are_shown_as_given_and_normalized(tmp_path, monkeypatch):
  monkeypatch.chdir(tmp_path)
  (tmp_path / "out").mkdir()
  for root in ("out", "out/", "./out", "out//"):
    assert open_dir(root).open_dir("jsonld").full_path() == "out/jsonld"
  assert open_dir(".").open_file("a.csv").full_path() == "a.csv"
  assert open_dir(f"{tmp_path}/").full_path() == str(tmp_path)
