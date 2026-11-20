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
"""util.filesystem on (fake) GCS: listing, ranged reads, writes."""

from unittest import mock

from google.cloud import storage
import pytest
from testsuite.lib import fake_gcs
from util.filesystem import open_dir
from util.filesystem.ranges import RangesReader


def _list(root: str) -> dict[str, str]:
  """The files under `root`, recursively, as {path: content}."""
  return {f.path: f.read() for f in open_dir(root).all_files(True)}


def test_nothing_is_written_to_gcs_to_open_a_dir_or_file():
  root = f"{fake_gcs.new_prefix()}/out"
  fake_gcs.env()  # Starts the fake GCS, which nothing else here writes to.
  directory = open_dir(root, create=True)
  directory.open_dir("a/b").open_file("c.txt")
  assert directory.all_files(include_subdirs=True) == []


@pytest.mark.parametrize("root, expected", [
    ("gs://bucket", "gs://bucket/jsonld"),
    ("gs://bucket/", "gs://bucket/jsonld"),
    ("gs://bucket/out", "gs://bucket/out/jsonld"),
    ("gs://bucket/out/", "gs://bucket/out/jsonld"),
    ("gs://bucket//out/./x", "gs://bucket/out/x/jsonld"),
])
def test_gcs_paths_are_shown_as_given_and_normalized(root, expected):
  assert open_dir(root).open_dir("jsonld").full_path() == expected


def test_gcs_objects_with_empty_or_dot_path_segments_are_skipped():
  root = f"{fake_gcs.new_prefix()}/input"
  for name in ["a.csv", "/x.csv", "d1//y.csv", "d1/./z.csv"]:
    fake_gcs.write_blob(f"{root}/{name}", "1")
  assert _list(root) == {"a.csv": "1"}


def test_a_gzip_encoded_gcs_object_is_downloaded_whole():
  """GCS ranges of it are of its compressed bytes, so it isn't streamed.

  The fake GCS doesn't decompress objects, so this checks the read path only.
  """
  gcs = f"{fake_gcs.new_prefix()}/input"
  fake_gcs.write_blob(f"{gcs}/data.csv", "a,b\n", content_encoding="gzip")
  [file] = open_dir(gcs).all_files()
  with mock.patch.object(storage.Blob,
                         "download_as_bytes",
                         autospec=True,
                         return_value=b"whole object") as download:
    with file.open_stream() as stream:
      assert stream.read() == "whole object"
  [call] = download.call_args_list
  assert call.args[1:] == () and call.kwargs == {}, call  # Not a range.


def test_a_short_ranged_read_fails_rather_than_truncate_the_file():

  def short_fetch(start: int, end: int) -> bytes:
    return b"x" * (end - start - 1)

  reader = RangesReader(short_fetch, [(0, 10)],
                        chunk_size=4,
                        name="gs://bucket/data.csv")
  with pytest.raises(OSError, match="Expected 4 bytes at offset 0 of gs://"):
    reader.read(10)


def test_a_gzip_encoded_gcs_object_has_no_byte_ranges_to_read():
  """So a large one is imported whole, not in parts."""
  gcs = f"{fake_gcs.new_prefix()}/input"
  fake_gcs.write_blob(f"{gcs}/data.csv", "a,b\n", content_encoding="gzip")
  fake_gcs.write_blob(f"{gcs}/plain.csv", "a,b\n")
  data, plain = sorted(open_dir(gcs).all_files(), key=lambda f: f.path)
  assert not data.can_read_ranges()
  with pytest.raises(ValueError, match="Can't read byte ranges"):
    data.read_bytes(0, 1)
  assert plain.can_read_ranges()
  assert open_dir(gcs).open_file("plain.csv").size() is None  # Not listed.
  assert not open_dir(gcs).open_file("plain.csv").can_read_ranges()


def test_a_file_uploaded_natively_in_an_unmarked_gcs_dir_can_be_rewritten():
  """A file written by another tool, in a dir with no marker, is rewritten."""
  root = f"{fake_gcs.new_prefix()}/output"
  fake_gcs.write_blob(f"{root}/sub/existing.txt", "old")
  open_dir(root).open_file("sub/existing.txt").write("new")
  assert open_dir(root).open_file("sub/existing.txt").read() == "new"
