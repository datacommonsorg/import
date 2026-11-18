"""util.filesystem on local disk and on (fake) GCS."""

import os
from unittest import mock

from google.cloud import storage
import pytest
from testsuite.lib import fake_gcs
from util.filesystem import open_dir
from util.filesystem import write_file

from util import filesystem


@pytest.fixture(name="root", params=["disk", "gcs"])
def fixture_root(request, tmp_path) -> str:
  """An empty dir on local disk or on fake GCS; returns its path or URL."""
  if request.param == "disk":
    return str(tmp_path)
  return f"{fake_gcs.new_prefix()}/root"


def _write_raw(root: str, path: str, data: str) -> None:
  """Writes a file without util.filesystem."""
  if filesystem.is_gcs_path(root):
    fake_gcs.write_blob(f"{root}/{path}", data)
  else:
    os.makedirs(os.path.dirname(os.path.join(root, path)), exist_ok=True)
    with open(os.path.join(root, path), "wb") as f:
      f.write(data.encode())


def test_dirs_and_files_have_full_paths_under_the_root(root):
  directory = open_dir(root)
  assert directory.full_path() == root
  assert directory.path == ""
  subdir = directory.open_dir("dir1/dir2")
  assert subdir.full_path() == f"{root}/dir1/dir2"
  assert subdir.name() == "dir2"
  file = subdir.open_file("dir3/foo.txt")
  assert file.path == "dir1/dir2/dir3/foo.txt"
  assert file.name() == "foo.txt"
  assert file.full_path() == f"{root}/dir1/dir2/dir3/foo.txt"
  assert file.store_path() == root


def test_a_written_file_reads_back(root):
  file = open_dir(root).open_file("dir/foo.txt")
  file.write("hello")
  assert file.read() == "hello"
  assert file.read_string_io().read() == "hello"
  with file.open_stream() as stream:
    assert stream.read() == "hello"
  file.write("again")
  assert open_dir(root).open_file("dir/foo.txt").read() == "again"


def test_write_file_writes_at_a_path_or_url(root):
  write_file(f"{root}/path/to/foo.txt", "hello")
  assert open_dir(root).open_file("path/to/foo.txt").read() == "hello"


def test_all_files_lists_the_files_written(root):
  directory = open_dir(root)
  subdir = directory.open_dir("dir1/dir2")
  subdir.open_file("dir3/foo.txt").write("")
  directory.open_file("bar.txt").write("")
  subdir.open_file("baz.txt").write("")
  assert sorted(f.full_path() for f in directory.all_files(True)) == [
      f"{root}/bar.txt", f"{root}/dir1/dir2/baz.txt",
      f"{root}/dir1/dir2/dir3/foo.txt"
  ]
  assert [f.full_path() for f in directory.all_files()] == [f"{root}/bar.txt"]


def test_a_missing_file_is_not_found(root):
  file = open_dir(root).open_file("nonexistent.txt")
  message = f"File not found: {root}/nonexistent.txt"
  with pytest.raises(FileNotFoundError, match=message):
    file.read()
  with pytest.raises(FileNotFoundError, match=message):
    file.open_stream()


# A BOM, CRLF line ends, a quoted line break and multibyte characters.
_RAW_TEXT = "\ufeffa,b\r\n\"x\r\ny\",\u00e9\u4e2d\r\n"


def test_text_is_read_unchanged(root):
  _write_raw(root, "data.csv", _RAW_TEXT)
  file = open_dir(root).open_file("data.csv")
  assert file.read() == _RAW_TEXT
  with file.open_stream() as stream:
    assert stream.read() == _RAW_TEXT


def test_text_is_written_unchanged(root):
  open_dir(root).open_file("data.csv").write(_RAW_TEXT)
  [file] = open_dir(root).all_files()
  assert file.read() == _RAW_TEXT


def test_a_local_dir_is_made_if_asked(tmp_path):
  path = str(tmp_path / "a" / "b")
  assert open_dir(path, create=True).full_path() == path
  assert os.path.isdir(path)
  open_dir(path, create=True)  # Already there.


def test_nothing_is_written_to_gcs_to_open_a_dir_or_file():
  root = f"{fake_gcs.new_prefix()}/out"
  directory = open_dir(root, create=True)
  directory.open_dir("a/b").open_file("c.txt")
  assert directory.all_files(include_subdirs=True) == []


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


@pytest.mark.parametrize("root, expected", [
    ("gs://bucket", "gs://bucket/jsonld"),
    ("gs://bucket/", "gs://bucket/jsonld"),
    ("gs://bucket/out", "gs://bucket/out/jsonld"),
    ("gs://bucket/out/", "gs://bucket/out/jsonld"),
    ("gs://bucket//out/./x", "gs://bucket/out/x/jsonld"),
])
def test_gcs_paths_are_shown_as_given_and_normalized(root, expected):
  assert open_dir(root).open_dir("jsonld").full_path() == expected


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
  assert filesystem.join_path(*parts) == expected


# --- Listing dirs on GCS ------------------------------------------------------
#
# On GCS a "directory" is only a name prefix. Tools that write GCS trees add
# directory marker objects ("d1/") for some directories or for none, so this
# tree has both: d1 has a marker, d1/d2 doesn't, and "empty" is only a marker.
# On disk, a marker is a directory.

_TREE = {
    "a.csv": "1",
    "d1/": "",
    "d1/b.csv": "22",
    "d1/d2/c.csv": "333",
    "empty/": "",
}


@pytest.fixture(name="input_dir", params=["disk", "gcs"])
def fixture_input_dir(request, tmp_path) -> str:
  """`_TREE` written on local disk or on fake GCS; returns its root."""
  if request.param == "disk":
    for name, content in _TREE.items():
      path = tmp_path / name
      if name.endswith("/"):
        path.mkdir(parents=True, exist_ok=True)
      else:
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(content)
    return str(tmp_path)
  root = f"{fake_gcs.new_prefix()}/input"
  for name, content in _TREE.items():
    fake_gcs.write_blob(f"{root}/{name}", content)
  return root


def _list(root: str, subdir: str, include_subdirs: bool) -> dict[str, str]:
  """The files `all_files` finds under `subdir`, as {path: content}."""
  directory = open_dir(root).open_dir(subdir, create=False)
  return {f.path: f.read() for f in directory.all_files(include_subdirs)}


@pytest.mark.parametrize("subdir, include_subdirs, expected", [
    ("", True, ["a.csv", "d1/b.csv", "d1/d2/c.csv"]),
    ("", False, ["a.csv"]),
    ("d1", True, ["d1/b.csv", "d1/d2/c.csv"]),
    ("d1", False, ["d1/b.csv"]),
    ("d1/d2", True, ["d1/d2/c.csv"]),
    ("empty", True, []),
    ("missing", True, []),
])
def test_all_files_lists_files_but_not_directory_markers(
    input_dir, subdir, include_subdirs, expected, caplog):
  # Reading each file checks that it is the listed object.
  assert _list(input_dir, subdir, include_subdirs) == {
      path: _TREE[path] for path in expected
  }
  assert not caplog.records  # Markers are skipped quietly.


def test_gcs_objects_with_empty_or_dot_path_segments_are_skipped():
  root = f"{fake_gcs.new_prefix()}/input"
  for name in ["a.csv", "/x.csv", "d1//y.csv", "d1/./z.csv"]:
    fake_gcs.write_blob(f"{root}/{name}", "1")
  assert _list(root, "", include_subdirs=True) == {"a.csv": "1"}


# --- Streaming files from GCS -------------------------------------------------
#
# File.open_stream decodes text as UTF-8 with newline="", so a BOM and CRLF
# line ends come through unchanged.

# Text with a quoted line break and multibyte characters, after a BOM.
_ROW = "\"x\r\ny\",\u00e9\u4e2d\r\n"
_SHORT_TEXT = "\ufeffa,b\r\n" + _ROW * 3
# Longer than one read of the text stream (8 KiB).
_LONG_TEXT = "\ufeffa,b\r\n" + _ROW * 2000


# Tiny chunks only on a short text: each chunk is a request to the fake GCS.
@pytest.mark.parametrize("text, chunk_size", [
    pytest.param("", 5, id="empty"),
    pytest.param(_SHORT_TEXT, 5, id="chunks split a CRLF and a character"),
    pytest.param(_LONG_TEXT, 8191, id="chunks just under a text read"),
    pytest.param(_LONG_TEXT, 10_001, id="chunks over a text read"),
])
def test_streaming_reads_the_file_text_unchanged(tmp_path, text, chunk_size):
  (tmp_path / "data.csv").write_bytes(text.encode())
  gcs = f"{fake_gcs.new_prefix()}/input"
  fake_gcs.write_blob(f"{gcs}/data.csv", text)
  [local_file] = open_dir(str(tmp_path)).all_files()
  [gcs_file] = open_dir(gcs).all_files()
  assert _read(local_file) == text
  assert _read(gcs_file, chunk_size=chunk_size) == text


def _read(file, **open_stream_kwargs) -> str:
  with file.open_stream(**open_stream_kwargs) as stream:
    return stream.read()


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


class _ShortReadBlob:
  """A blob whose ranged downloads return one byte less than asked for."""
  name = "data.csv"
  bucket = mock.Mock()
  bucket.name = "bucket"

  def download_as_bytes(self, start, end, checksum):
    del checksum  # Unused.
    return b"x" * (end - start)


def test_a_short_ranged_read_fails_rather_than_truncate_the_file():
  reader = filesystem._GcsRangeReader(_ShortReadBlob(), size=10, chunk_size=4)
  with pytest.raises(OSError, match="Expected 4 bytes at offset 0"):
    reader.read(10)


def test_a_file_uploaded_natively_in_an_unmarked_gcs_dir_can_be_rewritten():
  """A file written by another tool, in a dir with no marker, is rewritten."""
  root = f"{fake_gcs.new_prefix()}/output"
  fake_gcs.write_blob(f"{root}/sub/existing.txt", "old")
  open_dir(root).open_file("sub/existing.txt").write("new")
  assert open_dir(root).open_file("sub/existing.txt").read() == "new"
