import io
import os
import tempfile
import unittest
from unittest import mock

import pytest
from testsuite.lib import fake_gcs
from util.filesystem import create_store

from util import filesystem


class TestFilesystem(unittest.TestCase):

  def test_create_store_dir_new(self):
    with create_store("mem://", create_if_missing=True) as store:
      self.assertTrue(store.isdir())
      self.assertEqual(store.full_path(), "mem://")
      self.assertEqual(store.as_dir().full_path(), "mem://")

  def test_create_store_file_new(self):
    with create_store("mem://foo.txt",
                      create_if_missing=True,
                      treat_as_file=True) as store:
      self.assertFalse(store.isdir())
      self.assertEqual(store.full_path(), "mem://foo.txt")
      self.assertEqual(store.as_file().full_path(), "mem://foo.txt")

    # Create subdir as well
    with create_store("mem://path/to/foo.txt",
                      create_if_missing=True,
                      treat_as_file=True) as store:
      self.assertFalse(store.isdir())
      self.assertEqual(store.full_path(), "mem://path/to/foo.txt")
      self.assertEqual(store.as_file().full_path(), "mem://path/to/foo.txt")

  # Test that without create_if_missing, file opening fails
  def test_missing_file(self):
    with create_store("mem://") as store:
      with self.assertRaises(FileNotFoundError):
        store.as_dir().open_file("nonexistent.txt", create_if_missing=False)

  def test_create_store_defaults_to_dir(self):
    with create_store("mem://bar", create_if_missing=True) as store:
      self.assertTrue(store.isdir())
      self.assertEqual(store.full_path(), "mem://bar")

  # Test create_store for a file that already exists
  def test_create_store_file_existing(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      # Create a file "foo.txt" in temp_dir
      file_path = os.path.join(temp_dir, "foo.txt")
      with open(file_path, "w") as f:
        f.write("hello")

      # treat_as_file=True
      with create_store(file_path, create_if_missing=False,
                        treat_as_file=True) as store:
        self.assertFalse(store.isdir())
        self.assertEqual(store.full_path(), file_path)
        self.assertEqual(store.as_file().full_path(), file_path)

      # No treat_as_file param
      with create_store(file_path, create_if_missing=False) as store:
        self.assertFalse(store.isdir())
        self.assertEqual(store.full_path(), file_path)
        self.assertEqual(store.as_file().full_path(), file_path)

  # Test create_store for a directory that already exists
  def test_create_store_dir_existing(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      with create_store(str(temp_dir), create_if_missing=False) as store:
        self.assertEqual(store.full_path(), str(temp_dir))
        self.assertTrue(store.isdir())
        self.assertEqual(store.as_dir().full_path(), str(temp_dir))

  # Test read and write methods on File
  def test_file(self):
    with create_store("mem://dir/foo.txt",
                      create_if_missing=True,
                      treat_as_file=True) as store:
      file = store.as_file()
      file.write("hello")
      self.assertEqual(file.read(), "hello")
      with file.read_string_io() as f_stringio:
        self.assertEqual(f_stringio.read(), "hello")

  def test_dir(self):
    # Test open_dir and open_file methods on Dir
    with create_store("mem://") as store:
      dir = store.as_dir()
      subdir = dir.open_dir("dir1/dir2")
      self.assertEqual(subdir.full_path(), "mem://dir1/dir2")
      file = subdir.open_file("dir3/foo.txt")
      self.assertEqual(file.full_path(), "mem://dir1/dir2/dir3/foo.txt")
      dir.open_file("bar.txt")
      subdir.open_file("baz.txt")
      all_file_paths_including_subdirs = [
          file.full_path() for file in dir.all_files(include_subdirs=True)
      ]
      self.assertListEqual(all_file_paths_including_subdirs, [
          "mem://bar.txt", "mem://dir1/dir2/baz.txt",
          "mem://dir1/dir2/dir3/foo.txt"
      ])
      all_file_paths_excluding_subdirs = [
          file.full_path() for file in dir.all_files()
      ]
      self.assertListEqual(all_file_paths_excluding_subdirs, ["mem://bar.txt"])


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
  with create_store(root, mark_gcs_dirs=False) as store:
    directory = store.as_dir().open_dir(subdir, create=False)
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
# File.open_stream decodes text as fs.open(path, "r") does (utf-8,
# newline=""), so a BOM and CRLF line ends come through unchanged.

# Longer than one read of the text stream (8 KiB), with a quoted line break and
# multibyte characters that cross read boundaries.
_LONG_TEXT = "\ufeffa,b\r\n" + "\"x\r\ny\",\u00e9\u4e2d\r\n" * 2000


@pytest.mark.parametrize("text", ["", _LONG_TEXT], ids=["empty", "long"])
@pytest.mark.parametrize("chunk_size", [7, 8191, 10_001])
def test_streaming_reads_the_file_text_unchanged(tmp_path, text, chunk_size):
  (tmp_path / "data.csv").write_bytes(text.encode())
  gcs = f"{fake_gcs.new_prefix()}/input"
  fake_gcs.write_blob(f"{gcs}/data.csv", text)
  with create_store(str(tmp_path)) as local_store, \
       create_store(gcs, mark_gcs_dirs=False) as gcs_store:
    [local_file] = local_store.as_dir().all_files()
    [gcs_file] = gcs_store.as_dir().all_files()
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
  with create_store(gcs, mark_gcs_dirs=False) as store:
    [file] = store.as_dir().all_files()
    whole = io.StringIO("whole object")
    with mock.patch.object(store.fs, "open", return_value=whole) as fs_open:
      assert file.open_stream() is whole
    fs_open.assert_called_once_with("data.csv", "r")
    assert not file.can_read_ranges()  # So it isn't split into parts.


def test_a_short_ranged_read_fails_rather_than_truncate_the_file():

  def short_fetch(start: int, end: int) -> bytes:
    return b"x" * (end - start - 1)

  reader = filesystem._RangesReader(short_fetch, [(0, 10)],
                                    chunk_size=4,
                                    name="gs://bucket/data.csv")
  with pytest.raises(OSError, match="Expected 4 bytes at offset 0 of gs://"):
    reader.read(10)


@pytest.mark.parametrize("chunk_size", [7, 10_001])
def test_a_part_stream_reads_the_header_then_the_part(tmp_path, chunk_size):
  text = "h1,h2\n" + "".join(f"{i},\u00e9\n" for i in range(3000))
  (tmp_path / "data.csv").write_bytes(text.encode())
  gcs = f"{fake_gcs.new_prefix()}/input"
  fake_gcs.write_blob(f"{gcs}/data.csv", text)
  data = text.encode()
  header_end, start, end = 6, data.index(b"\n1000,") + 1, len(data) - 5
  expected = (data[:header_end] + data[start:end]).decode()
  with create_store(str(tmp_path)) as local_store, \
       create_store(gcs, mark_gcs_dirs=False) as gcs_store:
    for store in [local_store, gcs_store]:
      [file] = store.as_dir().all_files()
      with file.open_part_stream(header_end, start, end, chunk_size) as stream:
        assert stream.read() == expected
