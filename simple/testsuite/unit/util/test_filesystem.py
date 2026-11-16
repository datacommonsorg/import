import os
import shutil
import tempfile
import unittest

from testsuite.lib import fake_gcs
from util.filesystem import create_store


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


class TestGcsListing(unittest.TestCase):
  """Dir listings on (fake) GCS find what the local walk finds."""

  # Directory markers are written for some directories only, as GCS trees
  # written by other tools have them for some or none.
  _FILES = {"a.csv": "1", "d1/b.csv": "22", "d1/d2/c.csv": "333"}
  _MARKERS = ["d1/", "empty/"]

  def setUp(self):
    self.local = tempfile.mkdtemp()
    for rel, content in self._FILES.items():
      os.makedirs(os.path.dirname(os.path.join(self.local, rel)), exist_ok=True)
      with open(os.path.join(self.local, rel), "w") as f:
        f.write(content)
    os.makedirs(os.path.join(self.local, "empty"))
    self.gcs = f"{fake_gcs.new_prefix()}/input"
    fake_gcs.upload_dir(self.local, self.gcs)
    for marker in self._MARKERS:
      fake_gcs.write_blob(f"{self.gcs}/{marker}", "")

  def tearDown(self):
    shutil.rmtree(self.local)

  def _listing(self, root: str, subdir: str, include_subdirs: bool):
    with create_store(root, mark_gcs_dirs=False) as store:
      directory = store.as_dir().open_dir(subdir, create=False)
      return sorted(
          (f.path, f.size()) for f in directory.all_files(include_subdirs))

  def test_all_files_finds_the_files_and_sizes_that_the_local_walk_finds(self):
    for subdir in ["", "d1", "d1/d2", "empty"]:
      for include_subdirs in [True, False]:
        with self.subTest(subdir=subdir, include_subdirs=include_subdirs):
          self.assertEqual(self._listing(self.gcs, subdir, include_subdirs),
                           self._listing(self.local, subdir, include_subdirs))

  def test_all_files_skips_objects_with_empty_or_dot_segments(self):
    for name in ["/x.csv", "d1//y.csv", "d1/./z.csv"]:
      fake_gcs.write_blob(f"{self.gcs}/{name}", "1")
    self.assertEqual(self._listing(self.gcs, "", include_subdirs=True),
                     self._listing(self.local, "", include_subdirs=True))

  def test_size_of_a_file_that_was_not_listed(self):
    for root in [self.local, self.gcs]:
      with create_store(root, mark_gcs_dirs=False) as store:
        with self.subTest(root=root):
          file = store.as_dir().open_file("d1/d2/c.csv",
                                          create_if_missing=False)
          self.assertEqual(file.size(), 3)

  def test_is_subdir_with_and_without_directory_markers(self):
    expected = {
        "d1": True,
        "d1/d2": True,
        "empty": True,
        "a.csv": False,
        "missing": False
    }
    for root in [self.local, self.gcs]:
      with create_store(root, mark_gcs_dirs=False) as store:
        for path, is_dir in expected.items():
          with self.subTest(root=root, path=path):
            self.assertEqual(store.as_dir().is_subdir(path), is_dir)
