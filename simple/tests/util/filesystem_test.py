import os
import tempfile
import unittest

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
      self.assertEqual(file.size(), 5)
      file.write_bytes(b"bytes")
      self.assertEqual(file.read_bytes(), b"bytes")

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
      all_file_paths = [file.full_path() for file in dir.all_files()]
      self.assertListEqual(all_file_paths, [
          "mem://bar.txt", "mem://dir1/dir2/baz.txt",
          "mem://dir1/dir2/dir3/foo.txt"
      ])

  # Test copy_to method on File
  def test_copy_to(self):
    with create_store("mem://") as store:
      file1 = store.as_dir().open_file("foo.txt")
      file1.write("hello")
      file2 = store.as_dir().open_file("bar.txt")
      file1.copy_to(file2)
      self.assertEqual(file2.read(), "hello")
