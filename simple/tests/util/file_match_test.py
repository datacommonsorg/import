import tempfile
import unittest

from util.file_match import match
from util.filesystem import create_store


class TestFileMatch(unittest.TestCase):

  def test_match_nested_dir(self):

    store = create_store("temp://")
    file = store.as_dir().open_file("path/to/foo.csv")

    def yes(pattern: str) -> None:
      self.assertTrue(match(file, pattern))

    def no(pattern: str) -> None:
      self.assertFalse(match(file, pattern))

    def err(pattern: str) -> None:
      with self.assertRaises(ValueError):
        match(file, pattern)

    # File path: temp://path/to/foo.csv
    # Absolute path: /<generated>/path/to/foo.csv

    # No slashes: match file name
    yes("foo.csv")
    no("bar.csv")
    no("oo.csv")
    no("oo*.csv")
    yes("foo*.csv")  # Wildcard can be no characters
    yes("f*.csv")
    yes("*.csv")
    no("*.mcf")

    # With protocol: match protocol and full path
    yes("temp://path/to/**/*")
    yes("temp://**/foo.csv")
    yes("temp://**/*")
    no("temp://foo.csv")
    no("gs://**/*")

    # Leading single slash: partial match not allowed, match relative to input dir
    no("/foo.csv")
    no("/to/foo.csv")
    yes("/path/to/foo.csv")
    no("/*")
    yes("/**/*")
    no("/*/foo.csv")  # Single wildcard is a single level of nesting
    no("/*/*/*/foo.csv")  # Wrong depth with single wildcards
    yes("/*/*/foo.csv")  # Right depth with single wildcards
    no("/*/*/oo.csv")  # Right depth, wrong filename
    yes("/*/*/*oo.csv")  # Wildcard paths, wildcard in filename
    yes("/**/foo.csv")  # Double wildcard can be multiple levels of dirs
    no("/**/oo.csv")
    yes("/**/*/foo.csv")
    yes("/*/**/foo.csv")
    yes("/**/*/*/foo.csv")
    yes("/*/**/*/foo.csv")
    yes("/**/*/**/*/**/foo.csv")  # gettin silly with it
    no("/**/*/**/*/**/*/**/foo.csv")
    no("/*/**/*/*/foo.csv")
    no("/**/*/*/*/foo.csv")

    # Leading double slash - partial match not allowed, match absolute path
    abs_root = store.as_dir().syspath()
    self.assertTrue(abs_root.startswith("/"))
    self.assertFalse(abs_root.startswith("//"))
    self.assertTrue(abs_root.endswith("/"))
    no("//path/to/foo.csv")
    yes(f"/{abs_root}path/to/foo.csv")
    no("//to/foo.csv"
      )  # Leading slash means the match must be from the abs root
    no("//*/foo.csv")  # Single wildcard is a single level of nesting
    no("//*/*/*/foo.csv")  # Wrong depth with single wildcards
    yes("//**/foo.csv")  # Double wildcard can be multiple levels of dirs

    no("temp://*.csv")  # Single wildcard is a single level of nesting
    no("gs://**.csv")  # Wrong protocol

    yes("path/to/foo.csv")
    yes("to/foo.csv")  # Partial match allowed
    yes("*/foo.csv")  # Wrong depth, but partial match allowed
    yes("*/*/foo.csv")
    yes("*/to/foo.csv")
    yes("**/to/foo.csv")
    yes("**/foo.csv")

    # Double wildcards don't make sense in the name portion of a pattern.
    err("temp://**.csv")  # Use "temp://**/*.csv" instead
    err("**.csv")  # Use "*.csv" instead
    err("//**/to/**.csv")

  def test_os_abs_path(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      self.assertTrue((str(temp_dir)).startswith("/"))
      self.assertFalse((str(temp_dir)).startswith("//"))
      store = create_store(temp_dir)
      file = store.as_dir().open_file("path/to/foo.csv")

      def yes(pattern: str) -> None:
        self.assertTrue(match(file, pattern))

      def no(pattern: str) -> None:
        self.assertFalse(match(file, pattern))

      yes("/" + str(temp_dir) + "/path/to/foo.csv")
      no("//path/to/foo.csv")
      yes("/path/to/foo.csv")
      no("/to/foo.csv")
      yes("to/foo.csv")

  def test_match_in_input_dir(self):

    store = create_store("temp://")
    file = store.as_dir().open_file("foo.csv")

    def yes(pattern: str) -> None:
      self.assertTrue(match(file, pattern))

    def no(pattern: str) -> None:
      self.assertFalse(match(file, pattern))

    def err(pattern: str) -> None:
      with self.assertRaises(ValueError):
        match(file, pattern)

    # File path: temp://foo.csv

    yes("foo.csv")
    yes("foo*.csv")  # Wildcard can be no characters
    yes("*.csv")
    no("*.mcf")

    yes("/foo.csv")
    yes("/*foo.csv")
    no("/*/foo.csv")
    no("*/foo.csv")

    yes("**/foo.csv")
    yes("/**/foo.csv")
    yes("/*")
    yes("/**/*")

    no("/to/foo.csv")  # Extra dir
    yes("temp://**/foo.csv")
    no("gs://**/foo.csv")
    yes("temp://foo.csv")
    yes("temp://*.csv")

    err("**.csv")
    err("/**.csv")
    err("temp://**.csv")
