import tempfile
import unittest

from util.file_match import match
from util.filesystem import open_dir


class TestFileMatch(unittest.TestCase):

  def test_match_nested_dir(self):

    # Files are only paths: making them doesn't access GCS.
    file = open_dir("gs://bucket").open_file("path/to/foo.csv")

    def yes(pattern: str) -> None:
      self.assertTrue(match(file, pattern))

    def no(pattern: str) -> None:
      self.assertFalse(match(file, pattern))

    def err(pattern: str) -> None:
      with self.assertRaises(ValueError):
        match(file, pattern)

    # File path: gs://bucket/path/to/foo.csv

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
    yes("gs://bucket/path/to/**/*")
    yes("gs://**/foo.csv")
    yes("gs://**/*")
    no("gs://foo.csv")
    no("temp://**/*")

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

    no("gs://*.csv")  # Single wildcard is a single level of nesting
    no("temp://**.csv")  # Wrong protocol

    yes("path/to/foo.csv")
    yes("to/foo.csv")  # Partial match allowed
    yes("path/to*/foo.csv")  # Single wildcard in dir matches zero chars
    yes("path/t*/foo.csv")  # Single wildcard in dir matches one char
    yes("*/foo.csv")  # Wrong depth, but partial match allowed
    yes("*/*/foo.csv")
    yes("*/to/foo.csv")
    yes("**/to/foo.csv")
    yes("**/foo.csv")

    # Double wildcards don't make sense in the name portion of a pattern.
    err("gs://**.csv")  # Use "gs://**/*.csv" instead
    err("**.csv")  # Use "*.csv" instead
    err("//**/to/**.csv")

  def test_match_absolute_path(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      file = open_dir(temp_dir).open_file("path/to/foo.csv")

      def yes(pattern: str) -> None:
        self.assertTrue(match(file, pattern))

      def no(pattern: str) -> None:
        self.assertFalse(match(file, pattern))

      # Leading double slash - partial match not allowed, match absolute path
      self.assertTrue(temp_dir.startswith("/"))
      self.assertFalse(temp_dir.startswith("//"))
      no("//path/to/foo.csv")
      yes(f"/{temp_dir}/path/to/foo.csv")
      no("//to/foo.csv"
        )  # Leading slash means the match must be from the abs root
      no("//*/foo.csv")  # Single wildcard is a single level of nesting
      no("//*/*/*/foo.csv")  # Wrong depth with single wildcards
      yes("//**/foo.csv")  # Double wildcard can be multiple levels of dirs

  def test_os_abs_path(self):
    with tempfile.TemporaryDirectory() as temp_dir:
      self.assertTrue((str(temp_dir)).startswith("/"))
      self.assertFalse((str(temp_dir)).startswith("//"))
      file = open_dir(temp_dir).open_file("path/to/foo.csv")

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

    file = open_dir("gs://bucket").open_file("foo.csv")

    def yes(pattern: str) -> None:
      self.assertTrue(match(file, pattern))

    def no(pattern: str) -> None:
      self.assertFalse(match(file, pattern))

    def err(pattern: str) -> None:
      with self.assertRaises(ValueError):
        match(file, pattern)

    # File path: gs://bucket/foo.csv

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
    yes("gs://**/foo.csv")
    no("temp://**/foo.csv")
    yes("gs://bucket/foo.csv")
    yes("gs://bucket/*.csv")

    err("**.csv")
    err("/**.csv")
    err("gs://**.csv")

  def test_alternation_matching(self):
    root = open_dir("gs://bucket")

    # Test filename matching with standard brace expansion
    f1 = root.open_file("trade.csv")
    self.assertTrue(match(f1, "{trade.csv,trade.mcf}"))
    self.assertTrue(match(f1, "{trade.csv,other.mcf}"))
    self.assertFalse(match(f1, "{other.csv,trade.mcf}"))

    # Verify that it does not suffer from anchor splitting
    f1_extra = root.open_file("trade.csv_extra")
    self.assertFalse(match(f1_extra, "{trade.csv,trade.mcf}"))

    f1_prefix = root.open_file("extra_trade.mcf")
    self.assertFalse(match(f1_prefix, "{trade.csv,trade.mcf}"))

    # Test directory matching with standard brace expansion
    f2 = root.open_file("dirA/trade.csv")
    self.assertTrue(match(f2, "{dirA,dirB}/trade.csv"))
    self.assertTrue(match(f2, "{dirA,dirB}/*"))

    f3 = root.open_file("dirC/trade.csv")
    self.assertFalse(match(f3, "{dirA,dirB}/trade.csv"))

    # Test mixed path-depth top-level brace expansion (e.g. root CSV or nested MCF)
    f4 = root.open_file("match_events.csv")
    f5 = root.open_file("test1/input/schema.mcf")
    self.assertTrue(match(f4, "{match_events.csv,**/schema.mcf}"))
    self.assertTrue(match(f5, "{match_events.csv,**/schema.mcf}"))
    self.assertFalse(match(f1, "{match_events.csv,**/schema.mcf}"))
