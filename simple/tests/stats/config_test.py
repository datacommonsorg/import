# Copyright 2023 Google Inc.
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

import unittest

from stats.config import Config
from stats.data import ImportType
from util.filesystem import create_store
from util.filesystem import File

CONFIG_DATA = {
    "inputFiles": [
        {
            "pattern": "a.csv",
            "provenance": "Provenance21 Name"
        },
        {
            "pattern": "b.csv",
            "ignoreColumns": ["ignore1", "ignore2"]
        },
        {
            "pattern": "observations.csv",
            "importType": "observations"
        },
        {
            "pattern": "events.csv",
            "importType": "events"
        },
        {
            "pattern": "invalid_import_type.csv",
            "importType": "eVeNtS"
        },
    ],
}


class TestConfig(unittest.TestCase):

  def __init__(self, methodName: str = "runTest") -> None:
    super().__init__(methodName)
    self.maxDiff = None

  def make_file(self, path: str) -> File:
    return self.store.as_dir().open_file(path)

  def setUp(self):
    self.store = create_store("mem://")

  def tearDown(self):
    self.store.close()

  def test_ignore_columns(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.ignore_columns(self.make_file("a.csv")), [])
    self.assertEqual(config.ignore_columns(self.make_file("b.csv")),
                     ["ignore1", "ignore2"])
    self.assertEqual(config.ignore_columns(self.make_file("not_in_config.csv")),
                     [])

  def test_provenance_name(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.provenance_name(self.make_file("a.csv")),
                     "Provenance21 Name")
    self.assertEqual(config.provenance_name(self.make_file("b.csv")), "b.csv")

  def test_import_type(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.import_type(self.make_file("a.csv")),
                     ImportType.OBSERVATIONS, "default import type")
    self.assertEqual(config.import_type(self.make_file("observations.csv")),
                     ImportType.OBSERVATIONS, "observations import type")
    with self.assertRaisesRegex(ValueError, "Unsupported import type"):
      config.import_type(self.make_file("events.csv"))
    with self.assertRaisesRegex(ValueError, "Unsupported import type"):
      config.import_type(self.make_file("invalid_import_type.csv"))

  def test_empty_config(self):
    config = Config({})
    self.assertEqual(config.ignore_columns(self.make_file("a.csv")), [])
    self.assertEqual(config.provenance_name(self.make_file("a.csv")), "a.csv")

  def test_data_download_urls(self):
    self.assertListEqual(Config({}).data_download_urls(), [], "empty")
    self.assertListEqual(
        Config({
            "dataDownloadUrl": ["foo"]
        }).data_download_urls(), ["foo"], "one url")
    self.assertListEqual(
        Config({
            "dataDownloadUrl": ["foo", "bar"]
        }).data_download_urls(), ["foo", "bar"], "two urls")

  def test_input_file(self):
    self.assertDictEqual(
        Config({})._per_file_config(self.make_file("foo.csv")), {}, "empty")

    # (description, configured pattern, file path, expected per-file config)
    cases = [
        ("exact match", "foo.csv", "foo.csv", {
            "provenance": "y"
        }),
        ("subdir match", "foo.csv", "path/to/foo.csv", {
            "provenance": "y"
        }),
        ("subdir match with path", "path/to/*.csv", "path/to/foo.csv", {
            "provenance": "y"
        }),
        ("wildcard match", "foo*.csv", "foo1.csv", {
            "provenance": "y"
        }),
        ("different subdir", "path/to/*.csv", "foo.csv", {}),
        ("wrong subdir", "//to/*.csv", "path/to/foo.csv", {}),
        ("no exact match", "bar.csv", "foo.csv", {}),
        ("no wildcard match", "bar*.csv", "foo1.csv", {}),
    ]

    for description, pattern, file_path, expected in cases:
      with self.subTest(description):
        config = Config(
            {"inputFiles": [{
                "pattern": pattern,
                "provenance": "y"
            }]})
        actual = config._per_file_config(self.make_file(file_path))
        # A matched entry also carries the pattern it was keyed on.
        actual = {k: v for k, v in actual.items() if k != "pattern"}
        self.assertDictEqual(actual, expected, description)

  def test_input_files_must_be_a_list(self):
    """A dict `inputFiles` is rejected rather than silently accepted.

    MetadataValidator iterates inputFiles and skips anything that is not an
    object. Over a dict that yields pattern strings, so accepting the dict form
    would silently disable provenance and source validation.
    """
    with self.assertRaisesRegex(ValueError, "must be a list of objects"):
      Config({"inputFiles": {"foo.csv": {"x": "y"}}})

  def test_input_file_entry_must_have_a_pattern(self):
    with self.assertRaisesRegex(ValueError, "must specify 'pattern'"):
      Config({"inputFiles": [{"provenance": "dcid:Provenance1"}]})

  def test_input_file_format_is_validated_at_parse_time(self):
    # `format` is optional and only ever names the variable-per-row layout.
    Config({"inputFiles": [{"pattern": "foo.csv"}]})
    Config({"inputFiles": [{"pattern": "foo.csv", "format": "variablePerRow"}]})

    for unsupported in ["variablePerColumn", "INVALID"]:
      with self.subTest(format=unsupported):
        with self.assertRaisesRegex(ValueError, "Unsupported format"):
          Config(
              {"inputFiles": [{
                  "pattern": "foo.csv",
                  "format": unsupported
              }]})

  def test_column_mappings(self):
    config = Config({})
    self.assertDictEqual(config.column_mappings(self.make_file("foo.csv")), {},
                         "empty")

    config = Config(
        {"inputFiles": [{
            "pattern": "foo.csv",
            "columnMappings": {
                "x": "y"
            }
        }]})
    self.assertDictEqual(config.column_mappings(self.make_file("foo.csv")),
                         {"x": "y"})

  def test_row_entity_type(self):
    config = Config({})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "",
                     "empty")

    config = Config(
        {"inputFiles": [{
            "pattern": "foo.csv",
            "rowEntityType": "Foo"
        }]})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "Foo")

    config = Config({"inputFiles": [{"pattern": "foo.csv"}]})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "",
                     "unspecified")

  def test_entity_columns(self):
    config = Config({})
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")), [],
                         "empty")

    config = Config({
        "inputFiles": [{
            "pattern": "foo.csv",
            "importType": "entities",
            "columnsToResolve": ["foo", "bar"]
        }]
    })
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")),
                         ["foo", "bar"])

    config = Config({
        "inputFiles": [{
            "pattern": "foo.csv",
            "importType": "entities",
            "referenceColumns": ["baz", "qux"]
        }]
    })
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")),
                         ["baz", "qux"])

    config = Config({"inputFiles": [{"pattern": "foo.csv"}]})
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")), [],
                         "unspecified")

  def test_unknown_keys_are_rejected(self):
    with self.assertRaisesRegex(ValueError,
                                "Unknown key\\(s\\) in config.json"):
      Config({"unknownKey": True})

    with self.assertRaisesRegex(
        ValueError, "Unknown key\\(s\\) in 'inputFiles' entry for 'foo.csv'"):
      Config({"inputFiles": [{"pattern": "foo.csv", "unknownKey": "val"}]})

    with self.assertRaisesRegex(ValueError,
                                "Unknown key\\(s\\) in 'entities.Author'"):
      Config({"entities": {"Author": {"name": "Author", "unknownKey": "val"}}})

  def test_include_input_subdirs(self):
    config = Config({})
    self.assertFalse(config.include_input_subdirs())

    config = Config({"includeInputSubdirs": False})
    self.assertFalse(config.include_input_subdirs())

    config = Config({"includeInputSubdirs": True})
    self.assertTrue(config.include_input_subdirs())
