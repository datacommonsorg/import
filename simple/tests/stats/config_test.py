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
from stats.data import AggregationConfig
from stats.data import AggregationMethod
from stats.data import ImportType
from stats.data import InputFileFormat
from stats.data import Provenance
from stats.data import Source
from stats.data import StatVar
from stats.data import TimePeriod
from util.filesystem import create_store
from util.filesystem import File

CONFIG_DATA = {
    "inputFiles": {
        "a.csv": {
            "entityType": "Country",
            "provenance": "Provenance21 Name"
        },
        "b.csv": {
            "entityType": "",
            "ignoreColumns": ["ignore1", "ignore2"]
        },
        "observations.csv": {
            "importType": "observations"
        },
        "events.csv": {
            "importType": "events"
        },
        "invalid_import_type.csv": {
            "importType": "eVeNtS"
        },
    },
    "variables": {
        "Variable 1": {
            "group": "Parent Group/Child Group 1",
            "aggregation": {
                "period": "month",
                "method": "count"
            }
        },
        "Variable 2": {
            "group": "Parent Group/Child Group 1",
            "aggregation": {
                "period": "INVALID",
                "method": "count"
            }
        },
        "var3": {
            "name": "Var 3 Name",
            "description": "Var 3 Description",
            "searchDescriptions": ["Sentence 1", "Sentence 2"],
            "group": "Parent Group/Child Group 2",
        },
    },
    "sources": {
        "Source1 Name": {
            "url": "http://source1.com",
            "provenances": {
                "Provenance11 Name": "http://provenance11.com",
                "Provenance12 Name": "http://provenance12.com"
            }
        },
        "Source2 Name": {
            "url": "http://source2.com",
            "provenances": {
                "Provenance21 Name": "http://provenance21.com",
                "Provenance22 Name": "http://provenance22.com"
            }
        }
    },
}

SOURCE1 = Source(id="", name="Source1 Name", url="http://source1.com")
SOURCE2 = Source(id="", name="Source2 Name", url="http://source2.com")
PROVENANCE11 = Provenance(id="",
                          source_id="",
                          name="Provenance11 Name",
                          url="http://provenance11.com")
PROVENANCE12 = Provenance(id="",
                          source_id="",
                          name="Provenance12 Name",
                          url="http://provenance12.com")
PROVENANCE21 = Provenance(id="",
                          source_id="",
                          name="Provenance21 Name",
                          url="http://provenance21.com")
PROVENANCE22 = Provenance(id="",
                          source_id="",
                          name="Provenance22 Name",
                          url="http://provenance22.com")

PROVENANCES = {
    "Provenance11 Name": PROVENANCE11,
    "Provenance12 Name": PROVENANCE12,
    "Provenance21 Name": PROVENANCE21,
    "Provenance22 Name": PROVENANCE22,
}

PROVENANCE_SOURCES = {
    "Provenance11 Name": SOURCE1,
    "Provenance12 Name": SOURCE1,
    "Provenance21 Name": SOURCE2,
    "Provenance22 Name": SOURCE2,
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

  def test_variable(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(
        config.variable("Variable 1"),
        StatVar("", "Variable 1", group_path="Parent Group/Child Group 1"),
    )
    self.assertEqual(
        config.variable("Variable 2"),
        StatVar("", "Variable 2", group_path="Parent Group/Child Group 1"),
    )
    self.assertEqual(
        config.variable("var3"),
        StatVar(
            "",
            "Var 3 Name",
            description="Var 3 Description",
            search_descriptions=["Sentence 1", "Sentence 2"],
            group_path="Parent Group/Child Group 2",
        ),
    )
    self.assertEqual(
        config.variable("Variable with no config"),
        StatVar("", "Variable with no config"),
    )

  def test_entity_type(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.entity_type(self.make_file("a.csv")), "Country")
    self.assertEqual(config.entity_type(self.make_file("b.csv")), "")
    self.assertEqual(config.entity_type(self.make_file("not_in_config.csv")),
                     "")

  def test_ignore_columns(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(config.ignore_columns(self.make_file("a.csv")), [])
    self.assertEqual(config.ignore_columns(self.make_file("b.csv")),
                     ["ignore1", "ignore2"])
    self.assertEqual(config.ignore_columns(self.make_file("not_in_config.csv")),
                     [])

  def test_provenances_and_sources(self):
    config = Config(CONFIG_DATA)
    self.assertDictEqual(config.provenances, PROVENANCES)
    self.assertDictEqual(config.provenance_sources, PROVENANCE_SOURCES)

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
    self.assertEqual(config.import_type(self.make_file("events.csv")),
                     ImportType.EVENTS, "events import type")
    with self.assertRaisesRegex(ValueError, "Unsupported import type"):
      config.import_type(self.make_file("invalid_import_type.csv"))

  def test_aggregation(self):
    config = Config(CONFIG_DATA)
    self.assertEqual(
        config.aggregation("Variable 1"),
        AggregationConfig(TimePeriod.MONTH, AggregationMethod.COUNT),
        "valid date config")
    self.assertEqual(
        config.aggregation("var3"),
        AggregationConfig(TimePeriod.YEAR, AggregationMethod.COUNT),
        "default date config")
    with self.assertRaisesRegex(ValueError, "invalid period"):
      config.aggregation("Variable 2")

  def test_empty_config(self):
    config = Config({})
    self.assertEqual(config.variable("Variable 1"), StatVar("", "Variable 1"))
    self.assertEqual(config.entity_type(self.make_file("a.csv")), "")
    self.assertEqual(config.ignore_columns(self.make_file("a.csv")), [])
    self.assertDictEqual(config.provenances, {})
    self.assertDictEqual(config.provenance_sources, {})
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
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "foo.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("foo.csv")), {"x": "y"},
        "exact match")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "foo.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("path/to/foo.csv")), {"x": "y"},
        "subdir match")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "path/to/*.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("path/to/foo.csv")), {"x": "y"},
        "subdir match with path")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "path/to/*.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("foo.csv")), {}, "different subdir")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "//to/*.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("path/to/foo.csv")), {},
        "wrong subdir")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "foo*.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("foo1.csv")), {"x": "y"},
        "wildcard match")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "bar.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("foo.csv")), {}, "no exact match")
    self.assertDictEqual(
        Config({
            "inputFiles": {
                "bar*.csv": {
                    "x": "y"
                }
            }
        })._per_file_config(self.make_file("foo1.csv")), {},
        "no wildcard match")

  def test_input_file_format(self):
    config = Config({})
    self.assertEqual(config.format(self.make_file("foo.csv")), None, "empty")

    config = Config({"inputFiles": {"foo.csv": {"format": "variablePerRow"}}})
    self.assertEqual(config.format(self.make_file("foo.csv")),
                     InputFileFormat.VARIABLE_PER_ROW)

    config = Config(
        {"inputFiles": {
            "foo.csv": {
                "format": "variablePerColumn"
            }
        }})
    self.assertEqual(config.format(self.make_file("foo.csv")),
                     InputFileFormat.VARIABLE_PER_COLUMN)

    config = Config({"inputFiles": {"foo.csv": {"format": "INVALID"}}})
    with self.assertRaisesRegex(ValueError, "Unsupported format"):
      config.format(self.make_file("foo.csv"))

  def test_column_mappings(self):
    config = Config({})
    self.assertDictEqual(config.column_mappings(self.make_file("foo.csv")), {},
                         "empty")

    config = Config({"inputFiles": {"foo.csv": {"columnMappings": {"x": "y"}}}})
    self.assertDictEqual(config.column_mappings(self.make_file("foo.csv")),
                         {"x": "y"})

  def test_row_entity_type(self):
    config = Config({})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "",
                     "empty")

    config = Config({"inputFiles": {"foo.csv": {"rowEntityType": "Foo"}}})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "Foo")

    config = Config({"inputFiles": {"foo.csv": {}}})
    self.assertEqual(config.row_entity_type(self.make_file("foo.csv")), "",
                     "unspecified")

  def test_entity_columns(self):
    config = Config({})
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")), [],
                         "empty")

    config = Config(
        {"inputFiles": {
            "foo.csv": {
                "entityColumns": ["foo", "bar"]
            }
        }})
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")),
                         ["foo", "bar"])

    config = Config({"inputFiles": {"foo.csv": {}}})
    self.assertListEqual(config.entity_columns(self.make_file("foo.csv")), [],
                         "unspecified")
