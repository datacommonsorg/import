# Copyright 2024 Google Inc.
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

import json
import os
import shutil
import sqlite3
import tempfile
import unittest

import pandas as pd
from stats.config import Config
from stats.data import Triple
from stats.db import create_db
from stats.db import create_sqlite_config
from stats.entities_importer import EntitiesImporter
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from tests.stats.test_util import is_write_mode
from util.filehandler import LocalFileHandler

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "entities_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _compare_files(test: unittest.TestCase, output_path, expected_path):
  with open(output_path) as gotf:
    got = gotf.read()
    with open(expected_path) as wantf:
      want = wantf.read()
      test.assertEqual(got, want)


def _write_triples(db_path: str, output_path: str):
  with sqlite3.connect(db_path) as db:
    rows = db.execute("select * from triples").fetchall()
    triples = [Triple(*row) for row in rows]
    pd.DataFrame(triples).to_csv(output_path, index=False)


def _test_import(test: unittest.TestCase, test_name: str):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_file = f"{test_name}.csv"
    input_path = os.path.join(_INPUT_DIR, input_file)
    input_config_path = os.path.join(_INPUT_DIR, "config.json")
    db_path = os.path.join(temp_dir, f"{test_name}.db")

    output_triples_path = os.path.join(temp_dir, f"{test_name}.triples.db.csv")
    expected_triples_path = os.path.join(_EXPECTED_DIR,
                                         f"{test_name}.triples.db.csv")

    input_fh = LocalFileHandler(input_path)

    input_config_fh = LocalFileHandler(input_config_path)
    config = Config(data=json.loads(input_config_fh.read_string()))
    nodes = Nodes(config)

    db = create_db(create_sqlite_config(db_path))
    report_fh = LocalFileHandler(os.path.join(temp_dir, "report.json"))
    reporter = FileImportReporter(input_path, ImportReporter(report_fh))

    EntitiesImporter(input_fh=input_fh, db=db, reporter=reporter,
                     nodes=nodes).do_import()
    db.insert_triples(nodes.triples())
    db.commit_and_close()

    _write_triples(db_path, output_triples_path)

    if is_write_mode():
      shutil.copy(output_triples_path, expected_triples_path)
      return

    _compare_files(test, output_triples_path, expected_triples_path)


class TestEntitiesImporter(unittest.TestCase):

  def test_without_id_column(self):
    _test_import(self, "without_id_column")

  def test_with_id_column(self):
    _test_import(self, "with_id_column")

  def test_with_entity_columns(self):
    _test_import(self, "with_entity_columns")
