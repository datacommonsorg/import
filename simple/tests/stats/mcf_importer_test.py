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

import os
import shutil
import tempfile
import unittest

from stats.db import create_and_update_db
from stats.db import create_sqlite_config
from stats.mcf_importer import McfImporter
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import write_triples
from util.filesystem import create_store

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "mcf_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _test_import(test: unittest.TestCase,
                 test_name: str,
                 is_main_dc: bool,
                 raises_error: bool = False):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_store = create_store(_INPUT_DIR)
    temp_store = create_store(temp_dir)

    input_file_name = f"{test_name}.mcf"
    input_file = input_store.as_dir().open_file(input_file_name,
                                                create_if_missing=False)

    db_file_name = f"{test_name}.db"
    db_path = os.path.join(temp_dir, db_file_name)
    db_file = temp_store.as_dir().open_file(db_file_name)

    output_mcf_path = os.path.join(temp_dir, f"{test_name}.mcf")
    output_triples_path = os.path.join(temp_dir, f"{test_name}.triples.db.csv")
    expected_mcf_path = os.path.join(_EXPECTED_DIR, f"{test_name}.mcf")
    expected_triples_path = os.path.join(_EXPECTED_DIR,
                                         f"{test_name}.triples.db.csv")

    db = create_and_update_db(create_sqlite_config(db_file))
    report_file = temp_store.as_dir().open_file("report.json")
    reporter = FileImportReporter(input_file.full_path(),
                                  ImportReporter(report_file))

    output_store = create_store(output_mcf_path,
                                create_if_missing=True,
                                treat_as_file=True)
    output_file = output_store.as_file()
    importer = McfImporter(input_file=input_file,
                           output_file=output_file,
                           db=db,
                           reporter=reporter,
                           is_main_dc=is_main_dc)

    if raises_error:
      with test.assertRaises(ValueError):
        importer.do_import()
      return

    importer.do_import()

    db.commit_and_close()
    write_triples(db_path, output_triples_path)

    if not is_main_dc:
      if is_write_mode():
        shutil.copy(output_triples_path, expected_triples_path)
        return

      compare_files(test, output_triples_path, expected_triples_path)
    else:
      if is_write_mode():
        shutil.copy(output_mcf_path, expected_mcf_path)
        return

      compare_files(test, output_mcf_path, expected_mcf_path)

    input_store.close()
    output_store.close()
    temp_store.close()


class TestMcfImporter(unittest.TestCase):

  def test_basic_mcf(self):
    _test_import(self, "basic_mcf", is_main_dc=False)
    _test_import(self, "basic_mcf_main_dc", is_main_dc=True)
    _test_import(self, "invalid_mcf", is_main_dc=False, raises_error=True)
