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

from stats.mcf_importer import McfImporter
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from tests.stats.test_util import compare_files
from tests.stats.test_util import is_write_mode
from tests.stats.test_util import RecordingGraphWriter
from tests.stats.test_util import write_triples_list
from util.filesystem import create_store

_TEST_DATA_DIR = os.path.join(os.path.dirname(os.path.abspath(__file__)),
                              "test_data", "mcf_importer")
_INPUT_DIR = os.path.join(_TEST_DATA_DIR, "input")
_EXPECTED_DIR = os.path.join(_TEST_DATA_DIR, "expected")


def _test_import(test: unittest.TestCase,
                 test_name: str,
                 raises_error: bool = False):
  test.maxDiff = None

  with tempfile.TemporaryDirectory() as temp_dir:
    input_store = create_store(_INPUT_DIR)
    temp_store = create_store(temp_dir)

    input_file_name = f"{test_name}.mcf"
    input_file = input_store.as_dir().open_file(input_file_name,
                                                create_if_missing=False)

    output_triples_path = os.path.join(temp_dir, f"{test_name}.triples.csv")
    expected_triples_path = os.path.join(_EXPECTED_DIR,
                                         f"{test_name}.triples.csv")

    graph_writer = RecordingGraphWriter()
    report_file = temp_store.as_dir().open_file("report.json")
    reporter = FileImportReporter(input_file.full_path(),
                                  ImportReporter(report_file))

    importer = McfImporter(input_file=input_file,
                           graph_writer=graph_writer,
                           reporter=reporter)

    if raises_error:
      with test.assertRaises(ValueError):
        importer.do_import()
      return

    importer.do_import()

    graph_writer.commit_and_close()
    write_triples_list(graph_writer.triples, output_triples_path)

    if is_write_mode():
      shutil.copy(output_triples_path, expected_triples_path)
      return

    compare_files(test, output_triples_path, expected_triples_path)

    input_store.close()
    temp_store.close()


class TestMcfImporter(unittest.TestCase):

  def test_basic_mcf(self):
    _test_import(self, "basic_mcf")

  def test_invalid_mcf(self):
    _test_import(self, "invalid_mcf", raises_error=True)

  def test_provenance_source(self):
    _test_import(self, "provenance_source")
