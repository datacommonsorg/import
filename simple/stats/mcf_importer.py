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

import logging

from kg_util.mcf_parser import mcf_to_triples
import pandas as pd
from stats import constants
from stats.data import RowEntity
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filehandler import FileHandler

_ID = 'ID'


class McfImporter(Importer):
  """Imports a MCF file.

  For main DC, the file is simply copied to the output directory.
  For custom DC, the MCF nodes are inserted as triples in the DB.
    """

  def __init__(self, input_fh: FileHandler, output_fh: FileHandler, db: Db,
               reporter: FileImportReporter, is_main_dc: bool) -> None:
    self.input_fh = input_fh
    self.output_fh = output_fh
    self.db = db
    self.reporter = reporter
    self.input_file_name = self.input_fh.basename()
    self.is_main_dc = is_main_dc

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      # For main DC, simply copy the file over.
      if self.is_main_dc:
        self.output_fh.write_string(self.input_fh.read_string())
      else:
        triples = list(
            map(lambda x: _to_triple(x),
                mcf_to_triples(self.input_fh.read_string_io())))
        logging.info("Inserting %s triples from %s", len(triples),
                     self.input_file_name)
        self.db.insert_triples(triples)
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e


def _to_triple(parser_triple: list[str]) -> Triple:
  [subject_id, predicate, value, value_type] = parser_triple
  if value_type == _ID:
    return Triple(subject_id, predicate, object_id=value)
  else:
    return Triple(subject_id, predicate, object_value=value)
