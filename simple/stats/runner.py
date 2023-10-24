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

import json
import logging

from stats import constants
from stats.config import Config
from stats.importer import SimpleStatsImporter
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from stats.reporter import ImportReporter
from util.filehandler import create_file_handler
from util.filehandler import FileHandler


class Runner:
  """Runs and coordinates all imports.
    """

  def __init__(
      self,
      input_path: str,
      output_dir: str,
      entity_type: str = None,
      ignore_columns: list[str] = list(),
  ) -> None:
    self.input_fh = create_file_handler(input_path)
    self.output_dir_fh = create_file_handler(output_dir)
    self.process_dir_fh = self.output_dir_fh.make_file(
        f"{constants.PROCESS_DIR_NAME}/")
    self.reporter = ImportReporter(report_fh=self.process_dir_fh.make_file(
        constants.REPORT_JSON_FILE_NAME))
    self.entity_type = entity_type
    self.ignore_columns = ignore_columns

    self.config = Config(data={})
    if self.input_fh.isdir:
      config_fh = self.input_fh.make_file(constants.CONFIG_JSON_FILE_NAME)
      if not config_fh.exists():
        raise FileNotFoundError(
            "Config file must be provided for importing directories.")
      self.config = Config(data=json.loads(config_fh.read_string()))

    self.nodes = Nodes(self.config)

    self.output_dir_fh.make_dirs()
    self.process_dir_fh.make_dirs()

  def run(self):
    try:
      if not self.input_fh.isdir:
        self.reporter.report_started(import_files=[self.input_fh.basename()])
        self._run_single_import(input_file_fh=self.input_fh,
                                reporter=self.reporter.import_file(
                                    self.input_fh.basename()),
                                entity_type=self.entity_type,
                                ignore_columns=self.ignore_columns)
      else:
        input_files = sorted(self.input_fh.list_files(extension=".csv"))
        self.reporter.report_started(import_files=input_files)
        if not input_files:
          raise RuntimeError("Not input CSVs found.")
        for input_file in input_files:
          self._run_single_import(
              input_file_fh=self.input_fh.make_file(input_file),
              reporter=self.reporter.import_file(input_file),
              entity_type=self.config.entity_type(input_file),
              ignore_columns=self.config.ignore_columns(input_file))

      # Generate triples.
      self.nodes.triples(
          self.output_dir_fh.make_file(constants.TRIPLES_FILE_NAME))
      # Report done.
      self.reporter.report_done()
    except Exception as e:
      logging.exception("Error running import")
      self.reporter.report_failure(error=str(e))

  def _run_single_import(self,
                         input_file_fh: FileHandler,
                         reporter: FileImportReporter,
                         entity_type: str = None,
                         ignore_columns: list[str] = []):
    logging.info("Importing file: %s", input_file_fh)
    basename = input_file_fh.basename()
    observations_fh = self.output_dir_fh.make_file(
        f"{constants.OBSERVATIONS_FILE_NAME_PREFIX}_{basename}")
    debug_resolve_fh = self.process_dir_fh.make_file(
        f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{basename}")
    importer = SimpleStatsImporter(input_fh=input_file_fh,
                                   observations_fh=observations_fh,
                                   debug_resolve_fh=debug_resolve_fh,
                                   reporter=reporter,
                                   nodes=self.nodes,
                                   entity_type=entity_type,
                                   ignore_columns=ignore_columns)
    importer.do_import()
