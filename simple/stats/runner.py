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

from enum import StrEnum
import json
import logging

from stats import constants
from stats.config import Config
from stats.data import ImportType
from stats.db import create_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_config_from_env
from stats.db import ImportStatus
from stats.events_importer import EventsImporter
from stats.importer import Importer
import stats.nl as nl
from stats.nodes import Nodes
from stats.observations_importer import ObservationsImporter
from stats.reporter import ImportReporter
from util.filehandler import create_file_handler


class RunMode(StrEnum):
  CUSTOM_DC = "customdc"
  MAIN_DC = "maindc"


class Runner:
  """Runs and coordinates all imports.
    """

  def __init__(self,
               input_dir: str,
               output_dir: str,
               mode: RunMode = RunMode.CUSTOM_DC) -> None:
    self.mode = mode
    self.input_dir_fh = create_file_handler(input_dir, is_dir=True)
    if not self.input_dir_fh.isdir:
      raise NotADirectoryError(
          f"Input path must be a directory: {input_dir}. If it is a GCS path, ensure it ends with a '/'."
      )

    self.output_dir_fh = create_file_handler(output_dir, is_dir=True)
    self.nl_dir_fh = self.output_dir_fh.make_file(f"{constants.NL_DIR_NAME}/")
    self.process_dir_fh = self.output_dir_fh.make_file(
        f"{constants.PROCESS_DIR_NAME}/")

    self.output_dir_fh.make_dirs()
    self.nl_dir_fh.make_dirs()
    self.process_dir_fh.make_dirs()

    self.reporter = ImportReporter(report_fh=self.process_dir_fh.make_file(
        constants.REPORT_JSON_FILE_NAME))

    config_fh = self.input_dir_fh.make_file(constants.CONFIG_JSON_FILE_NAME)
    if not config_fh.exists():
      raise FileNotFoundError("Config file must be provided.")
    self.config = Config(data=json.loads(config_fh.read_string()))

    def _get_db_config() -> dict:
      if self.mode == RunMode.MAIN_DC:
        logging.info("Using Main DC config.")
        return create_main_dc_config(self.output_dir_fh.path)
      # Attempt to get from env (cloud sql, then sqlite),
      # then config file, then default.
      db_cfg = get_cloud_sql_config_from_env()
      if db_cfg:
        logging.info("Using Cloud SQL settings from env.")
        return db_cfg
      db_cfg = get_sqlite_config_from_env()
      if db_cfg:
        logging.info("Using SQLite settings from env.")
        return db_cfg
      logging.info("Using default DB settings.")
      return create_sqlite_config(
          self.output_dir_fh.make_file(constants.DB_FILE_NAME).path)

    self.db = create_db(_get_db_config())
    self.nodes = Nodes(self.config)

  def run(self):
    try:
      # Run all data imports.
      self._run_imports()

      # Generate triples.
      triples = self.nodes.triples()
      # Write triples to DB.
      self.db.insert_triples(triples)

      # Generate SV sentences.
      nl.generate_sv_sentences(
          list(self.nodes.variables.values()),
          self.nl_dir_fh.make_file(constants.SENTENCES_FILE_NAME))

      # Write import info to DB.
      self.db.insert_import_info(status=ImportStatus.SUCCESS)

      # Commit and close DB.
      self.db.commit_and_close()

      # Report done.
      self.reporter.report_done()
    except Exception as e:
      logging.exception("Error running import")
      self.reporter.report_failure(error=str(e))

  def _run_imports(self):
    input_files = sorted(self.input_dir_fh.list_files(extension=".csv"))
    self.reporter.report_started(import_files=input_files)
    if not input_files:
      raise RuntimeError("Not input CSVs found.")
    for input_file in input_files:
      self._run_single_import(input_file)

  def _run_single_import(self, input_file: str):
    logging.info("Importing file: %s", input_file)
    self._create_importer(input_file).do_import()

  def _create_importer(self, input_file: str) -> Importer:
    import_type = self.config.import_type(input_file)
    input_fh = self.input_dir_fh.make_file(input_file)
    debug_resolve_fh = self.process_dir_fh.make_file(
        f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{input_file}")
    reporter = self.reporter.import_file(input_file)

    if import_type == ImportType.OBSERVATIONS:
      return ObservationsImporter(input_fh=input_fh,
                                  db=self.db,
                                  debug_resolve_fh=debug_resolve_fh,
                                  reporter=reporter,
                                  nodes=self.nodes)

    if import_type == ImportType.EVENTS:
      return EventsImporter(input_fh=input_fh,
                            db=self.db,
                            debug_resolve_fh=debug_resolve_fh,
                            reporter=reporter,
                            nodes=self.nodes)

    raise ValueError(f"Unsupported import type: {import_type} ({input_file})")
