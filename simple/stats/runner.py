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
from stats import schema
from stats import stat_var_hierarchy_generator
from stats.cache import generate_svg_cache
from stats.config import Config
from stats.data import ImportType
from stats.data import InputFileFormat
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import Triple
from stats.data import VerticalSpec
from stats.db import create_and_update_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_config_from_env
from stats.db import ImportStatus
from stats.entities_importer import EntitiesImporter
from stats.events_importer import EventsImporter
from stats.importer import Importer
from stats.mcf_importer import McfImporter
import stats.nl as nl
from stats.nodes import Nodes
from stats.observations_importer import ObservationsImporter
from stats.reporter import ImportReporter
import stats.schema_constants as sc
from stats.variable_per_row_importer import VariablePerRowImporter
from util.filehandler import create_file_handler
from util.filehandler import FileHandler


class RunMode(StrEnum):
  CUSTOM_DC = "customdc"
  SCHEMA_UPDATE = "schemaupdate"
  MAIN_DC = "maindc"


class Runner:
  """Runs and coordinates all imports.
    """

  def __init__(self,
               config_file: str,
               input_dir: str,
               output_dir: str,
               mode: RunMode = RunMode.CUSTOM_DC) -> None:
    assert config_file or input_dir, "One of config_file or input_dir must be specified"
    assert output_dir, "output_dir must be specified"

    self.mode = mode
    self.input_handlers: list[FileHandler] = []
    # "Special" file handlers.
    # i.e. if files of these types are present, they are handled in specific ways.
    self.special_handlers: dict[str, FileHandler] = {}
    self.svg_specialized_names: ParentSVG2ChildSpecializedNames = {}

    # Config file driven.
    if config_file:
      config_fh = create_file_handler(config_file, is_dir=False)
      if not config_fh.exists():
        raise FileNotFoundError("Config file must be provided.")
      self.config = Config(data=json.loads(config_fh.read_string()))

      input_urls = self.config.data_download_urls()
      if not input_urls:
        raise ValueError("Data Download URLs not found in config.")
      for input_url in input_urls:
        self.input_handlers.append(create_file_handler(input_url, is_dir=True))

    #Input dir driven.
    else:
      input_dir_fh = create_file_handler(input_dir, is_dir=True)
      if not input_dir_fh.isdir:
        raise NotADirectoryError(
            f"Input path must be a directory: {input_dir}. If it is a GCS path, ensure it ends with a '/'."
        )
      self.input_handlers.append(input_dir_fh)

      config_fh = input_dir_fh.make_file(constants.CONFIG_JSON_FILE_NAME)
      if not config_fh.exists():
        raise FileNotFoundError("Config file must be provided.")
      self.config = Config(data=json.loads(config_fh.read_string()))

    self.special_file_to_type = self.config.special_files()

    # Output directories
    self.output_dir_fh = create_file_handler(output_dir, is_dir=True)
    self.nl_dir_fh = self.output_dir_fh.make_file(f"{constants.NL_DIR_NAME}/")
    self.process_dir_fh = self.output_dir_fh.make_file(
        f"{constants.PROCESS_DIR_NAME}/")

    self.output_dir_fh.make_dirs()
    self.nl_dir_fh.make_dirs()
    self.process_dir_fh.make_dirs()

    # Reporter.
    self.reporter = ImportReporter(report_fh=self.process_dir_fh.make_file(
        constants.REPORT_JSON_FILE_NAME))

    self.nodes = Nodes(self.config)
    self.db = None

  def run(self):
    try:
      if (self.db is None):
        self.db = create_and_update_db(self._get_db_config())

      if self.mode == RunMode.SCHEMA_UPDATE:
        logging.info("Skipping imports because run mode is schema update.")

      elif self.mode == RunMode.CUSTOM_DC or self.mode == RunMode.MAIN_DC:
        self._run_imports_and_do_post_import_work()

      else:
        raise ValueError(f"Unsupported mode: {self.mode}")

      # Commit and close DB.
      self.db.commit_and_close()

      # Report done.
      self.reporter.report_done()
    except Exception as e:
      logging.exception("Error updating stats")
      self.reporter.report_failure(error=str(e))

  def _get_db_config(self) -> dict:
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

  def _run_imports_and_do_post_import_work(self):
    # (SQL only) Drop data in existing tables (except import metadata).
    # Also drop indexes for faster writes.
    self.db.maybe_clear_before_import()

    # Import data from all input files.
    self._run_all_data_imports()

    # Generate triples.
    triples = self.nodes.triples()
    # Write triples to DB.
    self.db.insert_triples(triples)

    # Generate SVG hierarchy.
    self._generate_svg_hierarchy()

    # Generate SVG cache.
    self._generate_svg_cache()

    # Generate NL artifacts (sentences, embeddings, topic cache).
    self._generate_nl_artifacts()

    # Write import info to DB.
    self.db.insert_import_info(status=ImportStatus.SUCCESS)

  def _generate_nl_artifacts(self):
    triples: list[Triple] = []
    # Get topic triples if generating topics else get SV triples.
    generate_topics = self.config.generate_topics()
    if generate_topics:
      triples = self.db.select_triples_by_subject_type(sc.TYPE_TOPIC)
    else:
      triples = self.db.select_triples_by_subject_type(
          sc.TYPE_STATISTICAL_VARIABLE)

    # Generate sentences.
    nl.generate_nl_sentences(triples, self.nl_dir_fh)

    # If generating topics, fetch svpg triples as well and generate topic cache
    if generate_topics:
      triples = triples + self.db.select_triples_by_subject_type(
          sc.TYPE_STAT_VAR_PEER_GROUP)
      nl.generate_topic_cache(triples, self.nl_dir_fh)

  def _generate_svg_hierarchy(self):
    if self.mode == RunMode.MAIN_DC:
      logging.info("Hierarchy generation not supported for main dc, skipping.")
      return
    if not self.config.generate_hierarchy():
      logging.info("Hierarchy generation not enabled, skipping.")
      return

    logging.info("Generating SVG hierarchy.")
    sv_triples = self.db.select_triples_by_subject_type(
        sc.TYPE_STATISTICAL_VARIABLE)
    if not sv_triples:
      logging.info("No SV triples found, skipping SVG generating hierarchy.")
    logging.info("Generating SVG hierarchy for %s SV triples.", len(sv_triples))

    vertical_specs: list[VerticalSpec] = []
    vertical_specs_fh = self.special_handlers.get(
        constants.VERTICAL_SPECS_FILE_TYPE)
    if vertical_specs_fh:
      logging.info("Loading vertical specs from: %s",
                   vertical_specs_fh.basename())
      vertical_specs = stat_var_hierarchy_generator.load_vertical_specs(
          vertical_specs_fh.read_string())

    # Collect all dcids that can be used to generate SVG names and get their schema names.
    schema_dcids = list(
        self._triples_dcids(sv_triples) |
        self._vertical_specs_dcids(vertical_specs))
    dcid2name = schema.get_schema_names(schema_dcids, self.db)

    sv_hierarchy_result = stat_var_hierarchy_generator.generate(
        triples=sv_triples, vertical_specs=vertical_specs, dcid2name=dcid2name)
    self.svg_specialized_names = sv_hierarchy_result.svg_specialized_names
    logging.info("Inserting %s SVG triples into DB.",
                 len(sv_hierarchy_result.svg_triples))
    self.db.insert_triples(sv_hierarchy_result.svg_triples)

  # Returns all unique predicates and object ids from the specified triples.
  def _triples_dcids(self, triples: list[Triple]) -> set[str]:
    dcids: set[str] = set()
    for triple in triples:
      if triple.predicate and triple.object_id:
        dcids.add(triple.predicate)
        dcids.add(triple.object_id)
    return dcids

  # Returns all unique pop types and verticals from the specified vertical specs.
  def _vertical_specs_dcids(self,
                            vertical_specs: list[VerticalSpec]) -> set[str]:
    dcids: set[str] = set()
    for vertical_spec in vertical_specs:
      if vertical_spec.population_type:
        dcids.add(vertical_spec.population_type)
      dcids.update(vertical_spec.verticals)
    return dcids

  def _generate_svg_cache(self):
    generate_svg_cache(self.db, self.svg_specialized_names)

  # If the fh is a "special" file, append it to the self.special_handlers dict.
  # Returns true if it is, otherwise false.
  def _maybe_set_special_fh(self, fh: FileHandler) -> bool:
    file_name = fh.basename()
    file_type = self.special_file_to_type.get(file_name)
    if file_type:
      self.special_handlers[file_type] = fh
      return True
    return False

  def _run_all_data_imports(self):
    input_fhs: list[FileHandler] = []
    input_mcf_fhs: list[FileHandler] = []
    for input_handler in self.input_handlers:
      if not input_handler.isdir:
        if self._maybe_set_special_fh(input_handler):
          continue
        input_file_name = input_handler.basename()
        if input_file_name.endswith(".mcf"):
          input_mcf_fhs.append(input_handler)
        else:
          input_fhs.append(input_handler)
      else:
        for input_file in sorted(input_handler.list_files(extension=".csv")):
          fh = input_handler.make_file(input_file)
          if not self._maybe_set_special_fh(fh):
            input_fhs.append(fh)
        for input_file in sorted(input_handler.list_files(extension=".mcf")):
          fh = input_handler.make_file(input_file)
          if not self._maybe_set_special_fh(fh):
            input_mcf_fhs.append(fh)
        for input_file in sorted(input_handler.list_files(extension=".json")):
          fh = input_handler.make_file(input_file)
          self._maybe_set_special_fh(fh)

      self.reporter.report_started(import_files=list(
          map(lambda fh: fh.basename(), input_fhs + input_mcf_fhs)))
      for input_fh in input_fhs:
        self._run_single_import(input_fh)
      for input_mcf_fh in input_mcf_fhs:
        self._run_single_mcf_import(input_mcf_fh)

  def _run_single_import(self, input_fh: FileHandler):
    logging.info("Importing file: %s", input_fh.basename())
    self._create_importer(input_fh).do_import()

  def _run_single_mcf_import(self, input_mcf_fh: FileHandler):
    logging.info("Importing MCF file: %s", input_mcf_fh.basename())
    self._create_mcf_importer(input_mcf_fh, self.output_dir_fh,
                              self.mode == RunMode.MAIN_DC).do_import()

  def _create_mcf_importer(self, input_fh: FileHandler,
                           output_dir_fh: FileHandler,
                           is_main_dc: bool) -> Importer:
    mcf_file_name = input_fh.basename()
    output_fh = output_dir_fh.make_file(mcf_file_name)
    reporter = self.reporter.import_file(mcf_file_name)
    return McfImporter(input_fh=input_fh,
                       output_fh=output_fh,
                       db=self.db,
                       reporter=reporter,
                       is_main_dc=is_main_dc)

  def _create_importer(self, input_fh: FileHandler) -> Importer:
    input_file = input_fh.basename()
    import_type = self.config.import_type(input_file)
    debug_resolve_fh = self.process_dir_fh.make_file(
        f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{input_file}")
    reporter = self.reporter.import_file(input_file)

    if import_type == ImportType.OBSERVATIONS:
      input_file_format = self.config.format(input_file)
      if input_file_format == InputFileFormat.VARIABLE_PER_ROW:
        return VariablePerRowImporter(input_fh=input_fh,
                                      db=self.db,
                                      reporter=reporter,
                                      nodes=self.nodes)
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

    if import_type == ImportType.ENTITIES:
      return EntitiesImporter(input_fh=input_fh,
                              db=self.db,
                              reporter=reporter,
                              nodes=self.nodes)

    raise ValueError(f"Unsupported import type: {import_type} ({input_file})")
