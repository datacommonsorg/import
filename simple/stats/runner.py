from enum import StrEnum
import json
import logging
from typing import Optional

import fs.path as fspath
from stats import constants
from stats import schema
from stats import stat_var_hierarchy_generator
from stats.config import Config
from stats.data import ImportType
from stats.data import InputFileFormat
from stats.data import McfNode
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import Triple
from stats.data import VerticalSpec
from stats.db import create_and_update_db
from stats.db import create_main_dc_config
from stats.db import create_sqlite_config
from stats.db import get_cloud_sql_config_from_env
from stats.db import get_sqlite_path_from_env
from stats.db import ImportStatus
from stats.db_cache import get_db_cache_from_env
from stats.entities_importer import EntitiesImporter
from stats.events_importer import EventsImporter
from stats.importer import Importer
from stats.mcf_importer import McfImporter
import stats.nl as nl
from stats.nodes import Nodes
from stats.observations_importer import ObservationsImporter
from stats.reporter import ImportReporter
import stats.schema_constants as sc
from stats.svg_cache import generate_svg_cache
from stats.variable_per_row_importer import VariablePerRowImporter
from util.file_match import match
from util.filesystem import create_store
from util.filesystem import Dir
from util.filesystem import File
from util.filesystem import Store


class RunMode(StrEnum):
  CUSTOM_DC = "customdc"
  SCHEMA_UPDATE = "schemaupdate"
  MAIN_DC = "maindc"
  DCP_BRIDGE = "dcpbridge"


class Runner:
  """Runs and coordinates all imports."""

  def __init__(
      self,
      config_file_path: str,
      input_dir_path: str,
      output_dir_path: str,
      mode: RunMode = RunMode.CUSTOM_DC,
  ) -> None:
    assert (config_file_path or
            input_dir_path), "One of config_file or input_dir must be specified"
    assert output_dir_path, "output_dir must be specified"

    self.mode = mode

    # File systems, both input and output. Must be closed when run finishes.
    self.all_stores: list[Store] = []
    # Input-only stores
    self.input_stores: list[Store] = []

    # "Special" file handlers.
    # i.e. if files of these types are present, they are handled in specific ways.
    self.special_files: dict[str, File] = {}
    self.svg_specialized_names: ParentSVG2ChildSpecializedNames = {}

    # Config file driven (input paths pulled from config)
    if config_file_path:
      self._read_config_from_file(config_file_path)

      input_urls = self.config.data_download_urls()
      if not input_urls and self.mode != RunMode.SCHEMA_UPDATE:
        raise ValueError("Data Download URLs not found in config.")
      for input_url in input_urls:
        input_store = create_store(input_url)
        self.all_stores.append(input_store)
        self.input_stores.append(input_store)

    # Input dir driven (config file found in input dir)
    else:
      input_store = create_store(input_dir_path)
      self.all_stores.append(input_store)
      self.input_stores.append(input_store)

      self._read_config_from_file(
          config_file_path=constants.CONFIG_JSON_FILE_NAME,
          config_file_dir=input_store.as_dir(),
      )

    # Get dict of special file type string to special file name.
    # Example entry: verticalSpecsFile -> vertical_specs.json
    self.special_file_names_by_type = self.config.special_files()

    # New option to traverse subdirs of input dir(s). Defaults to false.
    self.include_input_subdirs = self.config.include_input_subdirs()

    # Output directories
    output_store = create_store(output_dir_path, create_if_missing=True)
    if self.include_input_subdirs:
      for input_store in self.input_stores:
        _check_not_overlapping(input_store, output_store)
    self.all_stores.append(output_store)
    self.output_dir = output_store.as_dir()
    self.nl_dir = self.output_dir.open_dir(constants.NL_DIR_NAME)
    self.process_dir = self.output_dir.open_dir(constants.PROCESS_DIR_NAME)

    # Reporter.
    self.reporter = ImportReporter(
        report_file=self.process_dir.open_file(constants.REPORT_JSON_FILE_NAME))

    self.nodes = Nodes(self.config)
    self.db = None
    self.db_cache = None

  def run(self):
    try:
      if self.db is None:
        self.db = create_and_update_db(self._get_db_config())
        self.db_cache = get_db_cache_from_env()

      if self.mode == RunMode.SCHEMA_UPDATE:
        logging.info("Skipping imports because run mode is schema update.")

      elif self.mode == RunMode.CUSTOM_DC or self.mode == RunMode.MAIN_DC or self.mode == RunMode.DCP_BRIDGE:
        self._run_imports_and_do_post_import_work()

      else:
        raise ValueError(f"Unsupported mode: {self.mode}")

      # Commit and close DB.
      self.db.commit_and_close()

      # Report done.
      self.reporter.report_done()

      # Close all file storage.
      for store in self.all_stores:
        store.close()
      logging.info("File storage closed.")

    except Exception as e:
      logging.exception("Error updating stats")
      self.reporter.report_failure(error=str(e))

  def _read_config_from_file(self,
                             config_file_path: str,
                             config_file_dir: Optional[Dir] = None) -> Config:
    try:
      if config_file_dir:
        raw_config = config_file_dir.open_file(config_file_path,
                                               create_if_missing=False).read()
      else:
        with create_store(config_file_path) as config_store:
          raw_config = config_store.as_file().read()
    except FileNotFoundError:
      if self.mode == RunMode.SCHEMA_UPDATE:
        logging.warning("Config file not found. Defaulting to empty config.")
        raw_config = None
      else:
        raise

    config_data = json.loads(raw_config) if raw_config else {}
    self.config = Config(data=config_data)

  def _get_db_config(self) -> dict:
    if self.mode == RunMode.MAIN_DC:
      logging.info("Using Main DC config.")
      return create_main_dc_config(self.output_dir.path)
    # Attempt to get from env (cloud sql, then sqlite),
    # then config file, then default.
    db_cfg = get_cloud_sql_config_from_env()
    if db_cfg:
      logging.info("Using Cloud SQL settings from env.")
      return db_cfg
    sqlite_path_from_env = get_sqlite_path_from_env()
    if sqlite_path_from_env:
      logging.info("Using SQLite settings from env.")
      sqlite_env_store = create_store(sqlite_path_from_env,
                                      create_if_missing=True,
                                      treat_as_file=True)
      self.all_stores.append(sqlite_env_store)
      sqlite_file = sqlite_env_store.as_file()
    else:
      logging.info("Using default SQLite settings.")
      sqlite_file = self.output_dir.open_file(constants.DB_FILE_NAME)
    return create_sqlite_config(sqlite_file)

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

    if self.mode == RunMode.DCP_BRIDGE:
      self._export_data_to_files_for_dcp()

    # Write import info to DB.
    self.db.insert_import_info(status=ImportStatus.SUCCESS)

    # Flush the DB cache if it exists.
    if self.db_cache:
      logging.info("Database cache is configured. Clearing cache.")
      self.db_cache.clear()

  def _generate_nl_artifacts(self):
    triples: list[Triple] = []
    topic_triples = self.db.select_triples_by_subject_type(sc.TYPE_TOPIC)
    sv_triples = self.db.select_triples_by_subject_type(
        sc.TYPE_STATISTICAL_VARIABLE)
    triples = topic_triples + sv_triples

    # Generate sentences.
    nl.generate_nl_sentences(triples, self.nl_dir)

    # If generating topics, fetch svpg triples as well and generate topic cache
    if topic_triples:
      sv_peer_group_triples = self.db.select_triples_by_subject_type(
          sc.TYPE_STAT_VAR_PEER_GROUP)
      topic_cache_triples = topic_triples + sv_peer_group_triples
      nl.generate_topic_cache(topic_cache_triples, self.nl_dir)

  def _export_data_to_files_for_dcp(self):
    logging.info("Exporting data to files for DCP Bridge.")
    import pandas as pd
    
    # 1. Export observations to MCF
    obs_tuples = self.db.engine.fetch_all("select * from observations")
    if obs_tuples:
      columns = ["entity", "variable", "date", "value", "provenance", "unit", "scaling_factor", "measurement_method", "observation_period", "properties"]
      df = pd.DataFrame(obs_tuples, columns=columns)
      
      import hashlib
      
      mcf_nodes = []
      for i, row in df.iterrows():
        node_lines = []
        node_lines.append(f"Node: E:obs->{i}")
        node_lines.append("typeOf: dcid:StatVarObservation")
        node_lines.append(f"observationAbout: dcid:{row['entity']}")
        node_lines.append(f"variableMeasured: dcid:{row['variable']}")
        node_lines.append(f"observationDate: \"{row['date']}\"")
        node_lines.append(f"value: {row['value']}")
        
        # Generate a unique dcid for the observation
        content_str = f"{row['entity']}_{row['variable']}_{row['date']}_{row['value']}"
        obs_hash = hashlib.md5(content_str.encode()).hexdigest()
        node_lines.append(f"dcid: \"dc/o/{obs_hash}\"")
        
        if pd.notna(row['unit']) and row['unit']:
          node_lines.append(f"unit: dcid:{row['unit']}")
        if pd.notna(row['measurement_method']) and row['measurement_method']:
          node_lines.append(f"measurementMethod: dcid:{row['measurement_method']}")
        if pd.notna(row['observation_period']) and row['observation_period']:
          node_lines.append(f"observationPeriod: \"{row['observation_period']}\"")
        if pd.notna(row['scaling_factor']) and row['scaling_factor']:
          node_lines.append(f"scalingFactor: {row['scaling_factor']}")
          
        mcf_nodes.append("\n".join(node_lines))
        
      mcf = "\n\n".join(mcf_nodes)
      obs_file = self.output_dir.open_file("observations.mcf")
      obs_file.write(mcf)
      logging.info("Exported %s observations to %s", len(df), obs_file.full_path())

    # 2. Export triples to MCF
    triples_tuples = self.db.engine.fetch_all("select * from triples")
    if triples_tuples:
      nodes = {}
      for tuple in triples_tuples:
        subject_id, predicate, object_id, object_value = tuple
        node = nodes.get(subject_id)
        if not node:
          node = McfNode(subject_id)
          nodes[subject_id] = node
        
        if object_id:
          node.add_triple(Triple(subject_id, predicate, object_id=object_id))
        else:
          node.add_triple(Triple(subject_id, predicate, object_value=object_value))
      
      mcf = "\n\n".join(map(lambda node: node.to_mcf(), nodes.values()))
      mcf_file = self.output_dir.open_file("schema.mcf")
      mcf_file.write(mcf)
      logging.info("Exported %s nodes to %s", len(nodes), mcf_file.full_path())

    # 3. Upload to GCS and Trigger Workflow
    self._upload_and_trigger_dcp_workflow()

  def _upload_and_trigger_dcp_workflow(self):
    import os
    import subprocess
    import json
    from datetime import datetime
    
    bucket = os.getenv("DCP_INGESTION_BUCKET")
    spanner_instance = os.getenv("SPANNER_INSTANCE_ID")
    spanner_database = os.getenv("SPANNER_DATABASE_ID")
    region = os.getenv("GCP_REGION", "us-central1")
    project = os.getenv("GCP_PROJECT")
    workflow_name = os.getenv("WORKFLOW_NAME")
    
    if not all([bucket, spanner_instance, spanner_database, project, workflow_name]):
      logging.error("Missing required environment variables for DCP Bridge. Skipping upload and trigger.")
      logging.error("Required: DCP_INGESTION_BUCKET, SPANNER_INSTANCE_ID, SPANNER_DATABASE_ID, GCP_PROJECT, WORKFLOW_NAME")
      return
      
    logging.info("Uploading artifacts to GCS bucket: %s", bucket)
    
    import_gcs_dir = f"gs://{bucket}/imports/dcpbridge_testing/"
    files_uploaded = False
    
    # Upload observations if file exists
    obs_file = self.output_dir.open_file("observations.mcf")
    obs_local_path = obs_file.syspath() if hasattr(obs_file, 'syspath') else obs_file.path
    if obs_local_path and os.path.exists(obs_local_path):
      obs_gcs_path = f"{import_gcs_dir}observations.mcf"
      logging.info("Uploading %s to %s", obs_local_path, obs_gcs_path)
      subprocess.run(["gcloud", "storage", "cp", obs_local_path, obs_gcs_path], check=True)
      files_uploaded = True
    
    # Upload schema if file exists
    mcf_file = self.output_dir.open_file("schema.mcf")
    mcf_local_path = mcf_file.syspath() if hasattr(mcf_file, 'syspath') else mcf_file.path
    if mcf_local_path and os.path.exists(mcf_local_path):
      schema_gcs_path = f"{import_gcs_dir}schema.mcf"
      logging.info("Uploading %s to %s", mcf_local_path, schema_gcs_path)
      subprocess.run(["gcloud", "storage", "cp", mcf_local_path, schema_gcs_path], check=True)
      files_uploaded = True
      
    # Upload NL artifacts
    nl_local_path = self.nl_dir.syspath() if hasattr(self.nl_dir, 'syspath') else self.nl_dir.path
    if nl_local_path and os.path.exists(nl_local_path):
      nl_gcs_path = f"gs://{bucket}/nl/"
      logging.info("Uploading NL artifacts from %s to %s", nl_local_path, nl_gcs_path)
      subprocess.run(["gcloud", "storage", "cp", "-r", nl_local_path, nl_gcs_path], check=True)
      
    if not files_uploaded:
      logging.warning("No artifacts to import. Skipping workflow trigger.")
      return
      
    # Construct import list with single directory entry
    prov_names = list(self.config.provenances.keys())
    logical_import_name = prov_names[0] if prov_names else "DCP_Bridge_Import"
    
    import_list = [{"importName": logical_import_name, "graphPath": import_gcs_dir}]
      
    # Construct payload
    import_name = f"dcp_bridge_{datetime.now().strftime('%Y%m%d%H%M%S')}"
    
    payload = {
      "spannerInstanceId": spanner_instance,
      "spannerDatabaseId": spanner_database,
      "importName": import_name,
      "importList": json.dumps(import_list),
      "tempLocation": f"gs://{bucket}/temp",
      "region": region
    }
    
    payload_str = json.dumps(payload)
    
    # Trigger workflow
    logging.info("Triggering Cloud Workflow: %s", workflow_name)
    cmd = [
      "gcloud", "workflows", "run", workflow_name,
      f"--project={project}",
      f"--location={region}",
      f"--data={payload_str}"
    ]
    
    logging.info("Running command: %s", " ".join(cmd))
    result = subprocess.run(cmd, capture_output=True, text=True)
    if result.returncode == 0:
      logging.info("Successfully triggered workflow. Output: %s", result.stdout)
    else:
      logging.error("Failed to trigger workflow. Error: %s", result.stderr)

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
    vertical_specs_file = self.special_files.get(
        constants.VERTICAL_SPECS_FILE_TYPE)
    if vertical_specs_file:
      logging.info("Loading vertical specs from: %s",
                   vertical_specs_file.name())
      vertical_specs = stat_var_hierarchy_generator.load_vertical_specs(
          vertical_specs_file.read())

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

  def _check_if_special_file(self, file: File) -> bool:
    for file_type in self.special_file_names_by_type.keys():
      if file_type in self.special_files:
        # Already found this special file.
        continue
      file_name = self.special_file_names_by_type[file_type]
      if match(file, file_name):
        self.special_files[file_type] = file
        return True
    return False

  def _run_all_data_imports(self):
    input_files: list[File] = []
    input_csv_files: list[File] = []
    input_mcf_files: list[File] = []

    for input_store in self.input_stores:
      if input_store.isdir():
        input_files.extend(input_store.as_dir().all_files(
            self.include_input_subdirs))
      else:
        input_files.append(input_store.as_file())

    for input_file in input_files:
      if self._check_if_special_file(input_file):
        continue
      if match(input_file, "*.csv"):
        input_csv_files.append(input_file)
      if match(input_file, "*.mcf"):
        input_mcf_files.append(input_file)

    # Sort input files alphabetically.
    input_csv_files.sort(key=lambda f: f.full_path())
    input_mcf_files.sort(key=lambda f: f.full_path())

    logging.info(f"Found {len(input_csv_files)} csv files to import")
    logging.info(f"Found {len(input_mcf_files)} mcf files to import")

    self.reporter.report_started(import_files=list(input_csv_files +
                                                   input_mcf_files))
    for input_csv_file in input_csv_files:
      self._run_single_import(input_csv_file)
    for input_mcf_file in input_mcf_files:
      self._run_single_mcf_import(input_mcf_file)

  def _run_single_import(self, input_file: File):
    logging.info("Importing file: %s", input_file)
    self._create_importer(input_file).do_import()

  def _run_single_mcf_import(self, input_mcf_file: File):
    logging.info("Importing MCF file: %s", input_mcf_file)
    self._create_mcf_importer(input_mcf_file, self.output_dir,
                              self.mode == RunMode.MAIN_DC).do_import()

  def _create_mcf_importer(self, input_file: File, output_dir: Dir,
                           is_main_dc: bool) -> Importer:
    # Right now, this overwrites any file with the same name,
    # so if different input sources have files with the same relative path,
    # they will clobber each others output. Treating this as an edge case
    # for now since it only affects the main DC case, but we could resolve
    # it in the future by allowing input sources to be mapped to output
    # locations.
    output_file = output_dir.open_file(input_file.path)
    reporter = self.reporter.get_file_reporter(input_file)
    return McfImporter(
        input_file=input_file,
        output_file=output_file,
        db=self.db,
        reporter=reporter,
        is_main_dc=is_main_dc,
    )

  def _create_importer(self, input_file: File) -> Importer:
    import_type = self.config.import_type(input_file)
    sanitized_path = input_file.full_path().replace("://",
                                                    "_").replace("/", "_")
    debug_resolve_file = self.process_dir.open_file(
        f"{constants.DEBUG_RESOLVE_FILE_NAME_PREFIX}_{sanitized_path}")
    reporter = self.reporter.get_file_reporter(input_file)

    if import_type == ImportType.OBSERVATIONS:
      input_file_format = self.config.format(input_file)
      if input_file_format == InputFileFormat.VARIABLE_PER_ROW:
        return VariablePerRowImporter(
            input_file=input_file,
            db=self.db,
            reporter=reporter,
            nodes=self.nodes,
            mode=self.mode,
        )
      return ObservationsImporter(
          input_file=input_file,
          db=self.db,
          debug_resolve_file=debug_resolve_file,
          reporter=reporter,
          nodes=self.nodes,
      )

    if import_type == ImportType.EVENTS:
      return EventsImporter(
          input_file=input_file,
          db=self.db,
          debug_resolve_file=debug_resolve_file,
          reporter=reporter,
          nodes=self.nodes,
      )

    if import_type == ImportType.ENTITIES:
      return EntitiesImporter(input_file=input_file,
                              db=self.db,
                              reporter=reporter,
                              nodes=self.nodes)

    raise ValueError(
        f"Unsupported import type: {import_type} ({input_file.full_path()})")


def _check_not_overlapping(input_store: Store, output_store: Store):
  input_path = input_store.full_path()
  output_path = output_store.full_path()
  if (fspath.issamedir(input_path, output_path) or
      fspath.isparent(input_path, output_path) or
      fspath.isparent(output_path, input_path)):
    raise ValueError(
        f"Input path (${input_path}) overlaps with output dir ({output_path})")
