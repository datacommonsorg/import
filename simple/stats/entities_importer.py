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

import pandas as pd
from stats import constants
from stats.data import RowEntity
from stats.data import Triple
from stats.db import Db
from stats.importer import Importer
from stats.nodes import Nodes
from stats.reporter import FileImportReporter
from util.filesystem import File


class EntitiesImporter(Importer):
  """Imports a single entities input file.

  Key behaviors at this time:
  - All un-ignored columns will be encoded as property triples.
  - If an id column was configured, it will be used as the entity dcid. Else a new dcid will be generated for each entity.
  - Columns specified as entity columns will be encoded as object_id in the triples tables. Others will be encoded as object_value.
    + Currently this importer does not resolve any entities and all entities are assumed to be pre-resolved into dcids.
    """

  def __init__(self, input_file: File, db: Db, reporter: FileImportReporter,
               nodes: Nodes) -> None:
    self.input_file = input_file
    self.db = db
    self.reporter = reporter
    self.nodes = nodes
    self.config = nodes.config
    self.ignore_columns = self.config.ignore_columns(self.input_file)
    self.provenance = self.nodes.provenance(self.input_file).id

    self.row_entity_type = self.config.row_entity_type(self.input_file)
    assert self.row_entity_type, f"Row entity type must be specified: {self.input_file.full_path()}"

    self.id_column = self.config.id_column(self.input_file)
    # Reassigned when renaming columns.
    self.entity_columns = set(self.config.entity_columns(self.input_file))

    self.df = pd.DataFrame()

  def do_import(self) -> None:
    self.reporter.report_started()
    try:
      self._read_csv()
      self._drop_ignored_columns()
      self._sanitize_values()
      self._rename_columns()
      self._write_row_entity_triples()
      self.reporter.report_success()
    except Exception as e:
      self.reporter.report_failure(str(e))
      raise e

  def _read_csv(self) -> None:
    # Read CSVs with the following behaviors:
    # - Strip leading whitespaces
    # - Treat comma as a thousands separator
    self.df = pd.read_csv(self.input_file.read_string_io(),
                          skipinitialspace=True,
                          thousands=",")
    logging.info("Read %s rows.", self.df.index.size)
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()

  def _drop_ignored_columns(self):
    if self.ignore_columns:
      self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

  def _sanitize_values(self):
    # Convert to best possible dtypes (i.e. keep ints as ints even when some values are NaN)
    self.df = self.df.convert_dtypes()

  def _rename_columns(self) -> None:
    renamed = {}

    # Rename property columns to their IDs
    property_column_names = self.df.columns
    property_ids = [
        self.nodes.property(property_column_name).dcid
        for property_column_name in property_column_names
    ]

    for col, id in zip(property_column_names, property_ids):
      renamed[col] = id
      # If column is an entity reference, use the ID.
      if col in self.entity_columns:
        self.entity_columns.remove(col)
        self.entity_columns.add(id)

    self.df = self.df.rename(columns=renamed)

  def _write_row_entity_triples(self) -> None:
    # Add event type node - it will be written to DB later.
    # This is to avoid duplicate entity types in scenarios where entities of the same type
    # are spread across files.
    self.nodes.entity_type(self.row_entity_type, self.input_file)

    # All property columns would've been renamed to their dcids by now.
    # So use the id column's dcid as the id column name.
    id_column_name = self.nodes.property(
        self.id_column).dcid if self.id_column else ""

    triples: list[Triple] = []
    for index, row in self.df.iterrows():
      # If id column is configured, use it as the entity dcid else generate based on row index.
      dcid = row[
          id_column_name] if id_column_name else f"{self.row_entity_type}_{index}"

      prop_object_values: dict[str, str] = {}
      prop_object_ids: dict[str, str] = {}

      for i, (k, v) in enumerate(row.items()):
        if pd.isna(v):
          continue
        if k in self.entity_columns:
          if "," in v:
            ids = list(map(lambda x: x.strip(), v.split(",")))
            prop_object_ids[k] = ids
          else:
            prop_object_ids[k] = v
        else:
          prop_object_values[k] = v

      row_entity = RowEntity(dcid,
                             self.row_entity_type,
                             provenance_id=self.provenance,
                             prop_object_values=prop_object_values,
                             prop_object_ids=prop_object_ids)
      triples.extend(row_entity.triples())

    self.db.insert_triples(triples)
