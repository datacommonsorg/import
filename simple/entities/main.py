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

import os
import sys
from absl import flags
from absl import app
from absl import logging
import pandas as pd
import json

# For importing util
_CODEDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, os.path.join(_CODEDIR, "../"))

from util import dcclient as dc

FLAGS = flags.FLAGS


class Defaults:
    DATA_DIR = ".data"
    INPUT_DIR = os.path.join(DATA_DIR, "input")
    OUTPUT_DIR = os.path.join(DATA_DIR, "output")


flags.DEFINE_enum("entity_type", None, ["Country", "City"],
                  "The type of entities in the CSV.")
flags.DEFINE_string("input_dir", Defaults.INPUT_DIR, "The input directory.")
flags.DEFINE_string("output_dir", Defaults.OUTPUT_DIR, "The output directory.")


class Const:
    COLUMN_ENTITY = "entity"
    COLUMN_VARIABLE = "variable"
    COLUMN_DATE = "date"
    COLUMN_VALUE = "value"
    OBSERVATIONS_FILE_NAME = "observations.csv"
    REPORT_ITEMS = "items"
    REPORT_ITEM_TITLE = "title"
    REPORT_ITEM_DETAILS = "details"
    REPORT_FILE_NAME = "report.json"


class SimpleEntitiesImporter:

    def __init__(self, input_dir: str, output_dir: str,
                 entity_type: str) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.observations_file = os.path.join(output_dir,
                                              Const.OBSERVATIONS_FILE_NAME)
        self.report_file = os.path.join(output_dir, Const.REPORT_FILE_NAME)
        self.entity_type = entity_type
        self.df = pd.DataFrame()

    def do_import(self) -> None:
        self._init()
        self._read_csvs()
        self._rename_columns()
        self._resolve_entities()
        self._unpivot_variables()
        self._reorder_columns()
        self._write_csv()

    def _init(self):
        os.makedirs(self.output_dir, exist_ok=True)

    def _read_csvs(self) -> None:
        files = [
            os.path.join(self.input_dir, filename)
            for filename in os.listdir(self.input_dir)
        ]
        df = pd.DataFrame()
        for file in files:
            df = pd.concat([df, pd.read_csv(file)])
        logging.info("Read %s rows.", df.index.size)
        self.df = df

    def _rename_columns(self) -> None:
        df = self.df
        df.columns.values[0] = Const.COLUMN_ENTITY
        df.columns.values[1] = Const.COLUMN_DATE

    def _resolve_entities(self) -> None:
        df = self.df
        # get first (0th) column
        column = df.iloc[:, 0]
        entities = column.tolist()
        logging.info("Resolving %s entities of type %s.", len(entities),
                     self.entity_type)
        dcids = dc.resolve_entities(entities=entities,
                                    entity_type=self.entity_type)
        logging.info("Resolved %s of %s entities.", len(dcids), len(entities))
        column.replace(dcids, inplace=True)
        unresolved = set(entities).difference(set(dcids.keys()))
        if unresolved:
            unresolved_list = list(unresolved)
            logging.warning("# unresolved entities which will be dropped: %s",
                            len(unresolved_list))
            logging.warning("Dropped entities: %s", unresolved_list)
            df.drop(df[df.iloc[:, 0].isin(values=unresolved)].index,
                    inplace=True)

    def _unpivot_variables(self) -> None:
        self.df = self.df.melt(
            id_vars=[Const.COLUMN_ENTITY, Const.COLUMN_DATE],
            var_name=Const.COLUMN_VARIABLE,
            value_name=Const.COLUMN_VALUE,
        ).dropna()

    def _reorder_columns(self) -> None:
        self.df = self.df.reindex(columns=[
            Const.COLUMN_ENTITY,
            Const.COLUMN_VARIABLE,
            Const.COLUMN_DATE,
            Const.COLUMN_VALUE,
        ])

    def _write_csv(self) -> None:
        logging.info("Writing %s observations to: %s", self.df.index.size,
                     self.observations_file)
        self.df.to_csv(self.observations_file, index=False)


def main(_):
    importer = SimpleEntitiesImporter(
        input_dir=FLAGS.input_dir,
        output_dir=FLAGS.output_dir,
        entity_type=FLAGS.entity_type,
    )
    importer.do_import()


if __name__ == "__main__":
    app.run(main)
