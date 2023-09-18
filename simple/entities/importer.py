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
from absl import logging
import pandas as pd
import constants

# For importing util
_CODEDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, os.path.join(_CODEDIR, "../"))

from util import dc_client as dc


# TODO: Add support for units.
class SimpleEntitiesImporter:

    def __init__(self, input_dir: str, output_dir: str,
                 entity_type: str) -> None:
        self.input_dir = input_dir
        self.output_dir = output_dir
        self.observations_file = os.path.join(output_dir,
                                              constants.OBSERVATIONS_FILE_NAME)
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
        df.columns.values[0] = constants.COLUMN_DCID
        df.columns.values[1] = constants.COLUMN_DATE

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
            id_vars=[constants.COLUMN_DCID, constants.COLUMN_DATE],
            var_name=constants.COLUMN_VARIABLE,
            value_name=constants.COLUMN_VALUE,
        ).dropna()

    def _reorder_columns(self) -> None:
        self.df = self.df.reindex(columns=[
            constants.COLUMN_DCID,
            constants.COLUMN_VARIABLE,
            constants.COLUMN_DATE,
            constants.COLUMN_VALUE,
        ])

    def _write_csv(self) -> None:
        logging.info("Writing %s observations to: %s", self.df.index.size,
                     self.observations_file)
        self.df.to_csv(self.observations_file, index=False)
