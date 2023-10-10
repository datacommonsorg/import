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
from util.filehandler import FileHandler


# TODO: Add support for units.
class SimpleStatsImporter:
    """Imports a single input file.
    """

    def __init__(
            self,
            input_fh: FileHandler,
            observations_fh: FileHandler,
            debug_resolve_fh: FileHandler,
            entity_type: str,
            ignore_columns: list[str] = list(),
    ) -> None:
        self.input_fh = input_fh
        self.observations_fh = observations_fh
        self.debug_resolve_fh = debug_resolve_fh
        self.entity_type = entity_type
        self.ignore_columns = ignore_columns
        self.df = pd.DataFrame()
        self.debug_resolve_df = None

    def do_import(self) -> None:
        self._read_csv()
        self._drop_ignored_columns()
        self._trim_values()
        self._resolve_entities()
        self._rename_columns()

        self._write_csvs()

    def _read_csv(self) -> None:
        self.df = pd.read_csv(self.input_fh.read_string_io(), dtype="str")
        logging.info("Read %s rows.", self.df.index.size)

    def _drop_ignored_columns(self):
        if self.ignore_columns:
            self.df.drop(columns=self.ignore_columns, axis=1, inplace=True)

    def _trim_values(self):
        self.df = self.df.map(lambda value: value.strip()
                              if isinstance(value, str) else value)

    def _rename_columns(self) -> None:
        df = self.df
        df.columns.values[0] = constants.COLUMN_DCID
        df.columns.values[1] = constants.COLUMN_DATE

    def _resolve_entities(self) -> None:
        df = self.df
        # get first (0th) column
        column = df.iloc[:, 0]

        pre_resolved_entities = {}

        def remove_pre_resolved(entity: str) -> bool:
            if entity.startswith(constants.DCID_OVERRIDE_PREFIX):
                pre_resolved_entities[entity] = entity[
                    len(constants.DCID_OVERRIDE_PREFIX):].strip()
                return False
            return True

        entities = list(filter(remove_pre_resolved, column.tolist()))

        logging.info("Found %s entities pre-resolved.",
                     len(pre_resolved_entities))

        logging.info("Resolving %s entities of type %s.", len(entities),
                     self.entity_type)
        dcids = self._resolve(entity_column_name=df.columns[0],
                              entities=entities)
        logging.info("Resolved %s of %s entities.", len(dcids), len(entities))

        # Replace resolved entities.
        column.replace(dcids, inplace=True)
        unresolved = set(entities).difference(set(dcids.keys()))
        unresolved_list = list(unresolved)

        # Replace pre-resolved entities without the "dcid:" prefix.
        column.replace(pre_resolved_entities, inplace=True)

        if unresolved_list:
            logging.warning("# unresolved entities which will be dropped: %s",
                            len(unresolved_list))
            logging.warning("Dropped entities: %s", unresolved_list)
            df.drop(df[df.iloc[:, 0].isin(values=unresolved_list)].index,
                    inplace=True)
        self._create_debug_resolve_dataframe(
            resolved=dcids,
            pre_resolved=pre_resolved_entities,
            unresolved=unresolved_list,
        )

    def _resolve(self, entity_column_name: str,
                 entities: list[str]) -> dict[str, str]:
        lower_case_entity_name = entity_column_name.lower()

        # Check if the entities can be resolved locally.
        # If so, return them by prefixing the values as appropriate.
        if lower_case_entity_name in constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES:
            prefix = constants.PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES[
                lower_case_entity_name]
            return dict([(entity, f"{prefix}{entity}") for entity in entities])

        # Resolve entities externally.
        property_name = constants.EXTERNALLY_RESOLVED_INPUT_COLUMNS_TO_PREFIXES.get(
            lower_case_entity_name, constants.PROPERTY_DESCRIPTION)
        return dc.resolve_entities(entities=entities,
                                   entity_type=self.entity_type,
                                   property_name=property_name)

    def _create_debug_resolve_dataframe(
        self,
        resolved: dict[str, str],
        pre_resolved: dict[str, str],
        unresolved: list[str],
    ):
        # Add unresolved inputs first
        inputs = unresolved[:]
        dcids = [constants.DEBUG_UNRESOLVED_DCID] * len(unresolved)

        # Add pre-resolved next.
        inputs.extend(list(pre_resolved.keys()))
        dcids.extend(list(pre_resolved.values()))

        # Add resolved inputs and dcids
        inputs.extend(list(resolved.keys()))
        dcids.extend(list(resolved.values()))

        # Create browser links
        links = []
        for dcid in dcids:
            if dcid == constants.DEBUG_UNRESOLVED_DCID:
                links.append("")
            else:
                links.append(f"{constants.DC_BROWSER}/{dcid}")

        # Create dataframe
        self.debug_resolve_df = pd.DataFrame({
            constants.DEBUG_COLUMN_INPUT: inputs,
            constants.DEBUG_COLUMN_DCID: dcids,
            constants.DEBUG_COLUMN_LINK: links,
        })

    def _write_csvs(self) -> None:
        logging.info("Writing %s observations to: %s", self.df.index.size,
                     self.observations_fh)
        self.observations_fh.write_string(self.df.to_csv(index=False))
        if self.debug_resolve_df is not None:
            logging.info("Writing resolutions (for debugging) to: %s",
                         self.debug_resolve_fh)
            self.debug_resolve_fh.write_string(
                self.debug_resolve_df.to_csv(index=False))
