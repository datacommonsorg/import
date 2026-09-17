# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Helper I/O utilities for ImportDiffer."""

import json
import os
import sys
import pandas as pd

_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_DATA_DIR = os.path.dirname(os.path.dirname(_SCRIPT_DIR))
sys.path.append(os.path.join(_DATA_DIR, 'util'))

from file_util import FileIO
from mcf_file_util import load_mcf_nodes


def load_mcf_files(path: str) -> list:
    """Loads all sharded mcf files in the given directory and returns a combined MCF node list."""
    nodes_dict = load_mcf_nodes(path)
    return list(nodes_dict.values())


def write_csv_data(df: pd.DataFrame, dest: str, file: str):
    """Writes a dataframe to a CSV file with the given path."""
    path = os.path.join(dest, file)
    with FileIO(path, mode='w', encoding='utf-8') as out_file:
        df.to_csv(out_file, index=False, mode='w', header=True)


def write_json_data(data, dest: str, file: str):
    """Writes data to a JSON file with the given path."""
    path = os.path.join(dest, file)
    with FileIO(path, mode='w', encoding='utf-8') as out_file:
        json.dump(data, out_file, indent=4)


def write_mcf_nodes(nodes: list, dest: str, file: str):
    """Writes mcf nodes to a file with the given path."""
    path = os.path.join(dest, file)
    with FileIO(path, mode='w', encoding='utf-8') as out_file:
        for node in nodes:
            if 'Node' in node:
                out_file.write(f'Node: {node["Node"]}\n')
            elif 'dcid' in node:
                out_file.write(f'dcid: {node["dcid"]}\n')

            for key, value in node.items():
                if key in ['Node', 'dcid']:
                    continue
                out_file.write(f'{key}: {value}\n')
            out_file.write('\n')


def load_data(path: str) -> list:
    """Loads data from the given path (local or GCS, single file or wildcard) and returns MCF node list."""
    return load_mcf_files(path)
