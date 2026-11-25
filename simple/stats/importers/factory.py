# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Factory selecting the right Importer implementation for an input file."""

from stats.config.config import Config
from stats.graph.data import ImportType
from stats.graph.nodes import Nodes
from stats.importers.entities_importer import EntitiesImporter
from stats.importers.file_split import FilePart
from stats.importers.importer import Importer
from stats.importers.mcf_importer import McfImporter
from stats.importers.variable_per_row_importer import VariablePerRowImporter
from stats.writer.graph_writer import GraphWriter
from util.filesystem import File


def create_importer_for_file(
    config: Config,
    input_file: File,
    graph_writer: GraphWriter,
    nodes: Nodes,
    part: FilePart | None = None,
) -> Importer:
  """Returns the importer of `input_file`, or of its `part`."""
  if input_file.path.lower().endswith(".mcf"):
    return McfImporter(
        input_file=input_file,
        graph_writer=graph_writer,
        nodes=nodes,
        part=part,
    )

  import_type = config.import_type(input_file)

  match import_type:

    case ImportType.OBSERVATIONS:
      return VariablePerRowImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          nodes=nodes,
          part=part,
      )

    case ImportType.ENTITIES:
      return EntitiesImporter(
          input_file=input_file,
          graph_writer=graph_writer,
          nodes=nodes,
          part=part,
      )

    case _:
      raise ValueError(
          f"Unsupported import type: {import_type} ({input_file.full_path()})")
