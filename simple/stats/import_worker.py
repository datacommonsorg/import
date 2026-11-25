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
"""Single-task worker logic executed inside each child process."""

from dataclasses import dataclass
import os
import tempfile

from stats.config.config import Config
from stats.config.config import ImportConfig
from stats.graph.data import ImportCounts
from stats.graph.data import Provenance
from stats.graph.data import Source
from stats.graph.nodes import Nodes
from stats.importers.factory import create_importer_for_file
from stats.importers.file_split import FilePart
from stats.writer.jsonld_stream_writer import JsonLdStreamWriter
from stats.writer.obs_id_collisions import save_obs_ids
from stats.writer.shard_naming import part_shard_tag
from util.filesystem import File
from util.filesystem import GcsObject
from util.filesystem import open_dir


@dataclass
class ImportProcResult:
  """What a worker process sends back to the coordinator for one task.

  A task is a file, or a part of a split file (see `ImportTask`).
  `sources` and `provenances` are all of the worker's nodes, keyed as in
  `Nodes`; the coordinator merges them with `Nodes.merge_metadata`. The
  collision fields mirror the worker's `JsonLdStreamWriter` stats, which cover
  collisions within the task only. `counts` are the importer's counts of the
  nodes it wrote, for the ingestion report. `obs_ids_path` is where the
  task's observation ids were saved, or None if it had none.
  """
  file_collision_counts: dict[str, int]
  file_sample_collisions: dict[str, list[str]]
  sources: dict[str, Source]
  provenances: dict[str, Provenance]
  processed_imports: set[str]
  counts: ImportCounts
  obs_ids_path: str | None


@dataclass(frozen=True)
class WorkerArgs:
  """Arguments that are the same for every worker process in a run."""
  output_dir_path: str
  import_names: list[str]
  config: ImportConfig
  jsonld_dir_name: str
  # The local dir where workers save observation ids for the coordinator.
  obs_ids_dir: str


@dataclass(frozen=True)
class ImportTask:
  """What a worker process imports: a file, or one part of a split file.

  `size` is the bytes the task reads.
  """
  file_rel_path: str
  gcs_object: GcsObject | None
  input_dir_path: str
  size: int
  part: FilePart | None = None  # None for the whole file.

  @classmethod
  def of(cls, file: File, part: FilePart | None = None) -> "ImportTask":
    size = part.size if part else (file.size() or 0)
    return cls(file.path, file.gcs_object(), file.store_path(), size, part)


def run_import_task(task: ImportTask, args: WorkerArgs) -> ImportProcResult:
  """Imports one file, or part of one, in a worker process; returns its results.

  The worker starts from an empty `Nodes`: a CSV needs only its provenance id,
  which comes from config. The Provenance and Source nodes that an MCF file
  defines are returned for the coordinator to merge. The task's observation
  ids, if any, are saved to a new file in `args.obs_ids_dir` (see
  `stats.writer.obs_id_collisions`). The coordinator found the file, and its GCS
  object if any, so neither is looked up again.
  """
  input_file = open_dir(task.input_dir_path).open_file(
      task.file_rel_path, gcs_object=task.gcs_object)

  config = Config(args.config)
  nodes = Nodes(config=config)
  graph_writer = JsonLdStreamWriter(
      open_dir(args.output_dir_path),
      args.import_names,
      config,
      jsonld_dir_name=args.jsonld_dir_name,
      shard_tag=part_shard_tag(task.part.index) if task.part else "")
  importer = create_importer_for_file(config, input_file, graph_writer, nodes,
                                      task.part)
  try:
    importer.do_import()
  except BaseException:
    graph_writer.close_after_error()
    raise
  graph_writer.commit_and_close()
  obs_ids_path = None
  if len(graph_writer.obs_ids):
    fd, obs_ids_path = tempfile.mkstemp(suffix=".npy", dir=args.obs_ids_dir)
    os.close(fd)
    save_obs_ids(graph_writer.obs_ids.sorted(), obs_ids_path)

  return ImportProcResult(
      file_collision_counts=dict(graph_writer.file_collision_counts),
      file_sample_collisions=dict(graph_writer.file_sample_collisions),
      sources=dict(nodes.sources),
      provenances=dict(nodes.provenances),
      processed_imports=graph_writer.processed_imports(),
      counts=importer.counts,
      obs_ids_path=obs_ids_path,
  )
