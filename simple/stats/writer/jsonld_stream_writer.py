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
"""A GraphWriter implementation that streams JSON-LD shards directly to GCS/Disk."""

from collections import defaultdict
from collections.abc import Collection
from datetime import datetime
from datetime import timezone
import logging
from typing import Optional

import numpy as np
import pandas as pd
from stats import constants
from stats.config.config import Config
from stats.graph.data import ImportType
from stats.graph.data import Triple
from stats.graph.data import validate_numeric_values
from stats.graph.data import validate_variable_values
from stats.writer.graph_writer import GraphWriter
from stats.writer.jsonld_serializer import build_node_shard
from stats.writer.jsonld_serializer import build_observation_shard
from stats.writer.jsonld_serializer import col_to_list
from stats.writer.jsonld_serializer import DCID_URL
from stats.writer.jsonld_serializer import format_collision_sample
from stats.writer.jsonld_serializer import prefix_dcid
from stats.writer.obs_id_collisions import MAX_SAMPLES_PER_FILE
from stats.writer.obs_id_collisions import ObsIds
from stats.writer.shard_naming import file_shard_stem
from stats.writer.shard_naming import node_part_stem
from stats.writer.shard_sink import ShardSink
from util.filesystem import Dir
from util.filesystem import File

_CHUNK_SIZE = 10000


class JsonLdStreamWriter(GraphWriter):
  """A GraphWriter implementation that streams triples and observations directly to JSON-LD shards on GCS/Disk.

  Not thread-safe: the coordinator and each worker process have their own.
  """

  def __init__(self,
               output_dir: Dir,
               import_names: list[str],
               config: Config,
               jsonld_dir_name: Optional[str] = None,
               shard_tag: str = "") -> None:
    """
    Args:
      jsonld_dir_name: For a worker's writer, the run folder that the
        coordinator's writer created.
      shard_tag: Added to the names of shards written for a part of a split
        file, e.g. "p001" in "observation-data-p001-00000.jsonld" or
        "node-nodes-p001-00000-<uuid>.jsonld".
    """
    self.config = config

    # The run's name, from its imports (e.g. ["oecd", "ilo"] -> "oecd_ilo", or
    # ALL_IMPORTS): names its timestamped output folder below, and is the
    # default folder for global triples (see _global_triples_import).
    self.import_name = "_".join(import_names).replace("/", "_")

    if jsonld_dir_name:
      # A worker's writer: the coordinator's writer created the run folder.
      self.jsonld_dir = output_dir.open_dir("jsonld", create=False).open_dir(
          jsonld_dir_name, create=False)
    else:
      timestamp = datetime.now(timezone.utc).strftime("%Y%m%d_%H%M%S_%f")
      self.jsonld_dir = output_dir.open_dir("jsonld").open_dir(
          f"{self.import_name}_{timestamp}")

    self.obs_shard_index = 0
    self.shard_tag = shard_tag
    self.node_shard_index = 0
    self.ns_map = {"dcid": DCID_URL}
    self._sink = ShardSink(self.jsonld_dir)
    # Triples of no import (e.g. the root StatVarGroups), written to the folder
    # of _global_triples_import at commit_and_close.
    self._global_triples: list[Triple] = []
    # The imports with shards from this writer or, on the coordinator, from
    # the workers' writers merged into it (see merge_worker).
    self._processed_imports: set[str] = set()
    self.obs_ids = ObsIds()
    self.file_collision_counts: dict[str, int] = defaultdict(int)
    self.file_sample_collisions: dict[str, list[str]] = defaultdict(list)

  def track_observation_hash(
      self,
      ids: np.ndarray,
      chunk_rows: list[tuple],
      file_name: str,
  ) -> None:
    """Tracks 64-bit observation id prefixes and records any collisions.

    Only collisions with observations that this writer saw are found: a worker
    sees one file. See `stats.writer.obs_id_collisions` for collisions between
    files.
    """
    collisions = np.flatnonzero(self.obs_ids.add(ids))
    if not len(collisions):
      return
    self.file_collision_counts[file_name] += len(collisions)
    # Only the samples still missing are formatted, however many collisions.
    samples = self.file_sample_collisions[file_name]
    for i in collisions[:max(0, MAX_SAMPLES_PER_FILE - len(samples))].tolist():
      samples.append(format_collision_sample(chunk_rows[i]))

  def processed_imports(self) -> set[str]:
    """Returns the imports with shards from this writer (see __init__)."""
    return set(self._processed_imports)

  def merge_worker(self, imports: set[str], file_counts: dict[str, int],
                   file_samples: dict[str, list[str]]) -> None:
    """Adds a worker's writer state: its processed imports and collisions."""
    self._processed_imports.update(imports)
    self.merge_collisions(file_counts, file_samples)

  def merge_collisions(self, file_counts: dict[str, int],
                       file_samples: dict[str, list[str]]) -> None:
    """Adds collisions found elsewhere to this writer's end-of-run summary.

    Args:
      file_counts: collision count per input file path.
      file_samples: sample descriptions per input file path.
    """
    for f_name, count in file_counts.items():
      self.file_collision_counts[f_name] += count
      samples = self.file_sample_collisions[f_name]
      samples.extend(
          file_samples.get(f_name,
                           [])[:max(0, MAX_SAMPLES_PER_FILE - len(samples))])

  def _write_observation_shards(
      self,
      df: pd.DataFrame,
      import_name: str,
      file_name: str,
      entity_properties: frozenset[str] = frozenset(),
  ):
    dir_name = import_name.replace("/", "_")
    n = len(df)
    col_lists = [col_to_list(df[c]) for c in constants.OBSERVATION_COLUMNS]
    for i in range(0, n, _CHUNK_SIZE):
      if i == 0 and n <= _CHUNK_SIZE:
        chunk_records = list(zip(*col_lists))
      else:
        end = i + _CHUNK_SIZE
        chunk_records = list(zip(*(c[i:end] for c in col_lists)))
      shard_index = self.obs_shard_index
      self.obs_shard_index += 1
      name, text = build_observation_shard(
          chunk_records,
          shard_index,
          self.ns_map,
          track_hash_fn=self.track_observation_hash,
          file_name=file_name,
          shard_tag=self.shard_tag,
          entity_properties=entity_properties)
      self._sink.write(dir_name, name, text)

  def write_observations(self,
                         observations_df: pd.DataFrame,
                         input_file: File,
                         entity_properties: Collection[str] = ()):
    if observations_df.empty:
      return
    validate_variable_values(observations_df, input_file.path)
    validate_numeric_values(observations_df, input_file.path)

    import_name = self.config.import_name(input_file)
    self._processed_imports.add(import_name)
    prefixed_entity_props = frozenset(prefix_dcid(p) for p in entity_properties)
    self._write_observation_shards(observations_df,
                                   import_name,
                                   file_name=input_file.path,
                                   entity_properties=prefixed_entity_props)

  def _write_node_shards(self,
                         triples: list[Triple],
                         import_name: str,
                         file_name: str = ""):
    dir_name = import_name.replace("/", "_")
    i = 0
    n = len(triples)
    while i < n:
      chunk = []
      end = min(i + _CHUNK_SIZE, n)
      chunk.extend(triples[i:end])
      i = end

      # Expand boundary to keep subject together
      if i < n:
        boundary_subject = chunk[-1].subject_id
        while i < n and triples[i].subject_id == boundary_subject:
          chunk.append(triples[i])
          i += 1

      name, text = build_node_shard(chunk,
                                    self.node_shard_index,
                                    self.ns_map,
                                    file_name=file_name,
                                    shard_tag=self.shard_tag)
      self._sink.write(dir_name, name, text)
      self.node_shard_index += 1

  def write_triples(self,
                    triples: list[Triple],
                    input_file: File = None,
                    provenance_dir: str = None):
    if not triples:
      return

    if not provenance_dir and input_file:
      provenance_dir = self.config.import_name(input_file)

    if not provenance_dir:
      self._global_triples.extend(triples)
      return

    self._processed_imports.add(provenance_dir)
    self._write_node_shards(triples,
                            provenance_dir,
                            file_name=input_file.path if input_file else "")

  def import_folders(self) -> list[str]:
    """Returns the sorted imports whose folders this writer writes.

    These are the processed imports plus the import that global triples go to
    (see _global_triples_import), which is written at commit_and_close.
    """
    return sorted(self._processed_imports | {self._global_triples_import()})

  def _global_triples_import(self) -> str:
    """Returns the import whose folder global triples are written to.

    The run's own import name if it has a folder (or no import has one), else
    the alphabetically first processed import, so that the choice does not
    depend on processing order.
    """
    if not self._processed_imports or self.import_name in self._processed_imports:
      return self.import_name
    return min(self._processed_imports)

  def commit_and_close(self):
    if self._global_triples:
      target_import = self._global_triples_import()
      self._processed_imports.add(target_import)
      self._write_node_shards(self._global_triples, target_import)
      self._global_triples = []

    self._sink.finish()

  def log_collision_summary(self) -> None:
    """Logs the observation @id collisions of every file, if any.

    The coordinator calls this once its writer holds the collisions of every
    file (see `merge_collisions`).
    """
    total_collisions = sum(self.file_collision_counts.values())
    if not total_collisions:
      return
    summary_lines = [
        f"Observation @ID Collision Summary: Total of {total_collisions} observation @id collisions detected across {len(self.file_collision_counts)} file(s):"
    ]
    for f_name, count in self.file_collision_counts.items():
      summary_lines.append(f"  File '{f_name}': {count} collision(s)")
      for sample in self.file_sample_collisions[f_name]:
        summary_lines.append(f"    - {sample}")
    logging.warning("\n".join(summary_lines))

  def close_after_error(self) -> None:
    """Waits for the shards in flight, ignoring errors, after a failed import.

    So that nothing is still being written once the failure is reported.
    """
    self._sink.finish(raise_errors=False)

  def delete_part_shards(self, input_file: File) -> None:
    """Deletes the shards of every part of `input_file`."""
    folder = self.config.import_name(input_file).replace("/", "_")
    is_obs = (not input_file.path.lower().endswith(".mcf") and
              self.config.import_type(input_file) == ImportType.OBSERVATIONS)
    stem = (file_shard_stem(input_file.path)
            if is_obs else node_part_stem(input_file.path))
    self._sink.delete_part_shards(folder, stem, is_obs=is_obs)
