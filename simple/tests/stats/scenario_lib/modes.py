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
"""Execution modes. Every mode must produce the same loader-view facts.

Modes change *how* the preprocessor runs, never *what* it should produce, so a
single golden per scenario is compared against all of them. Differences are
either bugs or must be declared as known issues.

All switches are applied from the test side (patches / env vars); no
production code is changed to support them.
"""

import contextlib
import dataclasses
from typing import Callable, Iterator, Mapping
from unittest import mock

import pandas as pd


@contextlib.contextmanager
def _no_patches() -> Iterator[None]:
  yield


_real_read_csv = pd.read_csv


def _read_csv_with_tiny_chunks(*args, **kwargs):
  if kwargs.get("chunksize"):
    kwargs["chunksize"] = 2
  return _real_read_csv(*args, **kwargs)


@contextlib.contextmanager
def _tiny_chunk_patches() -> Iterator[None]:
  """Forces every chunk / shard boundary to be crossed on tiny fixtures.

  - JSON-LD shards hold 1 entry (observations) or 1 subject (nodes) instead of
    10,000.
  - CSVs read in chunks are read 2 rows at a time instead of 10,000.
  """
  with mock.patch("stats.jsonld_stream_writer._CHUNK_SIZE", 1), \
       mock.patch.object(pd, "read_csv", _read_csv_with_tiny_chunks):
    yield


@dataclasses.dataclass(frozen=True)
class Mode:
  name: str
  description: str
  use_multiprocessing: bool
  env: Mapping[str, str] = dataclasses.field(default_factory=dict)
  patches: Callable[[], contextlib.AbstractContextManager] = _no_patches


MODES: dict[str, Mode] = {
    m.name: m for m in [
        Mode(name="threaded",
             description=("use_multiprocessing=False (files imported by a"
                          " thread pool). Goldens are generated in this mode."),
             use_multiprocessing=False),
        Mode(name="tiny_chunks",
             description="threaded, with shard and CSV chunk sizes shrunk to"
             " 1-2 so every boundary is exercised.",
             use_multiprocessing=False,
             patches=_tiny_chunk_patches),
        Mode(name="multiprocess",
             description=("use_multiprocessing=True: the production code path"
                          " (one worker process per file)."),
             use_multiprocessing=True),
        Mode(name="rdflib_nodes",
             description=("threaded, with FAST_NODE_EXPORT=false (the legacy"
                          " rdflib node writer)."),
             use_multiprocessing=False,
             env={"FAST_NODE_EXPORT": "false"}),
    ]
}
