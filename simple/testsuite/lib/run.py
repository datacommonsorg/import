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
"""Runs the real preprocessor (stats.runner.Runner) hermetically.

Every test in the suite goes through run_preprocessor(): it takes an input
directory (config.json plus data files), runs Runner in the requested mode
with a fake DC API and a fake GCS, and returns the canonical output (see
canonical.py) plus what the run asked the DC API.

Modes change *how* the preprocessor runs, never *what* it must produce:

  threaded      use_multiprocessing=False, local disk. The fast default.
  multiprocess  use_multiprocessing=True (production code path), local disk.
  gcs           multiprocess, input and output on (fake) GCS: the production
                deployment. Only this mode produces a success handshake.
  tiny_chunks   threaded, with shard and CSV chunk sizes shrunk to 1-2 so that
                every chunk/shard boundary is crossed even by tiny inputs.
"""

import contextlib
import dataclasses
import json
import os
import shutil
import tempfile
import traceback
from typing import Any, Callable, Iterator
from unittest import mock

import pandas as pd

from testsuite.lib import canonical, fake_gcs
from testsuite.lib.dc_stub import DcApiStub

WORKFLOW_ID = "testsuite-run"


@contextlib.contextmanager
def _no_patches() -> Iterator[None]:
    yield


_real_read_csv = pd.read_csv


def _read_csv_tiny_chunks(*args, **kwargs):
    if kwargs.get("chunksize"):
        kwargs["chunksize"] = 2
    return _real_read_csv(*args, **kwargs)


@contextlib.contextmanager
def _tiny_chunks() -> Iterator[None]:
    with mock.patch("stats.jsonld_stream_writer._CHUNK_SIZE", 1), \
         mock.patch.object(pd, "read_csv", _read_csv_tiny_chunks):
        yield


@dataclasses.dataclass(frozen=True)
class Mode:
    name: str
    use_multiprocessing: bool = False
    gcs: bool = False
    env: dict = dataclasses.field(default_factory=dict)
    patches: Callable[[], contextlib.AbstractContextManager] = _no_patches


MODES = {
    m.name: m
    for m in [
        Mode("threaded"),
        Mode("multiprocess", use_multiprocessing=True),
        Mode("gcs", use_multiprocessing=True, gcs=True),
        Mode("tiny_chunks", patches=_tiny_chunks),
    ]
}
ALL_MODES = tuple(MODES)


@dataclasses.dataclass
class RunResult:
    output: dict  # Canonical output (see canonical.py).
    dc_requests: list
    dc_unexpected: list
    log_dir: str  # Local copy of the raw output, for debugging.


def run_preprocessor(input_dir: str,
                     mode: Mode | str = "threaded",
                     dc_graph: dict | None = None,
                     runner_kwargs: dict | None = None,
                     config_file: str | None = None) -> RunResult:
    """Runs Runner on a copy of input_dir and returns its canonical output.

  By default Runner discovers config.json in the input dir. With config_file
  (a path relative to input_dir), Runner is driven by that config file
  instead; "<input>" in it (e.g. in dataDownloadUrl) is replaced by the
  input location of this run.
  """
    if isinstance(mode, str):
        mode = MODES[mode]
    work = tempfile.mkdtemp(prefix=f"testsuite-{mode.name}-")
    local_out = os.path.join(work, "output")
    gcs_root = fake_gcs.new_prefix()
    if mode.gcs:
        in_path = f"{gcs_root}/input"
        out_path = f"{gcs_root}/output"
    else:
        in_path = os.path.join(work, "input")
        out_path = local_out
    staged = os.path.join(work, "staged")
    shutil.copytree(input_dir, staged)
    if config_file:
        config_path = os.path.join(staged, config_file)
        with open(config_path) as f:
            text = f.read().replace("<input>", in_path)
        with open(config_path, "w") as f:
            f.write(text)
    if mode.gcs:
        fake_gcs.upload_dir(staged, in_path)
    else:
        shutil.copytree(staged, in_path)

    error = None
    with DcApiStub(dc_graph) as stub:
        env = {
            "DC_API_ROOT": stub.root,
            "DC_API_KEY": "",
            "NO_PROXY": "127.0.0.1,localhost",
            "no_proxy": "127.0.0.1,localhost",
            "WORKFLOW_EXECUTION_ID": WORKFLOW_ID,
            "TEMP_LOCATION": f"{gcs_root}/tmp",
            **fake_gcs.env(),
            **mode.env,
        }
        # The writer caches custom namespace prefixes in a module global; start
        # every run from a clean slate so runs cannot influence each other.
        with mock.patch.dict(os.environ, env), \
             mock.patch("stats.jsonld_stream_writer._KNOWN_CUSTOM_PREFIXES", set()), \
             mode.patches():
            from stats.runner import \
                Runner  # pylint: disable=import-outside-toplevel
            try:
                Runner(config_file_path=(f"{in_path}/{config_file}"
                                         if config_file else None),
                       input_dir_path=None if config_file else in_path,
                       output_dir_path=out_path,
                       use_multiprocessing=mode.use_multiprocessing,
                       **(runner_kwargs or {})).run()
            except Exception as e:  # pylint: disable=broad-except
                error = {
                    "type": type(e).__name__,
                    "message": str(e),
                    "traceback": "".join(traceback.format_exception(e)),
                }
        requests, unexpected = list(stub.requests), list(stub.unexpected)

    if mode.gcs:
        fake_gcs.download_dir(out_path, local_out)
    handshake_text = fake_gcs.read_text(
        f"{gcs_root}/tmp/datacommons/ingestion_records/{WORKFLOW_ID}.json")
    report = _read_json(os.path.join(local_out, "process", "report.json"))

    tokens = {in_path: "<input>", out_path: "<output>", gcs_root: "<gcs>"}
    folders, contexts = canonical.load_jsonld(os.path.join(
        local_out, "jsonld"))
    output = {
        "folders":
        folders,
        "contexts":
        contexts,
        "report":
        canonical.normalize_report(report, tokens),
        "handshake":
        canonical.normalize_handshake(
            json.loads(handshake_text) if handshake_text else None, tokens),
        "error":
        None,
    }
    if error:
        output["error"] = {
            "type": error["type"],
            "message": canonical.replace_paths(error["message"], tokens),
        }
        output["_traceback"] = error["traceback"]
    return RunResult(output=output,
                     dc_requests=requests,
                     dc_unexpected=unexpected,
                     log_dir=work)


def _read_json(path: str) -> Any:
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def write_input_dir(files: dict[str, str | bytes], config: dict | None) -> str:
    """Materializes {relative path: content} (+ config.json) in a temp dir."""
    root = tempfile.mkdtemp(prefix="testsuite-input-")
    for rel, content in files.items():
        path = os.path.join(root, rel)
        os.makedirs(os.path.dirname(path), exist_ok=True)
        mode = "wb" if isinstance(content, bytes) else "w"
        with open(path, mode, **({} if mode == "wb" else {
                "newline": ""
        })) as f:
            f.write(content)
    if config is not None:
        with open(os.path.join(root, "config.json"), "w") as f:
            json.dump(config, f, indent=2)
    return root
