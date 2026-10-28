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
with a fake GCS, and returns the canonical output (see canonical.py).

The preprocessor must not call the Data Commons API. DC_API_ROOT points at a
closed local port, so any call fails the run instead of reaching the network.

Modes change *how* the preprocessor runs, never *what* it must produce:

  threaded      use_multiprocessing=False, local disk. The fast default.
  production    use_multiprocessing=True, input and output on (fake) GCS: how
                the ingestion workflow runs it. Only this mode produces a
                success handshake.
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
from testsuite.lib import canonical
from testsuite.lib import fake_gcs

WORKFLOW_ID = "testsuite-run"
# Nothing listens on port 9 (discard): calls fail fast with a connection error.
_NO_DC_API = "http://127.0.0.1:9"


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
    m.name: m for m in [
        Mode("threaded"),
        Mode("production", use_multiprocessing=True, gcs=True),
        Mode("tiny_chunks", patches=_tiny_chunks),
    ]
}
ALL_MODES = tuple(MODES)

# Set to keep each run's work directory (raw output) for debugging.
_KEEP_WORK_DIRS_ENV = "TESTSUITE_KEEP_WORK_DIRS"


@dataclasses.dataclass
class RunResult:
  output: dict  # Canonical output (see canonical.py).


def run_preprocessor(input_dir: str,
                     mode: Mode | str = "threaded",
                     runner_kwargs: dict | None = None,
                     config_file: str | None = None) -> RunResult:
  """Runs Runner on a copy of input_dir and returns its canonical output.

  By default Runner discovers config.json in the input dir. With config_file
  (a path relative to input_dir), Runner is driven by that config file
  instead; "<input>" in it (e.g. in dataDownloadUrl) is replaced by the
  input location of this run.

  The run's work directory is deleted afterwards, unless the
  TESTSUITE_KEEP_WORK_DIRS environment variable is set.
  """
  if isinstance(mode, str):
    mode = MODES[mode]
  work = tempfile.mkdtemp(prefix=f"testsuite-{mode.name}-")
  try:
    return RunResult(
        output=_run_in(work, input_dir, mode, runner_kwargs, config_file))
  finally:
    if not os.environ.get(_KEEP_WORK_DIRS_ENV):
      shutil.rmtree(work, ignore_errors=True)


@dataclasses.dataclass(frozen=True)
class _Paths:
  """Where one run reads and writes."""
  local_out: str  # Local copy of the output, read back after the run.
  gcs_root: str  # This run's prefix on the fake GCS.
  input: str  # What Runner reads: local, or on GCS in production mode.
  output: str  # What Runner writes: local, or on GCS in production mode.

  @property
  def temp(self) -> str:
    """TEMP_LOCATION, where the workflow handshake is written."""
    return f"{self.gcs_root}/temp"


def _run_in(work: str, input_dir: str, mode: Mode, runner_kwargs: dict | None,
            config_file: str | None) -> dict:
  """Stages input_dir under work, runs Runner and reads its output back."""
  gcs_root = fake_gcs.new_prefix()
  local_out = os.path.join(work, "output")
  if mode.gcs:
    paths = _Paths(local_out, gcs_root, f"{gcs_root}/input",
                   f"{gcs_root}/output")
  else:
    paths = _Paths(local_out, gcs_root, os.path.join(work, "input"), local_out)
  _stage_input(input_dir, os.path.join(work, "staged"), paths, mode,
               config_file)
  error = _run_runner(paths, mode, runner_kwargs, config_file)
  return _read_output(paths, mode, error)


def _stage_input(input_dir: str, staged: str, paths: _Paths, mode: Mode,
                 config_file: str | None) -> None:
  """Copies input_dir to where Runner reads it (local disk or GCS)."""
  shutil.copytree(input_dir, staged)
  if config_file:
    config_path = os.path.join(staged, config_file)
    with open(config_path, encoding="utf-8") as f:
      text = f.read().replace("<input>", paths.input)
    with open(config_path, "w", encoding="utf-8") as f:
      f.write(text)
  if mode.gcs:
    fake_gcs.upload_dir(staged, paths.input)
  else:
    shutil.copytree(staged, paths.input)


def _run_runner(paths: _Paths, mode: Mode, runner_kwargs: dict | None,
                config_file: str | None) -> dict | None:
  """Runs Runner; returns the exception it raised, as a dict, or None."""
  env = {
      "DC_API_ROOT": _NO_DC_API,
      "DC_API_KEY": "",
      "WORKFLOW_EXECUTION_ID": WORKFLOW_ID,
      "TEMP_LOCATION": paths.temp,
      **fake_gcs.env(),
      **mode.env,
  }
  # The writer caches custom namespace prefixes in a module global; start
  # every run from a clean slate so runs cannot influence each other.
  with mock.patch.dict(os.environ, env), \
       mock.patch("stats.jsonld_stream_writer._KNOWN_CUSTOM_PREFIXES", set()), \
       mode.patches():
    from stats.runner import Runner  # pylint: disable=import-outside-toplevel
    try:
      Runner(config_file_path=(f"{paths.input}/{config_file}"
                               if config_file else None),
             input_dir_path=None if config_file else paths.input,
             output_dir_path=paths.output,
             use_multiprocessing=mode.use_multiprocessing,
             **(runner_kwargs or {})).run()
    except Exception as e:  # pylint: disable=broad-except
      return {
          "type": type(e).__name__,
          "message": str(e),
          "traceback": "".join(traceback.format_exception(e)),
      }
  return None


def _read_output(paths: _Paths, mode: Mode, error: dict | None) -> dict:
  """Reads the run's records, report and handshake into canonical form."""
  if mode.gcs:
    fake_gcs.download_dir(paths.output, paths.local_out)
  handshake_text = fake_gcs.read_text(
      f"{paths.temp}/datacommons/ingestion_records/{WORKFLOW_ID}.json")
  report = _read_json(os.path.join(paths.local_out, "process", "report.json"))
  tokens = {
      paths.input: "<input>",
      paths.output: "<output>",
      paths.gcs_root: "<gcs>"
  }
  output = {
      "records":
          canonical.load_jsonld(os.path.join(paths.local_out, "jsonld")),
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
  return output


def _read_json(path: str) -> Any:
  if not os.path.exists(path):
    return None
  with open(path, encoding="utf-8") as f:
    return json.load(f)


def write_input_dir(files: dict[str, str | bytes], config: dict | None) -> str:
  """Materializes {relative path: content} (+ config.json) in a temp dir."""
  root = tempfile.mkdtemp(prefix="testsuite-input-")
  for rel, content in files.items():
    path = os.path.join(root, rel)
    os.makedirs(os.path.dirname(path), exist_ok=True)
    mode = "wb" if isinstance(content, bytes) else "w"
    text_args = {} if mode == "wb" else {"newline": "", "encoding": "utf-8"}
    with open(path, mode, **text_args) as f:
      f.write(content)
  if config is not None:
    with open(os.path.join(root, "config.json"), "w", encoding="utf-8") as f:
      json.dump(config, f, indent=2)
  return root
