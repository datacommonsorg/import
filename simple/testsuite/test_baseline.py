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
"""Differential tests: this checkout vs. a baseline git ref, same inputs.

For refactors and performance work, where the output must not change at all:

  pytest testsuite/test_baseline.py --baseline=origin/master

The baseline's `simple/` directory is exported with `git archive` into a temp
dir, this testsuite is copied next to it, and the baseline runs in a
subprocess. Inputs: every e2e dataset, in threaded and gcs modes, plus
generated MCF and observation inputs. Any difference in records, report,
handshake or error fails. Skipped unless --baseline is given.
"""

import json
import os
import shutil
import subprocess
import sys
import tempfile

from hypothesis import given
from hypothesis import note
from hypothesis import settings
import pytest
from testsuite.lib import canonical
from testsuite.lib import run
from testsuite.lib import strategies as gen
from testsuite.test_e2e import DATASETS
from testsuite.test_e2e import E2E_DIR

_SIMPLE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
_REPO_DIR = os.path.dirname(_SIMPLE_DIR)


@pytest.fixture(scope="module")
def baseline_simple_dir(request):
  ref = request.config.getoption("--baseline")
  if not ref:
    pytest.skip("pass --baseline=<git ref> to run differential tests")
  root = tempfile.mkdtemp(prefix="testsuite-baseline-")
  archive = subprocess.run(["git", "archive", ref, "simple"],
                           cwd=_REPO_DIR,
                           check=True,
                           capture_output=True).stdout
  subprocess.run(["tar", "-x", "-C", root], input=archive, check=True)
  simple = os.path.join(root, "simple")
  shutil.rmtree(os.path.join(simple, "testsuite"), ignore_errors=True)
  shutil.copytree(os.path.join(_SIMPLE_DIR, "testsuite"),
                  os.path.join(simple, "testsuite"),
                  ignore=shutil.ignore_patterns("__pycache__"))
  return simple


def _run_baseline(simple_dir: str, **request) -> dict:
  work = tempfile.mkdtemp(prefix="testsuite-baseline-run-")
  req_path = os.path.join(work, "request.json")
  out_path = os.path.join(work, "output.json")
  with open(req_path, "w") as f:
    json.dump(request, f)
  proc = subprocess.run([
      sys.executable, "-m", "testsuite.lib.baseline_runner", req_path, out_path
  ],
                        cwd=simple_dir,
                        capture_output=True,
                        text=True,
                        env={
                            **os.environ, "PYTHONPATH": simple_dir
                        })
  if proc.returncode:
    pytest.fail(f"baseline run failed:\n{proc.stderr[-4000:]}", pytrace=False)
  with open(out_path) as f:
    return json.load(f)


def _run_current(**request) -> dict:
  output = run.run_preprocessor(request["input_dir"],
                                request["mode"],
                                dc_graph=request.get("dc_graph"),
                                runner_kwargs=request.get("runner_kwargs"),
                                config_file=request.get("config_file")).output
  return json.loads(
      json.dumps({
          k: v for k, v in output.items() if k != "_traceback"
      }))


def _compare(simple_dir: str, **request) -> None:
  baseline = _run_baseline(simple_dir, **request)
  current = _run_current(**request)
  assert current == baseline, (
      "output differs from the baseline (- baseline, + this checkout):\n" +
      canonical.diff(baseline, current, "baseline", "current"))


@pytest.mark.parametrize("mode", ["threaded", "gcs"])
@pytest.mark.parametrize("dataset", DATASETS)
def test_e2e_dataset_matches_baseline(baseline_simple_dir, dataset, mode):
  with open(os.path.join(E2E_DIR, dataset, "run.json")) as f:
    spec = json.load(f)
  _compare(baseline_simple_dir,
           input_dir=os.path.join(E2E_DIR, dataset, "input"),
           mode=mode,
           dc_graph=spec.get("dc_api"),
           runner_kwargs=spec.get("runner_kwargs"),
           config_file=spec.get("config_file"))


@settings(max_examples=max(5, settings().max_examples // 2))
@given(mcf=gen.mcf_file(), obs=gen.observation_csv())
def test_generated_input_matches_baseline(baseline_simple_dir, mcf, obs):
  text, _ = mcf
  note(f"MCF:\n{text}\nCSV:\n{obs.text!r}")
  config = {
      "inputFiles": [
          {
              "pattern": "*.mcf",
              "provenance": "dcid:TestProvenance"
          },
          {
              "pattern": "data.csv",
              "provenance": "dcid:TestProvenance",
              "columnMappings": obs.column_mappings,
              "ignoreColumns": obs.ignore_columns,
          },
      ],
      "importProxyEntities": False,
  }
  input_dir = run.write_input_dir(
      {
          "provenance.mcf": gen.PROVENANCE_MCF,
          "nodes.mcf": text,
          "data.csv": obs.text
      }, config)
  _compare(baseline_simple_dir, input_dir=input_dir, mode="threaded")
