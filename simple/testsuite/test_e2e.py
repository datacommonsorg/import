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
"""End-to-end: these input files produce exactly this output, in every mode.

Each folder under e2e/ is one dataset:

  e2e/<name>/input/        the input directory (config.json + data files)
  e2e/<name>/run.json      description, plus optional "runner_kwargs" and
                           "config_file"
  e2e/<name>/expected.json the canonical output (see lib/canonical.py):
                           every JSON-LD record per import folder, the
                           report.json summary, the workflow handshake and
                           the error, if any

Every dataset runs in every execution mode (lib/run.py), and each aspect of
the output is its own test, so a failure names the dataset, the mode and what
differs:

  test_e2e[kitchen_sink-production-records]

Modes must not change the output. The one exception is the success handshake,
which only the production mode (GCS output) writes.

Records whose @type is listed in run.json "any_folder_types" (default:
dcid:Source) are compared without regard to the import folder they land in:
with several provenances a shared Source node is written to one of the
folders, and which one is not specified. They are listed under
"records.any_folder" in expected.json; everything else under
"records.folders".

  pytest testsuite/test_e2e.py --update-expected   # rewrite expected.json
"""

import functools
import glob
import json
import os

import pytest
from testsuite.lib import canonical
from testsuite.lib import run

E2E_DIR = os.path.join(os.path.dirname(__file__), "e2e")
DATASETS = sorted(
    os.path.basename(os.path.dirname(p))
    for p in glob.glob(os.path.join(E2E_DIR, "*", "run.json")))
ASPECTS = ("records", "report", "handshake", "error")

# Known bugs, as xfails. Strict ones fail with XPASS once fixed, so the entry
# must then be removed. Non-strict ones are for nondeterministic bugs.
# Keys: (dataset, mode, aspect); "*" matches any value.
_PROV_URL = "use_multiprocessing drops dcid:provenanceUrl from observations"
_REPORT = "with use_multiprocessing, report.json leaves every file NOT_STARTED"
_SPLIT_NODE = ("node shards are cut without keeping a subject's triples"
               " together, so a node is split across shards")
_GCS_CONFIG = (
    "--config_file on GCS fails with 'Not a file': create_store() opens"
    " the path as a directory first, which always works on GCS")

KNOWN_ISSUES: dict[tuple[str, str, str], tuple[str, bool]] = {
    ("kitchen_sink", "production", "records"): (_PROV_URL, True),
    ("multi_import", "production", "records"):
        (_PROV_URL + "; a CSV worker's auto-created provenance (named after"
         " its id) can also replace the MCF one", True),
    ("multi_import", "tiny_chunks", "records"): (_SPLIT_NODE, True),
    ("kitchen_sink", "production", "report"): (_REPORT, True),
    ("multi_import", "production", "report"): (_REPORT, True),
    ("kitchen_sink", "tiny_chunks", "records"): (_SPLIT_NODE, True),
    ("config_file_driven", "tiny_chunks", "records"): (_SPLIT_NODE, True),
    ("config_file_driven", "production", "*"): (_GCS_CONFIG, True),
}


def _known_issue(dataset: str, mode: str, aspect: str):
  for key in ((dataset, mode, aspect), (dataset, "*", aspect),
              (dataset, mode, "*"), ("*", mode, aspect)):
    if key in KNOWN_ISSUES:
      return KNOWN_ISSUES[key]
  return None


def _selected_modes(config) -> list[str]:
  value = config.getoption("--modes")
  return [m for m in value.split(",") if m] if value else list(run.ALL_MODES)


def pytest_generate_tests(metafunc):
  if {"dataset", "mode", "aspect"} <= set(metafunc.fixturenames):
    params = []
    for dataset in DATASETS:
      for mode in _selected_modes(metafunc.config):
        for aspect in ASPECTS:
          marks = []
          known = _known_issue(dataset, mode, aspect)
          if known:
            marks.append(pytest.mark.xfail(reason=known[0], strict=known[1]))
          params.append(
              pytest.param(dataset,
                           mode,
                           aspect,
                           marks=marks,
                           id=f"{dataset}-{mode}-{aspect}"))
    metafunc.parametrize("dataset,mode,aspect", params)


def _spec(dataset: str) -> dict:
  with open(os.path.join(E2E_DIR, dataset, "run.json"), encoding="utf-8") as f:
    return json.load(f)


@functools.lru_cache(maxsize=None)
def _output(dataset: str, mode: str) -> dict:
  spec = _spec(dataset)
  result = run.run_preprocessor(os.path.join(E2E_DIR, dataset, "input"),
                                mode,
                                runner_kwargs=spec.get("runner_kwargs"),
                                config_file=spec.get("config_file"))
  return result.output


def _expected_path(dataset: str) -> str:
  return os.path.join(E2E_DIR, dataset, "expected.json")


def _write_expected(dataset: str) -> None:
  reference = _output(dataset, "threaded")
  expected = {
      "records": _records_view(dataset, reference["folders"]),
      "report": reference["report"],
      # Only GCS output gets a success handshake; a failure handshake is
      # written in every mode.
      "handshake": _output(dataset, "production")["handshake"],
      "error": reference["error"],
  }
  with open(_expected_path(dataset), "w", encoding="utf-8") as f:
    f.write(canonical.dumps(expected))


_DEFAULT_ANY_FOLDER_TYPES = ["dcid:Source"]


def _records_view(dataset: str, folders: dict) -> dict:
  """Splits records into folder-bound ones and folder-independent ones."""
  any_types = set(
      _spec(dataset).get("any_folder_types", _DEFAULT_ANY_FOLDER_TYPES))
  bound, anywhere = {}, []
  for folder, kinds in folders.items():
    bound[folder] = {}
    for kind, records in kinds.items():
      bound[folder][kind] = {}
      for rid, rec in records.items():
        copies = rec if isinstance(rec, list) else [rec]
        if any(c.get("@type") in any_types for c in copies):
          anywhere.extend(copies)
        else:
          bound[folder][kind][rid] = rec
  anywhere.sort(key=lambda r: json.dumps(r, sort_keys=True))
  return {"folders": bound, "any_folder": anywhere}


def _actual(dataset: str, output: dict, aspect: str):
  if aspect == "records":
    return _records_view(dataset, output["folders"])
  return output[aspect]


def test_e2e(dataset: str, mode: str, aspect: str, update_expected: bool):
  if update_expected and mode == "threaded" and aspect == ASPECTS[0]:
    _write_expected(dataset)
  if not os.path.exists(_expected_path(dataset)):
    pytest.fail(f"{_expected_path(dataset)} is missing; generate it with"
                " --update-expected and review it")
  with open(_expected_path(dataset), encoding="utf-8") as f:
    expected = json.load(f)[aspect]
  output = _output(dataset, mode)
  actual = _actual(dataset, output, aspect)
  if aspect == "handshake" and mode != "production" and not output["error"]:
    expected = None  # Local output: no success handshake.
  if output["error"] and aspect != "error" and "_traceback" in output and (
      not expected_error(dataset)):
    pytest.fail(f"run failed:\n{output['_traceback']}", pytrace=False)
  if actual != expected:
    pytest.fail(f"{dataset} [{mode}] {aspect} differs from expected.json"
                f" (- expected, + actual):\n" +
                canonical.diff(expected, actual),
                pytrace=False)


def expected_error(dataset: str):
  path = _expected_path(dataset)
  if not os.path.exists(path):
    return None
  with open(path, encoding="utf-8") as f:
    return json.load(f).get("error")
