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
"""Node-level cases: this exact input produces exactly these JSON-LD records.

Each `## ` section of cases/*.md is one test (format: lib/casefile.py). The
input is run through the real Runner; the records the case adds on top of the
file's defaults must equal the case's ```expect``` block.

  pytest testsuite/test_cases.py                      # run all cases
  pytest testsuite/test_cases.py -k "mcf_nodes"       # one file
  pytest testsuite/test_cases.py --update-expected    # (re)write expect blocks
"""

import collections
import functools
import glob
import json
import os

import pytest
from testsuite.lib import canonical
from testsuite.lib import casefile
from testsuite.lib import run

CASES_DIR = os.path.join(os.path.dirname(__file__), "cases")
_CASE_FILES = sorted(glob.glob(os.path.join(CASES_DIR, "*.md")))
_PENDING_UPDATES: dict[str,
                       dict[str,
                            list[tuple[str,
                                       str]]]] = (collections.defaultdict(dict))


def _params():
  for path in _CASE_FILES:
    parsed = casefile.parse(path)
    for case in parsed.cases:
      yield pytest.param(path,
                         case.name,
                         id=f"{os.path.basename(path)[:-3]}::{case.name}")


def _modes(case: casefile.Case) -> list[str]:
  value = case.options.get("modes", "threaded")
  if value == "all":
    return list(run.ALL_MODES)
  return [m.strip() for m in value.split(",") if m.strip()]


def _inputs(blocks: list[casefile.Block], default_config: dict | None,
            default_graph: dict | None):
  files, config, graph = {}, default_config, default_graph
  for block in blocks:
    if block.kind in ("mcf", "csv"):
      files[block.attrs.get("file", f"case.{block.kind}")] = block.text
    elif block.kind == "config":
      config = casefile.deep_merge(config or {}, json.loads(block.text))
    elif block.kind == "dc_api":
      graph = casefile.deep_merge(graph or {}, json.loads(block.text))
  return files, config, graph


@functools.lru_cache(maxsize=None)
def _parsed(path: str) -> casefile.CaseFile:
  return casefile.parse(path)


def _records(output: dict) -> collections.Counter:
  return collections.Counter(
      json.dumps(rec, sort_keys=True, ensure_ascii=False)
      for _, _, _, rec in canonical.all_records(output))


@functools.lru_cache(maxsize=None)
def _baseline(path: str, mode: str) -> collections.Counter:
  """Records produced by the file's defaults alone."""
  files, config, graph = _inputs(_parsed(path).defaults, None, None)
  result = run.run_preprocessor(run.write_input_dir(files, config), mode, graph)
  return _records(result.output)


def _check(case: casefile.Case, path: str, mode: str,
           update: bool) -> list[str]:
  defaults_files, default_config, default_graph = _inputs(
      _parsed(path).defaults, None, None)
  files, config, graph = _inputs(case.blocks, default_config, default_graph)
  result = run.run_preprocessor(
      run.write_input_dir({
          **defaults_files,
          **files
      }, config), mode, graph)
  output = result.output
  problems = []
  if result.dc_unexpected:
    problems.append("the fake DC API got requests it does not support"
                    f" (extend lib/dc_stub.py): {result.dc_unexpected}")

  expect_error = case.block("expect-error")
  if output["error"]:
    actual_error = {
        "type": output["error"]["type"],
        "contains": [output["error"]["message"].split("\n")[0][:200]],
    }
    if output["handshake"] and output["handshake"].get("errors"):
      actual_error["handshake_error_types"] = sorted(
          {e.get("errorType") for e in output["handshake"]["errors"]})
    if update and not case.xfail:
      _PENDING_UPDATES[path][case.name] = [("expect-error",
                                            json.dumps(actual_error, indent=2))]
      return []
    if not expect_error:
      return [f"run failed unexpectedly:\n{output['_traceback']}"]
    want = json.loads(expect_error.text)
    if want.get("type") != output["error"]["type"]:
      problems.append(
          f"error type: want {want.get('type')}, got {output['error']['type']}")
    for text in want.get("contains", []):
      if text not in output["error"]["message"]:
        problems.append(f"error message lacks {text!r}:\n"
                        f"{output['error']['message']}")
    if "handshake_error_types" in want:
      got = actual_error.get("handshake_error_types", [])
      if sorted(want["handshake_error_types"]) != got:
        problems.append(f"handshake error types: want "
                        f"{want['handshake_error_types']}, got {got}")
    if any(True for _ in canonical.all_records(output)):
      problems.append("a failed run must not leave JSON-LD output behind")
    return problems
  if expect_error:
    return ["expected the run to fail, but it succeeded"]

  baseline = _baseline(path, mode)
  actual = _records(output)
  added = sorted((actual - baseline).elements())
  removed = sorted((baseline - actual).elements())
  added_records = [json.loads(r) for r in added]
  removed_records = [json.loads(r) for r in removed]
  shared = casefile.load_shared(_parsed(path))
  where = f"{os.path.relpath(path)}:{case.line}"
  if update and not case.xfail:
    blocks = [("expect",
               casefile.format_records(
                   casefile.compact_records(added_records, shared)))]
    if removed_records:
      blocks.append(("expect-removed",
                     casefile.format_records(
                         casefile.compact_records(removed_records, shared))))
    _PENDING_UPDATES[path][case.name] = blocks
    return []
  block = case.block("expect")
  if block is None:
    return [
        "case has no ```expect``` block; run with --update-expected to"
        " generate it, then review it"
    ]
  want = json.loads(block.text)
  want = casefile.resolve_records(want if isinstance(want, list) else [want],
                                  shared, where)
  want_sorted = sorted(
      json.dumps(
          canonical.canonical_record(r), sort_keys=True, ensure_ascii=False)
      for r in want)
  # "@id": "*" matches any id (handy for hand-written observation records,
  # whose ids are content hashes). Resolve wildcards against unmatched records.
  unmatched = collections.Counter(added) - collections.Counter(want_sorted)
  resolved = []
  for w in want_sorted:
    rec = json.loads(w)
    if rec.get("@id") != "*":
      resolved.append(w)
      continue
    rest = {k: v for k, v in rec.items() if k != "@id"}
    hit = next((a for a in unmatched.elements() if {
        k: v for k, v in json.loads(a).items() if k != "@id"
    } == rest), None)
    if hit:
      unmatched[hit] -= 1
      resolved.append(hit)
    else:
      resolved.append(w)
  want_sorted = sorted(resolved)
  if want_sorted != added:
    problems.append("records differ (- expected, + actual):\n" +
                    canonical.diff([json.loads(r)
                                    for r in want_sorted], added_records))
  removed_block = case.block("expect-removed")
  want_removed = casefile.resolve_records(
      json.loads(removed_block.text) if removed_block else [], shared, where)
  want_removed = sorted(
      json.dumps(
          canonical.canonical_record(r), sort_keys=True, ensure_ascii=False)
      for r in want_removed)
  if want_removed != removed:
    problems.append(
        "records of the defaults that the case removes differ (- expected,"
        " + actual; see ```expect-removed```):\n" +
        canonical.diff([json.loads(r) for r in want_removed], removed_records))
  return problems


@pytest.mark.parametrize("path,name", list(_params()))
def test_case(path: str, name: str, update_expected: bool):
  case = next(c for c in _parsed(path).cases if c.name == name)
  failures = []
  for mode in _modes(case):
    problems = _check(case, path, mode, update_expected)
    failures.extend(f"[{mode}] {p}" for p in problems)
  location = f"{os.path.relpath(path)}:{case.line}"
  if case.xfail:
    if not failures:
      pytest.fail(
          f"{location}: marked xfail ({case.xfail}) but now passes;"
          " remove the xfail line",
          pytrace=False)
    pytest.xfail(case.xfail)
  if failures:
    pytest.fail(f"{location} '{name}'\n\n" + "\n\n".join(failures),
                pytrace=False)


def teardown_module(module):  # pylint: disable=unused-argument
  for path, updates in _PENDING_UPDATES.items():
    casefile.write_updates(casefile.parse(path), updates)
  _PENDING_UPDATES.clear()
