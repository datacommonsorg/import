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
"""Runs one scenario in one execution mode and evaluates the result.

Evaluation layers, all applied to the loader view (what the graph ingests):
1. Error scenarios: the run must fail with the declared error, write the
   declared failure handshake, and upload no shards.
2. Golden: rendered facts must equal expected/facts.tsv (generated in
   'threaded' mode).
3. Hand-written expectations from scenario.toml (must_contain,
   must_not_contain, must_contain_observation). These are independent of the
   golden, so a wrong golden cannot be blessed silently.
4. Invariants (see invariants.py).
5. Row oracle (see oracle.py) for variable-per-row scenarios.

Known issues are strict. A declared issue waives one named check in named
modes, and the test fails if that check no longer fails ("no longer
reproduces"), so fixes can't go unnoticed.
"""

import collections
import contextlib
import dataclasses
import inspect
import json
import os
import pathlib
import shutil
import traceback
from typing import Any, Iterator
from unittest import mock

from stats import runner as runner_module
from stats.runner import Runner
from tests.stats.scenario_lib import facts as facts_lib
from tests.stats.scenario_lib import invariants
from tests.stats.scenario_lib import modes as modes_lib
from tests.stats.scenario_lib import oracle
from tests.stats.scenario_lib import spec
from tests.stats.scenario_lib.dc_api_stub import DcApiStub

from util import dc_client

INPUT_DIR_PLACEHOLDER = "{{INPUT_DIR}}"
_MAX_LINES_PER_CHECK = 15

# Other test modules replace dc_client functions with MagicMocks and never
# restore them. Capture the real functions at import (collection) time and put
# them back for every run, so scenario results never depend on test order.
_REAL_DC_CLIENT_FUNCTIONS = {
    name: value
    for name, value in vars(dc_client).items()
    if inspect.isfunction(value) and value.__module__ == dc_client.__name__
}


@dataclasses.dataclass
class RunResult:
  scenario: spec.Scenario
  mode: modes_lib.Mode
  view: facts_lib.LoaderView
  error: BaseException | None
  error_traceback: str
  handshake: dict[str, Any] | None
  dc_requests: list[dict[str, Any]]
  dc_unexpected: list[str]


@contextlib.contextmanager
def _real_dc_client() -> Iterator[None]:
  with contextlib.ExitStack() as stack:
    for name, function in _REAL_DC_CLIENT_FUNCTIONS.items():
      stack.enter_context(mock.patch.object(dc_client, name, function))
    yield


class _CapturingFile:

  def __init__(self, sink: dict[str, Any]) -> None:
    self._sink = sink

  def write(self, content: str) -> None:
    self._sink["payload"] = json.loads(content)


class _CapturingStore:
  """Stands in for the GCS handshake file; records what is written."""

  def __init__(self, sink: dict[str, Any]) -> None:
    self._sink = sink

  def __enter__(self) -> "_CapturingStore":
    return self

  def __exit__(self, *exc_info) -> None:
    pass

  def as_file(self) -> _CapturingFile:
    return _CapturingFile(self._sink)

  def close(self) -> None:
    pass


def _capturing_create_store(sink: dict[str, Any]):
  real_create_store = runner_module.create_store

  def create_store(path, *args, **kwargs):
    if "ingestion_records" in str(path):
      sink["path"] = str(path)
      return _CapturingStore(sink)
    return real_create_store(path, *args, **kwargs)

  return create_store


def _prepare_config_file(scenario: spec.Scenario,
                         input_dir: pathlib.Path) -> str | None:
  """For config-file mode, points dataDownloadUrl at the copied input dir."""
  name = scenario.runner.get("config_file")
  if not name:
    return None
  config_path = input_dir / name
  text = config_path.read_text().replace(INPUT_DIR_PLACEHOLDER, str(input_dir))
  config_path.write_text(text)
  return str(config_path)


def run_scenario(scenario: spec.Scenario, mode: modes_lib.Mode,
                 workdir: pathlib.Path) -> RunResult:
  input_dir = workdir / "input"
  output_dir = workdir / "output"
  shutil.copytree(scenario.input_dir, input_dir)
  config_file = _prepare_config_file(scenario, input_dir)
  runner_kwargs = {
      k: v
      for k, v in scenario.runner.items()
      if k in ("import_names", "import_proxy_entities")
  }

  handshake: dict[str, Any] = {}
  error: BaseException | None = None
  error_traceback = ""
  with DcApiStub(scenario.dc_api) as stub:
    env = {
        "DC_API_ROOT": stub.root,
        "DC_API_KEY": "",
        # Never send stub traffic through an HTTP proxy from the environment.
        "NO_PROXY": "127.0.0.1,localhost",
        "no_proxy": "127.0.0.1,localhost",
        # A local output dir never triggers the success handshake, but a
        # failure is always reported; these make it observable.
        "WORKFLOW_EXECUTION_ID": "scenario-test",
        "TEMP_LOCATION": "gs://scenario-test-bucket/tmp",
        **mode.env,
    }
    with mock.patch.dict(os.environ, env), \
         _real_dc_client(), \
         mock.patch("stats.jsonld_stream_writer._KNOWN_CUSTOM_PREFIXES",
                    set()), \
         mock.patch("stats.runner.create_store",
                    _capturing_create_store(handshake)), \
         mode.patches():
      try:
        Runner(config_file_path=config_file,
               input_dir_path=None if config_file else str(input_dir),
               output_dir_path=str(output_dir),
               use_multiprocessing=mode.use_multiprocessing,
               **runner_kwargs).run()
      except Exception as e:  # pylint: disable=broad-except
        error = e
        error_traceback = "".join(traceback.format_exception(e))
    dc_requests = list(stub.requests)
    dc_unexpected = list(stub.unexpected)

  return RunResult(scenario=scenario,
                   mode=mode,
                   view=facts_lib.load_loader_view(output_dir / "jsonld"),
                   error=error,
                   error_traceback=error_traceback,
                   handshake=handshake.get("payload"),
                   dc_requests=dc_requests,
                   dc_unexpected=dc_unexpected)


class _Evaluation:
  """Collects problems, applying strict known-issue waivers per check."""

  def __init__(self, scenario: spec.Scenario, mode: str) -> None:
    self.scenario = scenario
    self.mode = mode
    self.problems: list[str] = []
    self._evaluated_checks: set[str] = set()
    self._reproduced: set[tuple[str, str]] = set()

  def consider(self, check: str, failures: list[str]) -> None:
    self._evaluated_checks.add(check)
    issues = self.scenario.known_issues_for(check, self.mode)
    if failures and issues:
      self._reproduced.update((i.id, check) for i in issues)
      return
    shown = failures[:_MAX_LINES_PER_CHECK]
    self.problems.extend(f"[{check}] {f}" for f in shown)
    if len(failures) > len(shown):
      self.problems.append(
          f"[{check}] ... and {len(failures) - len(shown)} more")

  def finish(self) -> list[str]:
    for issue in self.scenario.known_issues:
      if self.mode not in issue.modes:
        continue
      if issue.check not in self._evaluated_checks:
        continue
      if (issue.id, issue.check) not in self._reproduced:
        self.problems.append(
            f"Known issue {issue.id} ({issue.check}) no longer reproduces in"
            f" mode '{self.mode}'. If it was fixed, remove it from"
            f" {self.scenario.name}/scenario.toml (and regenerate the golden"
            " if needed).")
    return self.problems


def evaluate(result: RunResult, write_golden: bool = False) -> list[str]:
  """Returns human-readable problems; an empty list means the run is good."""
  scenario = result.scenario
  evaluation = _Evaluation(scenario, result.mode.name)

  if scenario.expect == "error":
    _evaluate_expected_error(result, evaluation)
    return evaluation.finish()

  if result.error is not None:
    evaluation.consider("error", [
        f"run raised {type(result.error).__name__}: {result.error}\n"
        f"{result.error_traceback}"
    ])
    return evaluation.finish()
  evaluation.consider("error", [])

  rendered = result.view.render()
  if write_golden:
    scenario.expected_dir.mkdir(parents=True, exist_ok=True)
    scenario.facts_golden.write_text(rendered, encoding="utf-8")
  else:
    evaluation.consider("golden", _golden_diff(scenario, rendered))

  facts = result.view.facts
  evaluation.consider("must_contain",
                      _check_expectations(scenario, facts, result.view))

  violations = collections.defaultdict(list)
  for violation in invariants.check_all(result.view):
    violations[violation.check].append(violation.detail)
  for check in spec.INVARIANT_CHECKS:
    evaluation.consider(check, violations.get(check, []))

  if scenario.oracle == "vpr":
    evaluation.consider(
        "row_oracle",
        [v.detail for v in oracle.check_rows(scenario, result.view)])

  if result.dc_unexpected:
    evaluation.problems.append(
        f"[dc_api] unexpected requests: {result.dc_unexpected}")
  return evaluation.finish()


def _evaluate_expected_error(result: RunResult,
                             evaluation: _Evaluation) -> None:
  expected = result.scenario.error
  assert expected is not None
  if result.error is None:
    evaluation.problems.append(
        f"expected the run to fail with {expected.type}, but it succeeded")
    return
  actual_type = type(result.error).__name__
  message = str(result.error)
  if actual_type != expected.type:
    evaluation.problems.append(
        f"expected {expected.type}, got {actual_type}: {message}")
  for fragment in expected.message_contains:
    if fragment not in message:
      evaluation.problems.append(
          f"error message is missing {fragment!r}: {message}")
  if expected.handshake_error_types:
    payload = result.handshake or {}
    if payload.get("status") != "FAILURE":
      evaluation.problems.append(
          f"expected a FAILURE handshake, got {payload or 'none'}")
    else:
      actual = sorted(e.get("errorType", "") for e in payload.get("errors", []))
      if actual != sorted(expected.handshake_error_types):
        evaluation.problems.append(
            f"handshake errorTypes {actual} !="
            f" {sorted(expected.handshake_error_types)}: {payload}")
  if result.view.shard_count:
    evaluation.problems.append(
        f"a failed run must not upload output, found {result.view.shard_count}"
        " JSON-LD shard(s)")


def _golden_diff(scenario: spec.Scenario, rendered: str) -> list[str]:
  if not scenario.facts_golden.exists():
    return [
        f"missing golden {scenario.facts_golden.relative_to(spec.SCENARIOS_DIR)}"
        "; generate it with TEST_MODE=write"
    ]
  expected = facts_lib.parse_rendered(
      scenario.facts_golden.read_text(encoding="utf-8"))
  actual = facts_lib.parse_rendered(rendered)
  if expected == actual:
    return []
  removed = sorted(set(expected) - set(actual))
  added = sorted(set(actual) - set(expected))
  lines = [f"facts differ from golden: -{len(removed)} +{len(added)}"]
  lines.extend(f"- {line}" for line in removed[:_MAX_LINES_PER_CHECK])
  lines.extend(f"+ {line}" for line in added[:_MAX_LINES_PER_CHECK])
  if not removed and not added:
    lines.append("(same facts, different order or multiplicity)")
  return ["\n".join(lines)]


def _check_expectations(scenario: spec.Scenario, facts: set[facts_lib.Fact],
                        view: facts_lib.LoaderView) -> list[str]:
  failures = []
  for pattern in scenario.must_contain:
    if not any(pattern.matches(f) for f in facts):
      failures.append(f"missing expected fact: {pattern.describe()}")
  for pattern in scenario.must_not_contain:
    hits = sorted(f.line() for f in facts if pattern.matches(f))
    if hits:
      failures.append(f"forbidden fact present ({pattern.describe()}): "
                      f"{hits[:3]}")
  for pattern in scenario.must_contain_observation:
    if not any(
        _observation_matches(pattern, r) for r in view.observation_records):
      failures.append(f"missing expected observation: {pattern.describe()}")
  return failures


def _observation_matches(pattern: spec.ObservationPattern,
                         record: facts_lib.Record) -> bool:
  predicates = {
      "about": "observationAbout",
      "variable": "variableMeasured",
      "date": "observationDate",
  }
  for field, wanted in pattern.fields.items():
    if field == "folder":
      if record.folder != wanted:
        return False
      continue
    values = record.objects(predicates.get(field, field))
    if not any(_same_value(v, wanted) for v in values):
      return False
  return True


def _same_value(fact: facts_lib.Fact, wanted: Any) -> bool:
  if fact.kind == facts_lib.KIND_LIT:
    actual = facts_lib.decode_literal(fact.obj)
  else:
    actual = fact.obj
  if isinstance(wanted, (int, float)) and not isinstance(wanted, bool):
    try:
      return float(actual) == float(wanted)
    except (TypeError, ValueError):
      return False
  return str(actual) == str(wanted)
