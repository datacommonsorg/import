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
"""Builders for test inputs and expected JSON-LD records.

Inputs and expectations are built separately, on purpose:

  * Input builders (mcf_node, mcf) write exactly the text the preprocessor
    reads. Values are raw MCF, e.g. name='"Paris, France"' or count='42'.
  * Expectation builders (record, ref) write the JSON-LD a test expects.
    Values are Python values, e.g. name="Paris, France" or count=42.

Nothing here computes an expected record from an input. If it did, the
builders would become a second implementation of the preprocessor.

Typical tests:

  def test_quoted_string_with_comma_is_one_value():
    result = run_mcf(mcf_node("dcid:T", name='"Paris, France"'))
    assert_records(result.added, [record("T", "Thing", name="Paris, France")])

  VALUES = [Case("integer", given="42", expect=42), ...]

  @cases(*VALUES)
  def test_value(given, expect):
    result = run_mcf(mcf_node("dcid:T", prop=given))
    assert_records(result.added, [record("T", "Thing", prop=expect)])
"""

import collections
import csv
import dataclasses
import functools
import io
import json
import shutil
from typing import Any

import pytest
from testsuite.lib import canonical
from testsuite.lib import run as runlib

# --- Shared inputs ------------------------------------------------------------

PROVENANCE = "TestProvenance"
SOURCE = "TestSource"

# The provenance and source every test imports under. Records these produce
# on their own are subtracted from Result.added (see run()).
PROVENANCE_MCF = f"""Node: dcid:{SOURCE}
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:{PROVENANCE}
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:{SOURCE}
"""

# Imports every *.mcf file under the test provenance.
MCF_CONFIG = {
    "inputFiles": [{
        "pattern": "*.mcf",
        "provenance": f"dcid:{PROVENANCE}"
    }]
}

# The CSV header obs_row() fills, and how obs_config() maps it by default.
OBS_HEADER = ["entity", "variable", "date", "value"]
OBS_MAPPINGS = {
    "dcid:observationAbout": "entity",
    "dcid:variableMeasured": "variable",
    "dcid:observationDate": "date",
    "dcid:value": "value",
}


def obs_config(mappings: dict[str, str] | None = None,
               pattern: str = "case*.csv",
               **file_options: Any) -> dict:
  """Config importing *.mcf and variable-per-row CSVs matching pattern.

  mappings defaults to OBS_MAPPINGS. file_options are added to the CSV's
  inputFiles entry, e.g. obs_config(ignoreColumns=["notes"]).
  """
  return {
      "inputFiles": [
          *MCF_CONFIG["inputFiles"],
          {
              "pattern": pattern,
              "provenance": f"dcid:{PROVENANCE}",
              "columnMappings": mappings or OBS_MAPPINGS,
              **file_options,
          },
      ]
  }


def entities_config(row_type: str = "Author",
                    id_column: str | None = "author_id",
                    **file_options: Any) -> dict:
  """Config importing *.mcf and case*.csv as entities of row_type.

  Pass id_column=None for no idColumn. file_options are added to the CSV's
  inputFiles entry.
  """
  entry = {
      "pattern": "case*.csv",
      "importType": "entities",
      "rowEntityType": row_type,
      "provenance": f"dcid:{PROVENANCE}",
      **file_options,
  }
  if id_column:
    entry["idColumn"] = id_column
  return {"inputFiles": [*MCF_CONFIG["inputFiles"], entry]}


# --- Known bugs and parametrized cases ----------------------------------------

# A strict xfail for a known bug: the test fails with XPASS once the bug is
# fixed, so the marker has to be removed then.
known_bug = functools.partial(pytest.mark.xfail, strict=True)


@dataclasses.dataclass(frozen=True)
class Case:
  """One row of a parametrized test.

  Case("integer", given="42", expect=42)
  Case("quoted number", given='"00123"', expect="00123", bug="loses zeros")
  """
  name: str  # The pytest id, e.g. test_value[integer].
  given: Any  # The input, written as the preprocessor reads it.
  expect: Any  # The expected output.
  bug: str | None = None  # A known bug: the case becomes a strict xfail.

  def param(self):
    marks = [known_bug(reason=self.bug)] if self.bug else []
    return pytest.param(self.given, self.expect, id=self.name, marks=marks)


def cases(*all_cases: Case):
  """Parametrizes a test(given, expect) with Cases.

  @cases(*VALUES)
  def test_value(given, expect): ...
  """
  return pytest.mark.parametrize("given,expect", [c.param() for c in all_cases])


# --- Input builders -----------------------------------------------------------


def mcf_node(subject: str,
             typeOf: str | None = "dcs:Thing",
             **props: str) -> str:  # pylint: disable=invalid-name
  """One MCF block. All values are raw MCF text.

  mcf_node("dcid:T", name='"x"') ->
    Node: dcid:T
    typeOf: dcs:Thing
    name: "x"

  Pass typeOf=None to leave the typeOf line out. Property names that are not
  Python identifiers can be passed with **{"name/with/slash": ...}.
  """
  lines = [f"Node: {subject}"]
  if typeOf is not None:
    lines.append(f"typeOf: {typeOf}")
  lines += [f"{prop}: {value}" for prop, value in props.items()]
  return "\n".join(lines) + "\n"


def mcf(*blocks: str) -> str:
  """Joins MCF blocks (from mcf_node or raw text) into one file."""
  return "\n".join(block.strip("\n") + "\n" for block in blocks)


def obs_row(entity: str = "country/USA",
            variable: str = "Count_Person",
            date: str = "2020",
            value: str = "1") -> list[str]:
  """One row for OBS_HEADER. All cells are raw CSV text."""
  return [entity, variable, date, value]


def csv_file(*rows: list[str],
             header: list[str] | None = None,
             quote_all: bool = False,
             line_end: str = "\n",
             bom: bool = False) -> str:
  """A CSV file. header defaults to OBS_HEADER.

  Cells are quoted only where CSV needs it (commas, quotes, newlines), unless
  quote_all is set.
  """
  out = io.StringIO()
  writer = csv.writer(out,
                      quoting=csv.QUOTE_ALL if quote_all else csv.QUOTE_MINIMAL,
                      lineterminator=line_end)
  writer.writerow(header or OBS_HEADER)
  writer.writerows(rows)
  return ("\ufeff" if bom else "") + out.getvalue()


# --- Expectation builders -----------------------------------------------------


def ref(dcid: str) -> dict:
  """An object reference as written in JSON-LD: ref("A") -> {"@id": "dcid:A"}.

  Full URIs are kept as they are.
  """
  if "://" in dcid or dcid.startswith("dcid:"):
    return {"@id": dcid}
  return {"@id": f"dcid:{dcid}"}


def record(dcid: str,
           types: str | list[str] | None = None,
           **props: Any) -> dict:
  """An expected JSON-LD node record.

  record("T", "Thing", name="x", parent=ref("P")) ->
    {"@id": "dcid:T", "@type": "dcid:Thing", "dcid:name": "x",
     "dcid:parent": {"@id": "dcid:P"}}

  A list of types becomes a list of "@type" values. Property names that are
  not Python identifiers can be passed with **{"c/prop/1": ...}.
  """
  out: dict[str, Any] = {"@id": f"dcid:{dcid}"}
  if isinstance(types, str):
    out["@type"] = f"dcid:{types}"
  elif types:
    out["@type"] = [f"dcid:{t}" for t in types]
  for prop, value in props.items():
    out[f"dcid:{prop}"] = value
  return out


def obs(about: str = "country/USA",
        variable: str = "Count_Person",
        date: Any = 2020,
        value: Any = 1,
        dims: dict[str, Any] | None = None,
        **props: Any) -> dict:
  """An expected observation record, without "@id".

  obs() is the observation obs_row() produces. Observation ids are content
  hashes; compare them through Result.observations, which drops them (the e2e
  expected.json files pin exact ids).

  dims are properties that are also listed in observationProperties (extra
  entity dimensions and custom properties), with their JSON-LD values, e.g.
  dims={"destination": ref("country/FRA")} or dims={"sex": "Female"}.
  props are other observation properties, e.g. unit=ref("USDollar").
  """
  dims = dims or {}
  out: dict[str, Any] = {
      "@type": "dcid:StatVarObservation",
      "dcid:observationAbout": ref(about),
      "dcid:observationDate": date,
      "dcid:provenance": ref(PROVENANCE),
      "dcid:value": value,
      "dcid:variableMeasured": {
          **ref(variable), "dcid:observationProperties": [
              "dcid:observationAbout", *(f"dcid:{d}" for d in dims)
          ]
      },
  }
  for dim, dim_value in dims.items():
    out[f"dcid:{dim}"] = dim_value
  for prop, value_ in props.items():
    out[f"dcid:{prop}"] = value_
  return out


def entity(dcid: str, row_type: str = "Author", **props: Any) -> dict:
  """An expected row of an entities import: typed, included in PROVENANCE."""
  return record(dcid, row_type, includedIn=ref(PROVENANCE), **props)


def class_node(name: str) -> dict:
  """The Class node an entities import writes for its row type."""
  return record(name,
                "Class",
                name=name,
                includedIn=[ref(PROVENANCE), ref(SOURCE)])


def property_node(name: str, dcid: str | None = None) -> dict:
  """The Property node written for a CSV column (dcid defaults to name)."""
  return record(dcid or name, "Property", name=name)


# --- Running ------------------------------------------------------------------

_OBSERVATION_TYPE = "dcid:StatVarObservation"


@dataclasses.dataclass
class Result:
  output: dict  # Canonical output of the run (see canonical.py).
  added: list[dict]  # Records not produced by the shared inputs alone.
  removed: list[dict]  # Shared-input records this run no longer produces.

  @property
  def error(self) -> dict | None:
    return self.output["error"]

  @property
  def observations(self) -> list[dict]:
    """Added observation records, without their "@id" (see obs())."""
    return [{
        k: v for k, v in r.items() if k != "@id"
    } for r in self.added if r.get("@type") == _OBSERVATION_TYPE]

  @property
  def nodes(self) -> list[dict]:
    """Added records that are not observations."""
    return [r for r in self.added if r.get("@type") != _OBSERVATION_TYPE]

  @property
  def records(self) -> list[dict]:
    """Every record the run wrote, shared or not."""
    return _records(_record_counts(self.output))


def run(files: dict[str, str],
        config: dict | None,
        mode: str = "threaded",
        shared_files: dict[str, str] | None = None) -> Result:
  """Runs the preprocessor on files plus the shared files.

  config is written as config.json; None leaves config.json out.

  shared_files defaults to {"provenance.mcf": PROVENANCE_MCF}. Records that
  the shared files produce on their own (under MCF_CONFIG, in the same mode)
  are left out of Result.added, so tests only list what their own input and
  config add. Shared records the run no longer writes are in Result.removed.
  """
  if shared_files is None:
    shared_files = {"provenance.mcf": PROVENANCE_MCF}
  output = _run(_freeze({**shared_files, **files}), _freeze(config), mode)
  if output["error"]:
    return Result(output=output, added=[], removed=[])
  # What the shared files write on their own, so tests can leave it out.
  shared_only = _record_counts(
      _run(_freeze(shared_files), _freeze(MCF_CONFIG), mode))
  actual = _record_counts(output)
  return Result(output=output,
                added=_records(actual - shared_only),
                removed=_records(shared_only - actual))


def run_mcf(*blocks: str, mode: str = "threaded") -> Result:
  """Runs MCF blocks as one nodes.mcf file under MCF_CONFIG."""
  return run({"nodes.mcf": mcf(*blocks)}, MCF_CONFIG, mode)


def run_csv(csv_text: str,
            config: dict | None = None,
            mode: str = "threaded",
            **run_kwargs: Any) -> Result:
  """Runs csv_text as case.csv. config defaults to obs_config()."""
  if config is None:
    config = obs_config()
  return run({"case.csv": csv_text}, config, mode, **run_kwargs)


def _freeze(value: dict | None) -> str:
  return json.dumps(value, sort_keys=True)


@functools.lru_cache(maxsize=256)
def _run(files_json: str, config_json: str, mode: str) -> dict:
  # Cached: the shared-files run is the same for most tests in a file.
  input_dir = runlib.write_input_dir(json.loads(files_json),
                                     json.loads(config_json))
  try:
    return runlib.run_preprocessor(input_dir, mode).output
  finally:
    shutil.rmtree(input_dir, ignore_errors=True)


def _record_counts(output: dict) -> collections.Counter:
  return collections.Counter(_key(rec) for _, _, rec in output["records"])


def _records(counts: collections.Counter) -> list[dict]:
  return [json.loads(k) for k in sorted(counts.elements())]


def _key(rec: dict) -> str:
  return json.dumps(_sorted_lists(rec), sort_keys=True, ensure_ascii=False)


def _sorted_lists(value: Any) -> Any:
  """Sorts every list, at any depth: multi-values have no order in JSON-LD."""
  if isinstance(value, dict):
    return {k: _sorted_lists(v) for k, v in value.items()}
  if isinstance(value, list):
    return sorted((_sorted_lists(v) for v in value),
                  key=lambda v: json.dumps(v, sort_keys=True))
  return value


# --- Assertions ---------------------------------------------------------------


def assert_records(actual: list[dict], expected: list[dict]) -> None:
  """Order-insensitive comparison of record lists (duplicates count)."""
  want = sorted(_key(r) for r in expected)
  got = sorted(_key(r) for r in actual)
  if want != got:
    pytest.fail("records differ (- expected, + actual):\n" + canonical.diff(
        [json.loads(r) for r in want], [json.loads(r) for r in got]),
                pytrace=False)


def assert_fails(
    result: Result,
    error_type: str,
    message: str,
    handshake_error_types: tuple[str, ...] = ("GENERIC_ERROR",)
) -> None:
  """The run failed with this error, reported it, and wrote no JSON-LD."""
  error = result.error
  if error is None:
    pytest.fail("expected the run to fail, but it succeeded", pytrace=False)
  assert error["type"] == error_type, error
  assert message in error["message"], error["message"]
  handshake = result.output["handshake"] or {}
  got_types = sorted({e.get("errorType") for e in handshake.get("errors", [])})
  assert got_types == sorted(handshake_error_types), handshake
  leftovers = result.output["records"]
  assert not leftovers, f"a failed run left JSON-LD behind: {leftovers[:3]}"
