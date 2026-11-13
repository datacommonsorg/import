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
"""
Builders for test inputs and expected JSON-LD records.

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

# The CSV header obs_row() fills, and columnMappings for it to pass to
# obs_config() as a convenience.
OBS_HEADER = ["about", "variable", "date", "value"]
OBS_MAPPINGS = {
    "dcid:observationAbout": "about",
    "dcid:variableMeasured": "variable",
    "dcid:observationDate": "date",
    "dcid:value": "value",
}


def obs_config(mappings: dict[str, str],
               pattern: str = "case*.csv",
               **file_options: Any) -> dict:
  """
  Config importing *.mcf and variable-per-row CSVs matching pattern.

  mappings is the CSV's columnMappings, e.g. OBS_MAPPINGS. file_options are
  added to the CSV's inputFiles entry, e.g.
  obs_config(OBS_MAPPINGS, ignoreColumns=["notes"]).
  """
  return {
      "inputFiles": [
          *MCF_CONFIG["inputFiles"],
          {
              "pattern": pattern,
              "provenance": f"dcid:{PROVENANCE}",
              "columnMappings": mappings,
              **file_options,
          },
      ]
  }


def same_name_mappings(*columns: str) -> dict[str, str]:
  """columnMappings that map each column to the property of the same name."""
  return {f"dcid:{column}": column for column in columns}


def entity_mappings(*columns: str, dcid: str = "author_id") -> dict[str, str]:
  """
  columnMappings of an entities CSV: its dcid column, its "type" column, and
  same_name_mappings(*columns).
  """
  return {
      "dcid:dcid": dcid,
      "dcid:typeOf": "type",
      **same_name_mappings(*columns)
  }


# columnMappings of an author_id, type, author_name, author_country CSV, a
# convenience for entities_config().
AUTHOR_MAPPINGS = entity_mappings("author_name", "author_country")


def entities_config(*, mappings: dict[str, str], **file_options: Any) -> dict:
  """
  Config importing *.mcf and case*.csv as entities.

  mappings is the CSV's columnMappings, e.g. AUTHOR_MAPPINGS or
  entity_mappings(...). file_options are added to the CSV's inputFiles entry.
  """
  entry = {
      "pattern": "case*.csv",
      "importType": "entities",
      "provenance": f"dcid:{PROVENANCE}",
      "columnMappings": mappings,
      **file_options,
  }
  return {"inputFiles": [*MCF_CONFIG["inputFiles"], entry]}


def without_column_mappings(config: dict) -> dict:
  """A copy of config with columnMappings left out of every inputFiles entry."""
  entries = [{
      key: value for key, value in entry.items() if key != "columnMappings"
  } for entry in config["inputFiles"]]
  return {**config, "inputFiles": entries}


# --- Known bugs and parametrized cases ----------------------------------------

# A strict xfail for a known bug: the test fails with XPASS once the bug is
# fixed, so the marker has to be removed then.
known_bug = functools.partial(pytest.mark.xfail, strict=True)


@dataclasses.dataclass(frozen=True)
class Case:
  """
  One row of a parametrized test.

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
  """
  Parametrizes a test(given, expect) with Cases.

  @cases(*VALUES)
  def test_value(given, expect): ...
  """
  return pytest.mark.parametrize("given,expect", [c.param() for c in all_cases])


# --- Input builders -----------------------------------------------------------


def mcf_node(subject: str,
             typeOf: str | None = "dcs:Thing",
             **props: str) -> str:  # pylint: disable=invalid-name
  """
  One MCF block. All values are raw MCF text.

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


def property_mcf(*names: str) -> str:
  """MCF defining a Property node, named after itself, for each of names."""
  return mcf(*(mcf_node(f"dcid:{name}", typeOf="dcs:Property", name=f'"{name}"')
               for name in names))


def obs_row(about: str = "country/USA",
            variable: str = "Count_Person",
            date: str = "2020",
            value: str = "1") -> list[str]:
  """
  One row for OBS_HEADER. All cells are raw CSV text.

  about is the observationAbout cell: the place or other entity the
  observation is about.
  """
  return [about, variable, date, value]


def csv_file(*rows: list[str],
             header: list[str] | None = None,
             quote_all: bool = False,
             line_end: str = "\n",
             bom: bool = False) -> str:
  """
  A CSV file. header defaults to OBS_HEADER.

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


def import_subdir(name: str,
                  csv_text: str | None = None,
                  *,
                  provenance: str = PROVENANCE,
                  source: str = SOURCE) -> dict[str, str]:
  """
  The files of import subdirectory name, for run_dir().

  Its config.json imports *.mcf and case*.csv under provenance, and its
  provenance.mcf defines provenance and source (with a url derived from
  source). csv_text defaults to one obs_row().
  """
  provenance_ref = f"dcid:{provenance}"
  config = {
      "inputFiles": [
          {
              "pattern": "*.mcf",
              "provenance": provenance_ref
          },
          {
              "pattern": "case*.csv",
              "provenance": provenance_ref,
              "columnMappings": OBS_MAPPINGS
          },
      ]
  }
  provenance_mcf = mcf(
      mcf_node(f"dcid:{source}",
               typeOf="dcs:Source",
               url=f'"https://{source.lower()}.example.org"'),
      mcf_node(provenance_ref, typeOf="dcs:Provenance",
               source=f"dcid:{source}"))
  return {
      f"{name}/config.json": json.dumps(config),
      f"{name}/provenance.mcf": provenance_mcf,
      f"{name}/case.csv": csv_text or csv_file(obs_row()),
  }


# --- Expectation builders -----------------------------------------------------


def ref(dcid: str) -> dict:
  """
  An object reference as written in JSON-LD.

  ref("A") -> {"@id": "dcid:A"}. Full URIs are kept as they are.
  """
  if "://" in dcid or dcid.startswith("dcid:"):
    return {"@id": dcid}
  return {"@id": f"dcid:{dcid}"}


def record(dcid: str,
           types: str | list[str] | None = None,
           **props: Any) -> dict:
  """
  An expected JSON-LD node record.

  record("T", "Thing", name="x", parent=ref("P")) ->
    {"@id": "dcid:T", "@type": "dcid:Thing", "dcid:name": "x",
     "dcid:parent": {"@id": "dcid:P"}}

  A list of types becomes a list of "@type" values. Property names that are
  not Python identifiers can be passed with **{"undata:sourceLink": ...}.
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
        **props: Any) -> dict:
  """
  An expected observation record, without "@id".

  obs() is the observation obs_row() produces. Observation ids are content
  hashes; compare them through Result.observations, which drops them (the e2e
  expected.json files pin exact ids).

  props are other observation properties with their JSON-LD values, e.g.
  destination=ref("country/FRA") or unit=ref("USDollar"). Which properties
  are dimensions is declared on the variable, not on the observation: check
  it with Result.dimensions().
  """
  out: dict[str, Any] = {
      "@type": "dcid:StatVarObservation",
      "dcid:observationAbout": ref(about),
      "dcid:observationDate": date,
      "dcid:provenance": ref(PROVENANCE),
      "dcid:value": value,
      "dcid:variableMeasured": ref(variable),
  }
  for prop, value_ in props.items():
    out[f"dcid:{prop}"] = value_
  return out


def entity(dcid: str, row_type: str = "Author", **props: Any) -> dict:
  """An expected row of an entities import: typed, included in PROVENANCE."""
  return record(dcid, row_type, includedIn=ref(PROVENANCE), **props)


def property_node(name: str) -> dict:
  """The Property node that property_mcf(name) defines."""
  return record(name, "Property", name=name)


# --- Running ------------------------------------------------------------------

_OBSERVATION_TYPE = "dcid:StatVarObservation"
_VARIABLE_MEASURED = "dcid:variableMeasured"
_OBSERVATION_PROPERTIES = "dcid:observationProperties"


def _dimension_names(declared: Any) -> list[str]:
  values = declared if isinstance(declared, list) else [declared]
  ids = [v["@id"] if isinstance(v, dict) else v for v in values]
  return [i.removeprefix("dcid:") for i in ids]


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
    """
    Added observation records, without their "@id" (see obs()).

    variableMeasured is reduced to the variable's id: statements about the
    variable embedded in it (observationProperties, today) are checked with
    dimensions().
    """
    out = []
    for r in self.added:
      if r.get("@type") != _OBSERVATION_TYPE:
        continue
      r = {k: v for k, v in r.items() if k != "@id"}
      variable = r.get(_VARIABLE_MEASURED)
      if isinstance(variable, dict):
        r[_VARIABLE_MEASURED] = {"@id": variable["@id"]}
      out.append(r)
    return out

  def dimensions(self, variable: str) -> list[str] | None:
    """
    The observationProperties declared for variable, in order, or None.

    Reads them wherever the run declares them: on the variable's node record,
    or embedded in the observations' variableMeasured (what CSV imports do
    today). Names are returned without "dcid:", whether written as strings or
    references. Every declaration must agree.
    """
    declared = []
    for r in self.added:
      if r.get("@id") == f"dcid:{variable}":
        declared.append(r.get(_OBSERVATION_PROPERTIES))
      embedded = r.get(_VARIABLE_MEASURED)
      if (isinstance(embedded, dict) and
          embedded.get("@id") == f"dcid:{variable}"):
        declared.append(embedded.get(_OBSERVATION_PROPERTIES))
    names = {tuple(_dimension_names(d)) for d in declared if d is not None}
    assert len(names) <= 1, f"conflicting observationProperties: {names}"
    return list(names.pop()) if names else None

  @property
  def nodes(self) -> list[dict]:
    """Added records that are not observations."""
    return [r for r in self.added if r.get("@type") != _OBSERVATION_TYPE]

  def of_type(self, type_name: str) -> list[dict]:
    """Added records of one type name, e.g. "Author"."""
    return [r for r in self.added if r.get("@type") == f"dcid:{type_name}"]

  @property
  def records(self) -> list[dict]:
    """Every record the run wrote, shared or not."""
    return _records(_record_counts(self.output))

  def folders_of(self, record_id: str) -> list[str]:
    """The import folder of every copy of a record, e.g. "dcid:X", sorted."""
    return sorted(folder for folder, _, record in self.output["records"]
                  if record.get("@id") == record_id)


def run(files: dict[str, str],
        config: dict | None,
        mode: str = runlib.DEFAULT_MODE,
        shared_files: dict[str, str] | None = None) -> Result:
  """
  Runs the preprocessor on files plus the shared files.

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
  shared_only = (
      _record_counts(  # What the shared files write on their own.
          _run(_freeze(shared_files), _freeze(MCF_CONFIG), mode))
      if shared_files else collections.Counter())
  actual = _record_counts(output)
  return Result(output=output,
                added=_records(actual - shared_only),
                removed=_records(shared_only - actual))


def run_dir(files: dict[str, str],
            config: dict | None = None,
            *,
            mode: str = runlib.DEFAULT_MODE,
            runner_kwargs: dict | None = None) -> Result:
  """
  Runs the preprocessor on a raw directory layout without shared files.

  Used for multi-import parent directories (runner_kwargs={"import_names": ...})
  where shared-provenance subtraction does not apply.
  """
  input_dir = runlib.write_input_dir(files, config)
  try:
    output = runlib.run_preprocessor(input_dir,
                                     mode,
                                     runner_kwargs=runner_kwargs).output
  finally:
    shutil.rmtree(input_dir, ignore_errors=True)
  if output["error"]:
    return Result(output=output, added=[], removed=[])
  return Result(output=output,
                added=_records(_record_counts(output)),
                removed=[])


def run_mcf(*blocks: str, mode: str = runlib.DEFAULT_MODE) -> Result:
  """Runs MCF blocks as one nodes.mcf file under MCF_CONFIG."""
  return run({"nodes.mcf": mcf(*blocks)}, MCF_CONFIG, mode)


def run_csv(csv_text: str,
            config: dict | None = None,
            mode: str = runlib.DEFAULT_MODE,
            **run_kwargs: Any) -> Result:
  """
  Runs csv_text as case.csv.

  config defaults to obs_config(OBS_MAPPINGS), for a CSV with OBS_HEADER.
  """
  if config is None:
    config = obs_config(OBS_MAPPINGS)
  return run({"case.csv": csv_text}, config, mode, **run_kwargs)


def _freeze(value: dict | None) -> str:
  """JSON for value, keeping key order (columnMappings order is input)."""
  return json.dumps(value)


@functools.lru_cache(maxsize=256)
def _run(files_json: str, config_json: str, mode: str) -> dict:
  """Runs once per input; the shared-files run is reused across tests."""
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
  """
  A hashable, order-preserving form of rec.

  Top-level multi-value lists are already sorted by load_jsonld; nested lists
  (e.g. observationProperties) keep their order because it is meaningful.
  """
  return json.dumps(rec, sort_keys=True, ensure_ascii=False)


# --- Assertions ---------------------------------------------------------------


@dataclasses.dataclass(frozen=True)
class Fails:
  """
  How a run is expected to fail, for a Case's expect (see assert_fails).

  handshake lists the error types of the FAILURE handshake.
  """

  error_type: str
  message: str
  handshake: tuple[str, ...] = ("GENERIC_ERROR",)


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
  """
  The run failed with this error and reported it.

  Not checked: that the run wrote no JSON-LD. Workers upload their output as
  they finish, before metadata is validated, so a failed run can leave
  output behind.
  """
  error = result.error
  if error is None:
    pytest.fail("expected the run to fail, but it succeeded", pytrace=False)
  problems = []
  if error["type"] != error_type:
    problems.append(f"error type: want {error_type}, got {error['type']}")
  if message not in error["message"]:
    problems.append(f"error message lacks {message!r}:\n{error['message']}")
  handshake = result.output["handshake"] or {}
  got_types = sorted({e.get("errorType") for e in handshake.get("errors", [])})
  if got_types != sorted(handshake_error_types):
    problems.append(f"handshake error types: want"
                    f" {sorted(handshake_error_types)}, got {got_types}")
  if problems:
    pytest.fail("\n".join(problems), pytrace=False)
