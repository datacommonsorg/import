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
Provenances and sources: how they're registered, attached and validated.

Every inputFiles entry names a provenance, which an MCF file must define with
a source. Most tests here replace the shared provenance.mcf (see
_provenance_mcf) to vary that.
"""

import pytest
from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases

_ONE_ROW = b.csv_file(b.obs_row())
# Modes that must give the same result: threaded, and production
# (multiprocessing), where each file is imported by its own worker process.
_MODES = ("threaded", "production")


def _provenance_mcf(with_source: bool = True,
                    **provenance_props: str) -> dict[str, str]:
  """
  A provenance.mcf to use as shared_files: the test provenance and source.

  provenance_props are extra properties of the provenance (raw MCF). With
  with_source=False, the source node is left out and the provenance links to
  no source, unless provenance_props sets one.
  """
  blocks = []
  if with_source:
    blocks.append(
        b.mcf_node(f"dcid:{b.SOURCE}",
                   typeOf="dcs:Source",
                   name='"Test Source"',
                   url='"https://source.example.org"'))
    provenance_props = {"source": f"dcid:{b.SOURCE}", **provenance_props}
  blocks.append(
      b.mcf_node(f"dcid:{b.PROVENANCE}",
                 typeOf="dcs:Provenance",
                 name='"Test Provenance"',
                 **provenance_props))
  return {"provenance.mcf": b.mcf(*blocks)}


# --- Registered nodes ---------------------------------------------------------
#
# Source and Provenance nodes are written twice into the same folder: once
# from the MCF triples and once from the node registry, which adds `domain`
# and, when missing, a `name` equal to the id.


def test_source_url_adds_a_domain():
  result = b.run_mcf(
      b.mcf_node("dcid:OtherSource",
                 typeOf="dcs:Source",
                 name='"Other Source"',
                 url='"https://data.example.com/path"'))
  from_mcf = b.record("OtherSource",
                      "Source",
                      name="Other Source",
                      url="https://data.example.com/path")
  from_registry = {**from_mcf, "dcid:domain": "data.example.com"}
  b.assert_records(result.added, [from_mcf, from_registry])


def test_provenance_without_a_name_gets_its_id_as_name():
  result = b.run_mcf(
      b.mcf_node("dcid:OtherSource2",
                 typeOf="dcs:Source",
                 url='"https://other.example.com"'),
      b.mcf_node("dcid:OtherProvenance",
                 typeOf="dcs:Provenance",
                 source="dcid:OtherSource2"))
  source = b.record("OtherSource2", "Source", url="https://other.example.com")
  provenance = b.record("OtherProvenance",
                        "Provenance",
                        source=b.ref("OtherSource2"))
  b.assert_records(result.added, [
      source,
      {
          **source, "dcid:name": "OtherSource2",
          "dcid:domain": "other.example.com"
      },
      provenance,
      {
          **provenance, "dcid:name": "OtherProvenance"
      },
  ])


# --- Provenance URL -----------------------------------------------------------

_PROVENANCE_URL = "https://source.example.org/dataset"
_PROVENANCE_WITH_URL = _provenance_mcf(url=f'"{_PROVENANCE_URL}"')


@pytest.mark.parametrize("mode", ("threaded", "production"))
def test_provenance_url_stays_on_the_provenance_node(mode):
  """Observations reference their provenance; only that node has its url."""
  result = b.run_csv(_ONE_ROW, mode=mode, shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.observations, [b.obs()])
  provenance_nodes = [
      r for r in result.records if r["@id"] == f"dcid:{b.PROVENANCE}"
  ]
  assert provenance_nodes
  assert all(r.get("dcid:url") == _PROVENANCE_URL for r in provenance_nodes)


# --- Production (multiprocessing) path ----------------------------------------
#
# In production mode each file is imported in its own worker process. MCF
# workers finish first; CSV workers then start from the provenances and
# sources the MCF files registered.

# Several CSV workers, so that without the MCF stage finishing first, a CSV
# worker's result would likely be merged before the MCF one.
_NUM_CSVS = 8
_MANY_CSVS = {
    f"case{i}.csv": b.csv_file(b.obs_row(f"country/C{i}"))
    for i in range(_NUM_CSVS)
}


def test_csv_workers_use_the_provenance_defined_in_mcf():
  """
  CSV workers find the MCF provenance instead of auto-creating their own.

  The record assertions (nothing added to or removed from what the shared MCF
  writes) catch an auto-created provenance replacing the MCF one, which
  depends on merge order.
  """
  result = b.run(_MANY_CSVS,
                 b.obs_config(b.OBS_MAPPINGS),
                 mode="production",
                 shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.of_type("Provenance"), [])
  b.assert_records(result.of_type("Source"), [])
  b.assert_records(result.removed, [])
  b.assert_records(result.observations, [
      b.obs(f"country/C{i}") for i in range(_NUM_CSVS)
  ])


# --- Default provenance and source --------------------------------------------
#
# Files whose provenance is dcid:c/p/default use the built-in default
# provenance and source. They are written only when some file uses them, or
# when there are no MCF-defined metadata nodes at all.
#
# TODO(gmechali): Delete this section in the follow-up PR that removes the
# built-in default provenance and source (Provenance/Source nodes will come
# only from MCF).

_DEFAULT_PROVENANCE = b.record("c/p/default",
                               "Provenance",
                               name="Custom Import",
                               source=b.ref("c/s/default"),
                               url="custom-import")
_DEFAULT_SOURCE = b.record("c/s/default", "Source", name="Custom Data Commons")
_DEFAULT_PROVENANCE_CSV = {
    "pattern": "case*.csv",
    "provenance": "dcid:c/p/default",
    "columnMappings": b.OBS_MAPPINGS,
}


@pytest.mark.parametrize("mode", _MODES)
def test_csv_only_run_writes_the_default_provenance_and_source(mode):
  result = b.run({"case.csv": _ONE_ROW},
                 {"inputFiles": [_DEFAULT_PROVENANCE_CSV]},
                 mode=mode,
                 shared_files={})
  b.assert_records(result.of_type("Provenance"), [_DEFAULT_PROVENANCE])
  b.assert_records(result.of_type("Source"), [_DEFAULT_SOURCE])


@pytest.mark.parametrize("mode", _MODES)
def test_default_provenance_used_next_to_mcf_metadata_is_written(mode):
  result = b.run(
      {"case.csv": _ONE_ROW},
      {"inputFiles": [*b.MCF_CONFIG["inputFiles"], _DEFAULT_PROVENANCE_CSV]},
      mode=mode)
  b.assert_records(result.of_type("Provenance"), [_DEFAULT_PROVENANCE])
  b.assert_records(result.of_type("Source"), [_DEFAULT_SOURCE])


# --- Failed files -------------------------------------------------------------
#
# Every file of a stage runs even if others fail. Once the stage is done, the
# run fails with one error that names every failed file, and an MCF failure
# keeps the CSV stage from starting.

_BAD_VALUE = ("ValueError: Invalid non-numeric value(s) found in observation"
              " values of file")


@pytest.mark.parametrize("mode", _MODES)
def test_every_failed_csv_is_reported_together(mode):
  result = b.run(
      {
          "case_a.csv": b.csv_file(b.obs_row("country/A")),
          "case_bad1.csv": b.csv_file(b.obs_row("country/B", value="abc")),
          "case_bad2.csv": b.csv_file(b.obs_row("country/C", value="xyz")),
          "case_d.csv": b.csv_file(b.obs_row("country/D")),
      },
      b.obs_config(b.OBS_MAPPINGS),
      mode=mode)
  assert result.error["type"] == "FileImportsFailedError"
  assert result.error["message"].startswith("2 files failed to import:\n"
                                            f"  case_bad1.csv: {_BAD_VALUE}")
  assert f"\n  case_bad2.csv: {_BAD_VALUE}" in result.error["message"]


def _file_statuses(result: b.Result) -> dict[str, str]:
  """The status of each file in report.json, by path."""
  return {
      path: entry["status"]
      for path, entry in result.output["report"]["importFiles"].items()
  }


def _observed_places(result: b.Result) -> set[str]:
  """The observationAbout of every written observation."""
  return {
      r["dcid:observationAbout"]["@id"]
      for r in result.records
      if r.get("@type") == "dcid:StatVarObservation"
  }


@pytest.mark.parametrize("mode", _MODES)
def test_files_after_a_failed_one_still_run(mode):
  """
  A failure does not cancel the rest of its stage, and the good files still
  use the provenance defined in MCF.

  Threaded runs write no JSON-LD once a file fails, so the report shows that
  the good files succeeded. Production workers upload their output as they
  finish (leaving it behind is a known gap), so the good files' observations
  show that they ran. The production report does not show
  file statuses; see _REPORT in test_e2e.py.
  """
  result = b.run(
      {
          "case_a.csv": b.csv_file(b.obs_row("country/A")),
          "case_bad.csv": b.csv_file(b.obs_row("country/B", value="abc")),
          "case_c.csv": b.csv_file(b.obs_row("country/C")),
      },
      b.obs_config(b.OBS_MAPPINGS),
      mode=mode,
      shared_files=_PROVENANCE_WITH_URL)
  assert result.error["type"] == "ValueError"
  assert result.error["message"].startswith(
      "Invalid non-numeric value(s) found in observation values of file"
      " 'case_bad.csv'")
  if mode == "threaded":
    assert _file_statuses(result) == {
        "<input>/provenance.mcf": "SUCCESS",
        "<input>/case_a.csv": "SUCCESS",
        "<input>/case_bad.csv": "FAILURE",
        "<input>/case_c.csv": "SUCCESS",
    }
    return
  assert _observed_places(result) == {"dcid:country/A", "dcid:country/C"}


@pytest.mark.parametrize("mode", _MODES)
def test_failed_mcf_files_are_reported_together_and_stop_the_csv_stage(mode):
  result = b.run(
      {
          "bad1.mcf": "typeOf: dcs:Thing\nNode: dcid:T1\n",
          "bad2.mcf": "typeOf: dcs:Thing\nNode: dcid:T2\n",
          "case.csv": _ONE_ROW,
      },
      b.obs_config(b.OBS_MAPPINGS),
      mode=mode)
  assert result.error["type"] == "FileImportsFailedError"
  assert result.error["message"].startswith(
      "2 files failed to import:\n  bad1.mcf: AssertionError:")
  assert "\n  bad2.mcf: AssertionError:" in result.error["message"]
  assert not [
      r for r in result.records if r.get("@type") == "dcid:StatVarObservation"
  ]


# --- Validation ---------------------------------------------------------------

_MISSING_SOURCES = ("Metadata Validation Failed: Linked sources are missing"
                    " for defined provenances:")

INVALID_PROVENANCES = [
    Case("inputFiles entry without a provenance",
         given={
             "inputFiles": [
                 *b.MCF_CONFIG["inputFiles"], {
                     "pattern": "case*.csv",
                     "columnMappings": b.OBS_MAPPINGS
                 }
             ]
         },
         expect=b.Fails(
             "ValueError", "Could not determine import name: missing"
             " 'provenance' configuration for file 'case.csv'.")),
    Case("provenance without a namespace prefix",
         given=b.obs_config(b.OBS_MAPPINGS, provenance=b.PROVENANCE),
         expect=b.Fails(
             "ValueError", "The 'provenance' property must be a valid DCID or"
             f" URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL)."
             f" Found invalid provenance: '{b.PROVENANCE}'")),
    # The message talks about missing *sources*, and the error type is
    # GENERIC_ERROR, although the validator has a MISSING_PROVENANCE type.
    Case("provenance that no MCF defines",
         given=b.obs_config(b.OBS_MAPPINGS,
                            provenance="dcid:UndefinedProvenance"),
         expect=b.Fails("ValueError", _MISSING_SOURCES)),
]


@cases(*INVALID_PROVENANCES)
def test_invalid_provenance_config(given, expect):
  result = b.run_csv(_ONE_ROW, given)
  b.assert_fails(result, expect.error_type, expect.message, expect.handshake)


def test_provenance_without_a_source_fails():
  result = b.run_csv(_ONE_ROW, shared_files=_provenance_mcf(with_source=False))
  b.assert_fails(result, "ValueError", _MISSING_SOURCES)


def test_provenance_linked_to_an_undefined_source_is_accepted():
  result = b.run_csv(_ONE_ROW,
                     shared_files=_provenance_mcf(
                         with_source=False, source="dcid:ElsewhereSource"))
  assert result.error is None, result.error
  b.assert_records(result.observations, [b.obs()])


def test_provenance_declared_only_in_the_config_sources_block_fails():
  """The legacy 'sources' config block is rejected; provenances live in MCF."""
  config = {
      "sources": {
          "Config Source": {
              "url": "https://config-source.example.org",
              "provenances": {
                  "Config Provenance": "https://config-source.example.org/p"
              },
          }
      },
      "inputFiles": [b.obs_config(b.OBS_MAPPINGS)["inputFiles"][-1]],
  }
  result = b.run_csv(_ONE_ROW, config, shared_files={})
  b.assert_fails(result,
                 "ValueError",
                 "Unknown key(s) in config.json: ['sources']",
                 handshake_error_types=())
