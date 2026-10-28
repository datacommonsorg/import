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
"""Provenances and sources: how they're registered, attached and validated.

Every inputFiles entry names a provenance, which an MCF file must define with
a source. Most tests here replace the shared provenance.mcf (see
_provenance_mcf) to vary that.
"""

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import known_bug

_ONE_ROW = b.csv_file(b.obs_row())


def _provenance_mcf(with_source: bool = True,
                    **provenance_props: str) -> dict[str, str]:
  """A provenance.mcf to use as shared_files: the test provenance and source.

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


# --- Provenance URL on observations -------------------------------------------

_PROVENANCE_WITH_URL = _provenance_mcf(
    url='"https://source.example.org/dataset"')


def test_provenance_url_is_copied_onto_observations():
  result = b.run_csv(_ONE_ROW, shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.observations,
                   [b.obs(provenanceUrl="https://source.example.org/dataset")])


@known_bug(reason="with use_multiprocessing=True (production), observations"
           " lose dcid:provenanceUrl")
def test_provenance_url_survives_the_production_path():
  result = b.run_csv(_ONE_ROW,
                     mode="production",
                     shared_files=_PROVENANCE_WITH_URL)
  b.assert_records(result.observations,
                   [b.obs(provenanceUrl="https://source.example.org/dataset")])


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
         given=b.obs_config(provenance=b.PROVENANCE),
         expect=b.Fails(
             "ValueError", "The 'provenance' property must be a valid DCID or"
             f" URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL)."
             f" Found invalid provenance: '{b.PROVENANCE}'")),
    # The message talks about missing *sources*, and the error type is
    # GENERIC_ERROR, although the validator has a MISSING_PROVENANCE type.
    Case("provenance that no MCF defines",
         given=b.obs_config(provenance="dcid:UndefinedProvenance"),
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
  """A provenance from the config's sources block does not count for MCF."""
  config = {
      "sources": {
          "Config Source": {
              "url": "https://config-source.example.org",
              "provenances": {
                  "Config Provenance": "https://config-source.example.org/p"
              },
          }
      },
      "inputFiles": [b.obs_config()["inputFiles"][-1]],
  }
  result = b.run_csv(_ONE_ROW, config, shared_files={})
  b.assert_fails(result, "ValueError", _MISSING_SOURCES)
