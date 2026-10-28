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
MCF nodes: MCF text in, exact JSON-LD node records out.

Every test imports its MCF under the shared test provenance (see
lib/builders.py). Records the provenance produces on its own are not listed.
"""

from testsuite.lib import builders as b
from testsuite.lib.builders import Case
from testsuite.lib.builders import cases
from testsuite.lib.builders import known_bug

# --- Property values ----------------------------------------------------------
#
# One case per rule: given is the value as written in MCF, expect is the value
# in JSON-LD. To pin a new rule, add a case.

VALUES = [
    # Numbers
    Case("integer", given="42", expect=42),
    Case("negative integer", given="-3", expect=-3),
    Case("decimal", given="0.25", expect=0.25),
    Case("decimal with trailing zero", given="2.0", expect=2.0),
    Case("leading zeros are dropped", given="007", expect=7),
    Case("exponent stays a string", given="1e5", expect="1e5"),
    Case("inf stays a string", given="inf", expect="inf"),
    Case("quoted number stays a string",
         given='"00123"',
         expect="00123",
         bug="quoted values that look numeric become numbers, so \"00123\""
         " loses its leading zeros"),
    # Other literals
    Case("boolean is a string", given="true", expect="true"),
    Case("quoted string", given='"Thing One"', expect="Thing One"),
    Case("quoted string with a comma is one value",
         given='"Paris, France"',
         expect="Paris, France"),
    Case("unicode is preserved",
         given='"Zürich – 東京 🚀"',
         expect="Zürich – 東京 🚀"),
    Case("complex value is kept verbatim",
         given="[10 20 dcs:Years]",
         expect="[10 20 dcs:Years]"),
    # Custom namespaces stay plain strings for now; they become references
    # once the loader supports custom namespaces.
    Case("custom namespace value is a string",
         given="undata:Other",
         expect="undata:Other"),
    # References
    Case("dcid reference", given="dcid:Thing1", expect=b.ref("Thing1")),
    Case("dcs reference is normalized",
         given="dcs:country/USA",
         expect=b.ref("country/USA")),
    Case("schema reference is normalized",
         given="schema:Place",
         expect=b.ref("Place")),
    Case("comma-separated references",
         given="dcid:A, dcid:B, dcs:C",
         expect=[b.ref("A"), b.ref("B"), b.ref("C")]),
    Case("repeated references are deduplicated",
         given="dcid:A, dcid:A",
         expect=b.ref("A")),
]


@cases(*VALUES)
def test_value(given, expect):
  result = b.run_mcf(b.mcf_node("dcid:T", prop=given))
  b.assert_records(result.added, [b.record("T", "Thing", prop=expect)])


def test_all_values_on_one_node():
  """The value rules must not interfere with each other."""
  working = [c for c in VALUES if not c.bug]
  result = b.run_mcf(
      b.mcf_node("dcid:T", **{
          f"p{i}": c.given for i, c in enumerate(working)
      }))
  b.assert_records(result.added, [
      b.record("T", "Thing", **{
          f"p{i}": c.expect for i, c in enumerate(working)
      })
  ])


# --- Subjects and types -------------------------------------------------------


def test_basic_node():
  result = b.run_mcf(b.mcf_node("dcid:Thing1", name='"Thing One"'))
  b.assert_records(result.added,
                   [b.record("Thing1", "Thing", name="Thing One")])


def test_dcid_property_names_a_local_node():
  result = b.run_mcf(b.mcf_node("LocalName", dcid='"Thing2"'))
  b.assert_records(result.added, [b.record("Thing2", "Thing")])


def test_custom_namespace_subject_becomes_dcid():
  result = b.run_mcf(b.mcf_node("undata:Thing16"))
  b.assert_records(result.added, [b.record("Thing16", "Thing")])


@cases(
    Case("dcs", given="dcs:Thing", expect="Thing"),
    Case("schema", given="schema:Thing", expect="Thing"),
    Case("quoted", given='"Thing"', expect="Thing"),
    Case("several types",
         given="dcs:Thing, dcs:Place",
         expect=["Place", "Thing"]),
)
def test_type(given, expect):
  result = b.run_mcf(b.mcf_node("dcid:T", typeOf=given))
  b.assert_records(result.added, [b.record("T", expect)])


# --- Whole nodes --------------------------------------------------------------
#
# One case per node shape: given is the node's MCF properties (typeOf included),
# expect is the record's types and properties. Every property must pass
# through, and nothing else may be added. To pin a new shape, add a case.
# Source and Provenance nodes are special (they are also registered and written
# a second time); they are covered in test_provenance.py.

_MCF_OBSERVATION_BUG = (
    "StatVarObservation nodes from MCF pass through as plain nodes: no"
    " dcid:provenance and no observationProperties on the variable")


def _variable(dcid: str, *observation_properties: str) -> dict:
  """The variableMeasured of an observation, with observationProperties."""
  return {
      **b.ref(dcid), "dcid:observationProperties": [
          f"dcid:{p}" for p in observation_properties
      ]
  }


NODES = [
    Case("statistical variable",
         given=dict(typeOf="dcs:StatisticalVariable",
                    populationType="dcs:Person",
                    measuredProperty="dcs:count",
                    statType="dcs:measuredValue",
                    gender="dcs:Female",
                    name='"Female population"'),
         expect=dict(types="StatisticalVariable",
                     populationType=b.ref("Person"),
                     measuredProperty=b.ref("count"),
                     statType=b.ref("measuredValue"),
                     gender=b.ref("Female"),
                     name="Female population")),
    Case("place with several types",
         given=dict(typeOf="dcs:City, dcs:Place",
                    name='"Paris"',
                    containedInPlace="dcid:country/FRA, dcid:Earth",
                    latitude="48.8566",
                    population="2102650"),
         expect=dict(types=["City", "Place"],
                     name="Paris",
                     containedInPlace=[b.ref("Earth"),
                                       b.ref("country/FRA")],
                     latitude=48.8566,
                     population=2102650)),
    # Observations can be written as MCF nodes too. They must come out like
    # CSV observations: with the provenance, and with the variable's
    # observationProperties listing their entity dimensions in MCF order.
    Case("observation",
         given=dict(typeOf="dcs:StatVarObservation",
                    observationAbout="dcid:country/USA",
                    variableMeasured="dcid:Count_Person",
                    observationDate='"2020"',
                    value="100"),
         expect=dict(types="StatVarObservation",
                     observationAbout=b.ref("country/USA"),
                     variableMeasured=_variable("Count_Person",
                                                "observationAbout"),
                     observationDate=2020,
                     value=100,
                     provenance=b.ref(b.PROVENANCE)),
         bug=_MCF_OBSERVATION_BUG),
    Case("observation with all standard properties",
         given=dict(typeOf="dcs:StatVarObservation",
                    observationAbout="dcid:geoId/06",
                    variableMeasured="dcid:Median_Income",
                    observationDate='"2021-06"',
                    value="5.5",
                    unit="dcs:USDollar",
                    scalingFactor="100",
                    measurementMethod="dcs:CensusACS",
                    observationPeriod='"P1Y"'),
         expect=dict(types="StatVarObservation",
                     observationAbout=b.ref("geoId/06"),
                     variableMeasured=_variable("Median_Income",
                                                "observationAbout"),
                     observationDate="2021-06",
                     value=5.5,
                     unit=b.ref("USDollar"),
                     scalingFactor=100,
                     measurementMethod=b.ref("CensusACS"),
                     observationPeriod="P1Y",
                     provenance=b.ref(b.PROVENANCE)),
         bug=_MCF_OBSERVATION_BUG),
    Case("observation with a second entity dimension",
         given=dict(typeOf="dcs:StatVarObservation",
                    observationAbout="dcid:country/USA",
                    destination="dcid:country/FRA",
                    variableMeasured="dcid:Count_Trip",
                    observationDate="2020",
                    value="7"),
         expect=dict(types="StatVarObservation",
                     observationAbout=b.ref("country/USA"),
                     destination=b.ref("country/FRA"),
                     variableMeasured=_variable("Count_Trip",
                                                "observationAbout",
                                                "destination"),
                     observationDate=2020,
                     value=7,
                     provenance=b.ref(b.PROVENANCE)),
         bug=_MCF_OBSERVATION_BUG),
    Case("custom property",
         given={
             "typeOf": "dcs:Thing",
             "c/prop/1": '"custom value"',
             "description": '"has a custom property"'
         },
         expect={
             "types": "Thing",
             "c/prop/1": "custom value",
             "description": "has a custom property"
         }),
]


@cases(*NODES)
def test_node_passes_through(given, expect):
  result = b.run_mcf(b.mcf_node("dcid:T", **given))
  b.assert_records(result.added, [b.record("T", **expect)])


# --- File structure -----------------------------------------------------------


def test_one_node_in_two_blocks_is_merged():
  result = b.run_mcf(b.mcf_node("dcid:T", name='"First"'),
                     b.mcf_node("dcid:T", typeOf=None, description='"Second"'))
  b.assert_records(result.added,
                   [b.record("T", "Thing", name="First", description="Second")])


def test_comments_and_blank_lines_are_ignored():
  result = b.run_mcf("// A comment\n# Another comment\n\nNode: dcid:T\n\n"
                     "typeOf: dcs:Thing\n")
  b.assert_records(result.added, [b.record("T", "Thing")])


def test_context_namespace_expands_to_full_uri():
  result = b.run_mcf('Context:\nnamespace: "ex=http://example.org/"\n',
                     b.mcf_node("dcid:T", sameAs="ex:item/1"))
  b.assert_records(
      result.added,
      [b.record("T", "Thing", sameAs=b.ref("http://example.org/item/1"))])


@known_bug(reason="object values written as l:<local id> are not resolved to"
           " the referenced node's dcid (points at dcid:L2, not dcid:Parent)")
def test_local_reference_resolves_to_dcid():
  result = b.run_mcf(b.mcf_node("L1", dcid='"Child"', parent="l:L2"),
                     b.mcf_node("L2", dcid='"Parent"'))
  b.assert_records(result.added, [
      b.record("Child", "Thing", parent=b.ref("Parent")),
      b.record("Parent", "Thing"),
  ])


# --- Invalid MCF --------------------------------------------------------------


def test_node_without_a_dcid_fails():
  result = b.run_mcf(b.mcf_node("NoDcid"))
  b.assert_fails(result, "ValueError", "dcid not specified for node: NoDcid")


def test_line_without_a_colon_fails():
  result = b.run_mcf("Node: dcid:T\nthis line has no colon\n")
  b.assert_fails(result, "AssertionError",
                 "Line 2: Malformed line without a colon delimiter")


def test_property_before_any_node_fails():
  result = b.run_mcf("typeOf: dcs:Thing\nNode: dcid:T\n")
  b.assert_fails(result, "AssertionError",
                 "Line 1: Prop-Values before Node or Context block")


# --- Limits -------------------------------------------------------------------
#
# MCF values are limited to 65535 characters (a MySQL TEXT column).


def test_value_of_65535_characters_is_accepted():
  value = "x" * (2**16 - 1)
  result = b.run_mcf(b.mcf_node("dcid:Big", name=f'"{value}"'))
  b.assert_records(result.added, [b.record("Big", "Thing", name=value)])


def test_value_of_65536_characters_fails():
  result = b.run_mcf(b.mcf_node("dcid:Big", name=f'"{"x" * 2**16}"'))
  b.assert_fails(result, "ValueError", "too long")
