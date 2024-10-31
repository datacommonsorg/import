# Copyright 2024 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# Set of properties that should not be considered for building the hierarchy.
# Ref[1]: https://source.corp.google.com/piper///depot/google3/datacommons/import/mcf_vocab.cc;l=142;rcl=621036089
# Includes all props from [1].
# Also includes a Custom DC specific property "includedIn".
SV_HIERARCHY_PROPS_BLOCKLIST: set[str] = {
    "dcid", "typeOf", "isPublic", "provenance", "resMCFFile", "keyString",
    "populationType", "constraintProperties", "name", "label", "alternateName",
    "description", "descriptionUrl", "memberOf", "utteranceTemplate",
    "searchDescription", "source", "footnote", "isNormalizable",
    "denominatorForNormalization", "measuredProperty", "measurementMethod",
    "measurementDenominator", "measurementQualifier", "scalingFactor", "unit",
    "statType", "censusACSTableId", "includedIn"
}

PREDICATE_TYPE_OF = "typeOf"
PREDICATE_POPULATION_TYPE = "populationType"
PREDICATE_MEMBER_OF = "memberOf"
PREDICATE_SPECIALIZATION_OF = "specializationOf"
PREDICATE_NAME = "name"
PREDICATE_SEARCH_DESCRIPTION = "searchDescription"
PREDICATE_MEASURED_PROPERTY = "measuredProperty"
PREDICATE_DESCRIPTION = "description"
PREDICATE_UNIT = "unit"
PREDICATE_SCALING_FACTOR = "scalingFactor"
PREDICATE_MEASUREMENT_METHOD = "measurementMethod"
PREDICATE_OBSERVATION_PERIOD = "observationPeriod"
PREDICATE_RELEVANT_VARIABLE = "relevantVariable"
PREDICATE_RELEVANT_VARIABLE_LIST = "relevantVariableList"
PREDICATE_MEMBER = "member"
PREDICATE_MEMBER_LIST = "memberList"

# The set of standard observation properties with first class support in our APIs and FE.
STANDARD_OBSERVATION_PROPERTIES: set[str] = {
    PREDICATE_UNIT, PREDICATE_SCALING_FACTOR, PREDICATE_MEASUREMENT_METHOD,
    PREDICATE_OBSERVATION_PERIOD
}

TYPE_STATISTICAL_VARIABLE = "StatisticalVariable"
TYPE_STATISTICAL_VARIABLE_GROUP = "StatVarGroup"
TYPE_TOPIC = "Topic"
TYPE_STAT_VAR_PEER_GROUP = "StatVarPeerGroup"

DEFAULT_POPULATION_TYPE = "Thing"

CUSTOM_SVG_PREFIX = "c/g/"
ROOT_SVG_ID = "dc/g/Root"

DEFAULT_CUSTOM_ROOT_SVG_ID = f"{CUSTOM_SVG_PREFIX}Root"
DEFAULT_CUSTOM_ROOT_SVG_NAME = "Custom Variables"
