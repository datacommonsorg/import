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

TYPE_STATISTICAL_VARIABLE = "StatisticalVariable"
TYPE_STATISTICAL_VARIABLE_GROUP = "StatVarGroup"
TYPE_TOPIC = "Topic"

DEFAULT_POPULATION_TYPE = "Thing"

CUSTOM_SVG_PREFIX = "c/g/"
ROOT_SVG_ID = "dc/g/Root"
