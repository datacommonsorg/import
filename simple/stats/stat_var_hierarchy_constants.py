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
SV_HIERARCHY_PROPS_BLOCKLIST: set[str] = {
    "dcid", "typeOf", "provenance", "populationType", "name", "label",
    "alternateName", "description", "memberOf", "utteranceTemplate", "source",
    "footnote", "measuredProperty", "measurementMethod",
    "measurementDenominator", "measurementQualifier", "scalingFactor", "unit",
    "statType", "includedIn"
}

PREDICATE_TYPE_OF = "typeOf"
PREDICATE_POPULATION_TYPE = "populationType"
TYPE_STATISTICAL_VARIABLE = "StatisticalVariable"
DEFAULT_POPULATION_TYPE = "Thing"
