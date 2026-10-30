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

PREDICATE_UNIT = "unit"
PREDICATE_SCALING_FACTOR = "scalingFactor"
PREDICATE_MEASUREMENT_METHOD = "measurementMethod"
PREDICATE_OBSERVATION_PERIOD = "observationPeriod"

# The set of standard observation properties with first class support in our APIs and FE.
STANDARD_OBSERVATION_PROPERTIES: set[str] = {
    PREDICATE_UNIT, PREDICATE_SCALING_FACTOR, PREDICATE_MEASUREMENT_METHOD,
    PREDICATE_OBSERVATION_PERIOD
}

CUSTOM_SVG_PREFIX = "c/g/"
ROOT_SVG_ID = "dc/g/Root"
ROOT_SVG_NAME = "Data Commons Variables"

DEFAULT_CUSTOM_ROOT_SVG_ID = f"{CUSTOM_SVG_PREFIX}Root"
DEFAULT_CUSTOM_ROOT_SVG_NAME = "Custom Variables"
