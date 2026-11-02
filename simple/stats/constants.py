# Copyright 2023 Google Inc.
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

import os

# Defaults.
DEFAULT_DATA_DIR = ".data"
DEFAULT_INPUT_DIR = os.path.join(DEFAULT_DATA_DIR, "input")
DEFAULT_OUTPUT_DIR = os.path.join(DEFAULT_DATA_DIR, "output")
DEFAULT_FROZEN_TIME = "2023-01-01"

CONFIG_JSON_FILE_NAME = "config.json"
ALL_IMPORTS = "ALL_IMPORTS"
REPORT_JSON_FILE_NAME = "report.json"
PROCESS_DIR_NAME = "process"

# Standard NA/null value representations
STANDARD_NA_VALUES = ["-", "nil", "n/a", "na", "none", "null", "<na>"]

# Observations CSV columns.
COLUMN_VARIABLE = "variable"
COLUMN_DATE = "date"
COLUMN_VALUE = "value"
COLUMN_PROVENANCE = "provenance"
COLUMN_ENTITY = "entity"

# Observation property columns
COLUMN_UNIT = "unit"
COLUMN_SCALING_FACTOR = "scaling_factor"
COLUMN_MEASUREMENT_METHOD = "measurement_method"
COLUMN_OBSERVATION_PERIOD = "observation_period"
COLUMN_PROPERTIES = "properties"

# Standard observation column ordering used by the graph writer.
OBSERVATION_COLUMNS = [
    COLUMN_ENTITY,
    COLUMN_VARIABLE,
    COLUMN_DATE,
    COLUMN_VALUE,
    COLUMN_PROVENANCE,
    COLUMN_UNIT,
    COLUMN_SCALING_FACTOR,
    COLUMN_MEASUREMENT_METHOD,
    COLUMN_OBSERVATION_PERIOD,
    COLUMN_PROPERTIES,
]

# Columns that need namespace stripping in observation DataFrames
COLUMNS_TO_STRIP_NAMESPACES = [
    COLUMN_ENTITY,
    COLUMN_VARIABLE,
    COLUMN_PROVENANCE,
    COLUMN_UNIT,
    COLUMN_MEASUREMENT_METHOD,
    COLUMN_OBSERVATION_PERIOD,
]
