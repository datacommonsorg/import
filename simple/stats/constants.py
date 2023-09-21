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
DEFAULT_INPUT_PATH = os.path.join(DEFAULT_DATA_DIR, "input")
DEFAULT_OUTPUT_DIR = os.path.join(DEFAULT_DATA_DIR, "output")

OBSERVATIONS_FILE_NAME = "observations.csv"
DEBUG_RESOLVE_FILE_NAME = "debug_resolve.csv"

DCID_OVERRIDE_PREFIX = "dcid:"

# Observations CSV columns.
COLUMN_DCID = "dcid"
COLUMN_VARIABLE = "variable"
COLUMN_DATE = "date"
COLUMN_VALUE = "value"

# Debug CSV columns and values
DEBUG_COLUMN_NAME = "name"
DEBUG_COLUMN_DCID = "dcid"
DEBUG_COLUMN_LINK = "link"
DEBUG_UNRESOLVED_DCID = "*UNRESOLVED*"

# DC links
DC_HOME = "https://datacommons.org"
DC_BROWSER = "https://datacommons.org/browser"
