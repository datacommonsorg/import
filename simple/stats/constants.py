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

CONFIG_JSON_FILE_NAME = "config.json"

OBSERVATIONS_FILE_NAME_PREFIX = "observations"
DEBUG_RESOLVE_FILE_NAME_PREFIX = "debug_resolve"
PROCESS_DIR_NAME = "process"

PROPERTY_DESCRIPTION = "description"
"""
Dictionary of columns names that are considered to be already resolved 
to prefixes that the corresponding values should be prefixed with before the CSV is imported.

e.g. if a column is named "geoId" and the values of that column in the CSV are: 01, 02; 
then the importer will update the values to geoId/01, geoId/02 before importing them.

Note that the lookup into these columns will always be lower-cased so ensure that 
the keys in the dictionary are lower-case as well.

Also, keep the keys sorted so it's easier to spot check should the list get large.
"""
PRE_RESOLVED_INPUT_COLUMNS_TO_PREFIXES = dict([("dcid", ""),
                                               ("geoId".lower(), "geoId/")])
"""
Dictionary of columns names that need to be externally resolved 
to property names that should be used for resolution.

Note that the lookup into these columns will always be lower-cased so ensure that 
the keys in the dictionary are lower-case as well.

Also, keep the keys sorted so it's easier to spot check should the list get large.
"""
EXTERNALLY_RESOLVED_INPUT_COLUMNS_TO_PREFIXES = dict([
    ("lat#lng", "geoCoordinate"), ("name", PROPERTY_DESCRIPTION),
    ("wikidataId".lower(), "wikidataId")
])

DCID_OVERRIDE_PREFIX = "dcid:"

# Observations CSV columns.
COLUMN_DCID = "dcid"
COLUMN_VARIABLE = "variable"
COLUMN_DATE = "date"
COLUMN_VALUE = "value"

# Debug CSV columns and values
DEBUG_COLUMN_INPUT = "input"
DEBUG_COLUMN_DCID = "dcid"
DEBUG_COLUMN_LINK = "link"
DEBUG_UNRESOLVED_DCID = "*UNRESOLVED*"

# DC links
DC_HOME = "https://datacommons.org"
DC_BROWSER = "https://datacommons.org/browser"
