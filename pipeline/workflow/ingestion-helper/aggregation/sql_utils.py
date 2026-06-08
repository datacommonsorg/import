# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.


def _escape_sql_literal(val: str) -> str:
    r"""Escapes a string literal for use in nested BigQuery/Spanner queries.

    This is required because the query string travels through two SQL parsers:
    1. BigQuery parses the EXTERNAL_QUERY double-quoted string literal.
    2. Spanner parses the resulting inner query's single-quoted string literal.

    To ensure the value is correctly matched and prevent SQL injection:
    - Backslashes (\) are escaped to 4 backslashes (\\\\) so they survive
      both decodings (\\\\ -> \\ -> \). Otherwise, they may escape quotes
      or be interpreted as control characters (like \b becoming backspace).
      Double quotes (") are escaped to \\" to prevent terminating BQ string.
    - Single quotes (') are escaped to '' to prevent terminating Spanner string.
    """
    return val.replace('\\', '\\\\\\\\').replace('"', '\\"').replace("'", "''")
