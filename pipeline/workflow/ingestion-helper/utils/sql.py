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
"""SQL utility functions for database schema parsing."""

def parse_sql_to_statements(sql_content: str) -> list[str]:
    """Parses a SQL script string into a list of individual DDL statements.

    It filters out comments (starting with '--') to prevent semicolons
    inside comments from interfering with statement splitting.
    """
    clean_lines = []
    for line in sql_content.splitlines():
        clean_line = line.split('--')[0]
        if clean_line.strip():
            clean_lines.append(clean_line.rstrip())
    clean_sql = "\n".join(clean_lines)

    statements = []
    for s in clean_sql.split(';'):
        s_stripped = s.strip()
        if s_stripped:
            statements.append(s_stripped)
    return statements
