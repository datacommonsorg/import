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
"""Tests for SQL utility functions."""

import unittest
from utils.sql import parse_sql_to_statements

class TestSqlUtils(unittest.TestCase):

    def test_parse_sql_to_statements_basic(self):
        sql = "CREATE TABLE A; CREATE TABLE B;"
        statements = parse_sql_to_statements(sql)
        self.assertEqual(statements, ["CREATE TABLE A", "CREATE TABLE B"])

    def test_parse_sql_to_statements_with_comments(self):
        sql = """
        -- This is a comment; with a semicolon
        CREATE TABLE A;
        -- Another comment;
        CREATE TABLE B (col INT64);
        """
        statements = parse_sql_to_statements(sql)
        self.assertEqual(statements, [
            "CREATE TABLE A",
            "CREATE TABLE B (col INT64)"
        ])

    def test_parse_sql_to_statements_empty(self):
        sql = "   ;  -- just comment; \n  ; "
        statements = parse_sql_to_statements(sql)
        self.assertEqual(statements, [])

if __name__ == '__main__':
    unittest.main()
