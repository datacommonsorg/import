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

"""Unit tests for the aggregation configuration validator."""

import json
import os
import sys
import unittest
from unittest.mock import mock_open, patch
import jsonschema
import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from aggregation import validate_config

# =============================================================================
# Mock YAML Configurations for Testing
# =============================================================================

# 1. A perfectly valid config containing all possible types and fields
VALID_ALL_TYPES_YAML = """
aggregations:
  - type: linked_edges
    imports: ["*"]
    stage: 1
    disabled: false

  - type: place
    source_type: County
    destination_type: State
    allow_multiple_to_places: true
    imports: ["ImportA", "ImportB"]
    stage: 2

  - type: stat_var
    ancestor_sv_id: Count_Person
    source_sv_ids: ["Count_Person_Male", "Count_Person_Female"]
    skip_all_sources_present_check: true
    output_import_name: "Aggregated_Pop"
    imports: ["ImportC"]
    stage: 3

  - type: entity
    entity_types: ["MortalityEvent"]
    location_props: ["location"]
    date_prop: "date"
    agg_date_formats: ["%Y"]
    imports: ["ImportD"]

  - type: provenance_summary
    imports: ["*"]

  - type: stat_var_groups
    imports: ["*"]
"""

# 2. Invalid: Missing required type field
INVALID_MISSING_TYPE_YAML = """
aggregations:
  - imports: ["*"]
"""

# 3. Invalid: Missing required imports field
INVALID_MISSING_IMPORTS_YAML = """
aggregations:
  - type: linked_edges
"""

# 4. Invalid: imports is a string instead of an array
INVALID_IMPORTS_TYPE_YAML = """
aggregations:
  - type: linked_edges
    imports: "*"
"""

# 5. Invalid: stage is a string instead of an integer
INVALID_STAGE_TYPE_YAML = """
aggregations:
  - type: linked_edges
    imports: ["*"]
    stage: "first"
"""

# 6. Invalid: stage is 0 (minimum is 1)
INVALID_STAGE_VALUE_YAML = """
aggregations:
  - type: linked_edges
    imports: ["*"]
    stage: 0
"""

# 7. Invalid: empty imports list (minItems: 1)
INVALID_EMPTY_IMPORTS_YAML = """
aggregations:
  - type: linked_edges
    imports: []
"""

# 8. Invalid place rollup: missing required source_type
INVALID_PLACE_MISSING_FIELD_YAML = """
aggregations:
  - type: place
    destination_type: State
    imports: ["*"]
"""

# 9. Invalid stat var: missing required source_sv_ids
INVALID_STAT_VAR_MISSING_FIELD_YAML = """
aggregations:
  - type: stat_var
    ancestor_sv_id: Count_Person
    imports: ["*"]
"""

# 10. Invalid stat var: empty source_sv_ids list
INVALID_STAT_VAR_EMPTY_SVS_YAML = """
aggregations:
  - type: stat_var
    ancestor_sv_id: Count_Person
    source_sv_ids: []
    imports: ["*"]
"""

# 11. Invalid entity: missing required location_props
INVALID_ENTITY_MISSING_FIELD_YAML = """
aggregations:
  - type: entity
    entity_types: ["Event"]
    imports: ["*"]
"""

# 12. Malformed YAML (Indentation error)
MALFORMED_YAML = """
aggregations:
  - type: linked_edges
  imports:
  - "*"
"""


class TestConfigValidator(unittest.TestCase):

    def setUp(self):
        # Load the actual schema from the workspace to ensure tests remain realistic
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        with open(self.schema_path, "r") as f:
            self.schema_json = json.load(f)

    def _get_mock_open(self, yaml_content):
        """Helper to mock open() for both the schema JSON and the target YAML."""
        def side_effect(path, *args, **kwargs):
            if "schema.json" in path:
                return mock_open(read_data=json.dumps(self.schema_json))().__enter__()
            else:
                return mock_open(read_data=yaml_content)().__enter__()
        return side_effect

    # =============================================================================
    # Success Test Cases
    # =============================================================================

    @patch('builtins.open')
    def test_validate_config_success_all_types(self, mock_file_open):
        """Verifies that a comprehensive, valid config with all types passes validation."""
        mock_file_open.side_effect = self._get_mock_open(VALID_ALL_TYPES_YAML)

        aggregations = validate_config("aggregation.yaml", self.schema_path)
        
        self.assertEqual(len(aggregations), 6)
        self.assertEqual(aggregations[0]["type"], "linked_edges")
        self.assertEqual(aggregations[1]["source_type"], "County")
        self.assertEqual(aggregations[2]["ancestor_sv_id"], "Count_Person")
        self.assertEqual(aggregations[3]["entity_types"], ["MortalityEvent"])

    # =============================================================================
    # Schema Constraint Test Cases
    # =============================================================================

    @patch('builtins.open')
    def test_validate_config_missing_type(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_MISSING_TYPE_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("'type' is a required property", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_missing_imports(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_MISSING_IMPORTS_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("'imports' is a required property", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_invalid_imports_type(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_IMPORTS_TYPE_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("is not of type 'array'", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_invalid_stage_type(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_STAGE_TYPE_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("is not of type 'integer'", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_invalid_stage_value(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_STAGE_VALUE_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("is less than the minimum of 1", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_empty_imports_list(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_EMPTY_IMPORTS_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("should be non-empty", ctx.exception.message)

    # =============================================================================
    # Conditional Dependency Test Cases (OneOf/Dependencies)
    # =============================================================================

    @patch('builtins.open')
    def test_validate_config_place_missing_field(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_PLACE_MISSING_FIELD_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        # Validation fails because place rollup requires source_type
        self.assertIn("is not valid under any of the given schemas", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_stat_var_missing_field(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_STAT_VAR_MISSING_FIELD_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("is not valid under any of the given schemas", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_stat_var_empty_source_svs(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_STAT_VAR_EMPTY_SVS_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("should be non-empty", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_entity_missing_field(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(INVALID_ENTITY_MISSING_FIELD_YAML)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("is not valid under any of the given schemas", ctx.exception.message)

    # =============================================================================
    # File System & Syntax Error Test Cases
    # =============================================================================

    @patch('builtins.open')
    def test_validate_config_yaml_syntax_error(self, mock_file_open):
        mock_file_open.side_effect = self._get_mock_open(MALFORMED_YAML)
        with self.assertRaises(yaml.YAMLError):
            validate_config("aggregation.yaml", self.schema_path)

    def test_validate_config_missing_config_file(self):
        with self.assertRaises(FileNotFoundError):
            validate_config("non_existent_config.yaml", "schema.json")

    def test_validate_config_missing_schema_file(self):
        # We patch os.path.exists to simulate config existing but schema missing
        with patch('os.path.exists', side_effect=lambda path: "aggregation.yaml" in path):
            with self.assertRaises(FileNotFoundError):
                validate_config("aggregation.yaml", "non_existent_schema.json")

    @patch('builtins.open')
    def test_validate_config_missing_aggregations_key(self, mock_file_open):
        """Verifies that missing the required 'aggregations' root key raises ValidationError."""
        missing_aggregations_yaml = """
        some_other_key: []
        """
        mock_file_open.side_effect = self._get_mock_open(missing_aggregations_yaml)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("'aggregations' is a required property", ctx.exception.message)

    @patch('builtins.open')
    def test_validate_config_empty_file(self, mock_file_open):
        """Verifies that a completely empty configuration file raises ValidationError."""
        empty_yaml = ""
        mock_file_open.side_effect = self._get_mock_open(empty_yaml)
        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config("aggregation.yaml", self.schema_path)
        self.assertIn("'aggregations' is a required property", ctx.exception.message)


if __name__ == '__main__':
    unittest.main()
