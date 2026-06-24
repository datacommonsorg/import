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

"""Unit tests for the aggregation configuration validator using real temporary files."""

import os
import sys
import tempfile
import textwrap
import unittest
import jsonschema
import yaml

sys.path.append(os.path.dirname(os.path.dirname(__file__)))

from aggregation import validate_config


class TestValidatorSuccess(unittest.TestCase):
    """Verifies successful validation paths for valid configurations."""

    def setUp(self):
        # Load the actual schema from the workspace to ensure tests remain realistic
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")

    def test_validate_config_success_all_types(self):
        """Verifies that a comprehensive, valid config with all types passes validation."""
        valid_all_types_yaml = textwrap.dedent("""\
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
        """)

        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(valid_all_types_yaml)

            aggregations = validate_config(config_path, self.schema_path)
            
            self.assertEqual(len(aggregations), 6)
            self.assertEqual(aggregations[0]["type"], "linked_edges")
            self.assertEqual(aggregations[1]["source_type"], "County")
            self.assertEqual(aggregations[2]["ancestor_sv_id"], "Count_Person")
            self.assertEqual(aggregations[3]["entity_types"], ["MortalityEvent"])


class TestValidatorSchemaConstraints(unittest.TestCase):
    """Verifies core schema constraint failures (types, required fields, values)."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")

    def test_validate_config_missing_type(self):
        """Verifies that missing the required 'type' field raises ValidationError."""
        invalid_missing_type_yaml = textwrap.dedent("""\
            aggregations:
              - imports: ["*"]
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_missing_type_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("'type' is a required property", ctx.exception.message)

    def test_validate_config_missing_imports(self):
        """Verifies that missing the required 'imports' field raises ValidationError."""
        invalid_missing_imports_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_missing_imports_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("'imports' is a required property", ctx.exception.message)

    def test_validate_config_invalid_imports_type(self):
        """Verifies that imports field being a string instead of an array raises ValidationError."""
        invalid_imports_type_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
                imports: "*"
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_imports_type_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is not of type 'array'", ctx.exception.message)

    def test_validate_config_invalid_stage_type(self):
        """Verifies that stage field being a string instead of an integer raises ValidationError."""
        invalid_stage_type_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
                imports: ["*"]
                stage: "first"
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_stage_type_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is not of type 'integer'", ctx.exception.message)

    def test_validate_config_invalid_stage_value(self):
        """Verifies that a stage value of 0 (minimum is 1) raises ValidationError."""
        invalid_stage_value_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
                imports: ["*"]
                stage: 0
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_stage_value_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is less than the minimum of 1", ctx.exception.message)

    def test_validate_config_empty_imports_list(self):
        """Verifies that an empty imports list raises ValidationError."""
        invalid_empty_imports_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
                imports: []
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_empty_imports_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("should be non-empty", ctx.exception.message)

    def test_validate_config_missing_aggregations_key(self):
        """Verifies that missing the required 'aggregations' root key raises ValidationError."""
        missing_aggregations_yaml = textwrap.dedent("""\
            some_other_key: []
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(missing_aggregations_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("'aggregations' is a required property", ctx.exception.message)

    def test_validate_config_empty_file(self):
        """Verifies that a completely empty configuration file raises ValidationError."""
        empty_yaml = ""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(empty_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("'aggregations' is a required property", ctx.exception.message)


class TestValidatorConditionalDependencies(unittest.TestCase):
    """Verifies type-specific conditional dependencies (OneOf / dependencies)."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")

    def test_validate_config_place_missing_field(self):
        """Verifies that a place step missing the required 'source_type' raises ValidationError."""
        invalid_place_missing_field_yaml = textwrap.dedent("""\
            aggregations:
              - type: place
                destination_type: State
                imports: ["*"]
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_place_missing_field_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is not valid under any of the given schemas", ctx.exception.message)

    def test_validate_config_stat_var_missing_field(self):
        """Verifies that a stat_var step missing the required 'source_sv_ids' raises ValidationError."""
        invalid_stat_var_missing_field_yaml = textwrap.dedent("""\
            aggregations:
              - type: stat_var
                ancestor_sv_id: Count_Person
                imports: ["*"]
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_stat_var_missing_field_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is not valid under any of the given schemas", ctx.exception.message)

    def test_validate_config_stat_var_empty_source_svs(self):
        """Verifies that a stat_var step with an empty source_sv_ids array raises ValidationError."""
        invalid_stat_var_empty_svs_yaml = textwrap.dedent("""\
            aggregations:
              - type: stat_var
                ancestor_sv_id: Count_Person
                source_sv_ids: []
                imports: ["*"]
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_stat_var_empty_svs_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("should be non-empty", ctx.exception.message)

    def test_validate_config_entity_missing_field(self):
        """Verifies that an entity step missing the required 'location_props' raises ValidationError."""
        invalid_entity_missing_field_yaml = textwrap.dedent("""\
            aggregations:
              - type: entity
                entity_types: ["Event"]
                imports: ["*"]
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(invalid_entity_missing_field_yaml)

            with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
                validate_config(config_path, self.schema_path)
            self.assertIn("is not valid under any of the given schemas", ctx.exception.message)


class TestValidatorErrorsAndFileSystem(unittest.TestCase):
    """Verifies file-system issues and non-schema parsing errors (YAML syntax)."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")

    def test_validate_config_yaml_syntax_error(self):
        """Verifies that malformed YAML syntax raises YAMLError."""
        malformed_yaml = textwrap.dedent("""\
            aggregations:
              - type: linked_edges
              imports:
              - "*"
        """)
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            with open(config_path, "w") as f:
                f.write(malformed_yaml)

            with self.assertRaises(yaml.YAMLError):
                validate_config(config_path, self.schema_path)

    def test_validate_config_missing_config_file(self):
        """Verifies that a missing config file path raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError):
            validate_config("non_existent_config.yaml", self.schema_path)

    def test_validate_config_missing_schema_file(self):
        """Verifies that a missing schema file path raises FileNotFoundError."""
        with tempfile.TemporaryDirectory() as tmpdir:
            config_path = os.path.join(tmpdir, "aggregation.yaml")
            # Create a real, valid config file so it exists
            with open(config_path, "w") as f:
                f.write("aggregations: []")
            
            # Pass the real config path, but a non-existent schema path
            with self.assertRaises(FileNotFoundError) as ctx:
                validate_config(config_path, "non_existent_schema.json")
            self.assertIn("JSON Schema file not found", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
