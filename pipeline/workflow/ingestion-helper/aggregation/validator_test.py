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
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        self.tmpdir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmpdir.name, "config.yaml")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_validate_config_success_all_types(self):
        """Verifies that a comprehensive, valid config with all 6 calculation types passes validation."""
        valid_all_types_yaml = textwrap.dedent("""\
            calculations:
              - type: PLACE_AGGREGATION
                input_imports:
                  - CensusACS5YearSurvey
                output_import: CensusACS5YearSurvey_AggCountry
                round: 1
                place_aggregation:
                  from_place_types: State
                  to_place_types: Country

              - type: STAT_VAR_AGGREGATION
                output_import: CensusACS5YearSurvey_HealthInsurance_StatVarAgg
                input_imports:
                  - CensusACS5YearSurvey
                stat_var_aggregation:
                  aggregations:
                    - ancestor_sv_id: Count_Person_NoHealthInsurance
                      source_sv_ids:
                        - dc/y0dvhk0sggzef

              - type: ENTITY_AGGREGATION
                output_import: FireFAMWEB_Agg
                input_imports:
                  - FireFAMWEB
                entity_aggregation:
                  entity_types:
                    - BurnedArea
                  location_props:
                    - location
                  date_prop: startDate
                  agg_date_formats:
                    - "%Y"

              - type: STAT_VAR_SERIES_AGGREGATION
                input_imports:
                  - NASA_NEXDCP30
                round: 1
                output_import: NASA_NEXDCP30_AggrDiffStats
                stat_var_series_aggregation:
                  aggr_funcs:
                    - max_diff_across_measurement_methods: {}

              - type: STAT_VAR_CALCULATION
                input_imports:
                  - EIA_Electricity
                output_import: Energy_StatVarCalculation
                stat_var_calculation:
                  calculations:
                    - input1:
                        sv_regex: Annual_Emissions
                      operation: DIVIDE
                      output:
                        sv: Annual_Emissions_Per_Capita

              - type: SUPER_ENUM_AGGREGATION
                input_imports:
                  - CensusACS5YearSurvey
                output_import: CensusACS5YearSurvey_SuperEnum
        """)

        with open(self.config_path, "w") as f:
            f.write(valid_all_types_yaml)

        calculations = validate_config(self.config_path, self.schema_path)

        self.assertEqual(len(calculations), 6)
        self.assertEqual(calculations[0]["type"], "PLACE_AGGREGATION")
        self.assertEqual(calculations[1]["type"], "STAT_VAR_AGGREGATION")
        self.assertEqual(calculations[2]["type"], "ENTITY_AGGREGATION")
        self.assertEqual(calculations[3]["type"], "STAT_VAR_SERIES_AGGREGATION")
        self.assertEqual(calculations[4]["type"], "STAT_VAR_CALCULATION")
        self.assertEqual(calculations[5]["type"], "SUPER_ENUM_AGGREGATION")


class TestValidatorSchemaConstraints(unittest.TestCase):
    """Verifies core schema constraint failures (types, required fields, values)."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        self.tmpdir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmpdir.name, "config.yaml")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_validate_config_missing_type(self):
        """Verifies that missing the required 'type' field raises ValidationError."""
        invalid_missing_type_yaml = textwrap.dedent("""\
            calculations:
              - input_imports:
                  - ImportA
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_missing_type_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertEqual(ctx.exception.validator, "required")

    def test_validate_config_missing_input_imports(self):
        """Verifies that missing the required 'input_imports' field raises ValidationError."""
        invalid_missing_imports_yaml = textwrap.dedent("""\
            calculations:
              - type: SUPER_ENUM_AGGREGATION
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_missing_imports_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'input_imports' is a required property", ctx.exception.message)

    def test_validate_config_invalid_input_imports_type(self):
        """Verifies that input_imports field being a string instead of an array raises ValidationError."""
        invalid_imports_type_yaml = textwrap.dedent("""\
            calculations:
              - type: SUPER_ENUM_AGGREGATION
                input_imports: "SingleImportString"
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_imports_type_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("is not of type 'array'", ctx.exception.message)

    def test_validate_config_empty_input_imports_list(self):
        """Verifies that an empty input_imports list raises ValidationError."""
        invalid_empty_imports_yaml = textwrap.dedent("""\
            calculations:
              - type: SUPER_ENUM_AGGREGATION
                input_imports: []
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_empty_imports_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("should be non-empty", ctx.exception.message)

    def test_validate_config_missing_calculations_key(self):
        """Verifies that missing the required 'calculations' root key raises ValidationError."""
        missing_calculations_yaml = textwrap.dedent("""\
            some_other_key: []
        """)
        with open(self.config_path, "w") as f:
            f.write(missing_calculations_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'calculations' is a required property", ctx.exception.message)

    def test_validate_config_empty_file(self):
        """Verifies that a completely empty configuration file raises ValidationError."""
        empty_yaml = ""
        with open(self.config_path, "w") as f:
            f.write(empty_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'calculations' is a required property", ctx.exception.message)


class TestValidatorConditionalDependencies(unittest.TestCase):
    """Verifies type-specific conditional sub-block dependencies."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        self.tmpdir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmpdir.name, "config.yaml")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_validate_config_place_missing_subblock(self):
        """Verifies that a PLACE_AGGREGATION step missing 'place_aggregation' raises ValidationError."""
        invalid_place_missing_yaml = textwrap.dedent("""\
            calculations:
              - type: PLACE_AGGREGATION
                input_imports:
                  - ImportA
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_place_missing_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'place_aggregation' is a required property", ctx.exception.message)

    def test_validate_config_stat_var_missing_subblock(self):
        """Verifies that a STAT_VAR_AGGREGATION step missing 'stat_var_aggregation' raises ValidationError."""
        invalid_stat_var_missing_yaml = textwrap.dedent("""\
            calculations:
              - type: STAT_VAR_AGGREGATION
                input_imports:
                  - ImportA
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_stat_var_missing_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'stat_var_aggregation' is a required property", ctx.exception.message)

    def test_validate_config_stat_var_empty_source_svs(self):
        """Verifies that an aggregation item with an empty source_sv_ids array raises ValidationError."""
        invalid_stat_var_empty_svs_yaml = textwrap.dedent("""\
            calculations:
              - type: STAT_VAR_AGGREGATION
                input_imports:
                  - ImportA
                stat_var_aggregation:
                  aggregations:
                    - ancestor_sv_id: Count_Person
                      source_sv_ids: []
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_stat_var_empty_svs_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("should be non-empty", ctx.exception.message)

    def test_validate_config_entity_missing_subblock(self):
        """Verifies that an ENTITY_AGGREGATION step missing 'entity_aggregation' raises ValidationError."""
        invalid_entity_missing_yaml = textwrap.dedent("""\
            calculations:
              - type: ENTITY_AGGREGATION
                input_imports:
                  - ImportA
        """)
        with open(self.config_path, "w") as f:
            f.write(invalid_entity_missing_yaml)

        with self.assertRaises(jsonschema.exceptions.ValidationError) as ctx:
            validate_config(self.config_path, self.schema_path)
        self.assertIn("'entity_aggregation' is a required property", ctx.exception.message)


class TestValidatorErrorsAndFileSystem(unittest.TestCase):
    """Verifies file-system issues and non-schema parsing errors (YAML syntax)."""

    def setUp(self):
        self.schema_path = os.path.join(os.path.dirname(__file__), "schema.json")
        self.tmpdir = tempfile.TemporaryDirectory()
        self.config_path = os.path.join(self.tmpdir.name, "config.yaml")

    def tearDown(self):
        self.tmpdir.cleanup()

    def test_validate_config_yaml_syntax_error(self):
        """Verifies that malformed YAML syntax raises YAMLError."""
        malformed_yaml = textwrap.dedent("""\
            calculations:
              - type: PLACE_AGGREGATION
              input_imports:
              - "ImportA"
        """)
        with open(self.config_path, "w") as f:
            f.write(malformed_yaml)

        with self.assertRaises(yaml.YAMLError):
            validate_config(self.config_path, self.schema_path)

    def test_validate_config_missing_config_file(self):
        """Verifies that a missing config file path raises FileNotFoundError."""
        with self.assertRaises(FileNotFoundError) as ctx:
            validate_config("non_existent_config.yaml", self.schema_path)
        self.assertIn("Aggregation config file not found", str(ctx.exception))

    def test_validate_config_missing_schema_file(self):
        """Verifies that a missing schema file path raises FileNotFoundError."""
        with open(self.config_path, "w") as f:
            f.write("calculations: []")

        with self.assertRaises(FileNotFoundError) as ctx:
            validate_config(self.config_path, "non_existent_schema.json")
        self.assertIn("JSON Schema file not found", str(ctx.exception))


if __name__ == '__main__':
    unittest.main()
