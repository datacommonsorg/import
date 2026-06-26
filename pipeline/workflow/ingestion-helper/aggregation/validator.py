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

"""Configuration validator and CLI tool for Data Commons aggregations."""

import argparse
import json
import logging
import os
import sys
from typing import Any, Dict, List
import yaml
import jsonschema

# ANSI escape codes for colored terminal output
GREEN = "\033[92m"
RED = "\033[91m"
RESET = "\033[0m"


def validate_config(config_file_path: str, schema_file_path: str) -> List[Dict[str, Any]]:
    """Loads and validates the aggregation YAML configuration against the JSON Schema.

    Args:
        config_file_path: Path to the aggregation.yaml configuration file.
        schema_file_path: Path to the aggregation_schema.json validation file.

    Returns:
        A list of validated aggregation dictionaries.

    Raises:
        FileNotFoundError: If either the config or schema file is missing.
        jsonschema.exceptions.ValidationError: If schema validation fails.
        yaml.YAMLError: If the YAML file is malformed.
    """
    if not os.path.exists(config_file_path):
        raise FileNotFoundError(f"Aggregation config file not found: {config_file_path}")
    if not os.path.exists(schema_file_path):
        raise FileNotFoundError(f"JSON Schema file not found: {schema_file_path}")

    # 1. Load and parse YAML
    try:
        with open(config_file_path, "r") as f:
            config = yaml.safe_load(f)
    except yaml.YAMLError as e:
        logging.error(f"Failed to parse YAML file {config_file_path}: {e}")
        raise e

    if config is None:
        config = {}

    # 2. Load JSON Schema
    try:
        with open(schema_file_path, "r") as f:
            schema = json.load(f)
    except Exception as e:
        logging.error(f"Failed to load JSON Schema file {schema_file_path}: {e}")
        raise e

    # 3. Validate against Schema
    try:
        jsonschema.validate(instance=config, schema=schema)
    except jsonschema.exceptions.ValidationError as e:
        logging.error(f"Schema validation failed for config {config_file_path}: {e.message}")
        raise e

    return config["aggregations"]


def main():
    """CLI entry point for standalone configuration validation."""
    logging.basicConfig(level=logging.INFO)
    parser = argparse.ArgumentParser(description="Validate Data Commons aggregation configuration files against the JSON Schema.")
    
    # Resolve default paths relative to this script's directory (aggregation/)
    curr_dir = os.path.dirname(os.path.abspath(__file__))
    default_config = os.path.join(curr_dir, "..", "aggregation.yaml")
    default_schema = os.path.join(curr_dir, "schema.json")

    parser.add_argument(
        "--config",
        type=str,
        default=default_config,
        help=f"Path to the aggregation YAML config file (default: {default_config})"
    )
    parser.add_argument(
        "--schema",
        type=str,
        default=default_schema,
        help=f"Path to the JSON Schema validation file (default: {default_schema})"
    )

    args = parser.parse_args()

    print(f"Validating '{os.path.basename(args.config)}' against '{os.path.basename(args.schema)}'...")

    try:
        aggregations = validate_config(args.config, args.schema)
        print(f"{GREEN}[SUCCESS] Configuration is valid!{RESET}")
        print(f"Parsed {len(aggregations)} aggregation steps successfully.")
        sys.exit(0)
    except FileNotFoundError as e:
        print(f"{RED}[ERROR] File not found: {e}{RESET}", file=sys.stderr)
        sys.exit(1)
    except jsonschema.exceptions.ValidationError as e:
        print(f"{RED}[ERROR] Schema Validation Failed:{RESET}", file=sys.stderr)
        print(f"{RED}  - Path: {'.'.join(str(p) for p in e.path)}{RESET}", file=sys.stderr)
        print(f"{RED}  - Message: {e.message}{RESET}", file=sys.stderr)
        sys.exit(1)
    except yaml.YAMLError as e:
        print(f"{RED}[ERROR] YAML Syntax Error: {e}{RESET}", file=sys.stderr)
        sys.exit(1)
    except Exception as e:
        print(f"{RED}[ERROR] Unexpected validation failure: {e}{RESET}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
