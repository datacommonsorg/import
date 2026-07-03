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

"""Aggregation Helper Cloud Run Job skeleton."""

import argparse
import json
import logging
import sys

def main():
    logging.basicConfig(level=logging.INFO)
    logging.info("Starting Aggregation Helper Job")

    parser = argparse.ArgumentParser(description="Run aggregation helper job.")
    parser.add_argument("--import_list", help="JSON string representing the list of imports to process.")
    
    args = parser.parse_args()

    if not args.import_list:
        logging.error("Missing required argument: --import_list")
        sys.exit(1)

    try:
        import_list = json.loads(args.import_list)
        logging.info(f"Received import list: {import_list}")
    except json.JSONDecodeError as e:
        logging.error(f"Failed to parse import_list JSON: {e}")
        sys.exit(1)

    if not isinstance(import_list, list):
        logging.error("Parsed import_list is not a list")
        sys.exit(1)

    # Dummy logic
    logging.info("Processing aggregation (dummy)...")
    for imp in import_list:
        logging.info(f"Processing import: {imp}")
    
    logging.info("Aggregation Helper Job completed successfully.")

if __name__ == "__main__":
    main()
