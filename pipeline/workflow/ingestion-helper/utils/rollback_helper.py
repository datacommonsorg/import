# Copyright 2025 Google LLC
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
"""Rollback helper logic for dataset ingestion."""

import logging
from typing import Any


def revert_import(spanner_client: Any,
                  import_name: str,
                  workflow_id: str | None = None,
                  dry_run: bool = False) -> tuple[bool, str | None, str | None]:
    """Reverts an import to its previous version in Spanner and sets state to STAGING.

    Args:
        spanner_client: Spanner database client instance.
        import_name: The name of the import to revert (e.g., 'foo:bar:import').
        workflow_id: The ID of the failed workflow execution.
        dry_run: Dry run mode.

    Returns:
        A tuple (status, current_version, previous_version) where status is True on success, or False on failure.
    """
    short_name = import_name.split(':')[-1]

    # 1. Fetch current latest version (active before rollback).
    current_version = spanner_client.get_import_latest_version(short_name)

    # 2. Fetch the latest version with status SUCCESS.
    success_history = spanner_client.get_import_version_history(short_name, limit=1, status="SUCCESS")
    if not success_history:
        logging.warning(
            f"No version with status SUCCESS found in ImportVersionHistory for '{short_name}'. Cannot revert.")
        return False, current_version, None

    last_successful_version = success_history[0]

    # 3. Ensure current version is not already equal to the rollback version.
    if current_version == last_successful_version:
        logging.warning(
            f"Current version '{current_version}' for '{short_name}' is already equal to rollback version. Cannot revert."
        )
        return False, current_version, None

    if not last_successful_version.startswith('gs://'):
        logging.error(
            f"Cannot revert '{short_name}': previous version '{last_successful_version}' is not a valid GCS path."
        )
        return False, current_version, None

    if dry_run:
        logging.info(
            f"[DRY RUN] Would revert '{short_name}' from '{current_version}' to last successful version: '{last_successful_version}'"
        )
        return True, current_version, last_successful_version

    logging.info(
        f"Reverting '{short_name}' from '{current_version}' to last successful version: '{last_successful_version}'"
    )

    # 3. Update ImportStatus to point to the last successful version and set state to STAGING, and record audit history.
    comment_str = f"Reverted batch workflow ({workflow_id})" if workflow_id else "Reverted import state"
    success = spanner_client.revert_import_state(
        import_name=short_name,
        new_latest_version_path=last_successful_version,
        previous_version=last_successful_version,
        workflow_id=workflow_id,
        comment=comment_str
    )

    if success:
        return True, current_version, last_successful_version
    else:
        return False, current_version, None


def revert_imports(spanner_client: Any,
                   import_list: list[Any],
                   workflow_id: str | None = None,
                   dry_run: bool = False) -> list[dict]:
    """Reverts a list of imports to their previous versions in Spanner and sets state to STAGING.

    Args:
        spanner_client: Spanner database client instance.
        import_list: List of import items (either strings or dicts containing 'importName').
        workflow_id: The ID of the failed workflow execution.
        dry_run: Dry run mode.

    Returns:
        A list of result dicts for each import.
    """
    results = []
    for item in import_list:
        if isinstance(item, dict):
            import_name = item.get("importName", str(item))
        else:
            import_name = str(item)

        try:
            status, failed_version, restored_version = revert_import(
                spanner_client, import_name, workflow_id=workflow_id, dry_run=dry_run
            )
            if status:
                results.append({
                    "importName": import_name,
                    "reverted": True,
                    "failedVersion": failed_version,
                    "restoredVersion": restored_version
                })
            else:
                results.append({
                    "importName": import_name,
                    "reverted": False,
                    "failedVersion": failed_version,
                    "restoredVersion": None,
                    "error": f"Failed to revert import '{import_name}'. No previous version found."
                })
        except Exception as e:
            logging.error(f"Error reverting import item {import_name}: {e}")
            results.append({
                "importName": import_name,
                "reverted": False,
                "failedVersion": None,
                "restoredVersion": None,
                "error": f"Error reverting import '{import_name}': {e}"
            })
    return results
