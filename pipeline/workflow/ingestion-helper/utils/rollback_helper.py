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
import posixpath
from typing import Any


def revert_import(spanner_client: Any,
                  import_name: str,
                  workflow_id: str,
                  dry_run: bool = False) -> tuple[bool, str | None]:
    """Reverts an import to its previous version in Spanner and sets state to STAGING.

    Args:
        spanner_client: Spanner database client instance.
        import_name: The name of the import to revert (e.g., 'foo:bar:import').
        workflow_id: The ID of the failed workflow execution.
        dry_run: Dry run mode.

    Returns:
        A tuple (status, previous_version) where status is True on success, or False on failure.
    """
    short_name = import_name.split(':')[-1]

    # 1. Fetch recent version history to determine current and previous version.
    history = spanner_client.get_import_version_history(short_name)
    if not history:
        logging.warning(
            f"No version history found in ImportVersionHistory for '{short_name}'. Cannot revert.")
        return False, None

    latest_version = history[0][0]

    previous_version = None
    for row in history[1:]:
        if row[0] != latest_version:
            previous_version = row[0]
            break

    if not previous_version:
        logging.warning(
            f"No previous different version found in ImportVersionHistory for '{short_name}'. Cannot revert.")
        return False, None

    # 2. Get current LatestVersion path from ImportStatus to preserve GCS parent directory.
    current_latest_version_path = spanner_client.get_import_latest_version(short_name) or ""

    if current_latest_version_path:
        parent_dir = posixpath.dirname(current_latest_version_path)
        new_latest_version_path = posixpath.join(parent_dir, previous_version)
    else:
        new_latest_version_path = previous_version

    if dry_run:
        logging.info(
            f"[DRY RUN] Would revert '{short_name}' from '{latest_version}' to last known good version: '{previous_version}' (Path: '{new_latest_version_path}')"
        )
        return True, previous_version

    logging.info(
        f"Reverting '{short_name}' from '{latest_version}' to last known good version: '{previous_version}' (Path: '{new_latest_version_path}')"
    )

    # 3. Update ImportStatus to point to previous version and set state to STAGING, and record audit history.
    comment_str = f"Reverted batch workflow ({workflow_id})"
    success = spanner_client.revert_import_state(
        import_name=short_name,
        new_latest_version_path=new_latest_version_path,
        previous_version=previous_version,
        workflow_id=workflow_id,
        comment=comment_str
    )

    if success:
        return True, previous_version
    else:
        return False, None


def revert_imports(spanner_client: Any,
                   import_list: list[Any],
                   workflow_id: str,
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
            status, previous_version = revert_import(
                spanner_client, import_name, workflow_id=workflow_id, dry_run=dry_run
            )
            if status:
                msg = (
                    f"Successfully reverted '{import_name}' to version '{previous_version}' and updated status to STAGING."
                    if not dry_run
                    else f"[DRY RUN] Would revert '{import_name}' to version '{previous_version}' and update status to STAGING."
                )
                results.append({
                    "importName": import_name,
                    "reverted": True,
                    "previousVersion": previous_version,
                    "message": msg
                })
            else:
                results.append({
                    "importName": import_name,
                    "reverted": False,
                    "previousVersion": None,
                    "message": f"Failed to revert import '{import_name}'. No previous version found."
                })
        except Exception as e:
            logging.error(f"Error reverting import item {import_name}: {e}")
            results.append({
                "importName": import_name,
                "reverted": False,
                "previousVersion": None,
                "message": f"Error reverting import '{import_name}': {e}"
            })
    return results
