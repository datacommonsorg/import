#!/bin/bash
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

# Promote a specified version (SHA or tag) to 'stable' in the Artifact Registry.
#
# Usage: [PROJECT_ID=<gcp_project_id>] ./promote.sh <version> [--dry-run]

set -e

VERSION=""
DRY_RUN=false
PROJECT_ID=${PROJECT_ID:-"datcom-ci"}

usage() {
  echo "Usage: [PROJECT_ID=<gcp_project_id>] $0 <version> [--dry-run]"
  echo "  <version>: Version (image tag or SHA) to promote (required)"
  echo "  --dry-run, -d: Dry run (only print the commands that would be executed)"
  echo "  PROJECT_ID: The GCP project hosting the registry and running the build (default: datcom-ci)"
  exit 1
}

# Parse arguments
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -d|--dry-run)
      DRY_RUN=true
      shift
      ;;
    -h|--help)
      usage
      ;;
    *)
      if [ -z "$VERSION" ]; then
        VERSION="$1"
      else
        echo "Error: Unknown argument '$1'"
        usage
      fi
      shift
      ;;
  esac
done

if [ -z "$VERSION" ]; then
  echo "Error: Version is required."
  usage
fi

run_cmd() {
  if [ "$DRY_RUN" = true ]; then
    echo "[DRY RUN] Would run: $*"
  else
    "$@"
  fi
}

# Promote to stable (runs in target project where the images are hosted)
echo "Promoting '$VERSION' to 'stable' in Artifact Registry (project: $PROJECT_ID)..."
run_cmd gcloud builds submit . \
  --config=update-version.yaml \
  --project="$PROJECT_ID" \
  --substitutions=_VERSION="$VERSION",_PROD_TAG=stable

echo "Promotion complete."
