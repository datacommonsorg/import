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

# Manual helper script for building and pushing a custom datacommons-data Docker image
# for local development/testing. This script is for manual builds only and is not used in CI/CD.
# Usage: ./simple/scripts/push_image.sh <image-tag> [project-id]
# e.g. ./simple/scripts/push_image.sh my-test-tag datcom-website-dev

set -e

IMAGE_TAG=$1
PROJECT_ID=${2:-datcom-ci}

if [[ $IMAGE_TAG == "" ]]; then
  echo "No image tag specified." >&2
  echo "Usage: ./simple/scripts/push_image.sh my-test-image-tag [project-id]" >&2
  exit 1
fi

SIMPLE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"

set -x

# This script sends only the contents of simple/ to Cloud Build, so the
# Dockerfile is at the top level and _BUILD_DIR is ".".
gcloud builds submit "$SIMPLE_DIR" \
  --async \
  --project="$PROJECT_ID" \
  --ignore-file="$SIMPLE_DIR/.dockerignore" \
  --config="$SIMPLE_DIR/cloudbuild.yaml" \
  --substitutions="_BUILD_DIR=.,_TAG=$IMAGE_TAG,_VERSION=$IMAGE_TAG,_PROJECT_ID=$PROJECT_ID"
