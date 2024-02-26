#!/bin/bash
# Copyright 2024 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -o allexport
source env.list
set +o allexport

if [[ $CONFIG_FILE != "" ]]; then
    docker run -it \
        --env-file env.list \
        -e GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json \
        -v $HOME/.config/gcloud/application_default_credentials.json:/gcp/creds.json:ro \
        -e CONFIG_FILE=/config.json \
        -v $CONFIG_FILE:/config.json:ro \
        gcr.io/datcom-ci/datacommons-simple:latest
else
    docker run -it \
        --env-file env.list \
        -e GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json \
        -v $HOME/.config/gcloud/application_default_credentials.json:/gcp/creds.json:ro \
        gcr.io/datcom-ci/datacommons-simple:latest
fi
