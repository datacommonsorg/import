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

if [[ $GCS_OUTPUT_DIR != "" ]]; then
    export OUTPUT_DIR=$GCS_OUTPUT_DIR
else
    export OUTPUT_DIR=/output/
fi

if [[ $CONFIG_FILE != "" ]]; then
    echo "DC_API_KEY=$DC_API_KEY"
    echo "CONFIG_FILE=$CONFIG_FILE"
    echo "OUTPUT_DIR=$OUTPUT_DIR"

    python3 -m stats.main \
        --mode=maindc \
        --config_file=$CONFIG_FILE \
        --output_dir=$OUTPUT_DIR
else
    if [[ $GCS_INPUT_DIR != "" ]]; then
        export INPUT_DIR=$GCS_INPUT_DIR
    else
        export INPUT_DIR=/input/
    fi

    echo "DC_API_KEY=$DC_API_KEY"
    echo "INPUT_DIR=$INPUT_DIR"
    echo "OUTPUT_DIR=$OUTPUT_DIR"

    python3 -m stats.main \
        --mode=maindc \
        --input_dir=$INPUT_DIR \
        --output_dir=$OUTPUT_DIR
fi

