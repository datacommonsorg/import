#!/bin/bash

# Decode a base64 encoded Observations proto string.
#
# Usage:
# ./decode_obs.sh <base64_string>
#
# Example:
# ./decode_obs.sh "Cg4KBzE5NDgtMTASAzEuMA=="
#

# --- Configuration ---
PROTO_FILE_PATH="storage.proto" # Replace with the actual path to your .proto file
MESSAGE_TYPE="org.datacommons.proto.Observations"
PROTO_DIR_PATH=$(dirname "$PROTO_FILE_PATH")

# Expects the base64 string as the first and only argument.
if [ "$#" -ne 1 ]; then
    echo "Usage: $0 <base64_string>"
    exit 1
fi

BASE64_STRING="$1"

echo "$BASE64_STRING" | base64 --decode | protoc --decode="$MESSAGE_TYPE" --proto_path="$PROTO_DIR_PATH" "$PROTO_FILE_PATH"
