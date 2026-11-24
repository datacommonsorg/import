# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Writes the GCS handshake JSON consumed by the Cloud Workflows ingestion pipeline."""

import json
import logging
import os

from stats.graph.data import FileValidationError
from stats.graph.data import PreprocessorError
from stats.report.model.summary import ValidationErrorType
from stats.writer.jsonld_stream_writer import JsonLdStreamWriter
from util.filesystem import is_gcs_path
from util.filesystem import write_file


def failure_error(e: BaseException, file_path: str = "") -> FileValidationError:
  """The handshake error for `e`: GENERIC_ERROR unless a PreprocessorError.

  `file_path` is the file whose import raised `e`, if known.
  """
  if isinstance(e, PreprocessorError):
    return FileValidationError(file=e.file_path or file_path,
                               error_type=e.error_type,
                               error_message=str(e))
  return FileValidationError(file=file_path,
                             error_type=ValidationErrorType.GENERIC_ERROR,
                             error_message=str(e) or type(e).__name__)


def build_import_list(graph_writer: JsonLdStreamWriter) -> list[dict[str, str]]:
  """The imports and their JSON-LD shards, for the ingestion workflow."""
  jsonld_path = graph_writer.jsonld_dir.full_path().rstrip("/")
  return [{
      "importName": name,
      "graphPath": f"{jsonld_path}/{name.replace('/', '_')}/**/*.jsonld"
  } for name in graph_writer.import_folders()]


def write_workflow_handoff(
    graph_writer: JsonLdStreamWriter | None,
    failure_errors: list[FileValidationError] | None = None,
) -> str | None:
  """Writes processed import metadata to a GCS handshake file for the ingestion workflow.

  Returns the handshake's path, or None if none was written.
  """
  workflow_id = os.getenv("WORKFLOW_EXECUTION_ID")
  if failure_errors is not None:
    errors_dict = [err.to_dict() for err in failure_errors]
    handshake_payload = {"status": "FAILURE", "errors": errors_dict}
  elif (workflow_id and graph_writer is not None and
        is_gcs_path(graph_writer.jsonld_dir.full_path())):
    handshake_payload = {
        "importList": json.dumps(build_import_list(graph_writer))
    }
  else:
    return None  # A successful run outside the workflow, or to local disk.

  temp_location = os.getenv("TEMP_LOCATION", "")

  if not workflow_id:
    logging.warning("WORKFLOW_EXECUTION_ID not set. Skipping GCS handshake.")
    return None

  if not temp_location.startswith("gs://"):
    logging.warning(
        "TEMP_LOCATION (%s) is not a GCS path. Skipping GCS handshake.",
        temp_location)
    return None

  output_json_path = f"{temp_location.rstrip('/')}/datacommons/ingestion_records/{workflow_id}.json"
  logging.info(
      "WORKFLOW_EXECUTION_ID set. Writing preprocessor result to GCS handshake path: %s",
      output_json_path)
  write_file(output_json_path, json.dumps(handshake_payload, indent=2))

  if failure_errors is None:
    logging.info("Successfully wrote preprocessor result to GCS.")
  return output_json_path
