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
"""Models for ingestion_report.json.

These types describe the report's shape and are the contract for anything
that reads it. They hold no counting logic: summary_generator builds them once
at the end of a run and serializes them.

The report has three levels:
  run          Metadata for the preprocessor run: workflow execution ID,
               mode, status, start and end time, folder names, output path.
  folders[]    One entry per import folder: file counts by type, number of
               provenances, observation and node counts.
  provenances  Per folder, one entry per provenance: ID, source ID, file
               count, observation count, date range and node counts.
"""

from enum import Enum
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic.alias_generators import to_camel


class RunMode(str, Enum):
  """How the preprocessor was run."""
  INGEST = "INGEST"
  # TODO: Add DRY_RUN once the workflow supports dry runs.


class RunStatus(str, Enum):
  """Outcome of the preprocessor run."""
  SUCCESS = "SUCCESS"
  FAILURE = "FAILURE"


class _ReportModel(BaseModel):
  """Base for report models: camelCase JSON keys, snake_case attributes."""
  model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True)


class RunMetadata(_ReportModel):
  """The `run` block: metadata for one preprocessor run."""
  workflow_execution_id: str = Field(
      description="Ingestion workflow execution ID")
  mode: RunMode = Field(description="How the preprocessor was run")
  status: RunStatus = Field(description="Outcome of the preprocessor run")
  start_time: str = Field(description="ISO-8601 UTC time the run started")
  end_time: str = Field(description="ISO-8601 UTC time the run ended")
  folder_names: list[str] = Field(
      default_factory=list, description="Import folders requested for the run")
  output_path: str = Field(
      default="", description="Path where the run's JSON-LD was written")


class Provenance(_ReportModel):
  """One entry in `folders[].provenances[]`."""
  id: str = Field(description="Provenance DCID")
  # TODO: Add source_id, file_count, observation_count, date_range,
  # node counts by type, and provenance-level errors.


class Folder(_ReportModel):
  """One entry in `folders[]`: summary of one imported folder."""
  folder_name: str = Field(description="Name of the imported folder")
  provenances: list[Provenance] = Field(
      default_factory=list, description="Provenances in this folder")
  # TODO: Add provenance_count, observation_count, file counts by type,
  # node counts by type, and configUri.


class IngestionReport(_ReportModel):
  """Root of ingestion_report.json."""
  run: RunMetadata = Field(description="Metadata for each preprocessor run")
  folders: list[Folder] = Field(default_factory=list,
                                description="One entry per import folder")
  # TODO: Add error field in the ingestion report.
