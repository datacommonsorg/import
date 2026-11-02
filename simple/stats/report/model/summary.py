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

from pydantic import BaseModel
from pydantic import Field


class RunMode(str, Enum):
  """How the preprocessor was run."""

  INGEST = "INGEST"
  # TODO: Add DRY_RUN once the workflow supports dry runs.


class RunStatus(str, Enum):
  """Outcome of the preprocessor run."""

  SUCCESS = "SUCCESS"
  FAILURE = "FAILURE"


class ExecutionMetadata(BaseModel):
  """The `run` block: metadata for one preprocessor run."""

  workflowExecutionId: str = Field(
      description="Ingestion workflow execution ID")
  mode: RunMode = Field(description="How the preprocessor was run")
  status: RunStatus = Field(description="Outcome of the preprocessor run")
  startTime: str = Field(description="ISO-8601 UTC time the run started")
  endTime: str = Field(description="ISO-8601 UTC time the run ended")
  folderNames: list[str] = Field(
      default_factory=list, description="Import folders requested for the run")
  outputPath: str = Field(
      default="", description="Path where the run's JSON-LD was written")


class Provenance(BaseModel):
  """One entry in `folders[].provenances[]`."""

  id: str = Field(description="Provenance DCID")
  # TODO: Add source_id, file_count, observation_count, date_range,
  # node counts by type, and provenance-level errors.


class Folder(BaseModel):
  """One entry in `folders[]`: summary of one imported folder."""

  folderName: str = Field(description="Name of the imported folder")
  provenances: list[Provenance] = Field(
      default_factory=list, description="Provenances in this folder")
  # TODO: Add provenance_count, observation_count, file counts by type,
  # node counts by type, and configUri.


class IngestionReport(BaseModel):
  """Root of ingestion_report.json."""

  run: ExecutionMetadata = Field(
      description="Metadata for each preprocessor run")
  folders: list[Folder] = Field(default_factory=list,
                                description="One entry per import folder")
  # TODO: Add error field in the ingestion report.


class FileSummary(BaseModel):
  """Counts for one imported file, returned by the worker that imported it.

  Not part of the report itself: SummaryGenerator merges these into the
  folder and provenance totals.
  """

  filePath: str = Field(description="Input file path")
  folder: str = Field(description="Import folder the file belongs to")
  provenance: str | None = Field(
      default=None, description="Provenance configured for the file")
  observations: int = Field(default=0,
                            description="StatVarObservation nodes written")
  # TODO: Add minDate, maxDate and nodesByType.
