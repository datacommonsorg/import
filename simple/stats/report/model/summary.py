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
at the end of a run and serializes them. The one exception is DateRange.widen,
the merge rule that the writer and summary_generator share.

The report has three levels:
  run          Metadata for the preprocessor run: workflow execution ID,
               mode, status, start and end time, folder names, output path.
  folders[]    One entry per import folder: file counts by type, number of
               provenances, observation and node counts.
  provenances  Per folder, one entry per provenance: ID, source ID, file
               count, observation count, date range and node counts.
"""

from enum import Enum
from enum import StrEnum
from typing import Self

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
  status: RunStatus | None = Field(
      default=None, description="Outcome of the preprocessor run")
  startTime: str = Field(description="ISO-8601 UTC time the run started")
  endTime: str = Field(default="",
                       description="ISO-8601 UTC time the run ended")
  folderNames: list[str] = Field(
      default_factory=list, description="Import folders requested for the run")
  outputPath: str = Field(default="",
                          description="Folder this report is written to")


class DateRange(BaseModel):
  """Earliest and latest observation dates, as they appear in the data.

  Dates are compared as strings, which orders ISO-8601 dates correctly
  (2005 < 2005-06 < 2006). Other formats are reported as-is.
  """

  min: str = Field(description="Earliest observation date")
  max: str = Field(description="Latest observation date")

  def widen(self, other: Self) -> Self:
    """Returns the smallest range that covers both this range and `other`."""
    return DateRange(min=min(self.min, other.min), max=max(self.max, other.max))


class NodeCounts(BaseModel):
  """Typed nodes written, other than StatVarObservations, and their types."""

  count: int = Field(
      default=0,
      description=("Number of (node, type) pairs written, so a node with "
                   "several types counts once per type; equals the sum of "
                   "byType"))
  byType: dict[str, int] = Field(
      default_factory=dict,
      description=("Number of nodes per type. A node with several types "
                   "counts under each."))


class ValidationErrorType(StrEnum):
  UNMAPPED_COLUMNS = "UNMAPPED_COLUMNS"
  MISSING_REQUIRED_COLUMNS = "MISSING_REQUIRED_COLUMNS"
  INVALID_CONFIGURATION = "INVALID_CONFIGURATION"
  MISSING_SOURCE = "MISSING_SOURCE"
  MISSING_PROVENANCE = "MISSING_PROVENANCE"
  GENERIC_ERROR = "GENERIC_ERROR"


class ReportError(BaseModel):
  """One error that failed the run, with the same fields as the handshake's."""

  file: str = Field(default="",
                    description="Input file at fault, or empty if unknown")
  errorType: ValidationErrorType = Field(description="Error type")
  problemColumns: list[str] = Field(default_factory=list,
                                    description="CSV columns at fault, if any")
  errorMessage: str = Field(default="", description="What went wrong")


class Provenance(BaseModel):
  """One entry in `folders[].provenances[]`."""

  id: str = Field(description="Provenance DCID")
  sourceId: str | None = Field(
      default=None, description="DCID of the provenance's source, if known")
  fileCount: int = Field(
      default=0, description="Input files imported under this provenance")
  observations: int = Field(default=0,
                            description="StatVarObservation nodes written")
  dateRange: DateRange | None = Field(
      default=None,
      description=("Observation dates covered, or null without dated "
                   "observations. Rows without a date count toward "
                   "observations but not toward dateRange."))
  nodes: NodeCounts = Field(default_factory=NodeCounts,
                            description="Typed nodes written")
  errors: list[ReportError] = Field(
      default_factory=list,
      description="Errors in this provenance's input files that failed the run")


class FileCounts(BaseModel):
  """`folders[].summary.files`: input files in one folder."""

  count: int = Field(default=0, description="Number of input files")
  byType: dict[str, int] = Field(
      default_factory=dict,
      description="Number of input files per lowercase extension")


class FolderSummary(BaseModel):
  """`folders[].summary`: totals for one imported folder."""

  files: FileCounts = Field(default_factory=FileCounts,
                            description="Input files in the folder")
  provenanceCount: int = Field(
      default=0, description="Number of provenances in the folder")
  observations: int = Field(
      default=0,
      description="StatVarObservation nodes written, over all provenances")
  nodes: NodeCounts = Field(
      default_factory=NodeCounts,
      description="Typed nodes written, over all provenances")


class Folder(BaseModel):
  """One entry in `folders[]`: summary of one imported folder."""

  folderName: str = Field(description="Name of the imported folder")
  summary: FolderSummary = Field(default_factory=FolderSummary,
                                 description="Totals for the folder")
  provenances: list[Provenance] = Field(
      default_factory=list, description="Provenances in this folder")


class IngestionReport(BaseModel):
  """Root of ingestion_report.json."""

  run: ExecutionMetadata = Field(
      description="Metadata for each preprocessor run")
  folders: list[Folder] = Field(default_factory=list,
                                description="One entry per import folder")
  errors: list[ReportError] = Field(
      default_factory=list,
      description=("Errors that failed the run and belong to no input file, "
                   "e.g. config or metadata errors"))


class FileSummary(BaseModel):
  """Counts for one imported file.

  Not part of the report itself. Runner builds one from each worker's result,
  adding the file's folder and provenance, and SummaryGenerator merges it into
  the folder and provenance totals.
  """

  filePath: str = Field(description="Input file path")
  folder: str = Field(description="Import folder the file belongs to")
  provenanceId: str = Field(description="ID of the file's provenance")
  observations: int = Field(default=0,
                            description="StatVarObservation nodes written")
  dateRange: DateRange | None = Field(
      default=None, description="Observation dates covered, if any")
  nodes: NodeCounts = Field(default_factory=NodeCounts,
                            description="Typed nodes written")
