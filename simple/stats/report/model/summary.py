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

from dataclasses import dataclass
from dataclasses import field
from enum import StrEnum


class RunMode(StrEnum):
  """How the preprocessor was run."""
  INGEST = "INGEST"
  # TODO: Add DRY_RUN once the workflow supports dry runs.


class RunStatus(StrEnum):
  """Outcome of the preprocessor run."""
  SUCCESS = "SUCCESS"
  FAILURE = "FAILURE"


@dataclass
class Run:
  """The `run` block: metadata for one preprocessor run."""
  workflow_execution_id: str | None
  mode: RunMode
  status: RunStatus
  start_time: str
  end_time: str
  folder_names: list[str] = field(default_factory=list)
  output_path: str = ""


@dataclass
class Provenance:
  """One entry in `folders[].provenances[]`."""
  id: str
  # TODO: Add source_id, file_count, observation_count, date_range, 
  #node counts by type, and provenance-level errors.


@dataclass
class Folder:
  """One entry in `folders[]`: summary of one import folder."""
  folder_name: str
  provenances: list[Provenance] = field(default_factory=list)
  # TODO: Add provenance_count, observation_count, file counts by type, 
  # node counts by type, and configUri.


@dataclass
class IngestionReport:
  """Root of ingestion_report.json."""
  run: Run
  folders: list[Folder] = field(default_factory=list)
  # TODO : Add error field in the ingestion report.