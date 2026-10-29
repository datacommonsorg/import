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
"""Generates the ingestion report for a preprocessor run.

This module holds all of the report's logic:
  - Recording facts as the run progresses: input files, observations, nodes.
  - Building the report models defined in model/summary.py and writing
    ingestion_report.json once per run, on success and on failure.

Report code must never change the outcome of a run, so errors while
collecting or writing the report are logged rather than raised.
"""

from util.filesystem import Dir
from util.filesystem import File

class SummaryGenerator:
  """Generates the ingestion report for a preprocessor run."""

  def __init__(self) -> None:
    """Creates a generator for one preprocessor run. """
    # TODO: Add record_run_start(start_time, folder_names) and counters for
    # input files, observations, and nodes.

  def write_summary(self, output_dir: Dir) -> File | None:
    """Writes the ingestion report as JSON into output_dir.

    Args:
      output_dir: Directory where the report should be written.

    Returns:
      The written report file, or None if writing failed.
    """
    # TODO: Build the report and write it as ingestion_report.json.
    return None
