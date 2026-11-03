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
"""
The difference categories the comparison reports, and their tallies.

FAILING categories change what ingestion loads: the set of observations and
their values, the merged properties of a node, the import report, or the
run's health. INFORMATIONAL categories only change how the same graph is laid
out (which folder or shard a record is in, how many records a subject is
written as, the bytes of a shard) or the warnings logged. --strict makes them
fail too.
"""

import dataclasses
import enum
from typing import Any


class Kind(enum.Enum):
  FAILING = "failing"
  INFORMATIONAL = "informational"


@dataclasses.dataclass(frozen=True)
class Category:
  name: str
  kind: Kind
  meaning: str


_F = Kind.FAILING
_I = Kind.INFORMATIONAL

CATEGORIES: tuple[Category, ...] = (
    # Run health.
    Category("exit_status_changed", _F,
             "The preprocessor exit status differs."),
    Category("error_lines_increased", _F,
             "The candidate logged more ERROR/CRITICAL lines or tracebacks."),
    Category("warning_lines_changed", _I,
             "The number of WARNING log lines differs."),
    # Reports.
    Category("report_json_changed", _F,
             "A field or importFiles entry of process/report.json differs."),
    Category("worker_reports_changed", _F,
             "A per-file process/report_*.json entry differs or is missing."),
    # Run directory and shards.
    Category("run_dir_name_changed", _F,
             "The JSON-LD run directory name (without timestamp) differs."),
    Category("jsonld_context_changed", _F,
             "The set of @context values used by the shards differs."),
    Category(
        "shard_set_changed", _I,
        "An observation shard name, or a folder's node shard count, "
        "exists on one side only."),
    Category(
        "shard_bytes_changed", _I,
        "A same-name observation shard differs in bytes, or a node shard "
        "has no byte-identical twin in the same folder."),
    # Observations, keyed by (variable, entity, date, provenance, unit,
    # measurement method, period, other properties).
    Category("obs_only_baseline", _F,
             "An observation key exists only in the baseline."),
    Category("obs_only_candidate", _F,
             "An observation key exists only in the candidate."),
    Category("obs_multiplicity_changed", _F,
             "An observation key is written a different number of times."),
    Category("obs_value_changed", _F,
             "Same key, different dcid:value or dcid:scalingFactor."),
    Category(
        "obs_properties_changed", _F,
        "Same key and value, different @id, @type or variableMeasured "
        "object."),
    Category("obs_provenance_url_changed", _F,
             "Same key, dcid:provenanceUrl added, removed or changed."),
    Category("obs_folder_moved", _I,
             "Same key, written to a different import folder."),
    # Nodes, keyed by @id; all records of an @id are merged first.
    Category("node_only_baseline", _F,
             "A node @id exists only in the baseline."),
    Category("node_only_candidate", _F,
             "A node @id exists only in the candidate."),
    Category("node_properties_changed", _F,
             "A property of a node differs after merging all its records."),
    Category("node_folder_moved", _I,
             "A node @id is written to a different set of folders."),
    Category(
        "node_records_changed", _I,
        "A node @id is written as a different number of records "
        "(split or duplicated), with the same merged properties."),
)

CATEGORIES_BY_NAME: dict[str, Category] = {c.name: c for c in CATEGORIES}


def sides(baseline: Any, candidate: Any) -> dict[str, Any]:
  """Returns the baseline and candidate halves of a sample."""
  return {"baseline": baseline, "candidate": candidate}


@dataclasses.dataclass
class Findings:
  """Per-category difference counts plus the first few samples of each."""

  max_samples: int
  counts: dict[str, int] = dataclasses.field(
      default_factory=lambda: {c.name: 0 for c in CATEGORIES})
  samples: dict[str, list[Any]] = dataclasses.field(
      default_factory=lambda: {c.name: [] for c in CATEGORIES})

  def add(self, name: str, count: int = 1, sample: Any = None) -> None:
    """Counts `count` differences of a category and keeps `sample`."""
    self.counts[name] += count
    if sample is not None:
      self.add_sample(name, sample)

  def add_sample(self, name: str, sample: Any) -> None:
    """Keeps a sample of an already counted difference, up to max_samples."""
    if len(self.samples[name]) < self.max_samples:
      self.samples[name].append(sample)

  def failing(self, strict: bool) -> dict[str, int]:
    """Returns the non-zero categories that make the verdict DIFFERENT."""
    return {
        name: count
        for name, count in self.counts.items()
        if count and (strict or CATEGORIES_BY_NAME[name].kind == Kind.FAILING)
    }
