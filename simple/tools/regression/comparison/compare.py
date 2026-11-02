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
"""Compares two preprocessor output directories: shards, reports, records."""

import concurrent.futures
import dataclasses
import tempfile
from typing import Any

from tools.regression.comparison import shards
from tools.regression.comparison.categories import Findings
from tools.regression.comparison.categories import sides
from tools.regression.comparison.nodes import NodeComparer
from tools.regression.comparison.observations import ObservationComparer
from tools.regression.comparison.report_json import compare_reports


@dataclasses.dataclass(frozen=True)
class SideOutput:
  output_dir: str
  input_root: str | None  # The --input_dir the side ran on, if any.


@dataclasses.dataclass(frozen=True)
class Comparison:
  findings: Findings
  stats: dict[str, Any]


def compare_outputs(baseline: SideOutput,
                    candidate: SideOutput,
                    scratch_dir: str,
                    workers: int | None = None,
                    max_samples: int = 5) -> Comparison:
  """Compares every output file of two runs.

  Args:
    baseline: The baseline output.
    candidate: The candidate output.
    scratch_dir: Where the spill buckets go, in a temporary subdirectory. They
      need about 50 bytes per observation plus the node shard bytes, per side.
    workers: Processes used to parse shards (default: one per CPU).
    max_samples: Samples kept per difference category.
  """
  findings = Findings(max_samples)
  trees = [shards.load_output_tree(s.output_dir) for s in (baseline, candidate)]
  with concurrent.futures.ProcessPoolExecutor(workers) as pool, \
       tempfile.TemporaryDirectory(dir=scratch_dir, prefix="compare-") as spill:
    observations = ObservationComparer(findings, pool, f"{spill}/obs")
    nodes = NodeComparer(findings, pool, f"{spill}/nodes")
    inventory = shards.compare_inventory(*trees, findings, pool)
    reports = compare_reports(baseline.output_dir, baseline.input_root,
                              candidate.output_dir, candidate.input_root,
                              findings)
    stats = {
        "inventory": inventory,
        "reports": reports,
        "observations": observations.compare(*trees),
        "nodes": nodes.compare(*trees),
    }
  contexts = [o | n for o, n in zip(observations.contexts, nodes.contexts)]
  if contexts[0] != contexts[1]:
    findings.add("jsonld_context_changed",
                 sample=sides(sorted(contexts[0]), sorted(contexts[1])))
  return Comparison(findings, stats)
