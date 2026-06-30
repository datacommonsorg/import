# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Aggregation package for Data Commons ingestion workflow.

This package contains modules for executing BigQuery queries and generating
linked relationship edges and provenance summaries in Spanner.
"""

from .bq_executor import BigQueryExecutor
from .linked_edge_generator import LinkedEdgeGenerator
from .provenance_summary_generator import ProvenanceSummaryGenerator
from .stat_var_aggregator import StatVarAggregator
from .place_aggregation_generator import PlaceAggregationGenerator
from .stat_var_group_generator import StatVarGroupGenerator
from .entity_aggregation_generator import EntityAggregationGenerator, EntityAggregationConfig

__all__ = [
    'BigQueryExecutor',
    'LinkedEdgeGenerator',
    'ProvenanceSummaryGenerator',
    'StatVarAggregator',
    'PlaceAggregationGenerator',
    'StatVarGroupGenerator',
    'EntityAggregationGenerator',
    'EntityAggregationConfig'
]
