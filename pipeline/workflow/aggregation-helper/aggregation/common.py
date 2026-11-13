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

import re
from typing import Dict, Tuple

from google.cloud.spanner_v1 import RequestOptions

BASE_PROVENANCE_PREFIX = "dc/base/"
TOPIC_LIST_PROVENANCE = "generated/TopicHierarchyLists"
STAT_VAR_GROUP_PROVENANCE = "generated/StatVarGroupsByConstraint"


def clean_bq_job_tag(value: str) -> str:
    """Converts a value into a valid BigQuery label value / job ID part.

    Lowercases the value, replaces any character other than a-z, 0-9 and '_'
    with '_', collapses consecutive underscores and truncates to 63 characters.
    E.g. 'UN_SDG/v2.1' -> 'un_sdg_v2_1'.
    """
    cleaned = re.sub(r"[^a-z0-9_]", "_", value.lower())
    cleaned = re.sub(r"_+", "_", cleaned)
    # BigQuery label values are limited to 63 characters.
    return cleaned[:63]


def build_bq_job_tags(
    is_base_dc: bool, aggr_type: str, import_name: str
) -> Tuple[Dict[str, str], str]:
    """Builds BigQuery job labels and a job ID prefix for an aggregation job.

    Args:
        is_base_dc: Whether the job runs for Base DC (otherwise DCP).
        aggr_type: Short aggregation type name, e.g. 'place_aggr'.
        import_name: Import name for the job.

    Returns:
        A (labels, job_id_prefix) tuple, e.g.
        ({'component': 'dcp', 'aggr_type': 'place_aggr', 'import': 'un_sdg'},
         'dcp_place_aggr_un_sdg_').
    """
    component = "base_dc" if is_base_dc else "dcp"
    aggr_type = clean_bq_job_tag(aggr_type)
    import_name = clean_bq_job_tag(import_name)

    labels = {
        "component": component,
        "aggr_type": aggr_type,
        "import": import_name,
    }
    job_id_prefix = f"{component}_{aggr_type}_{import_name}_"
    return labels, job_id_prefix


def get_low_priority_request_options() -> RequestOptions:
    """Returns a fresh Spanner RequestOptions instance with PRIORITY_LOW.

    Use for batch/background work so it yields CPU to serving traffic; an
    unset priority is treated as PRIORITY_HIGH. A new instance is returned on
    every call because the client library mutates the passed-in object (e.g.
    sets transaction_tag), so instances must not be shared across requests.
    """
    return RequestOptions(priority=RequestOptions.Priority.PRIORITY_LOW)


def get_provenance_prefix(is_base_dc: bool) -> str:
    """Returns the provenance prefix ('dc/base/' if base DC, else empty string)."""
    return BASE_PROVENANCE_PREFIX if is_base_dc else ""


def get_provenance_name(import_name: str, is_base_dc: bool) -> str:
    """Returns the full provenance name (e.g. 'dc/base/USFed_Census' or 'USFed_Census')."""
    return f"{get_provenance_prefix(is_base_dc)}{import_name}"


def get_generated_provenance_name(import_name: str, is_base_dc: bool) -> str:
    """Returns the generated provenance name for an import (e.g. 'dc/base/generated/USFed_Census' or 'generated/USFed_Census')."""
    return f"{get_provenance_prefix(is_base_dc)}generated/{import_name}"


def get_sql_generated_provenance_expr(
    is_base_dc: bool, source_col: str = "provenance"
) -> str:
    """Returns a SQL expression that transforms a source provenance into a scoped generated provenance."""
    if is_base_dc:
        return f"CONCAT('dc/base/generated/', REGEXP_REPLACE({source_col}, r'^dc/base/(generated/)?', ''))"
    else:
        return (
            f"CONCAT('generated/', REGEXP_REPLACE({source_col}, r'^(generated/)?', ''))"
        )


def _escape_sql_literal(val: str) -> str:
    r"""
    Escapes a string literal for use in nested BigQuery/Spanner queries.

    This is required because the query string travels through two SQL parsers:
    1. BigQuery parses the EXTERNAL_QUERY double-quoted string literal.
    2. Spanner parses the resulting inner query's single-quoted string literal.

    To ensure the value is correctly matched and prevent SQL injection:
    - Backslashes (\) are escaped to 4 backslashes (\\\\) so they survive
      both decodings (\\\\ -> \\ -> \). Otherwise, they may escape quotes
      or be interpreted as control characters (like \b becoming backspace).
      Double quotes (") are escaped to \\" to prevent terminating BQ string.
    - Single quotes (') are escaped to '' to prevent terminating Spanner string.
    """
    return val.replace("\\", "\\\\\\\\").replace('"', '\\"').replace("'", "''")


# Execution priority ranks for calculation types within a stage.
# Deterministically resolves execution order when multiple calculation types exist in the same stage.
#
# Tier 0 (0-3): Graph Topology & Schema Prerequisite Generation
#   - LINKED_EDGES (0): Computes transitive graph closures (linkedContainedInPlace, linkedMemberOf, linkedMember).
#     Must run first as place rollups rely on containment graph edges to determine target places.
#   - STAT_VAR_GROUPS (1): Constructs StatVarGroup nodes and hierarchy edges (specializationOf, memberOf).
#     Queries specializationOf and curated memberOf edges.
#   - PROVENANCE_SUMMARY (2): Generates summary statistics for observation tables in KeyValueStore.
#     Summary over observation data and place type edges.
#   - NODE_PROPERTIES (3): Aggregates node properties (types and resolved names) into Node table.
#
# Tier 1 (10-15): Data Rollups, Series, & Derived Calculations
#   - PLACE_AGGREGATION (10): Primary spatial rollup aggregating raw observations up geographic containment trees.
#   - STAT_VAR_AGGREGATION (11): Sums component variables into ancestor variables across spatial rollups.
#   - ENTITY_AGGREGATION (12): Aggregates event occurrences across spatial/temporal bounds.
#   - STAT_VAR_SERIES_AGGREGATION (13): Aggregates time series across temporal granularities (e.g. Daily -> Monthly).
#   - STAT_VAR_CALCULATION (14): Computes derived metrics/formulas (e.g. per-capita ratios like Income / Population).
#     Must run after rollups so numerators and denominators exist across all spatial and variable levels.
#   - SUPER_ENUM_AGGREGATION (15): Aggregates across enumerated concept categories.
#
# Tier 2 (20): Global Post-Processing
#   - EMBEDDING_GENERATION (20): Computes vector embeddings across finalized node properties.
#
# Note: The priority values are arbitrary and relative, used only to establish execution order.
CALCULATION_TYPE_PRIORITY = {
    "STAT_VAR_GROUPS": 0,
    "LINKED_EDGES": 1,
    "PROVENANCE_SUMMARY": 2,
    "NODE_PROPERTIES": 3,
    "PLACE_AGGREGATION": 10,
    "STAT_VAR_AGGREGATION": 11,
    "ENTITY_AGGREGATION": 12,
    "STAT_VAR_SERIES_AGGREGATION": 13,
    "STAT_VAR_CALCULATION": 14,
    "SUPER_ENUM_AGGREGATION": 15,
    "EMBEDDING_GENERATION": 20,
    "MATERIALIZED_EDGES": 20,
}
