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

BASE_PROVENANCE_PREFIX = "dc/base/"


def get_provenance_prefix(is_base_dc: bool) -> str:
    """Returns the provenance prefix ('dc/base/' if base DC, else empty string)."""
    return BASE_PROVENANCE_PREFIX if is_base_dc else ""


def get_provenance_name(import_name: str, is_base_dc: bool) -> str:
    """Returns the full provenance name (e.g. 'dc/base/USFed_Census' or 'USFed_Census')."""
    return f"{get_provenance_prefix(is_base_dc)}{import_name}"


def get_generated_provenance_name(import_name: str, is_base_dc: bool) -> str:
    """Returns the generated provenance name for an import (e.g. 'dc/base/generated/USFed_Census' or 'generated/USFed_Census')."""
    return f"{get_provenance_prefix(is_base_dc)}generated/{import_name}"


def get_sql_generated_provenance_expr(is_base_dc: bool, source_col: str = "provenance") -> str:
    """Returns a SQL expression that transforms a source provenance into a scoped generated provenance."""
    if is_base_dc:
        return f"CONCAT('dc/base/generated/', REGEXP_REPLACE({source_col}, r'^dc/base/(generated/)?', ''))"
    else:
        return f"CONCAT('generated/', REGEXP_REPLACE({source_col}, r'^(generated/)?', ''))"


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
    return val.replace('\\', '\\\\\\\\').replace('"', '\\"').replace("'", "''")


# Execution priority ranks for calculation types within a stage.
# Deterministically resolves execution order when multiple calculation types exist in the same stage.
#
# Tier 0 (0-2): Graph Topology & Schema Prerequisite Generation
#   - LINKED_EDGES (0): Computes transitive graph closures (linkedContainedInPlace, linkedMemberOf, linkedMember).
#     Must run first as place rollups rely on containment graph edges to determine target places.
#   - STAT_VAR_GROUPS (1): Constructs StatVarGroup nodes and hierarchy edges (specializationOf, memberOf).
#     Queries specializationOf and curated memberOf edges.
#   - PROVENANCE_SUMMARY (2): Generates summary statistics for observation tables in KeyValueStore.
#     Summary over observation data and place type edges.
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
CALCULATION_TYPE_PRIORITY = {
    "LINKED_EDGES": 0,
    "STAT_VAR_GROUPS": 1,
    "PROVENANCE_SUMMARY": 2,
    "PLACE_AGGREGATION": 10,
    "STAT_VAR_AGGREGATION": 11,
    "ENTITY_AGGREGATION": 12,
    "STAT_VAR_SERIES_AGGREGATION": 13,
    "STAT_VAR_CALCULATION": 14,
    "SUPER_ENUM_AGGREGATION": 15,
    "EMBEDDING_GENERATION": 20,
}

