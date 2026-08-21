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

import logging
from dataclasses import dataclass
from typing import List, Optional

from google.cloud import bigquery

from .bq_executor import BigQueryExecutor
from .common import (
    BASE_PROVENANCE_PREFIX,
    TOPIC_LIST_PROVENANCE,
    _escape_sql_literal,
    get_provenance_name,
    get_sql_generated_provenance_expr,
)



@dataclass
class LinkedEdgeConfig:
    """Configuration for linked edge generation."""
    import_names: Optional[List[str]] = None
    generate_topic_list_edges: bool = False


class LinkedEdgeGenerator:
    """Generates and ingests linked relationship edges (e.g., transitive closures) into Spanner for faster lookup."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the LinkedEdgeGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self,
                config: LinkedEdgeConfig) -> List[bigquery.job.QueryJob]:
        """Runs all global aggregations asynchronously and returns their jobs."""
        import_names = config.import_names

        if not import_names:
            logging.info("No imports specified. Skipping global aggregations.")
            return []

        logging.info(f"Running global aggregations for imports: {import_names}")

        jobs = [
            self.run_linked_contained_in_place(import_names),
            self.run_linked_member_of(import_names),
            self.run_linked_member(import_names)
        ]
        return [job for job in jobs if job]

    def run_linked_member_of(
            self,
            import_names: List[str] = None) -> Optional[bigquery.job.QueryJob]:
        """Expands membership hierarchies using memberOf and specializationOf."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        prefix = BASE_PROVENANCE_PREFIX if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")

        query = f"""  # nosec
        -- Pull base edges needed for memberOf aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_member_of` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate IN ('memberOf', 'specializationOf'){provenance_filter}");

        CREATE OR REPLACE TEMPORARY TABLE `temp_hierarchy` AS
        SELECT DISTINCT subject_id, predicate, object_id, provenance
        FROM `temp_base_member_of`;

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH RECURSIVE Ancestors AS (
          SELECT
            subject_id,
            object_id AS ancestor,
            1 AS level,
            provenance
          FROM
            temp_hierarchy
          WHERE
            predicate = 'memberOf'
          UNION ALL

          SELECT
            a.subject_id,
            t.object_id AS ancestor,
            a.level + 1,
            a.provenance
          FROM
            Ancestors AS a
          JOIN
            temp_hierarchy AS t
            ON a.ancestor = t.subject_id
          WHERE
            a.level <= 20 -- Limit to 20 levels
            AND t.predicate = 'specializationOf'
        ),
        NewEdges AS (
          SELECT DISTINCT
            subject_id,
            'linkedMemberOf' as predicate,
            ancestor as object_id,
            {prov_expr} as provenance
          FROM
            Ancestors
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          NewEdges
        """
        return self.executor.execute(query)

    def run_linked_contained_in_place(
            self,
            import_names: List[str] = None) -> Optional[bigquery.job.QueryJob]:
        """Expands place containment hierarchies."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        prefix = "dc/base/" if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")

        query = f"""  # nosec
        -- Pull base edges needed for containedInPlace aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_contained_in_place` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}",
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate = 'containedInPlace'{provenance_filter}");

        CREATE OR REPLACE TEMPORARY TABLE `temp_contained_in_place` AS
        SELECT subject_id, object_id, provenance
        FROM `temp_base_contained_in_place`;

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        with RECURSIVE Ancestors AS (
          SELECT
            subject_id,
            object_id AS ancestor_place,
            1 AS level,
            provenance
          FROM
            temp_contained_in_place
          UNION ALL

          SELECT
            a.subject_id,
            t.object_id AS ancestor_place,
            a.level + 1,
            a.provenance
          FROM
            Ancestors AS a
          JOIN
            temp_contained_in_place AS t
            ON a.ancestor_place = t.subject_id
          WHERE
            a.level <= 10 -- Limit to 10 levels
        ),
        NewEdges AS (
          SELECT DISTINCT
            subject_id,
            'linkedContainedInPlace' as predicate,
            ancestor_place as object_id,
            {prov_expr} as provenance
          FROM
            Ancestors
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          NewEdges
        """
        return self.executor.execute(query)

    def run_linked_member(
            self,
            import_names: List[str] = None) -> Optional[bigquery.job.QueryJob]:
        """Materializes reverse index 'linkedMember' edges from leaf variables to ancestor topics.

        Mixer uses 'linkedMember' for O(1) place existence checks without in-memory tree traversal.

        Transformation:
          Input:   Topic:Health -> relevantVariable -> Topic:Respiratory -> relevantVariable -> Var:Asthma
          Output:  Var:Asthma -> linkedMember -> Topic:Respiratory (prov: 'generated/{import}')
                   Var:Asthma -> linkedMember -> Topic:Health      (prov: 'generated/{import}')

        Args:
            import_names: List of import dataset names whose parent-child arcs will be traversed.

        Returns:
            BigQuery QueryJob if queries are submitted, or None if import_names is empty.
        """
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        prefix = "dc/base/" if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")

        query = f"""  # nosec
        -- Step 1: Extract all Topics and StatVarPeerGroups across Spanner.
        -- Uses a covering index seek on InEdge (predicate='typeOf', object_id) with subject_id
        -- stored in the index, requiring 0 base table lookups in Spanner.
        CREATE OR REPLACE TEMPORARY TABLE `temp_topics_and_peergroups` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS entity_id, object_id AS type_name FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        -- Step 2: Extract Topic and SVPG containment arcs ('relevantVariable' for Topics, 'member' for SVPGs)
        -- for active imports using an InEdge index seek on predicate.
        CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_and_peergroup_edges` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS parent_id, predicate, object_id AS child_id, provenance FROM Edge WHERE predicate IN ('relevantVariable', 'member'){provenance_filter}");

        -- Step 3: Keep only edges where 'relevantVariable' is on a Topic or 'member' is on a StatVarPeerGroup.
        -- Joining in BigQuery offloads all join processing from Spanner to BigQuery's vectorized engine.
        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_hierarchy` AS
        SELECT DISTINCT raw.parent_id, raw.child_id, raw.provenance
        FROM `temp_raw_topic_and_peergroup_edges` raw
        JOIN `temp_topics_and_peergroups` types ON raw.parent_id = types.entity_id
        WHERE (types.type_name = 'Topic' AND raw.predicate = 'relevantVariable')
           OR (types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member');

        -- Step 4: Recursively traverse hierarchy downward to leaf variables, then invert edge direction
        -- so leaf variables point back up to ancestor topics with predicate 'linkedMember'.
        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH RECURSIVE Descendants AS (
          -- Base case: direct children (depth 1)
          SELECT
            parent_id AS ancestor_topic_id,
            child_id AS descendant_node_id,
            1 AS depth,
            provenance
          FROM
            temp_topic_hierarchy
          UNION ALL

          -- Recursive case: climb down through subtopics and peer groups
          SELECT
            d.ancestor_topic_id,
            child.child_id AS descendant_node_id,
            d.depth + 1,
            d.provenance
          FROM
            Descendants AS d
          JOIN
            temp_topic_hierarchy AS child
            ON d.descendant_node_id = child.parent_id
          WHERE
            d.depth <= 20 -- Safety limit: max hierarchy depth of 20 levels
        ),
        NewEdges AS (
          -- Filter and invert: only emit (leaf_variable -> linkedMember -> ancestor_topic)
          SELECT DISTINCT
            descendant_node_id AS subject_id,
            'linkedMember' AS predicate,
            ancestor_topic_id AS object_id,
            {prov_expr} AS provenance
          FROM
            Descendants
          WHERE ancestor_topic_id IN (SELECT entity_id FROM temp_topics_and_peergroups WHERE type_name = 'Topic')
          AND descendant_node_id NOT IN (SELECT entity_id FROM temp_topics_and_peergroups)
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          NewEdges
        """
        return self.executor.execute(query)

    def run_topic_list_edges(self) -> Optional[bigquery.job.QueryJob]:
        """Materializes consolidated CSV list edges on Topics and StatVarPeerGroups.

        Mixer and Explore NL require comma-separated string literals ('relevantVariableList'
        and 'memberList') for instant O(1) child expansion.

        Transformation:
          Input:   Topic:Health -> relevantVariable -> Var:Asthma, Var:Cancer
                   SVPG:Ages    -> member           -> Var:0To17, Var:18To64
          Output:  Edge: Topic:Health -> relevantVariableList -> HashedNodeKey (prov: 'generated/TopicHierarchyLists')
                   Edge: SVPG:Ages    -> memberList           -> HashedNodeKey (prov: 'generated/TopicHierarchyLists')
                   Node: HashedNodeKey.value = "Var:Asthma,Var:Cancer" (or "Var:0To17,Var:18To64")

        Returns:
            BigQuery QueryJob executing the global aggregation and Spanner export.
        """
        dest = self.executor.get_spanner_destination_uri()
        output_provenance = get_provenance_name(TOPIC_LIST_PROVENANCE, self.is_base_dc)

        query = f"""  # nosec
        -- Step 1: Extract all Topic and StatVarPeerGroup definitions across Spanner (InEdge covering index scan).
        CREATE OR REPLACE TEMPORARY TABLE `temp_topics_and_peergroups` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS entity_id, object_id AS type_name FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        -- Step 2: Extract all raw containment arcs across the entire Spanner database (InEdge index scan).
        CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_and_peergroup_edges` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS parent_id, predicate, object_id AS child_id FROM Edge WHERE predicate IN ('relevantVariable', 'member')");

        -- Step 3: Aggregate direct child arcs into sorted CSV strings and compute SHA256 literal node keys:
        --   • Topic + relevantVariable       -> relevantVariableList (e.g. "VarA,VarB,SubTopicC")
        --   • StatVarPeerGroup + member      -> memberList (e.g. "VarA,VarB")
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_topic_lists` AS
        SELECT 
          raw.parent_id,
          -- Map to target list predicate based on container type
          CASE 
            WHEN types.type_name = 'Topic' AND raw.predicate = 'relevantVariable' THEN 'relevantVariableList'
            WHEN types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member' THEN 'memberList'
          END AS list_predicate,
          -- Combine distinct child DCIDs into a single alphabetically sorted CSV string
          STRING_AGG(DISTINCT raw.child_id, ',' ORDER BY raw.child_id) AS csv_member_list,
          -- Deterministic SHA256 node key: prefix(first 16 chars) + ':' + hex(sha256(csv_string))
          CONCAT(
            SUBSTR(TRIM(STRING_AGG(DISTINCT raw.child_id, ',' ORDER BY raw.child_id)), 1, 16),
            ':', 
            TO_HEX(SHA256(TRIM(STRING_AGG(DISTINCT raw.child_id, ',' ORDER BY raw.child_id))))
          ) AS literal_node_key,
          '{output_provenance}' AS provenance
        FROM `temp_raw_topic_and_peergroup_edges` raw
        JOIN `temp_topics_and_peergroups` types ON raw.parent_id = types.entity_id
        WHERE (types.type_name = 'Topic' AND raw.predicate = 'relevantVariable')
           OR (types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member')
        GROUP BY raw.parent_id, list_predicate;

        -- Step 4: Create the string literal nodes in Spanner Node table (stores CSV list in Node.value).
        EXPORT DATA
          OPTIONS(
            uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}'
          ) AS
        SELECT DISTINCT
          literal_node_key AS subject_id,
          csv_member_list AS value,
          CAST(NULL AS BYTES) AS bytes,
          '' AS name,
          CAST([] AS ARRAY<STRING>) AS types
        FROM `temp_aggregated_topic_lists`;

        -- Step 5: Insert relevantVariableList and memberList triples into Spanner Edge table.
        EXPORT DATA
          OPTIONS(
            uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}'
          ) AS
        SELECT DISTINCT
          parent_id AS subject_id,
          list_predicate AS predicate,
          literal_node_key AS object_id,
          provenance
        FROM `temp_aggregated_topic_lists`;
        """
        return self.executor.execute(query)

