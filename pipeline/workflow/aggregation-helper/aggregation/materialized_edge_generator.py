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
from .common import TOPIC_LIST_PROVENANCE, get_provenance_name


@dataclass
class MaterializedEdgeConfig:
    """Configuration for materialized edge generation."""

    enable_topic_hierarchy_lists: bool = True


class MaterializedEdgeGenerator:
    """Generates global materialized edges and literal nodes in Spanner for O(1) query lookups."""

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the MaterializedEdgeGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(
        self, config: Optional[MaterializedEdgeConfig] = None
    ) -> List[bigquery.job.QueryJob]:
        """Runs all enabled global edge materializations asynchronously and returns their jobs."""
        config = config or MaterializedEdgeConfig()
        jobs = []

        if config.enable_topic_hierarchy_lists:
            jobs.append(self.generate_topic_hierarchy_list_edges())

        # If adding new global edge materializations, append to jobs so they get executed.
        return [job for job in jobs if job]

    def generate_topic_hierarchy_list_edges(self) -> Optional[bigquery.job.QueryJob]:
        """Materializes consolidated CSV list edges on Topics and StatVarPeerGroups globally.

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
        logging.info(
            f"Generating topic and peer group list edges with output provenance '{output_provenance}'..."
        )

        query = f"""  # nosec

        -- Step 1: Extract all Topic and StatVarPeerGroup definitions across Spanner (InEdge covering index scan).
        CREATE OR REPLACE TEMPORARY TABLE `temp_topics_and_peergroups` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS entity_id, object_id AS type_name FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        -- Step 2: Extract all raw containment arcs across the entire Spanner database (InEdge index scan).
        CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_and_peergroup_edges` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS parent_id, predicate, object_id AS child_id FROM Edge WHERE predicate IN ('relevantVariable', 'member')");

        -- Step 3a: Aggregate direct child arcs into sorted CSV strings:
        --   • Topic + relevantVariable       -> relevantVariableList (e.g. "VarA,VarB,SubTopicC")
        --   • StatVarPeerGroup + member      -> memberList (e.g. "VarA,VarB")
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_topic_lists_raw` AS
        SELECT 
          raw.parent_id,
          -- Map to target list predicate based on container type
          CASE 
            WHEN types.type_name = 'Topic' AND raw.predicate = 'relevantVariable' THEN 'relevantVariableList'
            WHEN types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member' THEN 'memberList'
          END AS list_predicate,
          -- Combine distinct child DCIDs into a single alphabetically sorted CSV string
          STRING_AGG(DISTINCT raw.child_id, ',' ORDER BY raw.child_id) AS csv_member_list,
          '{output_provenance}' AS provenance
        FROM `temp_raw_topic_and_peergroup_edges` raw
        JOIN `temp_topics_and_peergroups` types ON raw.parent_id = types.entity_id
        WHERE (types.type_name = 'Topic' AND raw.predicate = 'relevantVariable')
           OR (types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member')
        GROUP BY raw.parent_id, list_predicate;

        -- Step 3b: Derive deterministic literal node keys: prefix(first 16 chars) + ':' + hex(sha256(csv_string))
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_topic_lists` AS
        SELECT
          parent_id,
          list_predicate,
          csv_member_list,
          CONCAT(
            SUBSTR(TRIM(csv_member_list), 1, 16),
            ':',
            TO_HEX(SHA256(TRIM(csv_member_list)))
          ) AS literal_node_key,
          provenance
        FROM `temp_aggregated_topic_lists_raw`;

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
