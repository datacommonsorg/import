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
from .common import BASE_PROVENANCE_PREFIX, _escape_sql_literal, get_sql_generated_provenance_expr


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
        if getattr(config, "generate_topic_list_edges", True):
            jobs.append(self.run_topic_list_edges(import_names))
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
        """Expands topic/SVGP descendants to identify leaf members."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        prefix = "dc/base/" if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")

        query = f"""  # nosec
        -- Pull base edges needed for member aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_member` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate IN ('relevantVariable', 'member'){provenance_filter}");

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_types` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_nodes` AS
        SELECT DISTINCT subject_id FROM `temp_topic_types` WHERE object_id = 'Topic'
        UNION DISTINCT
        SELECT DISTINCT subject_id FROM `temp_base_member` 
        WHERE subject_id LIKE '%/topic/%' OR subject_id LIKE 'dc/topic%';

        CREATE OR REPLACE TEMPORARY TABLE `temp_svpg_nodes` AS
        SELECT DISTINCT subject_id FROM `temp_topic_types` WHERE object_id = 'StatVarPeerGroup'
        UNION DISTINCT
        SELECT DISTINCT subject_id FROM `temp_base_member` 
        WHERE subject_id LIKE '%/svpg/%' OR subject_id LIKE 'dc/svpg%';

        CREATE OR REPLACE TEMPORARY TABLE `temp_all_topic_svpg_nodes` AS
        SELECT subject_id FROM `temp_topic_nodes`
        UNION DISTINCT
        SELECT subject_id FROM `temp_svpg_nodes`;

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_hierarchy` AS
        SELECT DISTINCT b.subject_id, b.object_id, b.provenance
        FROM `temp_base_member` b
        JOIN `temp_all_topic_svpg_nodes` n ON b.subject_id = n.subject_id;

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH RECURSIVE Descendants AS (
          SELECT
            subject_id,
            object_id AS descendant,
            1 AS level,
            provenance
          FROM
            temp_topic_hierarchy
          UNION ALL

          SELECT
            d.subject_id,
            t.object_id AS descendant,
            d.level + 1,
            d.provenance
          FROM
            Descendants AS d
          JOIN
            temp_topic_hierarchy AS t
            ON d.descendant = t.subject_id
          WHERE
            d.level <= 20 -- Limit to 20 levels
        ),
        NewEdges AS (
          SELECT DISTINCT
            descendant as subject_id,
            'linkedMember' as predicate,
            subject_id as object_id,
            {prov_expr} as provenance
          FROM
            Descendants
          WHERE subject_id IN (SELECT subject_id FROM temp_topic_nodes)
          AND descendant NOT IN (SELECT subject_id FROM temp_all_topic_svpg_nodes)
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

    def run_topic_list_edges(
            self,
            import_names: Optional[List[str]] = None) -> Optional[bigquery.job.QueryJob]:
        """Materializes relevantVariableList on Topics and memberList on SVPGs."""
        dest = self.executor.get_spanner_destination_uri()
        prefix = BASE_PROVENANCE_PREFIX if self.is_base_dc else ""
        output_provenance = f"{prefix}generated/TopicLists"

        query = f"""  # nosec
        -- Pull raw relevantVariable and member arcs across all active topics
        CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_edges` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN ('relevantVariable', 'member')");

        -- Pull global topic & SVPG type definitions to support cross-import schemas
        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_types` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, object_id FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_nodes` AS
        SELECT DISTINCT subject_id FROM `temp_topic_types` WHERE object_id = 'Topic'
        UNION DISTINCT
        SELECT DISTINCT subject_id FROM `temp_raw_topic_edges` 
        WHERE subject_id LIKE '%/topic/%' OR subject_id LIKE 'dc/topic%';

        CREATE OR REPLACE TEMPORARY TABLE `temp_svpg_nodes` AS
        SELECT DISTINCT subject_id FROM `temp_topic_types` WHERE object_id = 'StatVarPeerGroup'
        UNION DISTINCT
        SELECT DISTINCT subject_id FROM `temp_raw_topic_edges` 
        WHERE subject_id LIKE '%/svpg/%' OR subject_id LIKE 'dc/svpg%';

        -- Aggregate relevantVariable -> relevantVariableList for Topic nodes
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_relevant_variable_list` AS
        SELECT 
          e.subject_id,
          'relevantVariableList' AS predicate,
          STRING_AGG(DISTINCT e.object_id, ',' ORDER BY e.object_id) AS list_value,
          '{output_provenance}' AS provenance
        FROM `temp_raw_topic_edges` e
        JOIN `temp_topic_nodes` t ON e.subject_id = t.subject_id
        WHERE e.predicate = 'relevantVariable'
        GROUP BY e.subject_id;

        -- Aggregate member -> memberList for StatVarPeerGroup nodes
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_member_list` AS
        SELECT 
          e.subject_id,
          'memberList' AS predicate,
          STRING_AGG(DISTINCT e.object_id, ',' ORDER BY e.object_id) AS list_value,
          '{output_provenance}' AS provenance
        FROM `temp_raw_topic_edges` e
        JOIN `temp_svpg_nodes` s ON e.subject_id = s.subject_id
        WHERE e.predicate = 'member'
        GROUP BY e.subject_id;

        -- Generate DCGraph hashed keys for terminal literal nodes
        CREATE OR REPLACE TEMPORARY TABLE `temp_all_list_edges` AS
        WITH combined AS (
          SELECT subject_id, predicate, list_value, provenance FROM `temp_aggregated_relevant_variable_list`
          UNION ALL
          SELECT subject_id, predicate, list_value, provenance FROM `temp_aggregated_member_list`
        )
        SELECT 
          subject_id,
          predicate,
          list_value,
          CONCAT(SUBSTR(TRIM(list_value), 1, 16), ':', TO_HEX(SHA256(TRIM(list_value)))) AS object_id,
          provenance
        FROM combined;

        -- Export Node records (stores raw CSV string in Node.value)
        EXPORT DATA
          OPTIONS(
            uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}'
          ) AS
        SELECT DISTINCT
          object_id AS subject_id,
          list_value AS value,
          CAST(NULL AS BYTES) AS bytes,
          '' AS name,
          CAST([] AS ARRAY<STRING>) AS types
        FROM `temp_all_list_edges`;

        -- Export Edge records (links Topic/SVPG to Node key)
        EXPORT DATA
          OPTIONS(
            uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}'
          ) AS
        SELECT DISTINCT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM `temp_all_list_edges`;
        """
        return self.executor.execute(query)
