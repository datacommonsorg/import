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
from typing import List, Optional

from aggregation.bq_executor import BigQueryExecutor
from aggregation.sql_utils import _escape_sql_literal
from google.cloud import bigquery


class LinkedEdgeGenerator:
    """Generates and ingests linked relationship edges (e.g., transitive closures) into Spanner for faster lookup."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        """Initializes the LinkedEdgeGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc

    def run_all(self,
                import_names: List[str] = None) -> List[bigquery.job.QueryJob]:
        """Runs all global aggregations asynchronously and returns their jobs."""
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
        gen_graphs_prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        query = f"""  # nosec
        -- Pull base edges needed for containedInPlace aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_contained_in_place` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}",
          "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate = 'containedInPlace'{provenance_filter}");

        -- Pull existing generated edges to filter them out later
        CREATE OR REPLACE TEMPORARY TABLE `temp_existing_linked_contained_in_place` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate = 'linkedContainedInPlace'");

        CREATE OR REPLACE TEMPORARY TABLE `temp_contained_in_place` AS
        SELECT subject_id, object_id
        FROM `temp_base_contained_in_place`;

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        with RECURSIVE Ancestors AS (
          SELECT
            subject_id,
            object_id AS ancestor_place,
            1 AS level
          FROM
            temp_contained_in_place
          UNION ALL

          SELECT
            a.subject_id,
            t.object_id AS ancestor_place,
            a.level + 1
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
            '{gen_graphs_prov}' as provenance
          FROM
            Ancestors
        ),
        FilteredEdges AS (
          SELECT
            subject_id,
            predicate,
            object_id,
            provenance
          FROM
            NewEdges n
          WHERE NOT EXISTS (
            SELECT 1
            FROM `temp_existing_linked_contained_in_place` e
            WHERE n.subject_id = e.subject_id
              AND n.predicate = e.predicate
              AND n.object_id = e.object_id
              AND n.provenance = e.provenance
          )
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          FilteredEdges
        """
        return self.executor.execute(query)

    def run_linked_member_of(
            self,
            import_names: List[str] = None) -> Optional[bigquery.job.QueryJob]:
        """Expands membership hierarchies using memberOf and specializationOf."""
        if not import_names:
            return None

        dest = self.executor.get_spanner_destination_uri()
        safe_names = [_escape_sql_literal(name) for name in import_names]
        prefix = "dc/base/" if self.is_base_dc else ""
        provenances = [f"'{prefix}{name}'" for name in safe_names]
        provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
        gen_graphs_prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        query = f"""  # nosec
        -- Pull base edges needed for memberOf aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_member_of` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN ('memberOf', 'specializationOf'){provenance_filter}");

        -- Pull existing generated edges to filter them out later
        CREATE OR REPLACE TEMPORARY TABLE `temp_existing_linked_member_of` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate = 'linkedMemberOf'");

        CREATE OR REPLACE TEMPORARY TABLE `temp_hierarchy` AS
        SELECT DISTINCT subject_id, predicate, object_id
        FROM `temp_base_member_of`;

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH RECURSIVE Ancestors AS (
          SELECT
            subject_id,
            object_id AS ancestor,
            1 AS level
          FROM
            temp_hierarchy
          WHERE
            predicate = 'memberOf'
          UNION ALL

          SELECT
            a.subject_id,
            t.object_id AS ancestor,
            a.level + 1
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
            '{gen_graphs_prov}' as provenance
          FROM
            Ancestors
        ),
        FilteredEdges AS (
          SELECT
            subject_id,
            predicate,
            object_id,
            provenance
          FROM
            NewEdges n
          WHERE NOT EXISTS (
            SELECT 1
            FROM `temp_existing_linked_member_of` e
            WHERE n.subject_id = e.subject_id
              AND n.predicate = e.predicate
              AND n.object_id = e.object_id
              AND n.provenance = e.provenance
          )
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          FilteredEdges
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
        gen_graphs_prov = 'dc/base/GeneratedGraphs' if self.is_base_dc else 'GeneratedGraphs'

        query = f"""  # nosec
        -- Pull base edges needed for member aggregation
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_member` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN ('relevantVariable', 'member'){provenance_filter}");

        -- Pull existing generated edges to filter them out later
        CREATE OR REPLACE TEMPORARY TABLE `temp_existing_linked_member` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate = 'linkedMember'");

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_hierarchy` AS
        SELECT DISTINCT subject_id, object_id
        FROM `temp_base_member`
        WHERE (subject_id LIKE 'dc/topic%' OR subject_id LIKE 'dc/svpg%');

        EXPORT DATA
          OPTIONS( uri="{dest}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}' ) AS
        WITH RECURSIVE Descendants AS (
          SELECT
            subject_id,
            object_id AS descendant,
            1 AS level
          FROM
            temp_topic_hierarchy
          UNION ALL

          SELECT
            d.subject_id,
            t.object_id AS descendant,
            d.level + 1
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
            '{gen_graphs_prov}' as provenance
          FROM
            Descendants
          WHERE subject_id LIKE 'dc/topic%'
          AND descendant NOT LIKE 'dc/topic%'
          AND descendant NOT LIKE 'dc/svpg%'
        ),
        FilteredEdges AS (
          SELECT
            subject_id,
            predicate,
            object_id,
            provenance
          FROM
            NewEdges n
          WHERE NOT EXISTS (
            SELECT 1
            FROM `temp_existing_linked_member` e
            WHERE n.subject_id = e.subject_id
              AND n.predicate = e.predicate
              AND n.object_id = e.object_id
              AND n.provenance = e.provenance
          )
        )
        SELECT
          subject_id,
          predicate,
          object_id,
          provenance
        FROM
          FilteredEdges
        """
        return self.executor.execute(query)
