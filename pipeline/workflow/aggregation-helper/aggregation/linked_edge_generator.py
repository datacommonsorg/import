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
from google.cloud import spanner

from .bq_executor import BigQueryExecutor
from .common import (
    BASE_PROVENANCE_PREFIX,
    LINKED_PLACES_PROVENANCE,
    LINKED_SVGS_PROVENANCE,
    LINKED_TOPICS_PROVENANCE,
    _escape_sql_literal,
    get_provenance_name,
    get_sql_generated_provenance_expr,
)


@dataclass
class LinkedEdgeConfig:
    """Configuration for linked edge generation."""

    import_names: Optional[List[str]] = None
    enable_global_linked_edges: bool = False
    enable_delta_sync: Optional[bool] = None
    max_place_depth: int = 10
    max_topic_depth: int = 20
    max_svg_depth: int = 20
    deletion_chunk_size: int = 5000


class LinkedEdgeGenerator:
    """Generates and ingests linked relationship edges (e.g., transitive closures) into Spanner for faster lookup."""

    def __init__(self, executor: BigQueryExecutor, is_base_dc: bool = True) -> None:
        """Initializes the LinkedEdgeGenerator with the executor."""
        self.executor = executor
        self.is_base_dc = is_base_dc
        self._spanner_database = None

    @property
    def spanner_database(self):
        """Lazily initializes and returns the Spanner Database client."""
        if self._spanner_database is None:
            spanner_client = spanner.Client(
                project=self.executor.spanner_project_id,
                disable_builtin_metrics=True,
            )
            instance = spanner_client.instance(self.executor.instance_id)
            self._spanner_database = instance.database(self.executor.database_id)
        return self._spanner_database

    def _should_use_global_linked_edges(self, config: LinkedEdgeConfig) -> bool:
        """Determines whether to run global cross-provenance delta sync or scoped per-import calculations."""
        if config.enable_global_linked_edges:
            return True
        if config.enable_delta_sync is not None:
            return config.enable_delta_sync
        return False

    def run_all(
        self, config: Optional[LinkedEdgeConfig] = None
    ) -> List[bigquery.job.QueryJob]:
        """Runs all global aggregations asynchronously and returns their jobs."""
        config = config or LinkedEdgeConfig()
        import_names = config.import_names
        use_global = self._should_use_global_linked_edges(config)

        if not use_global and not import_names:
            logging.info(
                "No imports specified for scoped linked edges. Skipping global aggregations."
            )
            return []

        logging.info(
            f"Running linked edge aggregations (global={use_global}) for imports: {import_names}"
        )

        jobs = [
            self.run_linked_contained_in_place(import_names, config),
            self.run_linked_member_of(import_names, config),
            self.run_linked_member(import_names, config),
        ]
        return [job for job in jobs if job]

    def _active_imports_have_predicates(
        self, import_names: Optional[List[str]], predicates: List[str]
    ) -> bool:
        """Checks Spanner Edge table to see if active imports contain specified schema predicates."""
        if not import_names or "*" in import_names:
            return True  # Wildcard or empty import list always triggers calculation

        formatted_provenances = ", ".join(
            [
                f"'{get_provenance_name(name, self.is_base_dc)}'"
                for name in import_names
            ]
        )
        formatted_predicates = ", ".join([f"'{pred}'" for pred in predicates])

        query = f"""
        SELECT 1 FROM Edge@{{FORCE_INDEX=EdgeByProvenance}}
        WHERE provenance IN ({formatted_provenances})
          AND predicate IN ({formatted_predicates})
        LIMIT 1;
        """
        try:
            with self.spanner_database.snapshot() as snapshot:
                results = list(snapshot.execute_sql(query))
                return len(results) > 0
        except Exception as e:
            logging.warning(
                f"Error checking active import predicates in Spanner ({e}), defaulting to True."
            )
            return True

    def _sync_edge_deltas(
        self,
        temp_desired_table: str,
        output_provenance: str,
        predicate: str,
        config: LinkedEdgeConfig,
    ) -> Optional[bigquery.job.QueryJob]:
        """Computes BigQuery symmetric differences and applies minimal deltas to Spanner."""
        dest = self.executor.get_spanner_destination_uri()
        safe_prov_tag = output_provenance.replace("/", "_")
        current_table = f"temp_current_{safe_prov_tag}"
        to_insert_table = f"temp_insert_{safe_prov_tag}"
        to_delete_table = f"temp_delete_{safe_prov_tag}"

        # Snapshots current state including legacy generated/* provenances for seamless migration
        diff_query = f"""
        -- Fetch current materialized state from Spanner (including legacy generated/* provenances)
        CREATE OR REPLACE TEMPORARY TABLE `{current_table}` AS
        SELECT subject_id, predicate, object_id, provenance
        FROM EXTERNAL_QUERY("{self.executor.connection_id}",
          "SELECT subject_id, predicate, object_id, provenance FROM Edge WHERE predicate = '{predicate}' AND STARTS_WITH(provenance, 'generated/')");

        -- Compute Insert Delta (New paths)
        CREATE OR REPLACE TEMPORARY TABLE `{to_insert_table}` AS
        SELECT subject_id, predicate, object_id, provenance
        FROM `{temp_desired_table}`
        EXCEPT DISTINCT
        SELECT subject_id, predicate, object_id, provenance
        FROM `{current_table}`;

        -- Compute Delete Delta (Stale / Zombie paths & Legacy per-import provenances)
        CREATE OR REPLACE TEMPORARY TABLE `{to_delete_table}` AS
        SELECT subject_id, predicate, object_id, provenance
        FROM `{current_table}`
        EXCEPT DISTINCT
        SELECT subject_id, predicate, object_id, provenance
        FROM `{temp_desired_table}`;
        """
        self.executor.execute(diff_query)

        del_rows = []
        del_count = 0
        try:
            del_rows = list(
                self.executor.client.query(
                    f"SELECT subject_id, predicate, object_id, provenance FROM `{to_delete_table}`"
                ).result()
            )
            del_count = len(del_rows)
        except Exception as e:
            logging.warning(
                f"Could not read delete table {to_delete_table}: {e}"
            )

        has_inserts = False
        try:
            ins_rows = list(
                self.executor.client.query(
                    f"SELECT 1 FROM `{to_insert_table}` LIMIT 1"
                ).result()
            )
            has_inserts = len(ins_rows) > 0
        except Exception as e:
            logging.warning(
                f"Could not read insert table {to_insert_table}: {e}"
            )

        logging.info(
            f"Delta sync for '{output_provenance}' ({predicate}): "
            f"{'has edges' if has_inserts else '0'} to insert, {del_count} to delete"
        )

        # 1. Blind Mutation Chunked Deletions (if any)
        if del_count > 0:
            chunk_size = config.deletion_chunk_size
            for i in range(0, del_count, chunk_size):
                chunk = del_rows[i : i + chunk_size]
                keys = [
                    [
                        row["subject_id"],
                        row["predicate"],
                        row["object_id"],
                        row.get("provenance")
                        if hasattr(row, "get")
                        else (
                            row["provenance"]
                            if "provenance" in row
                            else output_provenance
                        ),
                    ]
                    for row in chunk
                ]
                keyset = spanner.KeySet(keys=keys)
                with self.spanner_database.batch() as batch:
                    batch.delete("Edge", keyset)
            logging.info(
                f"Successfully deleted {del_count} stale/legacy edges from {output_provenance}"
            )

        # 2. Bulk Insert New Edges via EXPORT DATA (if any)
        if has_inserts:
            insert_query = f"""  # nosec
            EXPORT DATA
              OPTIONS (
                uri = '{self.executor.get_spanner_destination_uri()}',
                format = 'CLOUD_SPANNER',
                table = 'Edge',
                mode = 'INSERT_OR_UPDATE'
              ) AS
            SELECT subject_id, predicate, object_id, provenance
            FROM `{to_insert_table}`;
            """
            return self.executor.execute(insert_query)

        return None

    def run_linked_member_of(
        self,
        import_names: Optional[List[str]] = None,
        config: Optional[LinkedEdgeConfig] = None,
    ) -> Optional[bigquery.job.QueryJob]:
        """Expands membership hierarchies using memberOf and specializationOf."""
        config = config or LinkedEdgeConfig(import_names=import_names)
        use_global = self._should_use_global_linked_edges(config)

        if not use_global:
            if not import_names:
                return None

            dest = self.executor.get_spanner_destination_uri()
            safe_names = [_escape_sql_literal(name) for name in import_names]
            prefix = BASE_PROVENANCE_PREFIX if self.is_base_dc else ""
            provenances = [f"'{prefix}{name}'" for name in safe_names]
            provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
            prov_expr = get_sql_generated_provenance_expr(
                self.is_base_dc, "provenance"
            )

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
                a.level <= {config.max_svg_depth}
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

        # Global Delta Sync Mode for DCP
        if not self._active_imports_have_predicates(
            config.import_names, ["memberOf", "specializationOf"]
        ):
            logging.info(
                "No memberOf or specializationOf triples found in active imports. Skipping global linkedMemberOf calculation."
            )
            return None

        query = f"""  # nosec
        -- Pull global base edges needed for memberOf aggregation across ALL provenances
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_member_of` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN ('memberOf', 'specializationOf')");

        CREATE OR REPLACE TEMPORARY TABLE `temp_desired_linked_svgs` AS
        WITH RECURSIVE Ancestors AS (
          SELECT
            subject_id,
            object_id AS ancestor,
            1 AS level,
            [subject_id, object_id] AS path
          FROM
            `temp_base_member_of`
          WHERE
            predicate = 'memberOf'
          UNION ALL

          SELECT
            a.subject_id,
            t.object_id AS ancestor,
            a.level + 1,
            ARRAY_CONCAT(a.path, [t.object_id]) AS path
          FROM
            Ancestors AS a
          JOIN
            `temp_base_member_of` AS t
            ON a.ancestor = t.subject_id
          WHERE
            a.level < {config.max_svg_depth}
            AND t.predicate = 'specializationOf'
            AND t.object_id NOT IN UNNEST(a.path)
        )
        SELECT DISTINCT
          subject_id,
          'linkedMemberOf' AS predicate,
          ancestor AS object_id,
          '{LINKED_SVGS_PROVENANCE}' AS provenance
        FROM
          Ancestors;
        """
        self.executor.execute(query)

        return self._sync_edge_deltas(
            temp_desired_table="temp_desired_linked_svgs",
            output_provenance=LINKED_SVGS_PROVENANCE,
            predicate="linkedMemberOf",
            config=config,
        )

    def run_linked_contained_in_place(
        self,
        import_names: Optional[List[str]] = None,
        config: Optional[LinkedEdgeConfig] = None,
    ) -> Optional[bigquery.job.QueryJob]:
        """Expands place containment hierarchies."""
        config = config or LinkedEdgeConfig(import_names=import_names)
        use_global = self._should_use_global_linked_edges(config)

        if not use_global:
            if not import_names:
                return None

            dest = self.executor.get_spanner_destination_uri()
            safe_names = [_escape_sql_literal(name) for name in import_names]
            prefix = "dc/base/" if self.is_base_dc else ""
            provenances = [f"'{prefix}{name}'" for name in safe_names]
            provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
            prov_expr = get_sql_generated_provenance_expr(
                self.is_base_dc, "provenance"
            )

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
                a.level <= {config.max_place_depth}
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

        # Global Delta Sync Mode for DCP
        if not self._active_imports_have_predicates(
            config.import_names, ["containedInPlace"]
        ):
            logging.info(
                "No containedInPlace triples found in active imports. Skipping global linkedContainedInPlace calculation."
            )
            return None

        query = f"""  # nosec
        CREATE OR REPLACE TEMPORARY TABLE `temp_base_contained_in_place` AS
        SELECT DISTINCT subject_id, object_id
        FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id, object_id FROM Edge WHERE predicate = 'containedInPlace'");

        CREATE OR REPLACE TEMPORARY TABLE `temp_desired_linked_places` AS
        WITH RECURSIVE TransitiveContainment AS (
          SELECT 
            subject_id AS child_id, 
            object_id AS parent_id, 
            1 AS depth,
            [subject_id, object_id] AS path
          FROM `temp_base_contained_in_place`
          UNION ALL
          SELECT 
            tc.child_id, 
            g.object_id AS parent_id, 
            tc.depth + 1 AS depth,
            ARRAY_CONCAT(tc.path, [g.object_id]) AS path
          FROM TransitiveContainment tc
          JOIN `temp_base_contained_in_place` g
            ON tc.parent_id = g.subject_id
          WHERE tc.depth < {config.max_place_depth}
            AND g.object_id NOT IN UNNEST(tc.path)
        )
        SELECT DISTINCT 
          child_id AS subject_id, 
          'linkedContainedInPlace' AS predicate, 
          parent_id AS object_id, 
          '{LINKED_PLACES_PROVENANCE}' AS provenance
        FROM TransitiveContainment;
        """
        self.executor.execute(query)

        return self._sync_edge_deltas(
            temp_desired_table="temp_desired_linked_places",
            output_provenance=LINKED_PLACES_PROVENANCE,
            predicate="linkedContainedInPlace",
            config=config,
        )

    def run_linked_member(
        self,
        import_names: Optional[List[str]] = None,
        config: Optional[LinkedEdgeConfig] = None,
    ) -> Optional[bigquery.job.QueryJob]:
        """Materializes reverse index 'linkedMember' edges from leaf variables to ancestor topics.

        Mixer uses 'linkedMember' for O(1) place existence checks without in-memory tree traversal.

        Args:
            import_names: List of import dataset names.
            config: Optional LinkedEdgeConfig dataclass instance.

        Returns:
            BigQuery QueryJob if queries are submitted, or None if skipped.
        """
        config = config or LinkedEdgeConfig(import_names=import_names)
        use_global = self._should_use_global_linked_edges(config)

        if not use_global:
            if not import_names:
                return None

            dest = self.executor.get_spanner_destination_uri()
            safe_names = [_escape_sql_literal(name) for name in import_names]
            prefix = "dc/base/" if self.is_base_dc else ""
            provenances = [f"'{prefix}{name}'" for name in safe_names]
            provenance_filter = f" AND provenance IN ({', '.join(provenances)})"
            prov_expr = get_sql_generated_provenance_expr(
                self.is_base_dc, "provenance"
            )

            query = f"""  # nosec
            -- Step 1: Extract all Topics and StatVarPeerGroups across Spanner.
            CREATE OR REPLACE TEMPORARY TABLE `temp_topics_and_peergroups` AS
            SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
              "SELECT subject_id AS entity_id, object_id AS type_name FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

            -- Step 2: Extract Topic and SVPG containment arcs for active imports
            CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_and_peergroup_edges` AS
            SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
              "SELECT subject_id AS parent_id, predicate, object_id AS child_id, provenance FROM Edge WHERE predicate IN ('relevantVariable', 'member'){provenance_filter}");

            -- Step 3: Keep only edges where 'relevantVariable' is on a Topic or 'member' is on a StatVarPeerGroup.
            CREATE OR REPLACE TEMPORARY TABLE `temp_topic_hierarchy` AS
            SELECT DISTINCT raw.parent_id, raw.child_id, raw.provenance
            FROM `temp_raw_topic_and_peergroup_edges` raw
            JOIN `temp_topics_and_peergroups` types ON raw.parent_id = types.entity_id
            WHERE (types.type_name = 'Topic' AND raw.predicate = 'relevantVariable')
               OR (types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member');

            -- Step 4: Recursively traverse hierarchy downward to leaf variables
            EXPORT DATA
              OPTIONS( uri="{dest}",
                format='CLOUD_SPANNER',
                spanner_options = '{{"table": "Edge"}}' ) AS
            WITH RECURSIVE Descendants AS (
              SELECT
                parent_id AS ancestor_topic_id,
                child_id AS descendant_node_id,
                1 AS depth,
                provenance
              FROM
                temp_topic_hierarchy
              UNION ALL

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
                d.depth <= {config.max_topic_depth}
            ),
            NewEdges AS (
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

        # Global Delta Sync Mode for DCP
        if not self._active_imports_have_predicates(
            config.import_names, ["relevantVariable", "member"]
        ):
            logging.info(
                "No relevantVariable or member triples found in active imports. Skipping global linkedMember calculation."
            )
            return None

        query = f"""  # nosec
        CREATE OR REPLACE TEMPORARY TABLE `temp_topics_and_peergroups` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS entity_id, object_id AS type_name FROM Edge WHERE predicate = 'typeOf' AND object_id IN ('Topic', 'StatVarPeerGroup')");

        CREATE OR REPLACE TEMPORARY TABLE `temp_raw_topic_and_peergroup_edges` AS
        SELECT * FROM EXTERNAL_QUERY("{self.executor.connection_id}", 
          "SELECT subject_id AS parent_id, predicate, object_id AS child_id FROM Edge WHERE predicate IN ('relevantVariable', 'member')");

        CREATE OR REPLACE TEMPORARY TABLE `temp_topic_hierarchy` AS
        SELECT DISTINCT raw.parent_id, raw.child_id
        FROM `temp_raw_topic_and_peergroup_edges` raw
        JOIN `temp_topics_and_peergroups` types ON raw.parent_id = types.entity_id
        WHERE (types.type_name = 'Topic' AND raw.predicate = 'relevantVariable')
           OR (types.type_name = 'StatVarPeerGroup' AND raw.predicate = 'member');

        CREATE OR REPLACE TEMPORARY TABLE `temp_desired_linked_topics` AS
        WITH RECURSIVE Descendants AS (
          SELECT
            parent_id AS ancestor_topic_id,
            child_id AS descendant_node_id,
            1 AS depth,
            [parent_id, child_id] AS path
          FROM
            `temp_topic_hierarchy`
          UNION ALL

          SELECT
            d.ancestor_topic_id,
            child.child_id AS descendant_node_id,
            d.depth + 1,
            ARRAY_CONCAT(d.path, [child.child_id]) AS path
          FROM
            Descendants AS d
          JOIN
            `temp_topic_hierarchy` AS child
            ON d.descendant_node_id = child.parent_id
          WHERE
            d.depth < {config.max_topic_depth}
            AND child.child_id NOT IN UNNEST(d.path)
        )
        SELECT DISTINCT
          descendant_node_id AS subject_id,
          'linkedMember' AS predicate,
          ancestor_topic_id AS object_id,
          '{LINKED_TOPICS_PROVENANCE}' AS provenance
        FROM
          Descendants
        WHERE ancestor_topic_id IN (SELECT entity_id FROM `temp_topics_and_peergroups` WHERE type_name = 'Topic')
          AND descendant_node_id NOT IN (SELECT entity_id FROM `temp_topics_and_peergroups`);
        """
        self.executor.execute(query)

        return self._sync_edge_deltas(
            temp_desired_table="temp_desired_linked_topics",
            output_provenance=LINKED_TOPICS_PROVENANCE,
            predicate="linkedMember",
            config=config,
        )
