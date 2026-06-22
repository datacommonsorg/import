import logging
from typing import List, Optional

from google.cloud import bigquery
from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal

class StatVarGroupGenerator:
    """Iteratively generates StatVarGroup nodes and hierarchical edges from MCF schemas."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True,
                 max_iterations: int = 50,
                 namespace: Optional[str] = None,
                 generated_provenance: Optional[str] = None,
                 should_filter_basic_population_type: bool = True) -> None:
        """Initializes the StatVarGroupGenerator with executor and configuration parameters.

        Args:
        ----
        executor: BigQueryExecutor instance.
        is_base_dc: Whether this is running in the base Data Commons environment.
        max_iterations: Max loop iterations for SV generation.
        namespace: Namespace for generated DCIDs.
        generated_provenance: Provenance for generated Edges.
        should_filter_basic_population_type = Filter basic population type SVGs.
        
        """
        self.executor = executor
        self.max_iterations = max_iterations
        self.namespace = namespace if namespace is not None else ('dc/' if is_base_dc else 'c/')
        self.generated_provenance = (
          generated_provenance
          if generated_provenance is not None
          else ('dc/base/GeneratedGraphs' if is_base_dc else 'GeneratedGraphs')
        )
        self.should_filter_basic_population_type = should_filter_basic_population_type

    def run_all(self,
                import_names: List[str] = None) -> List[bigquery.job.QueryJob]:
        """Runs all global aggregations asynchronously and returns their jobs."""
        if not import_names:
            logging.info("No imports specified. Skipping global aggregations.")
            return []

        logging.info(f"Running global aggregations for imports: {import_names}")

        jobs = [
            self.run_stat_var_group(),
        ]
        return [job for job in jobs if job]

    def run_stat_var_group(self) -> List[Optional[bigquery.job.QueryJob]]:
        """Compiles and executes the StatVarGroup generation script."""
        logging.info("Starting StatVarGroup generation script...")

        dest_uri = _escape_sql_literal(self.executor.get_spanner_destination_uri())
        conn_id = _escape_sql_literal(self.executor.connection_id)
        no_cache_config = bigquery.QueryJobConfig(use_query_cache=False)

        # =====================================================================
        # OPTIMIZATION: Two-Stage Fetch for StatVar Triples
        # =====================================================================
        logging.info("Fetching distinct constraint properties...")
        prep_query = f"""
            SELECT DISTINCT object_id
            FROM EXTERNAL_QUERY("{conn_id}", "SELECT object_id FROM Edge WHERE predicate = 'constraintProperties'")
        """
        prep_sv_job = self.executor.execute(prep_query, job_config=no_cache_config)
        constraint_props = [row.object_id for row in prep_sv_job]

        # Combine base predicates with the dynamically discovered constraint properties
        needed_predicates = ['populationType', 'constraintProperties'] + constraint_props

        # Format into a SQL-safe string for Spanner injection
        sv_predicates = [f"'{p.replace('\'', '')}'" for p in needed_predicates]
        sv_predicates_sql = ", ".join(sv_predicates)
        logging.info(f"Optimizing Spanner fetch. Pulling {len(sv_predicates)} specific predicates.")
        # =====================================================================

        # Using rf-string to safely pass SQL regex backslashes while enabling variable injection
        query = rf"""  # nosec
        DECLARE new_rows_found BOOL DEFAULT TRUE; -- Flag to continue loop
        DECLARE iteration_count INT64 DEFAULT 0; -- Iteration of loop
        DECLARE max_iterations INT64 DEFAULT @max_iterations; -- Maximum number of iterations to generate
        DECLARE namespace STRING DEFAULT @namespace; -- Namespace for generated DCIDs
        DECLARE generated_provenance STRING DEFAULT @generated_provenance; -- Provenance for generated Edges
        DECLARE uncategorized_svg STRING DEFAULT CONCAT(namespace, 'g/Uncategorized'); -- DCID for uncategorized SVs/SVGs
        DECLARE root_svg STRING DEFAULT CONCAT(namespace, 'g/Root'); -- DCID for root SVG
        DECLARE should_filter_basic_population_type BOOL DEFAULT @should_filter; -- Whether to filter basic population type SVGs. Default to true for base DC

        -- ============================================================================
        -- UDFs
        -- ============================================================================
        -- Generate readable name from MCF schema
        CREATE TEMP FUNCTION FormatName(input_string STRING) AS (
          (
            SELECT CONCAT(UPPER(SUBSTR(val, 1, 1)), SUBSTR(val, 2))
            FROM UNNEST([
              TRIM(
                REGEXP_REPLACE(
                  REGEXP_REPLACE(
                    REGEXP_REPLACE(
                      REGEXP_REPLACE(
                        REPLACE(input_string, '_', ' '),
                        r'([a-z])([A-Z])', r'\1 \2'
                      ),
                      r'([A-Z])([A-Z][a-z])', r'\1 \2'
                    ),
                    r'([A-Za-z])([0-9])', r'\1 \2'
                  ),
                  r'([0-9])([A-Za-z])', r'\1 \2'
                )
              )
            ]) AS val
          )
        );

        -- Checks whether a pouplationType is a basic population type.
        -- Based on the legacy Prophet IsBasicPopulationType.
        CREATE TEMP FUNCTION IsBasicPopulationType(populationType STRING) RETURNS BOOL AS (
          populationType IN (
            'Person', 'BLSWorker', 'USCWorker', 'Thing', 'Household',
            'HousingUnit', 'Place', 'Energy'
          )
        );

        -- ============================================================================
        -- Base Tables
        -- ============================================================================
        -- Fetch all StatVarGroupSpec nodes.
        CREATE OR REPLACE TEMP TABLE StatVarGroupSpec AS (
          SELECT DISTINCT subject_id
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id FROM Edge WHERE predicate = 'typeOf' AND object_id = 'StatVarGroupSpec'")
        );

        -- Fetch StatVarGroupSpec edges for relevant properties.
        CREATE OR REPLACE TEMP TABLE SpecObjects AS (
          SELECT DISTINCT E.subject_id, E.predicate, E.object_id
          FROM EXTERNAL_QUERY(
            "{conn_id}",
            '''
            SELECT subject_id, predicate, object_id 
            FROM Edge 
            WHERE predicate IN (
              'populationType', 
              'constraintProperties', 
              'vertical', 
              'dependentPropertyValue'
            )''') E
          JOIN StatVarGroupSpec S ON E.subject_id = S.subject_id
        );

        -- Resolve StatVarGroup spec object_ids to values. 
        CREATE OR REPLACE TEMP TABLE SpecValues AS (
          SELECT subject_id, predicate, object_id AS value
          FROM SpecObjects
          -- NOTE: Excluding DPVs for now.
          WHERE subject_id NOT IN (
            SELECT subject_id FROM SpecObjects WHERE predicate = 'dependentPropertyValue'
          )
        );

        -- Fetch all specializationOf edges.
        -- NOTE: specializationOf edges will be generated in this script. We only need the edges for the verticals here.
        CREATE OR REPLACE TEMP TABLE VerticalHierarchy AS (
          SELECT subject_id, object_id 
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id, object_id FROM Edge WHERE predicate = 'specializationOf'")
        );

        -- Find all vertical ancestors. 
        CREATE OR REPLACE TEMP TABLE VerticalAncestors AS (
          WITH RECURSIVE Ancestors AS (
            SELECT H.subject_id, H.object_id AS ancestor_svg, 1 AS level
            FROM VerticalHierarchy H
            JOIN (
              SELECT DISTINCT value AS subject_id FROM SpecValues WHERE predicate = 'vertical'
            ) V ON H.subject_id = V.subject_id   
            UNION ALL
            SELECT A.subject_id, H.object_id AS ancestor_svg, A.level + 1
            FROM Ancestors A
            JOIN VerticalHierarchy H ON A.ancestor_svg = H.subject_id
            WHERE A.level <= 10
          ) 
          SELECT DISTINCT subject_id, ARRAY_AGG(ancestor_svg ORDER BY ancestor_svg) AS ancestors
          FROM (SELECT DISTINCT subject_id, ancestor_svg FROM Ancestors)
          GROUP BY subject_id
        );

        -- Generate vertical spec.
        CREATE OR REPLACE TEMPORARY TABLE VerticalSpec AS (
          WITH PivotSpec AS (
            SELECT * FROM SpecValues
            PIVOT (
              ARRAY_AGG(value IGNORE NULLS ORDER BY value) 
              -- NOTE: Currently excluding ObsProp.
              -- This is because it can cause unexpected behavior when an SV is attached to a vertical due to another SV having a matching mprop.
              -- Instead, the SV will get attached only based on popType and cpops, so the result is deterministic per SV.
              -- TODO: Determine what the intended behavior is. 
              FOR predicate IN ('populationType', 'constraintProperties', 'vertical')
            )
          )
          SELECT 
            PivotSpec.subject_id,
            populationType[SAFE_OFFSET(0)] AS populationType,
            IFNULL(constraintProperties, []) AS constraintProperties,
            vertical,
            ARRAY(
              SELECT DISTINCT val
              FROM UNNEST(ARRAY_CONCAT(IFNULL(PivotSpec.vertical, []), IFNULL(A.ancestors, []))) AS val
              ORDER BY val
            ) AS linkedVertical
          FROM PivotSpec
          LEFT JOIN VerticalAncestors A ON A.subject_id IN UNNEST(PivotSpec.vertical)
          WHERE ARRAY_LENGTH(constraintProperties) <= 1 OR constraintProperties IS NULL
          ORDER BY subject_id
        );

        -- Fetch curated memberOf edges to identify which SVs to skip during generation.
        CREATE OR REPLACE TEMP TABLE CuratedMemberOf AS (
          SELECT subject_id AS statvar, object_id AS parent_svg
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id, object_id, provenance FROM Edge WHERE predicate = 'memberOf'")
          WHERE provenance != generated_provenance
        );
        
        -- Find all recursive ancestors for curated SVs to generate linkedMemberOf edges
        CREATE OR REPLACE TEMP TABLE CuratedLinkedEdges AS (
          WITH RECURSIVE Ancestors AS (
            SELECT statvar, parent_svg AS ancestor_svg, 1 AS level
            FROM CuratedMemberOf
            UNION ALL
            SELECT A.statvar, H.object_id AS ancestor_svg, A.level + 1
            FROM Ancestors A
            JOIN VerticalHierarchy H ON A.ancestor_svg = H.subject_id
            WHERE A.level <= 10
          )
          SELECT DISTINCT 
            statvar AS subject_id, 
            'linkedMemberOf' AS predicate, 
            ancestor_svg AS object_id, 
            generated_provenance AS provenance
          FROM Ancestors
        );

        -- Fetch all StatisticalVariable nodes.
        CREATE OR REPLACE TEMP TABLE StatVar AS (
          SELECT subject_id
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id FROM Edge WHERE predicate = 'typeOf' AND object_id = 'StatisticalVariable'")
        );

        -- Fetch relevant StatisticalVariable triples.
        CREATE OR REPLACE TEMP TABLE StatVarTriple AS (
          SELECT DISTINCT E.subject_id, E.predicate, E.object_id
          FROM EXTERNAL_QUERY("{conn_id}",
            "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN UNNEST([{sv_predicates_sql}])"
          ) E
          JOIN StatVar SV ON SV.subject_id = E.subject_id
        );

        -- Seed the intial data for iteratively generating SVGs.
        CREATE OR REPLACE TEMPORARY TABLE InitialData AS (
          WITH PopType AS (
            SELECT subject_id, object_id 
            FROM StatVarTriple 
            WHERE predicate = 'populationType'
              -- Exclude curated SVs from the auto-generation pipeline
              AND subject_id NOT IN (SELECT statvar FROM CuratedMemberOf)
          ),
          ConstraintProps AS (
            SELECT subject_id, ARRAY_AGG(object_id ORDER BY object_id) AS constraintProperties
            FROM StatVarTriple WHERE predicate = 'constraintProperties' GROUP BY subject_id
          ),
          Constraints AS (
            SELECT T.subject_id, ARRAY_AGG(CONCAT(FormatName(T.predicate), ' = ', FormatName(T.object_id)) ORDER BY T.predicate) AS pvs
            FROM StatVarTriple T
            JOIN ConstraintProps ON T.subject_id = ConstraintProps.subject_id
            WHERE predicate IN UNNEST(ConstraintProps.constraintProperties) GROUP BY subject_id 
          )
          SELECT
            CAST(NULL AS STRING) AS node1,
            CAST(NULL AS STRING) AS node2,
            '' AS node2name,
            IF(ARRAY_LENGTH(Constraints.pvs) > 0, 
              CONCAT(namespace, 'g/', PopType.object_id, '_', REPLACE(REPLACE(ARRAY_TO_STRING(Constraints.pvs, '_'), ' ', ''), '=', '-')), 
              CAST(NULL AS STRING)
            ) AS node3,
            IF(ARRAY_LENGTH(Constraints.pvs) > 0, 
              CONCAT(FormatName(PopType.object_id), ' With ', ARRAY_TO_STRING(Constraints.pvs, ', ')), 
              ''
            ) AS node3name,
            PopType.subject_id AS statvar,
            PopType.object_id AS populationType,
            ARRAY<STRING>[] AS constraintProperties,
            IFNULL(ConstraintProps.constraintProperties, ARRAY<STRING>[]) AS newConstraintProperties,
            IFNULL(Constraints.pvs, ARRAY<STRING>[]) AS attributes,
            0 AS iteration
          FROM PopType
          LEFT JOIN ConstraintProps ON ConstraintProps.subject_id = PopType.subject_id
          LEFT JOIN Constraints ON Constraints.subject_id = PopType.subject_id
        );

        -- Create Edges for top level SVs (those with no constraintProperties) to verticals.
        -- We only do this directly for basic population types. Non-basic ones get an intermediate group.
        CREATE OR REPLACE TEMP TABLE SVVerticalEdges AS (
          WITH ZeroConstraintStatVars AS (
            SELECT * FROM InitialData WHERE ARRAY_LENGTH(attributes) = 0
            AND (should_filter_basic_population_type AND IsBasicPopulationType(populationType))
          )
          SELECT DISTINCT
            SV.statvar AS subject_id, map.predicate, v AS object_id, generated_provenance AS provenance 
          FROM ZeroConstraintStatVars SV
          LEFT JOIN VerticalSpec VS 
            ON SV.populationType = VS.populationType AND IFNULL(ARRAY_LENGTH(VS.constraintProperties), 0) = 0
          CROSS JOIN UNNEST([
            STRUCT('memberOf' AS predicate, IF(IFNULL(ARRAY_LENGTH(VS.vertical), 0) = 0, ARRAY<STRING>[uncategorized_svg], VS.vertical) AS target_array),
            STRUCT(
              'linkedMemberOf' AS predicate, 
              IF(IFNULL(ARRAY_LENGTH(VS.linkedVertical), 0) = 0, ARRAY<STRING>[root_svg, uncategorized_svg], VS.linkedVertical) AS target_array
            )
          ]) AS map
          CROSS JOIN UNNEST(map.target_array) AS v
        );

        -- ============================================================================
        -- Iterative Loop
        -- ============================================================================
        -- Create table to hold the results from the iteration.
        CREATE OR REPLACE TEMP TABLE AllResults ( 
          node1 STRING, node2 STRING, node2name STRING, node3 STRING, node3name STRING,
          statvar STRING, populationType STRING, constraintProperties ARRAY<STRING>,
          newConstraintProperties ARRAY<STRING>, attributes ARRAY<STRING>, iteration INT64 
        );

        -- Seed 0-constraint SVs with non-basic population types so they generate an intermediate SVG
        INSERT INTO AllResults
        SELECT
          CAST(NULL AS STRING) AS node1,
          CONCAT(namespace, 'g/', populationType) AS node2,
          FormatName(populationType) AS node2name,
          CAST(NULL AS STRING) AS node3,
          CAST(NULL AS STRING) AS node3name,
          statvar,
          populationType,
          ARRAY<STRING>[] AS constraintProperties,
          ARRAY<STRING>[] AS newConstraintProperties,
          ARRAY<STRING>[] AS attributes,
          1 AS iteration
        FROM InitialData
        WHERE ARRAY_LENGTH(attributes) = 0
          AND NOT (should_filter_basic_population_type AND IsBasicPopulationType(populationType));

        WHILE new_rows_found AND iteration_count < max_iterations DO
          SET iteration_count = iteration_count + 1;
          SET new_rows_found = FALSE;
          
          -- Insert new distinct rows into AllResults
          INSERT INTO AllResults
          WITH CurrentIterationOutput AS (
            SELECT
              -- Set node1 to node3 of previous iteration.
              node3 AS node1,
              -- Remove one PV from node1 to produce node2.
              CONCAT(namespace, 'g/', populationType, '_', ARRAY_TO_STRING(ARRAY(
                SELECT IF(unnest_idx = target_idx, REPLACE(SPLIT(unnested, ' = ')[OFFSET(0)], ' ', ''), REPLACE(REPLACE(unnested, ' ', ''), '=', '-'))
                FROM UNNEST(attributes) AS unnested WITH OFFSET AS unnest_idx
              ), '_')) AS node2,
              CONCAT(FormatName(populationType), ' With ', ARRAY_TO_STRING(ARRAY(
                SELECT IF(unnest_idx = target_idx, SPLIT(unnested, ' = ')[OFFSET(0)], unnested)
                FROM UNNEST(attributes) AS unnested WITH OFFSET AS unnest_idx
              ), ', ')) AS node2name,
              -- Remove corresponding P from node2 to produce node3.
              IF(ARRAY_LENGTH(attributes) > 1, 
                CONCAT(namespace, 'g/', populationType, '_', ARRAY_TO_STRING(ARRAY( 
                  SELECT REPLACE(REPLACE(a, ' ', ''), '=', '-') 
                  FROM UNNEST(attributes) AS a WITH OFFSET AS a_idx WHERE a_idx != target_idx 
                ), '_')),
                IF(NOT (should_filter_basic_population_type AND IsBasicPopulationType(populationType)), 
                  CONCAT(namespace, 'g/', populationType), CAST(NULL AS STRING))
              ) AS node3,
              IF(ARRAY_LENGTH(attributes) > 1, 
                CONCAT(FormatName(populationType), ' With ', ARRAY_TO_STRING(ARRAY( 
                  SELECT a FROM UNNEST(attributes) AS a WITH OFFSET AS a_idx WHERE a_idx != target_idx 
                ), ', ')),
                IF(NOT (should_filter_basic_population_type AND IsBasicPopulationType(populationType)), FormatName(populationType), CAST(NULL AS STRING))
              ) AS node3name,
              statvar, populationType, newConstraintProperties AS constraintProperties,
              ARRAY(SELECT cp FROM UNNEST(newConstraintProperties) AS cp WITH OFFSET AS cp_idx WHERE cp_idx != target_idx) AS newConstraintProperties,
              ARRAY(SELECT a FROM UNNEST(attributes) AS a WITH OFFSET AS a_idx WHERE a_idx != target_idx) AS attributes,
              iteration + 1 AS iteration
            FROM InitialData AS T, UNNEST(T.attributes) AS attr WITH OFFSET AS target_idx
            WHERE T.iteration = iteration_count - 1
              AND ARRAY_LENGTH(T.attributes) >= 1
          ),
          NewDistinctRows AS (
            SELECT DISTINCT C.*
            FROM CurrentIterationOutput C
            LEFT JOIN AllResults AR ON C.statvar = AR.statvar 
              AND C.node1 IS NOT DISTINCT FROM AR.node1
              AND C.node2 IS NOT DISTINCT FROM AR.node2 
              AND C.node3 IS NOT DISTINCT FROM AR.node3
            WHERE AR.statvar IS NULL
          )
          SELECT * FROM NewDistinctRows;

          -- If new rows were inserted, continue the loop and update InitialData
          IF @@row_count > 0 THEN
            SET new_rows_found = TRUE;
            INSERT INTO InitialData
            SELECT * FROM AllResults WHERE iteration = iteration_count;
          END IF;
        END WHILE;

        -- ============================================================================
        -- Final Edges and Nodes Generation
        -- ============================================================================
        -- Create Edges for top level SVGs (those with 0 or 1 constraintProperties) to verticals.
        CREATE OR REPLACE TEMP TABLE SVGVerticalEdges AS (
          WITH FilteredSVG AS (
            SELECT * FROM AllResults WHERE ARRAY_LENGTH(constraintProperties) <= 1
          ),
          BaseJoined AS (
            SELECT
              SVG.statvar, COALESCE(SVG.node3, SVG.node2) AS svg_id, generated_provenance AS provenance,
              IF(IFNULL(ARRAY_LENGTH(VS.vertical), 0) = 0, ARRAY<STRING>[uncategorized_svg], VS.vertical) AS svg_targets,
              IF(IFNULL(ARRAY_LENGTH(VS.linkedVertical), 0) = 0, ARRAY<STRING>[root_svg, uncategorized_svg], VS.linkedVertical) AS statvar_targets
            FROM FilteredSVG SVG
            LEFT JOIN VerticalSpec VS 
              ON SVG.populationType = VS.populationType AND (
                (SVG.node3 IS NOT NULL AND IFNULL(ARRAY_LENGTH(VS.constraintProperties), 0) = 0) OR 
                (SVG.node3 IS NULL AND TO_JSON_STRING(SVG.constraintProperties) = TO_JSON_STRING(VS.constraintProperties))
              )
          ),
          RawSVGEdges AS (
            SELECT DISTINCT svg_id AS subject_id, 'specializationOf' AS predicate, v AS object_id, provenance
            FROM BaseJoined CROSS JOIN UNNEST(svg_targets) AS v
          ),
          -- Filter SVGs from Uncategorized if they're part of a different vertical.
          FilteredSVGEdges AS (
            SELECT subject_id, predicate, object_id, provenance 
            FROM (SELECT *, COUNTIF(object_id != uncategorized_svg) OVER(PARTITION BY subject_id, predicate, provenance) AS categorized_count FROM RawSVGEdges)
            WHERE object_id != uncategorized_svg OR categorized_count = 0
          ),
          RawStatVarEdges AS (
            SELECT DISTINCT statvar AS subject_id, 'linkedMemberOf' AS predicate, v AS object_id, provenance, svg_id AS parent_svg_id
            FROM BaseJoined CROSS JOIN UNNEST(statvar_targets) AS v
          ),
          -- Filter SVs from Uncategorized if none of their ancestor SVGs are uncategorized.
          FilteredStatVarEdges AS (
            SELECT DISTINCT subject_id, predicate, object_id, provenance
            FROM RawStatVarEdges r
            WHERE object_id != uncategorized_svg OR EXISTS (
                 SELECT 1 FROM FilteredSVGEdges f WHERE f.subject_id = r.parent_svg_id AND f.provenance = r.provenance AND f.object_id = uncategorized_svg
               )
          )
          SELECT subject_id, predicate, object_id, provenance FROM FilteredSVGEdges
          UNION ALL
          SELECT subject_id, predicate, object_id, provenance FROM FilteredStatVarEdges
        );

        -- Generate Spanner Node rows.
        CREATE OR REPLACE TEMP TABLE Node AS (
          SELECT DISTINCT node_data.subject_id, node_data.value, node_data.name, node_data.types
          FROM AllResults
          CROSS JOIN UNNEST([
            STRUCT(node2 AS subject_id, node2 AS value, node2name AS name, ['StatVarGroup'] AS types, node2 IS NOT NULL AS keep),
            STRUCT(node3, node3, node3name, ['StatVarGroup'], node3 IS NOT NULL),
            STRUCT(CONCAT(SUBSTR(node2name, 1, 16), ':', TO_BASE64(SHA256(node2name))), node2name, '', ARRAY<STRING>[], node2 IS NOT NULL),
            STRUCT(CONCAT(SUBSTR(node3name, 1, 16), ':', TO_BASE64(SHA256(node3name))), node3name, '', ARRAY<STRING>[], node3 IS NOT NULL)
          ]) AS node_data
          WHERE node_data.keep = TRUE
          ORDER BY subject_id
        );

        -- Generate Spanner Edge rows.
        CREATE OR REPLACE TEMP TABLE Edge AS (
          SELECT DISTINCT edge_data.subject_id, edge_data.predicate, edge_data.object_id, generated_provenance AS provenance
          FROM AllResults
          CROSS JOIN UNNEST([
            STRUCT(statvar AS subject_id, 'memberOf' AS predicate, node2 AS object_id, iteration = 1 AS keep),
            STRUCT(node2, 'typeOf', 'StatVarGroup', node2 IS NOT NULL),
            STRUCT(node2, 'name', CONCAT(SUBSTR(node2name, 1, 16), ':', TO_BASE64(SHA256(node2name))), node2 IS NOT NULL),
            STRUCT(node3, 'typeOf', 'StatVarGroup', node3 IS NOT NULL),
            STRUCT(node3, 'name', CONCAT(SUBSTR(node3name, 1, 16), ':', TO_BASE64(SHA256(node3name))), node3 IS NOT NULL),
            STRUCT(node1, 'specializationOf', node2, node1 IS NOT NULL AND node2 IS NOT NULL AND iteration > 1),
            STRUCT(node2, 'specializationOf', node3, node2 IS NOT NULL AND node3 IS NOT NULL),
            STRUCT(statvar, 'linkedMemberOf', node2, node2 IS NOT NULL),
            STRUCT(statvar, 'linkedMemberOf', node3, node3 IS NOT NULL)
          ]) AS edge_data
          WHERE edge_data.keep = TRUE
          UNION ALL SELECT * FROM SVVerticalEdges
          UNION ALL SELECT * FROM SVGVerticalEdges
          UNION ALL SELECT * FROM CuratedLinkedEdges
          ORDER BY subject_id, predicate, object_id
        );

        -- ============================================================================
        -- Exports
        -- ============================================================================
        EXPORT DATA
          OPTIONS(
            uri="{dest_uri}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Node"}}'
          ) AS (SELECT * FROM Node);

        EXPORT DATA
          OPTIONS(
            uri="{dest_uri}",
            format='CLOUD_SPANNER',
            spanner_options = '{{"table": "Edge"}}'
          ) AS (SELECT * FROM Edge);
        """

        job_config = bigquery.QueryJobConfig(
            use_query_cache=False,
            query_parameters=[
                bigquery.ScalarQueryParameter("namespace", "STRING", self.namespace),
                bigquery.ScalarQueryParameter("generated_provenance", "STRING", self.generated_provenance),
                bigquery.ScalarQueryParameter("max_iterations", "INT64", self.max_iterations),
                bigquery.ScalarQueryParameter("should_filter", "BOOL", self.should_filter_basic_population_type),
            ]
        )
        return self.executor.execute(query, job_config=job_config)