import logging
from dataclasses import dataclass
from typing import List, Optional

from google.cloud import bigquery
from .bq_executor import BigQueryExecutor
from .common import BASE_PROVENANCE_PREFIX, _escape_sql_literal, get_provenance_name, get_provenance_prefix, get_sql_generated_provenance_expr


@dataclass
class StatVarGroupConfig:
    """Configuration for statistical variable group generation."""
    pass


class StatVarGroupGenerator:
    """Iteratively generates StatVarGroup nodes and hierarchical edges from MCF schemas."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True,
                 max_iterations: int = 50,
                 namespace: Optional[str] = None,
                 generated_provenance: Optional[str] = None,
                 should_filter_basic_population_type: Optional[bool] = None,
                 should_prune_single_child_svgs: bool = False) -> None:
        """Initializes the StatVarGroupGenerator with executor and configuration parameters.

        Args:
        ----
        executor: BigQueryExecutor instance.
        is_base_dc: Whether this is running in the base Data Commons environment.
        max_iterations: Max loop iterations for SV generation.
        namespace: Namespace for generated DCIDs.
        generated_provenance: Provenance for generated Edges.
        should_filter_basic_population_type = Filter basic population type SVGs.
        should_prune_single_child_svgs = Prune single-child SVGs from the generated hierarchy.
        
        """
        self.executor = executor
        self.max_iterations = max_iterations
        self.namespace = namespace if namespace is not None else ('dc/' if is_base_dc else 'c/')
        self.is_base_dc = is_base_dc
        self.generated_provenance = (
          generated_provenance
          if generated_provenance is not None
          else (f'{BASE_PROVENANCE_PREFIX}GeneratedGraphs' if is_base_dc else 'GeneratedGraphs')
        )
        self.should_filter_basic_population_type = (
            should_filter_basic_population_type
            if should_filter_basic_population_type is not None
            else is_base_dc
        )
        self.should_prune_single_child_svgs = should_prune_single_child_svgs

    def run_all(self,
                config: Optional[StatVarGroupConfig] = None) -> List[bigquery.job.QueryJob]:
        """Runs all global aggregations asynchronously and returns their jobs."""
        logging.info("Running StatVarGroup generation for all provenances in DB.")

        jobs = [
            self.run_stat_var_group(),
        ]
        return [job for job in jobs if job]

    def run_stat_var_group(self) -> Optional[bigquery.job.QueryJob]:
        """Compiles and executes the StatVarGroup generation script."""
        logging.info("Starting StatVarGroup generation script for all DB provenances...")

        dest_uri = _escape_sql_literal(self.executor.get_spanner_destination_uri())
        conn_id = _escape_sql_literal(self.executor.connection_id)
        no_cache_config = bigquery.QueryJobConfig(use_query_cache=False)
        prov_expr = get_sql_generated_provenance_expr(self.is_base_dc, "provenance")
        gen_prov_prefix = f"{get_provenance_prefix(self.is_base_dc)}generated/"

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
        needed_predicates = ['populationType', 'measuredProperty', 'constraintProperties'] + constraint_props

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
        DECLARE generated_provenance_prefix STRING DEFAULT @generated_provenance_prefix; -- Prefix for generated Edges
        DECLARE uncategorized_svg STRING DEFAULT CONCAT(namespace, 'g/Uncategorized'); -- DCID for uncategorized SVs/SVGs
        DECLARE uncategorized_sv_svg STRING DEFAULT CONCAT(namespace, 'g/Uncategorized_Variables'); -- DCID for uncategorized SVs
        DECLARE root_svg STRING DEFAULT CONCAT(namespace, 'g/Root'); -- DCID for root SVG
        DECLARE should_filter_basic_population_type BOOL DEFAULT @should_filter; -- Whether to filter basic population type SVGs. Default to true for base DC
        DECLARE should_prune BOOL DEFAULT @should_prune; -- Whether to prune single-child SVGs from the generated hierarchy
        DECLARE pruning_iteration INT64 DEFAULT 0; -- Iteration of pruning loop
        DECLARE new_prunable_found BOOL DEFAULT TRUE; -- Whether new prunable nodes were found in the iteration

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

        -- Normalize a DPV string ("age=Years15Onwards" or "p=v" with possible surrounding
        -- quotes/whitespace) into the FormatName(p) = FormatName(v) form used in the SV
        -- attributes array, so exact equality can be checked against SV pvs.
        -- SV pvs are built as CONCAT(FormatName(predicate), ' = ', FormatName(object_id)).
        CREATE TEMP FUNCTION NormalizeDPV(dpv STRING) AS (
          CONCAT(
            FormatName(TRIM(SPLIT(dpv, '=')[SAFE_OFFSET(0)])),
            ' = ',
            FormatName(TRIM(SPLIT(dpv, '=')[SAFE_OFFSET(1)]))
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
              'statVarProperties',
              'constraintProperties', 
              'vertical', 
              'dependentPropertyValue'
            )''') E
          JOIN StatVarGroupSpec S ON E.subject_id = S.subject_id
        );

        -- Resolve StatVarGroup spec object_ids to values.
        CREATE OR REPLACE TEMP TABLE SpecValues AS (
          SELECT SO.subject_id, SO.predicate, E.value
          FROM EXTERNAL_QUERY(
            "{conn_id}",
            '''
            SELECT subject_id, value
            FROM Node 
            WHERE types IS NULL OR ARRAY_LENGTH(types) = 0''') E
          JOIN SpecObjects SO ON E.subject_id = SO.object_id
          WHERE predicate IN ('statVarProperties', 'dependentPropertyValue')
          UNION ALL
          SELECT subject_id, predicate, object_id AS value
          FROM SpecObjects
          WHERE predicate IN ('populationType', 'constraintProperties', 'vertical')
        );

        -- ============================================================================
        -- Dependent PropertyValue (DPV) Matching
        -- ============================================================================
        -- A DPV spec declares dependentPropertyValue entries (p=v pairs) that, when all
        -- match a SV's constraint property values exactly, mark those pvs as redundant.
        -- Matched pvs are stripped from the SV before hierarchy generation so the SV is
        -- categorized by its remaining (non-redundant) constraint properties only.
        --
        -- When multiple DPV specs match a SV, the most specific spec wins. Specificity is
        -- defined as: (1) more DPV matches, then (2) more required constraintProperties.
        -- Exact value equality is required for each DPV (e.g. age=Years15Onwards must
        -- match exactly; age=Years20Onwards will NOT match that DPV spec).
        --
        -- Pivot all DPV specs into arrays. DPV objects arrive as strings like
        -- "age=Years15Onwards" (and possibly quoted); the EXTERNAL_QUERY returns each
        -- object_id value separately so we aggregate them per spec subject_id.
        CREATE OR REPLACE TEMP TABLE DPVSpec AS (
          WITH PivotSpec AS (
            SELECT * FROM SpecValues
            PIVOT (
              ARRAY_AGG(value IGNORE NULLS ORDER BY value)
              FOR predicate IN (
                'populationType', 'statVarProperties', 'constraintProperties',
                'vertical', 'dependentPropertyValue'
              )
            )
          )
          SELECT
            subject_id AS spec_id,
            populationType[SAFE_OFFSET(0)] AS populationType,
            IFNULL(statVarProperties, []) AS statVarProperties,
            IFNULL(constraintProperties, []) AS constraintProperties,
            IFNULL(vertical, []) AS vertical,
            IFNULL(dependentPropertyValue, []) AS dependentPropertyValue,
            (
              SELECT ARRAY_TO_STRING(ARRAY(
                SELECT DISTINCT x 
                FROM UNNEST(
                  ARRAY_CONCAT(
                    IFNULL(constraintProperties, []),
                    ARRAY(SELECT TRIM(SPLIT(d, '=')[SAFE_OFFSET(0)]) FROM UNNEST(dependentPropertyValue) AS d)
                  )
                ) AS x ORDER BY x
              ), ',')
            ) AS cprops_key,
            ARRAY(
              SELECT NormalizeDPV(d)
              FROM UNNEST(IFNULL(dependentPropertyValue, [])) AS d
            ) AS normalized_dpvs
          FROM PivotSpec
          -- Only specs that actually declare DPVs are DPV specs.
          WHERE ARRAY_LENGTH(IFNULL(dependentPropertyValue, [])) > 0
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
              FOR predicate IN ('populationType', 'statVarProperties', 'constraintProperties', 'vertical')
            )
          ),
          -- Include specs with 0-1 cprops
          -- TODO: Evalute if/how to handle DPV specs with >=2 cprops.
          AllSpecs AS (
            SELECT subject_id,
              populationType[SAFE_OFFSET(0)] AS populationType,
              -- NOTE: Legacy vertical matching does not include measurementQualifier or measurementDenominator,
              -- so filtering out from StatVarProps.
              SPLIT(statVarProperties, ',')[SAFE_OFFSET(0)] AS statVarProperties,
              IFNULL(constraintProperties, []) AS constraintProperties,
              vertical
            FROM PivotSpec
            LEFT JOIN UNNEST(IFNULL(PivotSpec.statVarProperties, [])) AS statVarProperties
            WHERE ARRAY_LENGTH(IFNULL(constraintProperties, [])) <= 1
              -- Exclude specs with no vertical: they shouldn't participate in vertical matching.
              -- This is especially relevant for DPV specs, which may not declare a vertical.
              AND vertical IS NOT NULL
          )
          SELECT 
            AllSpecs.subject_id,
            populationType,
            statVarProperties,
            constraintProperties,
            vertical,
            ARRAY(
              SELECT DISTINCT val
              FROM UNNEST(ARRAY_CONCAT(IFNULL(vertical, []), IFNULL(A.ancestors, []))) AS val
              ORDER BY val
            ) AS linkedVertical
          FROM AllSpecs
          LEFT JOIN VerticalAncestors A ON A.subject_id IN UNNEST(AllSpecs.vertical)
          ORDER BY subject_id
        );

        -- Fetch curated memberOf edges to identify which SVs to skip during generation.
        CREATE OR REPLACE TEMP TABLE CuratedMemberOf AS (
          SELECT subject_id AS statvar, object_id AS parent_svg, provenance
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id, object_id, provenance FROM Edge WHERE predicate = 'memberOf'")
          WHERE NOT STARTS_WITH(provenance, generated_provenance_prefix)
        );
        
        -- Find all recursive ancestors for curated SVs to generate linkedMemberOf edges
        CREATE OR REPLACE TEMP TABLE CuratedLinkedEdges AS (
          WITH RECURSIVE Ancestors AS (
            SELECT statvar, parent_svg AS ancestor_svg, 1 AS level, provenance
            FROM CuratedMemberOf
            UNION ALL
            SELECT A.statvar, H.object_id AS ancestor_svg, A.level + 1, A.provenance
            FROM Ancestors A
            JOIN VerticalHierarchy H ON A.ancestor_svg = H.subject_id
            WHERE A.level <= 10
          )
          SELECT DISTINCT 
            statvar AS subject_id, 
            'linkedMemberOf' AS predicate, 
            ancestor_svg AS object_id, 
            {prov_expr} AS provenance
          FROM Ancestors
        );

        -- Fetch all StatisticalVariable nodes and their provenance.
        CREATE OR REPLACE TEMP TABLE StatVar AS (
          SELECT DISTINCT subject_id, provenance
          FROM EXTERNAL_QUERY("{conn_id}", "SELECT subject_id, provenance FROM Edge WHERE predicate = 'typeOf' AND object_id = 'StatisticalVariable'")
          WHERE NOT STARTS_WITH(provenance, generated_provenance_prefix)
        );

        -- Fetch relevant StatisticalVariable triples.
        -- For now, avoid any PVs that should have been converted to Quantities.
        -- This is to avoid generating improper groups.
        -- TODO: Ensure all Quantities are properly resolved in ingestion.
        CREATE OR REPLACE TEMP TABLE StatVarTriple AS (
          SELECT DISTINCT E.subject_id, E.predicate, E.object_id
          FROM EXTERNAL_QUERY("{conn_id}",
            "SELECT subject_id, predicate, object_id FROM Edge WHERE predicate IN UNNEST([{sv_predicates_sql}]) AND object_id NOT LIKE '[%'"
          ) E
          JOIN StatVar SV ON SV.subject_id = E.subject_id
        );

        -- Pre-compute per-SV aggregated data used by both SVDPVMatch and InitialData.
        -- This avoids scanning StatVarTriple twice for the same predicates.
        CREATE OR REPLACE TEMP TABLE SVBaseData AS (
          WITH SVPopType AS (
            SELECT DISTINCT subject_id, object_id AS populationType
            FROM StatVarTriple
            WHERE predicate = 'populationType'
          ),
          SVStatVarProps AS (
            SELECT subject_id,
              CONCAT(predicate, '=', object_id) AS statVarProperties
            FROM StatVarTriple
            WHERE predicate = 'measuredProperty' AND object_id IS NOT NULL
          ),
          SVStatVarPropsAgg AS (
            SELECT subject_id,
              ARRAY_AGG(statVarProperties) AS sv_statVarProperties
            FROM SVStatVarProps
            GROUP BY subject_id
          ),
          SVCprops AS (
            SELECT subject_id, ARRAY_AGG(object_id ORDER BY object_id) AS cprops
            FROM StatVarTriple
            WHERE predicate = 'constraintProperties'
            GROUP BY subject_id
          ),
          SVPvs AS (
            -- Reconstruct SV pvs in the same FormatName(p) = FormatName(v) form.
            SELECT
              T.subject_id,
              ARRAY_AGG(CONCAT(FormatName(T.predicate), ' = ', FormatName(T.object_id))
                        ORDER BY T.predicate, T.object_id) AS sv_pvs
            FROM StatVarTriple T
            JOIN SVCprops SC ON T.subject_id = SC.subject_id
            WHERE T.predicate IN UNNEST(SC.cprops)
            GROUP BY T.subject_id
          )
          SELECT
            PT.subject_id,
            PT.populationType,
            IFNULL(SVP.sv_statVarProperties, []) AS sv_statVarProperties,
            IFNULL(SC.cprops, []) AS cprops,
            IFNULL(SP.sv_pvs, []) AS sv_pvs,
            ARRAY_TO_STRING(IFNULL(SC.cprops, []), ',') AS cprops_key,
            SV.provenance
          FROM SVPopType PT
          JOIN StatVar SV ON SV.subject_id = PT.subject_id
          LEFT JOIN SVStatVarPropsAgg SVP ON SVP.subject_id = PT.subject_id
          LEFT JOIN SVCprops SC ON SC.subject_id = PT.subject_id
          LEFT JOIN SVPvs SP ON SP.subject_id = PT.subject_id
        );

        -- Match each SV to the single most-specific DPV spec (if any).
        -- A match requires:
        --   1. populationType equality
        --   2. At least one statVarProperties overlap (or spec has none)
        --   3. EXACT set equality: SV cprops must equal (spec cprops ∪ DPV predicates)
        --   4. ALL spec DPVs match exactly (SV has those p=v pairs)
        -- Result: one row per SV with the spec_id, dpv count, and the pvs to strip.
        CREATE OR REPLACE TEMP TABLE SVDPVMatch AS (
          WITH Matches AS (
            SELECT
              SV.subject_id,
              D.spec_id,
              ARRAY_LENGTH(D.dependentPropertyValue) AS dpv_count,
              ARRAY_LENGTH(D.constraintProperties) AS cprop_count,
              D.normalized_dpvs
            FROM SVBaseData SV
            JOIN DPVSpec D
              ON SV.populationType = D.populationType
            WHERE
              -- statVarProperties: overlap required (or spec has none)
              (
                ARRAY_LENGTH(D.statVarProperties) = 0
                OR EXISTS (
                  SELECT 1
                  FROM UNNEST(SV.sv_statVarProperties) AS svsp
                  JOIN UNNEST(D.statVarProperties) AS dsp
                  ON svsp = dsp
                )
              )
              -- EXACT set equality: SV cprops must equal (spec cprops ∪ DPV predicates).
              -- This prevents a spec with fewer cprops from matching an SV with extra cprops.
              AND ARRAY_LENGTH(SV.cprops) > 0
              AND SV.cprops_key = D.cprops_key
              -- All spec DPVs must match exactly (SV pvs contain the normalized DPV)
              AND ARRAY_LENGTH(SV.sv_pvs) > 0
              AND (
                SELECT COUNTIF(nd IN UNNEST(SV.sv_pvs))
                FROM UNNEST(D.normalized_dpvs) AS nd
              ) = ARRAY_LENGTH(D.dependentPropertyValue)
          ),
          -- Pick most specific: most DPVs first, then most cprops.
          Ranked AS (
            SELECT
              subject_id, spec_id, dpv_count, cprop_count, normalized_dpvs,
              ROW_NUMBER() OVER (
                PARTITION BY subject_id
                ORDER BY dpv_count DESC, cprop_count DESC
              ) AS rn
            FROM Matches
          )
          SELECT subject_id, spec_id, normalized_dpvs AS dpvs_to_strip
          FROM Ranked
          WHERE rn = 1
        );

        -- Seed the intial data for iteratively generating SVGs.
        CREATE OR REPLACE TEMPORARY TABLE InitialData AS (
          WITH Constraints AS (
            SELECT 
              T.subject_id, 
              ARRAY_AGG(T.predicate ORDER BY T.predicate, T.object_id) AS aligned_cps,
              ARRAY_AGG(CONCAT(FormatName(T.predicate), ' = ', FormatName(T.object_id)) ORDER BY T.predicate, T.object_id) AS pvs
            FROM StatVarTriple T
            JOIN SVBaseData SVB ON SVB.subject_id = T.subject_id
            LEFT JOIN SVDPVMatch M ON M.subject_id = T.subject_id
            WHERE T.predicate IN UNNEST(SVB.cprops)
              -- Strip DPVs that were matched to a spec: if the normalized pv (FormatName(p) = FormatName(v))
              -- is in dpvs_to_strip, exclude it from the hierarchy generation.
              AND CONCAT(FormatName(T.predicate), ' = ', FormatName(T.object_id))
                NOT IN UNNEST(IFNULL(M.dpvs_to_strip, ARRAY<STRING>[]))
            GROUP BY subject_id 
          )
          SELECT
            CAST(NULL AS STRING) AS node1,
            CAST(NULL AS STRING) AS node2,
            '' AS node2name,
            IF(ARRAY_LENGTH(Constraints.pvs) > 0, 
              CONCAT(namespace, 'g/', SVB.populationType, '_', REPLACE(REPLACE(ARRAY_TO_STRING(Constraints.pvs, '_'), ' ', ''), '=', '-')), 
              IF(NOT (should_filter_basic_population_type AND IsBasicPopulationType(SVB.populationType)),
                CONCAT(namespace, 'g/', SVB.populationType), CAST(NULL AS STRING))
            ) AS node3,
            IF(ARRAY_LENGTH(Constraints.pvs) > 0, 
              CONCAT(FormatName(SVB.populationType), ' With ', ARRAY_TO_STRING(Constraints.pvs, ', ')), 
              IF(NOT (should_filter_basic_population_type AND IsBasicPopulationType(SVB.populationType)),
                FormatName(SVB.populationType), CAST(NULL AS STRING))
            ) AS node3name,
            SVB.subject_id AS statvar,
            SVB.populationType,
            statVarProperties,
            ARRAY<STRING>[] AS constraintProperties,
            IFNULL(Constraints.aligned_cps, ARRAY<STRING>[]) AS newConstraintProperties,
            IFNULL(Constraints.pvs, ARRAY<STRING>[]) AS attributes,
            0 AS iteration,
            SVB.provenance
          FROM SVBaseData SVB
          LEFT JOIN UNNEST(SVB.sv_statVarProperties) AS statVarProperties
          LEFT JOIN Constraints ON Constraints.subject_id = SVB.subject_id
          -- Exclude curated SVs from the auto-generation pipeline
          WHERE NOT EXISTS (
            SELECT 1 
            FROM CuratedMemberOf 
            WHERE statvar = SVB.subject_id
          )
        );

        -- Create Edges for top level SVs (those with no constraintProperties) to verticals.
        -- We only do this directly for basic population types. Non-basic ones get an intermediate group.
        CREATE OR REPLACE TEMP TABLE SVVerticalEdges AS (
          WITH ZeroConstraintStatVars AS (
            SELECT * FROM InitialData WHERE ARRAY_LENGTH(attributes) = 0
            AND (should_filter_basic_population_type AND IsBasicPopulationType(populationType))
          )
          SELECT DISTINCT
            SV.statvar AS subject_id, map.predicate, v AS object_id, {prov_expr} AS provenance 
          FROM ZeroConstraintStatVars SV
          LEFT JOIN VerticalSpec VS 
            ON SV.populationType = VS.populationType 
            AND (VS.statVarProperties IS NULL OR SV.statVarProperties = VS.statVarProperties)
            AND IFNULL(ARRAY_LENGTH(VS.constraintProperties), 0) = 0
          CROSS JOIN UNNEST([
            STRUCT('memberOf' AS predicate, IF(IFNULL(ARRAY_LENGTH(VS.vertical), 0) = 0, ARRAY<STRING>[uncategorized_sv_svg], VS.vertical) AS target_array),
            STRUCT(
              'linkedMemberOf' AS predicate,
              IF(IFNULL(ARRAY_LENGTH(VS.linkedVertical), 0) = 0,
                ARRAY<STRING>[root_svg, uncategorized_svg, uncategorized_sv_svg],
                VS.linkedVertical
              ) AS target_array
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
          statvar STRING, populationType STRING, statVarProperties STRING,
          constraintProperties ARRAY<STRING>, newConstraintProperties ARRAY<STRING>,
          attributes ARRAY<STRING>, iteration INT64, provenance STRING
        );

        INSERT INTO AllResults
        SELECT * FROM InitialData;

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
              statvar, populationType, statVarProperties, newConstraintProperties AS constraintProperties,
              ARRAY(SELECT cp FROM UNNEST(newConstraintProperties) AS cp WITH OFFSET AS cp_idx WHERE cp_idx != target_idx) AS newConstraintProperties,
              ARRAY(SELECT a FROM UNNEST(attributes) AS a WITH OFFSET AS a_idx WHERE a_idx != target_idx) AS attributes,
              iteration + 1 AS iteration,
              provenance
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
          WITH TopLevelSVGs AS (
            -- Basic PopTypes: Attach the 1-constraint group (node2) to the vertical.
            SELECT DISTINCT node2 AS svg_id, statvar, constraintProperties, populationType, provenance
            FROM AllResults
            WHERE iteration > 0
              AND (should_filter_basic_population_type AND IsBasicPopulationType(populationType))
              AND ARRAY_LENGTH(constraintProperties) = 1
            UNION ALL
            -- Non-basic PopTypes: Attach the 0-constraint group (node3) to the vertical.
            SELECT DISTINCT node3 AS svg_id, statvar, ARRAY<STRING>[] AS constraintProperties, populationType, provenance
            FROM AllResults
            WHERE node3 IS NOT NULL
              AND NOT (should_filter_basic_population_type AND IsBasicPopulationType(populationType))
              AND ARRAY_LENGTH(constraintProperties) = 0
          ),
          BaseJoined AS (
            SELECT
              SVG.statvar, SVG.svg_id, {get_sql_generated_provenance_expr(self.is_base_dc, "SVG.provenance")} AS provenance,
              IF(IFNULL(ARRAY_LENGTH(VS.vertical), 0) = 0, ARRAY<STRING>[uncategorized_svg], VS.vertical) AS svg_targets,
              IF(IFNULL(ARRAY_LENGTH(VS.linkedVertical), 0) = 0, ARRAY<STRING>[root_svg, uncategorized_svg], VS.linkedVertical) AS statvar_targets
            FROM TopLevelSVGs SVG 
            LEFT JOIN VerticalSpec VS
              -- NOTE: Currently excluding StatVarProps.
              -- This is because it can cause unexpected behavior when an SVG is attached to a vertical
              -- due to another SVG having a matching mprop.
              -- Instead, the SVG will get attached only based on popType and cprops, so the result is deterministic per SV
              -- TODO: Determine what the intended behavior is.
              ON SVG.populationType = VS.populationType
              AND TO_JSON_STRING(SVG.constraintProperties) = TO_JSON_STRING(VS.constraintProperties)
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
          SELECT DISTINCT edge_data.subject_id, edge_data.predicate, edge_data.object_id, {prov_expr} AS provenance
          FROM AllResults
          CROSS JOIN UNNEST([
            STRUCT(statvar AS subject_id, 'memberOf' AS predicate, node3 AS object_id, iteration = 0 AND node3 IS NOT NULL AS keep),
            STRUCT(node2, 'typeOf', 'StatVarGroup', node2 IS NOT NULL),
            STRUCT(node2, 'name', CONCAT(SUBSTR(node2name, 1, 16), ':', TO_BASE64(SHA256(node2name))), node2 IS NOT NULL),
            STRUCT(node3, 'typeOf', 'StatVarGroup', node3 IS NOT NULL),
            STRUCT(node3, 'name', CONCAT(SUBSTR(node3name, 1, 16), ':', TO_BASE64(SHA256(node3name))), node3 IS NOT NULL),
            STRUCT(node1, 'specializationOf', node2, node1 IS NOT NULL AND node2 IS NOT NULL),
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
        -- Pruning: Remove single-child SVGs from the generated hierarchy.
        -- If a generated (non-vertical) SVG has exactly one child (SVG via specializationOf
        -- or SV via memberOf), that child is rolled up to the grandparent. This repeats
        -- until no SVG has only one child. Pruning is deterministic (order-independent):
        -- the set of prunable SVGs is fixed, and each node's effective parent is unique.
        -- Edges (memberOf, specializationOf, linkedMemberOf) and Nodes for pruned SVGs
        -- are removed. Redirected edges are added for the surviving children.
        -- linkedMemberOf edges pointing to pruned SVGs are removed; no new ones are
        -- needed since the effective parent was already an ancestor.
        -- ============================================================================
        IF should_prune THEN
          -- Identify generated SVGs (appear as node2/node3 in AllResults).
          -- Only generated SVGs are prunable; verticals/root/uncategorized are never pruned.
          CREATE OR REPLACE TEMP TABLE GeneratedSVGs AS (
            SELECT DISTINCT svg_id FROM (
              SELECT node2 AS svg_id FROM AllResults WHERE node2 IS NOT NULL
              UNION ALL
              SELECT node3 AS svg_id FROM AllResults WHERE node3 IS NOT NULL
            )
          );

          -- Build direct parent-child relationships from the Edge table.
          -- specializationOf: SVG child → SVG parent
          -- memberOf: SV child → SVG parent
          CREATE OR REPLACE TEMP TABLE ParentChild AS (
            SELECT subject_id AS child, object_id AS parent, predicate
            FROM Edge WHERE predicate = 'specializationOf'
            UNION ALL
            SELECT subject_id AS child, object_id AS parent, predicate
            FROM Edge WHERE predicate = 'memberOf'
          );

          -- Iteratively identify all prunable SVGs (generated SVGs with <= 1 child).
          -- Includes SVGs with 0 children (empty/orphaned groups) and SVGs whose child count
          -- drops to <= 1 as a result of pruning child SVGs.
          CREATE OR REPLACE TEMP TABLE CurrentParentChild AS (
            SELECT * FROM ParentChild
          );

          CREATE OR REPLACE TEMP TABLE PrunableSVGs (svg_id STRING);

          WHILE new_prunable_found AND pruning_iteration < 50 DO
            SET pruning_iteration = pruning_iteration + 1;
            SET new_prunable_found = FALSE;

            CREATE OR REPLACE TEMP TABLE NewPrunable AS (
              SELECT g.svg_id
              FROM GeneratedSVGs g
              LEFT JOIN CurrentParentChild pc ON g.svg_id = pc.parent
              WHERE g.svg_id NOT IN (SELECT svg_id FROM PrunableSVGs)
              GROUP BY g.svg_id
              HAVING COUNT(DISTINCT pc.child) <= 1
            );

            IF (SELECT COUNT(*) FROM NewPrunable) > 0 THEN
              SET new_prunable_found = TRUE;

              INSERT INTO PrunableSVGs SELECT svg_id FROM NewPrunable;

              -- Update CurrentParentChild by bypassing PrunableSVGs nodes recursively
              CREATE OR REPLACE TEMP TABLE CurrentParentChild AS (
                WITH RECURSIVE Walk AS (
                  SELECT child, parent, predicate
                  FROM ParentChild

                  UNION ALL

                  SELECT w.child, pc.parent, w.predicate
                  FROM Walk w
                  JOIN PrunableSVGs p ON w.parent = p.svg_id
                  JOIN ParentChild pc ON p.svg_id = pc.child
                )
                SELECT DISTINCT child, parent, predicate
                FROM Walk
                WHERE parent NOT IN (SELECT svg_id FROM PrunableSVGs)
                  AND child NOT IN (SELECT svg_id FROM PrunableSVGs)
              );
            END IF;
          END WHILE;

          -- Compute effective parent for each surviving child of a pruned SVG across ALL DAG paths.
          -- Use a recursive CTE to explore ALL paths through the DAG. Each path stops when it
          -- reaches a non-prunable ancestor.
          CREATE OR REPLACE TEMP TABLE EffectiveParent AS (
            WITH RECURSIVE
            WalkUp AS (
              -- Base: direct children of pruned SVGs
              SELECT
                child AS node_id,
                parent AS effective_parent,
                predicate,
                1 AS depth
              FROM ParentChild
              WHERE parent IN (SELECT svg_id FROM PrunableSVGs)

              UNION ALL

              -- Recursive: if effective_parent is prunable, walk up to its parents
              SELECT
                w.node_id,
                pc.parent AS effective_parent,
                w.predicate,
                w.depth + 1 AS depth
              FROM WalkUp w
              JOIN PrunableSVGs p ON w.effective_parent = p.svg_id
              JOIN ParentChild pc ON p.svg_id = pc.child
            )
            -- Filter to non-prunable ancestors for surviving (non-pruned) children
            SELECT DISTINCT
              node_id,
              effective_parent,
              predicate
            FROM WalkUp
            WHERE effective_parent NOT IN (SELECT svg_id FROM PrunableSVGs)
              AND node_id NOT IN (SELECT svg_id FROM PrunableSVGs)
          );

          -- Build redirected edges for non-pruned children of pruned SVGs.
          -- Pruned SVGs themselves are removed entirely (no redirected edges).
          -- The predicate is preserved: specializationOf for SVG children, memberOf for SV children.
          CREATE OR REPLACE TEMP TABLE RedirectedEdges AS (
            SELECT DISTINCT
              ep.node_id AS subject_id,
              ep.predicate,
              ep.effective_parent AS object_id,
              orig.provenance
            FROM EffectiveParent ep
            JOIN Edge orig ON orig.subject_id = ep.node_id AND orig.predicate = ep.predicate
            WHERE orig.object_id IN (SELECT svg_id FROM PrunableSVGs)
              AND NOT EXISTS (
              SELECT 1 FROM Edge e
              WHERE e.subject_id = ep.node_id
                AND e.predicate = ep.predicate
                AND e.object_id = ep.effective_parent
            )
          );

          -- Rebuild Edge: remove all edges involving pruned SVGs, add redirected edges.
          CREATE OR REPLACE TEMP TABLE Edge AS (
            SELECT DISTINCT subject_id, predicate, object_id, provenance
            FROM Edge
            WHERE subject_id NOT IN (SELECT svg_id FROM PrunableSVGs)
              AND object_id NOT IN (SELECT svg_id FROM PrunableSVGs)
            UNION ALL
            SELECT DISTINCT subject_id, predicate, object_id, provenance
            FROM RedirectedEdges
          );

          -- Rebuild Node: remove pruned SVGs.
          CREATE OR REPLACE TEMP TABLE Node AS (
            SELECT DISTINCT subject_id, value, name, types
            FROM Node
            WHERE subject_id NOT IN (SELECT svg_id FROM PrunableSVGs)
          );

        END IF;

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
                bigquery.ScalarQueryParameter("generated_provenance_prefix", "STRING", gen_prov_prefix),
                bigquery.ScalarQueryParameter("max_iterations", "INT64", self.max_iterations),
                bigquery.ScalarQueryParameter("should_filter", "BOOL", self.should_filter_basic_population_type),
                bigquery.ScalarQueryParameter("should_prune", "BOOL", self.should_prune_single_child_svgs),
            ]
        )
        return self.executor.execute(query, job_config=job_config)