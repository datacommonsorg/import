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
"""Generates and runs entity aggregations using BigQuery Federation."""

import logging
import re
from typing import Dict, List, Tuple
from google.cloud import bigquery
from .bq_executor import BigQueryExecutor
from .sql_utils import _escape_sql_literal


class EntityAggregationConfig:
    """Configuration for entity aggregation."""

    def __init__(self,
                 entity_types: List[str],
                 location_props: List[str],
                 date_prop: str,
                 agg_date_formats: List[str],
                 constraints: List[str],
                 output_import: str,
                 input_imports: List[str]):
        self.entity_types = entity_types
        self.location_props = location_props
        self.date_prop = date_prop
        self.agg_date_formats = agg_date_formats
        self.constraints = constraints
        self.output_import = output_import
        self.input_imports = input_imports


class EntityAggregationGenerator:
    """Generates and runs entity aggregations using BigQuery Federation."""

    def __init__(self,
                 executor: BigQueryExecutor,
                 is_base_dc: bool = True) -> None:
        self.executor = executor
        self.is_base_dc = is_base_dc

    def aggregate_entities(
            self,
            configs: List[EntityAggregationConfig]
    ) -> List[bigquery.job.QueryJob]:
        """Runs entity aggregations and returns their BigQuery jobs."""
        jobs = []
        for config in configs:
            logging.info(
                f"Generating entity aggregation for types: {config.entity_types}"
            )
            query = self._generate_sql(config)
            job = self.executor.execute(query)
            if job:
                jobs.append(job)
        return jobs

    def _parse_constraints(self, constraints: List[str]) -> List[Dict]:
        """Parses MCF-style constraints into structured metadata and SQL clauses."""
        parsed = []
        for c in constraints:
            if not c or ':' not in c:
                continue
            prop, val_str = [part.strip() for part in c.split(':', 1)]
            
            # 1. Wildcard constraint
            if val_str == '*':
                parsed.append({
                    'prop': prop,
                    'is_wildcard': True,
                    'sql_filter': None,
                    'val_str': val_str
                })
                continue

            # 2. Range constraint [Low - Unit] (e.g., [7 - M])
            range_low_match = re.match(r'^\[\s*([\d\.]+)\s*-\s*(\w+)\s*\]$', val_str)
            if range_low_match:
                low = range_low_match.group(1)
                parsed.append({
                    'prop': prop,
                    'is_wildcard': False,
                    'sql_filter': f"SAFE_CAST({prop}_val AS FLOAT64) >= {low}",
                    'val_str': val_str
                })
                continue

            # 3. Range constraint [Low High Unit] (e.g., [3 4 M])
            range_bounds_match = re.match(r'^\[\s*([\d\.]+)\s+([\d\.]+)\s+(\w+)\s*\]$', val_str)
            if range_bounds_match:
                low = range_bounds_match.group(1)
                high = range_bounds_match.group(2)
                parsed.append({
                    'prop': prop,
                    'is_wildcard': False,
                    'sql_filter': f"SAFE_CAST({prop}_val AS FLOAT64) >= {low} AND SAFE_CAST({prop}_val AS FLOAT64) <= {high}",
                    'val_str': val_str
                })
                continue

            # 4. Simple literal value constraint (e.g., magnitudeType: MagnitudeMl)
            parsed.append({
                'prop': prop,
                'is_wildcard': False,
                'sql_filter': f"{prop}_val = '{_escape_sql_literal(val_str)}'",
                'val_str': val_str
            })
        return parsed

    def _generate_sql(self, config: EntityAggregationConfig) -> str:
        """Generates the multi-statement SQL script for a single aggregation config."""
        connection_id = self.executor.connection_id
        dest = self.executor.get_spanner_destination_uri()
        
        prefix = "dc/base/" if self.is_base_dc else ""
        output_provenance = f"{prefix}{config.output_import}"
        
        safe_input_imports = [_escape_sql_literal(name) for name in config.input_imports]
        input_provenances = [f"'{prefix}{name}'" for name in safe_input_imports]
        input_provenances_str = ", ".join(input_provenances)

        entity_types_str = ", ".join([f"'{_escape_sql_literal(t)}'" for t in config.entity_types])
        
        # Parse constraints
        parsed_constraints = self._parse_constraints(config.constraints)
        location_props_str = ", ".join([f'"{_escape_sql_literal(p)}"' for p in config.location_props])
        
        # 1. Step 1: Extract Raw Entities and Properties
        sql_parts = []
        sql_parts.append(f"""
        -- Step 1: Extract raw entity IDs of the target types
        CREATE OR REPLACE TEMPORARY TABLE `temp_entities` AS
        SELECT DISTINCT subject_id AS entity_id, object_id AS entity_type
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT subject_id, object_id FROM Edge 
             WHERE predicate = "typeOf" 
               AND object_id IN ({entity_types_str}) 
               AND provenance IN ({input_provenances_str})''');

        -- Extract locations (supporting multiple location_props, filtering out latLong/ nodes)
        CREATE OR REPLACE TEMPORARY TABLE `temp_locations` AS
        SELECT DISTINCT subject_id AS entity_id, object_id AS location_id
        FROM EXTERNAL_QUERY("{connection_id}",
          '''SELECT subject_id, object_id FROM Edge 
             WHERE predicate IN ({location_props_str})''')
        WHERE subject_id IN (SELECT entity_id FROM `temp_entities`)
          AND NOT STARTS_WITH(object_id, 'latLong/');
        """)

        if config.date_prop:
            sql_parts.append(f"""
            -- Extract dates
            CREATE OR REPLACE TEMPORARY TABLE `temp_dates` AS
            SELECT DISTINCT subject_id AS entity_id, object_id AS raw_date
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id, object_id FROM Edge 
                 WHERE predicate = "{config.date_prop}"''')
            WHERE subject_id IN (SELECT entity_id FROM `temp_entities`);
            """)

        # Extract constraint values
        for c in parsed_constraints:
            sql_parts.append(f"""
            -- Extract constraint: {c['prop']}
            CREATE OR REPLACE TEMPORARY TABLE `temp_constraint_{c['prop']}` AS
            SELECT DISTINCT subject_id AS entity_id, object_id AS {c['prop']}_val
            FROM EXTERNAL_QUERY("{connection_id}",
              '''SELECT subject_id, object_id FROM Edge 
                 WHERE predicate = "{c['prop']}"''')
            WHERE subject_id IN (SELECT entity_id FROM `temp_entities`);
            """)

        # 2. Step 2: Apply Constraints and Buckets
        date_select_raw = "d.raw_date" if config.date_prop else "FORMAT_DATE('%Y-%m-%d', CURRENT_DATE())"
        date_join = "LEFT JOIN `temp_dates` d ON e.entity_id = d.entity_id" if config.date_prop else ""
        
        # Format date buckets and periods (using raw_date from CTE)
        date_bucket_cols = []
        for fmt in config.agg_date_formats:
            if fmt == 'YYYY':
                date_bucket_cols.append("LEFT(raw_date, 4) AS date_YYYY, 'P1Y' AS period_YYYY")
            elif fmt == 'YYYY-MM':
                date_bucket_cols.append("LEFT(raw_date, 7) AS date_YYYY_MM, 'P1M' AS period_YYYY_MM")
            elif fmt == 'YYYY-MM-DD':
                date_bucket_cols.append("LEFT(raw_date, 10) AS date_YYYY_MM_DD, 'P1D' AS period_YYYY_MM_DD")
        
        date_bucket_select = ", ".join(date_bucket_cols)
        
        # Constraint selects and joins
        cte_cons_selects = ", ".join([f"c_{c['prop']}.{c['prop']}_val" for c in parsed_constraints])
        if cte_cons_selects:
            cte_cons_selects = ", " + cte_cons_selects
            
        outer_cons_selects = ", ".join([f"{c['prop']}_val" for c in parsed_constraints])
        if outer_cons_selects:
            outer_cons_selects = ", " + outer_cons_selects

        cons_joins = "\n".join([f"JOIN `temp_constraint_{c['prop']}` c_{c['prop']} ON e.entity_id = c_{c['prop']}.entity_id" for c in parsed_constraints])
        
        # Constraint filters (only for non-wildcard constraints)
        filters = [c['sql_filter'] for c in parsed_constraints if c['sql_filter']]
        filter_clause = "AND " + " AND ".join(filters) if filters else ""
        date_null_filter = "\n            AND d.raw_date IS NOT NULL" if config.date_prop else ""

        sql_parts.append(f"""
        -- Step 2: Apply Constraints and Buckets
        CREATE OR REPLACE TEMPORARY TABLE `temp_filtered_events` AS
        WITH RawEvents AS (
          SELECT 
            e.entity_id,
            e.entity_type,
            l.location_id,
            {date_select_raw} AS raw_date
            {cte_cons_selects}
          FROM `temp_entities` e
          JOIN `temp_locations` l ON e.entity_id = l.entity_id
          {date_join}
          {cons_joins}
          WHERE TRUE
            {filter_clause}{date_null_filter}
        )
        SELECT 
          entity_id,
          entity_type,
          location_id,
          raw_date,
          {date_bucket_select}
          {outer_cons_selects}
        FROM RawEvents;
        """)

        # 3. Step 3: Group and Count (UNION ALL for multiple formats)
        group_parts = []
        for fmt in config.agg_date_formats:
            fmt_clean = fmt.replace('-', '_')
            period = 'P1Y' if fmt == 'YYYY' else ('P1M' if fmt == 'YYYY-MM' else 'P1D')
            wildcard_cols = [f"{c['prop']}_val" for c in parsed_constraints if c['is_wildcard']]
            wildcard_select = ", ".join(wildcard_cols)
            if wildcard_select:
                wildcard_select = ", " + wildcard_select
                
            group_by_cols = f"location_id, entity_type, date_{fmt_clean}, period_{fmt_clean}"
            if wildcard_cols:
                group_by_cols += ", " + ", ".join(wildcard_cols)

            group_parts.append(f"""
            SELECT 
              location_id,
              entity_type,
              date_{fmt_clean} AS obs_date,
              period_{fmt_clean} AS obs_period,
              COUNT(entity_id) AS event_count
              {wildcard_select}
            FROM `temp_filtered_events`
            GROUP BY {group_by_cols}
            """)
            
        union_groups_query = "\nUNION ALL\n".join(group_parts)
        
        sql_parts.append(f"""
        -- Step 3: Group and Count
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_counts` AS
        {union_groups_query};
        """)

        # 4. Step 4: Deterministic SV DCID Generation
        # We need to construct the serialization key for the SV.
        # Static SV properties: measuredProperty=count, statType=measuredValue
        # Plus populationType (from column), static constraints, and wildcard values.
        # We must sort all keys to ensure determinism.
        sv_props = {
            'measuredProperty': 'count',
            'statType': 'measuredValue'
        }
        # Add static constraints
        for c in parsed_constraints:
            if not c['is_wildcard']:
                sv_props[c['prop']] = c['val_str']
                
        # Build the serialization parts
        all_keys = ['populationType'] + list(sv_props.keys()) + [c['prop'] for c in parsed_constraints if c['is_wildcard']]
        sorted_keys = sorted(all_keys)
        
        flat_args = []
        for i, key in enumerate(sorted_keys):
            if i > 0:
                flat_args.append("','") # Add comma separator between pairs
            if key == 'populationType':
                flat_args.append("'populationType='")
                flat_args.append("entity_type")
            elif key in sv_props:
                flat_args.append(f"'{key}={sv_props[key]}'")
            else:
                flat_args.append(f"'{key}='")
                flat_args.append(f"COALESCE({key}_val, '')")
                
        concat_expr = f"CONCAT({', '.join(flat_args)})"
        
        # Build the SV Name SQL expression
        name_flat_args = ["'Count of '", "entity_type"]
        static_cons_desc = " and ".join([f"{c['prop']} {c['val_str']}" for c in parsed_constraints if not c['is_wildcard']])
        if static_cons_desc:
            name_flat_args.append(f"' with {static_cons_desc}'")
            
        # Add wildcards to name
        wildcard_constraints = [c for c in parsed_constraints if c['is_wildcard']]
        for c in wildcard_constraints:
            name_flat_args.append(f"', {c['prop']}='")
            name_flat_args.append(f"COALESCE({c['prop']}_val, 'unknown')")
            
        name_expr = f"CONCAT({', '.join(name_flat_args)})"

        # We create a temp table that contains the aggregated counts AND the generated SV DCID/Name
        sql_parts.append(f"""
        -- Step 4: Generate SV DCIDs and Names
        CREATE OR REPLACE TEMPORARY TABLE `temp_aggregated_with_sv` AS
        SELECT 
          *,
          CONCAT('dc/sv/gp/', CAST(FARM_FINGERPRINT({concat_expr}) AS STRING)) AS sv_dcid,
          {name_expr} AS sv_name
        FROM `temp_aggregated_counts`;
        """)

        # 5. Step 5: Export to Spanner
        
        # 5.1 Export SV Nodes (Only unique SVs)
        sql_parts.append(f"""
        -- Export SV Nodes to Spanner Node table
        EXPORT DATA
          OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Node"}}' ) AS
        SELECT DISTINCT
          sv_dcid AS subject_id,
          sv_name AS name,
          CAST(NULL AS STRING) AS value,
          CAST(NULL AS BYTES) AS bytes,
          ['StatisticalVariable'] AS types
        FROM `temp_aggregated_with_sv`;
        """)

        # 5.2 Export SV Edges
        edge_selects = []
        # Static typeOf
        edge_selects.append(f"SELECT DISTINCT sv_dcid AS subject_id, 'typeOf' AS predicate, 'StatisticalVariable' AS object_id, '{output_provenance}' AS provenance FROM `temp_aggregated_with_sv`")
        # Dynamic populationType
        edge_selects.append(f"SELECT DISTINCT sv_dcid AS subject_id, 'populationType' AS predicate, entity_type AS object_id, '{output_provenance}' AS provenance FROM `temp_aggregated_with_sv`")
        # Static SV props
        for k, v in sv_props.items():
            edge_selects.append(f"SELECT DISTINCT sv_dcid AS subject_id, '{k}' AS predicate, '{_escape_sql_literal(v)}' AS object_id, '{output_provenance}' AS provenance FROM `temp_aggregated_with_sv`")
        # Wildcard props
        for c in wildcard_constraints:
            edge_selects.append(f"SELECT DISTINCT sv_dcid AS subject_id, '{c['prop']}' AS predicate, {c['prop']}_val AS object_id, '{output_provenance}' AS provenance FROM `temp_aggregated_with_sv` WHERE {c['prop']}_val IS NOT NULL")
            
        union_edges_query = "\nUNION ALL\n".join(edge_selects)

        sql_parts.append(f"""
        -- Export SV Edges to Spanner Edge table
        EXPORT DATA
          OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Edge"}}' ) AS
        {union_edges_query};
        """)

        # 5.3 Export TimeSeries
        # We calculate the facet_id using the standard Farm Fingerprint of the facet fields.
        # We need to ensure we only export unique TimeSeries (sv_dcid, location_id, obs_period)
        sql_parts.append(f"""
        -- Export TimeSeries to Spanner TimeSeries table
        EXPORT DATA
          OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "TimeSeries"}}' ) AS
        WITH UniqueTimeSeries AS (
          SELECT DISTINCT
            sv_dcid,
            location_id,
            obs_period
          FROM `temp_aggregated_with_sv`
        ),
        PreparedTS AS (
          SELECT
            sv_dcid AS variable_measured,
            JSON_OBJECT('entity1', location_id) AS entities,
            '' AS extra_entities_id,
            JSON_OBJECT(
              'measurementMethod', 'DataCommonsAggregate',
              'observationPeriod', obs_period,
              'provenance', '{output_provenance}',
              'isDcAggregate', true
            ) AS facet,
            obs_period
          FROM UniqueTimeSeries
        )
        SELECT
          variable_measured,
          entities,
          extra_entities_id,
          CAST(FARM_FINGERPRINT(CONCAT(
            '{output_provenance}', '^',
            'DataCommonsAggregate', '^',
            obs_period, '^',
            '', '^', -- scalingFactor
            '', '^', -- unit
            'true'   -- isDcAggregate
          )) AS STRING) AS facet_id,
          facet
        FROM PreparedTS;
        """)

        # 5.4 Export Observations
        sql_parts.append(f"""
        -- Export Observations to Spanner Observation table
        EXPORT DATA
          OPTIONS( uri="{dest}", format='CLOUD_SPANNER', spanner_options = '{{"table": "Observation"}}' ) AS
        SELECT
          sv_dcid AS variable_measured,
          location_id AS entity1,
          '' AS extra_entities_id,
          CAST(FARM_FINGERPRINT(CONCAT(
            '{output_provenance}', '^',
            'DataCommonsAggregate', '^',
            obs_period, '^',
            '', '^',
            '', '^',
            'true'
          )) AS STRING) AS facet_id,
          obs_date AS date,
          CAST(event_count AS STRING) AS value
        FROM `temp_aggregated_with_sv`;
        """)

        return "\n".join(sql_parts)
