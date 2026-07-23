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
"""Base class and common configuration for Data Commons aggregation E2E tests."""

import os
import sys
import tempfile
import unittest
import logging
import json
from collections.abc import Mapping
from typing import Any, Dict, List
import yaml
from google.cloud import spanner
from google.cloud import bigquery

# Add aggregation-helper to sys.path (two levels up from this file)
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..', '..')))
from aggregation import BigQueryExecutor, AggregationOrchestrator, AggregationRunResult

# Configuration
PROJECT_ID = os.environ.get('PROJECT_ID', 'datcom-ci')
SPANNER_INSTANCE_ID = os.environ.get('SPANNER_INSTANCE_ID', 'datcom-spanner-test')
SPANNER_DATABASE_ID = os.environ.get('SPANNER_DATABASE_ID', 'dc_graph_stuniki_test')
BQ_CONNECTION_ID = os.environ.get(
    'BQ_CONNECTION_ID',
    'projects/datcom-ci/locations/us-central1/connections/spanner_stuniki_test_conn'
)
BQ_LOCATION = os.environ.get('BQ_LOCATION', 'us-central1')
ENABLE_EMBEDDINGS = os.environ.get('ENABLE_EMBEDDINGS', 'true').lower() == 'true'
BQ_DATASET_ID = os.environ.get('BQ_DATASET_ID', 'spanner_model_dataset')

logging.basicConfig(level=logging.INFO)


class AggregationIntegrationTestBase(unittest.TestCase):
    """Base class for aggregation integration tests, handling Spanner setup/teardown."""

    is_base_dc = True

    @classmethod
    def setUpClass(cls):
        logging.info("Setting up Spanner and BigQuery clients...")
        cls.spanner_client = spanner.Client(project=PROJECT_ID)
        cls.instance = cls.spanner_client.instance(SPANNER_INSTANCE_ID)
        cls.database = cls.instance.database(SPANNER_DATABASE_ID)
        cls.bq_client = bigquery.Client(project=PROJECT_ID)
        logging.info("Clients initialized successfully.")

    def setUp(self):
        logging.info("Clearing tables before test...")
        self.clear_tables()
        self.mock_nodes = []
        self.mock_edges = []
        self.mock_timeseries = []
        self.mock_observations = []

    def tearDown(self):
        logging.info("Clearing tables after test...")
        self.clear_tables()
        self.mock_nodes = []
        self.mock_edges = []
        self.mock_timeseries = []
        self.mock_observations = []

    def clear_tables(self):
        def _clear(transaction):
            # Delete in correct order due to constraints
            transaction.execute_update("DELETE FROM Cache WHERE TRUE")
            transaction.execute_update("DELETE FROM KeyValueStore WHERE TRUE")
            transaction.execute_update("DELETE FROM Observation WHERE TRUE")
            transaction.execute_update("DELETE FROM TimeSeries WHERE TRUE")
            transaction.execute_update("DELETE FROM Edge WHERE TRUE")
            transaction.execute_update("DELETE FROM NodeEmbedding WHERE TRUE")
            transaction.execute_update("DELETE FROM Node WHERE TRUE")
        
        self.database.run_in_transaction(_clear)
        logging.info("Tables cleared successfully.")

    def add_node(self, subject_id, name=None, value=None, types=None):
        """Adds a node to mock list."""
        self.mock_nodes.append((subject_id, name or subject_id, value, None, types or []))

    def add_edge(self, subject_id, predicate, object_id, import_name):
        """Adds an edge to mock list."""
        prefix = "dc/base/" if self.is_base_dc else ""
        provenance = f"{prefix}{import_name}"
        self.mock_edges.append((subject_id, predicate, object_id, provenance))

    def add_timeseries(self, variable, entity_id, method='CensusACS5yrSurvey', period='P1Y', unit='1', scaling='1', import_name='USFed_ConstantMaturityRates_Test', facet_id='facet1', is_dc_aggregate=False, extra_entities_id='', entities=None):
        """Adds a TimeSeries metadata row to mock list."""
        prefix = "dc/base/" if self.is_base_dc else ""
        provenance = f"{prefix}{import_name}"
        entities_dict = entities if entities is not None else {'entity1': entity_id}
        entities_json = json.dumps(entities_dict)
        facet_json = json.dumps({
            'measurementMethod': method,
            'observationPeriod': period,
            'unit': unit,
            'scalingFactor': scaling,
            'provenance': provenance,
            'isDcAggregate': is_dc_aggregate
        })
        
        # Avoid duplicates in mock list
        exists = False
        for ts in self.mock_timeseries:
            if ts[0] == variable and ts[1] == entities_json and ts[2] == extra_entities_id and ts[3] == facet_id:
                exists = True
                break
        if not exists:
            self.mock_timeseries.append((variable, entities_json, extra_entities_id, facet_id, facet_json))

    def add_observation(self, variable, entity_id, date, value, method='CensusACS5yrSurvey', period='P1Y', unit='1', scaling='1', import_name='USFed_ConstantMaturityRates_Test', facet_id='facet1', is_dc_aggregate=False, extra_entities_id='', entities=None):
        """Adds an Observation and ensures its parent TimeSeries exists."""
        self.add_timeseries(variable, entity_id, method, period, unit, scaling, import_name, facet_id, is_dc_aggregate, extra_entities_id, entities=entities)
        self.mock_observations.append((variable, entity_id, extra_entities_id, facet_id, date, str(value)))

    def flush_to_spanner(self):
        """Writes all accumulated mock data to Spanner and clears the buffers."""
        if self.mock_nodes:
            logging.info(f"Writing {len(self.mock_nodes)} Nodes to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Node',
                    columns=['subject_id', 'name', 'value', 'bytes', 'types'],
                    values=self.mock_nodes
                )
            self.mock_nodes = []
        if self.mock_edges:
            logging.info(f"Writing {len(self.mock_edges)} Edges to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Edge',
                    columns=['subject_id', 'predicate', 'object_id', 'provenance'],
                    values=self.mock_edges
                )
            self.mock_edges = []
        if self.mock_timeseries:
            logging.info(f"Writing {len(self.mock_timeseries)} TimeSeries to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='TimeSeries',
                    columns=['variable_measured', 'entities', 'extra_entities_id', 'facet_id', 'facet'],
                    values=self.mock_timeseries
                )
            self.mock_timeseries = []
        if self.mock_observations:
            logging.info(f"Writing {len(self.mock_observations)} Observations to Spanner...")
            with self.database.batch() as batch:
                batch.insert(
                    table='Observation',
                    columns=['variable_measured', 'entity1', 'extra_entities_id', 'facet_id', 'date', 'value'],
                    values=self.mock_observations
                )
            self.mock_observations = []

    def run_orchestrator(
        self,
        calculations: List[Dict[str, Any]],
        active_imports: List[str],
        dry_run: bool = False,
        run_sequential: bool = True,
        poll_interval: int = 3,
        skip_deletions: bool = False
    ) -> AggregationRunResult:
        """Helper to run AggregationOrchestrator with the given calculation configuration.

        Serializes the calculation definitions to a temporary YAML file so that validate_config()
        verifies schema compliance when the orchestrator initializes, then runs the pipeline.
        """
        tmp_path = None
        try:
            with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as tmp_file:
                tmp_path = tmp_file.name
                yaml.dump({"calculations": calculations}, tmp_file)

            orchestrator = AggregationOrchestrator(
                connection_id=BQ_CONNECTION_ID,
                project_id=PROJECT_ID,
                instance_id=SPANNER_INSTANCE_ID,
                database_id=SPANNER_DATABASE_ID,
                location=BQ_LOCATION,
                is_base_dc=self.is_base_dc,
                config_file_path=tmp_path,
                run_sequential=run_sequential,
                poll_interval=poll_interval,
                enable_embeddings=ENABLE_EMBEDDINGS,
                bq_dataset_id=BQ_DATASET_ID
            )
            return orchestrator.run(active_imports=active_imports, dry_run=dry_run, skip_deletions=skip_deletions)
        finally:
            if tmp_path and os.path.exists(tmp_path):
                os.remove(tmp_path)

