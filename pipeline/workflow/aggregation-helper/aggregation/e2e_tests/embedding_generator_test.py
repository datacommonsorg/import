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
"""Integration E2E tests for Data Commons EmbeddingGenerator.

Covers:
- EmbeddingGenerator (calling ML.GENERATE_EMBEDDING remote model, extracting raw nodes,
  natively in BigQuery, and exporting to Spanner NodeEmbedding table).

NOTE: This script is intended for local testing purposes only and is NOT
currently part of the CI pipeline.
"""

import os
import unittest
from google.cloud import bigquery
from google.cloud import spanner
from aggregation.e2e_tests.base import (
    AggregationIntegrationTestBase,
    PROJECT_ID,
    SPANNER_INSTANCE_ID,
    SPANNER_DATABASE_ID,
    BQ_CONNECTION_ID,
    BQ_LOCATION,
    ENABLE_EMBEDDINGS,
    BQ_MODEL_CONNECTION,
    BQ_DATASET_ID,
)
from aggregation import BigQueryExecutor, EmbeddingGenerator

class EmbeddingGeneratorIntegrationTest(AggregationIntegrationTestBase):
    """Integration E2E tests for EmbeddingGenerator."""

    def test_embedding_generation_no_filter(self):
        """Tests EMBEDDING_GENERATION with NoFilter type."""
        # 1. Setup mock nodes in Spanner
        self.add_node("dcid/TestStatVar", name="Median Income of San Francisco", types=["StatisticalVariable"])
        self.add_node("dcid/TestTopic", name="Income and Poverty in San Francisco", types=["Topic"])
        
        self.flush_to_spanner()

        # 2. Trigger embedding generation orchestrator
        os.environ['ENABLE_EMBEDDINGS'] = 'true'
        os.environ['BQ_MODEL_CONNECTION'] = BQ_MODEL_CONNECTION
        os.environ['BQ_DATASET_ID'] = BQ_DATASET_ID

        calculations = [
            {
                "name": "Node Embeddings Integration Test",
                "type": "EMBEDDING_GENERATION",
                "embedding_table": "NodeEmbedding",
                "embedding_generation": {
                    "specs": [
                        {
                            "embedding_label": "base_text_embedding",
                            "model_name": "NodeEmbeddingModel",
                            "model_endpoint": "text-embedding-005",
                            "task_type": "RETRIEVAL_DOCUMENT",
                            "node_types": ["StatisticalVariable", "Topic"],
                            "node_filter_type": "NoFilter"
                        }
                    ]
                }
            }
        ]

        try:
            res = self.run_orchestrator(calculations=calculations, active_imports=[])
            self.assertTrue(res.success)
            self.assertIn("GLOBAL", res.import_results)
            self.assertTrue(res.import_results["GLOBAL"].success)

            # 3. Verify results in Spanner NodeEmbedding table
            with self.database.snapshot() as snapshot:
                query = """
                    SELECT subject_id, embedding_label, embedding_content, ARRAY_LENGTH(embeddings)
                    FROM NodeEmbedding
                    ORDER BY subject_id
                """
                results = list(snapshot.execute_sql(query))
                self.assertEqual(len(results), 2)
                
                # Assert first result (TestStatVar)
                self.assertEqual(results[0][0], "dcid/TestStatVar")
                self.assertEqual(results[0][1], "base_text_embedding")
                self.assertEqual(results[0][3], 768)

                # Assert second result (TestTopic)
                self.assertEqual(results[1][0], "dcid/TestTopic")
                self.assertEqual(results[1][1], "base_text_embedding")
                self.assertEqual(results[1][3], 768)

        finally:
            if not ENABLE_EMBEDDINGS:
                os.environ.pop('ENABLE_EMBEDDINGS', None)
