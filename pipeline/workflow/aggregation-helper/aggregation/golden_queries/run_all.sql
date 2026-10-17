        -- 1. Generate embeddings natively in BigQuery
        CREATE TEMP TABLE embedding_staging AS
        SELECT 
          subject_id, 
          "test_embedding" AS embedding_label, 
          embedding_content_key,
          embedding_content, 
          node_types, 
          ml_generate_embedding_result AS embeddings
        FROM ML.GENERATE_EMBEDDING(
          MODEL `test-spanner-project.datacommons.TestModel`,
          (
                SELECT 
                  subject_id, 
                  CAST(FARM_FINGERPRINT(TO_JSON_STRING(embedding_content)) AS STRING) AS embedding_content_key,
                  TO_JSON_STRING(embedding_content) AS content, 
                  embedding_content, 
                  node_types 
                FROM `test-project.datacommons.temp_raw_nodes_test_embedding`
            ),
          STRUCT("TEST_TASK" AS task_type)
        );

        -- 2. Export back to Spanner
        EXPORT DATA OPTIONS(
          uri="spanner-uri",
          format="CLOUD_SPANNER",
          spanner_options='{"table": "CustomEmbeddingTable", "priority": "LOW"}'
        ) AS
        SELECT * FROM embedding_staging;
