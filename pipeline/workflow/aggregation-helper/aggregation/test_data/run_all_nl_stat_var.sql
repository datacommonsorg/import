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
                  r.subject_id, 
                  CAST(FARM_FINGERPRINT(m.sentence) AS STRING) AS embedding_content_key,
                  m.sentence AS content, 
                  JSON_OBJECT("title", r.subject_id, "sentence", m.sentence) AS embedding_content, 
                  r.node_types 
                FROM UNNEST(@nl_stat_vars) m
                INNER JOIN `test-project.datacommons.temp_raw_nodes_test_embedding` r ON r.subject_id = m.dcid
            ),
          STRUCT("TEST_TASK" AS task_type)
        );

        -- 2. Export back to Spanner
        EXPORT DATA OPTIONS(
          uri="spanner-uri",
          format="CLOUD_SPANNER",
          spanner_options='{"table": "CustomEmbeddingTable"}'
        ) AS
        SELECT * FROM embedding_staging;
