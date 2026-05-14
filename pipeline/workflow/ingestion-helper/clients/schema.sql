-- Copyright 2026 Google LLC
--
-- Licensed under the Apache License, Version 2.0 (the "License")
-- you may not use this file except in compliance with the License.
-- You may obtain a copy of the License at
--
--      http://www.apache.org/licenses/LICENSE-2.0
--
-- Unless required by applicable law or agreed to in writing, software
-- distributed under the License is distributed on an "AS IS" BASIS,
-- WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
-- See the License for the specific language governing permissions and
-- limitations under the License.
CREATE TABLE Node (
  subject_id STRING(1024) NOT NULL,
  value STRING(MAX),
  bytes BYTES(MAX),
  name STRING(MAX),
  types ARRAY<STRING(1024)>,
  last_update_timestamp TIMESTAMP OPTIONS (allow_commit_timestamp=true),
  name_tokenlist TOKENLIST AS (TOKENIZE_FULLTEXT(name)) HIDDEN,
) PRIMARY KEY(subject_id);

CREATE TABLE Edge (
  subject_id STRING(1024) NOT NULL,
  predicate STRING(1024) NOT NULL,
  object_id STRING(1024) NOT NULL,
  provenance STRING(1024) NOT NULL,
) PRIMARY KEY(subject_id, predicate, object_id, provenance),
INTERLEAVE IN Node;

CREATE TABLE TimeSeries (
  variable_measured STRING(1024) NOT NULL,
  entity1 STRING(1024) NOT NULL AS (JSON_VALUE(entities, '$.entity1')) STORED,
  extra_entities_id STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  entities JSON NOT NULL,
  facet JSON NOT NULL,
  entity2 STRING(1024) AS (JSON_VALUE(entities, '$.entity2')) STORED,
  entity3 STRING(1024) AS (JSON_VALUE(entities, '$.entity3')) STORED,
  observation_period STRING(1024) AS (JSON_VALUE(facet, '$.observationPeriod')) STORED,
  unit STRING(1024) AS (JSON_VALUE(facet, '$.unit')) STORED,
  measurement_method STRING(1024) AS (JSON_VALUE(facet, '$.measurementMethod')) STORED,
  scaling_factor STRING(1024) AS (JSON_VALUE(facet, '$.scalingFactor')) STORED,
  provenance STRING(1024) NOT NULL AS (JSON_VALUE(facet, '$.provenance')) STORED,
) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id), OPTIONS (
  columnar_policy = 'enabled'
);

CREATE TABLE Observation (
  variable_measured STRING(1024) NOT NULL,
  entity1 STRING(1024) NOT NULL,
  extra_entities_id STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  date STRING(32) NOT NULL,
  value STRING(MAX) NOT NULL,
) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id, date DESC),
  INTERLEAVE IN PARENT TimeSeries ON DELETE CASCADE, OPTIONS (
  columnar_policy = 'enabled'
);

CREATE INDEX TimeSeriesProvenance ON TimeSeries(provenance) OPTIONS (
  columnar_policy = 'enabled'
);

CREATE TABLE ImportStatus (
  ImportName STRING(MAX) NOT NULL,
  LatestVersion STRING(MAX),
  GraphPath STRING(MAX),
  State STRING(1024) NOT NULL,
  JobId STRING(1024),
  WorkflowId STRING(1024),
  ExecutionTime INT64,
  DataVolume INT64,
  DataImportTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  StatusUpdateTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  NextRefreshTimestamp TIMESTAMP,
) PRIMARY KEY(ImportName);

CREATE TABLE IngestionHistory (
  CompletionTimestamp TIMESTAMP NOT NULL OPTIONS ( allow_commit_timestamp = TRUE ),
  IngestionFailure Bool NOT NULL,
  WorkflowExecutionID STRING(1024) NOT NULL,
  DataflowJobID STRING(1024),
  IngestedImports ARRAY<STRING(MAX)>,
  ExecutionTime INT64,
  NodeCount INT64,
  EdgeCount INT64,
  ObservationCount INT64,
) PRIMARY KEY(CompletionTimestamp DESC);

CREATE TABLE ImportVersionHistory (
  ImportName STRING(MAX) NOT NULL,
  Version STRING(MAX) NOT NULL,
  UpdateTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  Comment STRING(MAX),
) PRIMARY KEY (ImportName, UpdateTimestamp DESC);

CREATE TABLE IngestionLock (
  LockID STRING(1024) NOT NULL,
  LockOwner STRING(1024),
  AcquiredTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
) PRIMARY KEY(LockID);

CREATE PROPERTY GRAPH DCGraph
  NODE TABLES(
    Node
      KEY(subject_id)
      LABEL Node PROPERTIES(
        bytes,
        name,
        subject_id,
        types,
        value)
  )
  EDGE TABLES(
    Edge
      KEY(subject_id, predicate, object_id, provenance)
      SOURCE KEY(subject_id) REFERENCES Node(subject_id)
      DESTINATION KEY(object_id) REFERENCES Node(subject_id)
      LABEL Edge PROPERTIES(
        object_id,
        predicate,
        provenance,
        subject_id)
  );

CREATE INDEX InEdge ON Edge(object_id, predicate, subject_id, provenance) OPTIONS (
  columnar_policy = 'enabled'
);


-- NodeEmbedding table, NodeEmbeddingIndex index and NodeEmbeddingModel model are necessary for embeddings to work properly.

CREATE TABLE NodeEmbedding (
  subject_id STRING(1024) NOT NULL,
  embedding_content STRING(MAX),
  types ARRAY<STRING(1024)>,
  embeddings ARRAY<FLOAT64>(vector_length=>768)
) PRIMARY KEY(subject_id),
INTERLEAVE IN PARENT Node ON DELETE CASCADE;

CREATE VECTOR INDEX NodeEmbeddingIndex
ON NodeEmbedding(embeddings)
WHERE embeddings IS NOT NULL
OPTIONS (
  distance_type = 'COSINE'
);

CREATE MODEL NodeEmbeddingModel
INPUT(
  content STRING(MAX),
  task_type STRING(MAX),
)
OUTPUT(
  embeddings
    STRUCT<
      statistics STRUCT<truncated BOOL, token_count FLOAT64>,
      values ARRAY<FLOAT64>>
)
REMOTE OPTIONS (
  endpoint = '{{ embeddings_endpoint }}'
);
