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
) PRIMARY KEY(subject_id), OPTIONS (
  columnar_policy = 'enabled'
);

CREATE TABLE Edge (
  subject_id STRING(1024) NOT NULL,
  predicate STRING(1024) NOT NULL,
  object_id STRING(1024) NOT NULL,
  provenance STRING(1024) NOT NULL,
) PRIMARY KEY(subject_id, predicate, object_id, provenance),
INTERLEAVE IN Node, OPTIONS (
  columnar_policy = 'enabled'
);

CREATE INDEX InEdge ON Edge(object_id, predicate, subject_id, provenance) OPTIONS (
  columnar_policy = 'enabled'
);

CREATE INDEX EdgeByProvenance ON Edge(provenance) OPTIONS (
  columnar_policy = 'enabled'
);

-- Foreign key constraints on Edge table

-- Optimization - helps Spanner's query optimizer plan efficient joins from Edge.object_id to Node.subject_id for multi-hop graph traversals
-- CONSTRAINT FKObject FOREIGN KEY(object_id) REFERENCES Node(subject_id);

-- For edge-node consistency
-- ALTER TABLE Edge SET INTERLEAVE IN PARENT Node;
-- CONSTRAINT FKPredicate FOREIGN KEY(predicate) REFERENCES Node(subject_id);
-- CONSTRAINT FKProvenance FOREIGN KEY(provenance) REFERENCES Node(subject_id);


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
  provenance STRING(1024) NOT NULL AS (JSON_VALUE(facet, '$.provenance')) STORED,
  last_update_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
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
  last_update_timestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
) PRIMARY KEY(variable_measured, entity1, extra_entities_id, facet_id, date DESC),
  INTERLEAVE IN PARENT TimeSeries ON DELETE CASCADE, OPTIONS (
  columnar_policy = 'enabled'
);

CREATE INDEX TimeSeriesByEntity1
ON TimeSeries(entity1)
OPTIONS (columnar_policy = 'enabled');

CREATE NULL_FILTERED INDEX TimeSeriesByEntity2
ON TimeSeries(entity2)
OPTIONS (columnar_policy = 'enabled');

CREATE NULL_FILTERED INDEX TimeSeriesByEntity3
ON TimeSeries(entity3, variable_measured, entity1, entity2)
OPTIONS (columnar_policy = 'enabled');

CREATE INDEX TimeSeriesByProvenance ON TimeSeries(provenance) OPTIONS (
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
  WorkflowExecutionID STRING(1024) NOT NULL,
  CreationTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  CompletionTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  IngestionFailure Bool,
  Status STRING(1024),
  Stage STRING(1024),
  DataflowJobID STRING(1024),
  IngestedImports ARRAY<STRING(MAX)>,
  ExecutionTime INT64,
  NodeCount INT64,
  EdgeCount INT64,
  ObservationCount INT64,
) PRIMARY KEY(WorkflowExecutionID);

CREATE TABLE ImportVersionHistory (
  ImportName STRING(MAX) NOT NULL,
  Version STRING(MAX) NOT NULL,
  UpdateTimestamp TIMESTAMP NOT NULL OPTIONS (allow_commit_timestamp=true),
  WorkflowExecutionID STRING(1024),
  Status STRING(1024),
  ExecutionTime INT64,
  NodeCount INT64,
  EdgeCount INT64,
  ObservationCount INT64,
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

CREATE TABLE Cache (
  type STRING(1024) NOT NULL,
  key STRING(1024) NOT NULL,
  provenance STRING(1024) NOT NULL,
  value JSON,
) PRIMARY KEY(type, key, provenance);




-- NodeEmbedding table, NodeEmbeddingIndex index and NodeEmbeddingModel model are necessary for embeddings to work properly.

CREATE TABLE {{ embedding_table }} (
  subject_id STRING(1024) NOT NULL,
  embedding_label STRING(1024) NOT NULL,
  embedding_content JSON,
  node_types ARRAY<STRING(1024)>,
  embeddings ARRAY<FLOAT64>(vector_length=>{{ embedding_space }})
) PRIMARY KEY(subject_id, embedding_label),
INTERLEAVE IN PARENT Node ON DELETE CASCADE;

CREATE VECTOR INDEX {{ embedding_index }}
ON {{ embedding_table }}(embeddings, embedding_label)
WHERE embeddings IS NOT NULL
OPTIONS (
  distance_type = 'COSINE'
);

{% for model in models %}
CREATE MODEL {{ model.name }}
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
  endpoint = '{{ model.endpoint }}'
);
{% endfor %}
