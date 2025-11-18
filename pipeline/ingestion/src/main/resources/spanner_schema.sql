CREATE PROTO BUNDLE (
  `org.datacommons.Observations`
)

CREATE TABLE Node (
  subject_id STRING(1024) NOT NULL,
  value STRING(MAX),
  bytes BYTES(MAX),
  name STRING(MAX),
  types ARRAY<STRING(1024)>,
  name_tokenlist TOKENLIST AS (TOKENIZE_FULLTEXT(name)) HIDDEN,
) PRIMARY KEY(subject_id)

CREATE TABLE Edge (
  subject_id STRING(1024) NOT NULL,
  predicate STRING(1024) NOT NULL,
  object_id STRING(1024) NOT NULL,
  provenance STRING(1024) NOT NULL,
) PRIMARY KEY(subject_id, predicate, object_id, provenance),
INTERLEAVE IN Node

CREATE TABLE Observation (
  variable_measured STRING(1024) NOT NULL,
  observation_about STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  observation_period STRING(1024),
  measurement_method STRING(1024),
  unit STRING(1024),
  scaling_factor STRING(1024),
  observations org.datacommons.Observations,
  import_name STRING(1024),
  provenance_url STRING(1024),
  is_dc_aggregate BOOL,
) PRIMARY KEY(variable_measured, observation_about, facet_id)

CREATE TABLE ImportStatus ( 
  ImportName STRING(MAX) NOT NULL,
  LatestVersion STRING(MAX),
  State STRING(1024) NOT NULL,
  JobId STRING(1024),
  WorkflowId STRING(1024),
  ExecutionTime INT64,
  DataImportTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  StatusUpdateTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
  NextRefreshDate DATE,
) PRIMARY KEY(ImportName)

CREATE TABLE IngestionHistory ( 
  CompletionTimestamp TIMESTAMP NOT NULL OPTIONS ( allow_commit_timestamp = TRUE ),
  WorkflowExecutionID STRING(1024) NOT NULL,
  IngestedImports ARRAY<STRING(MAX)>,
) PRIMARY KEY(CompletionTimestamp DESC)

CREATE TABLE IngestionLock ( 
  LockID STRING(1024) NOT NULL,
  LockOwner STRING(1024),
  AcquiredTimestamp TIMESTAMP OPTIONS ( allow_commit_timestamp = TRUE ),
) PRIMARY KEY(LockID)