
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
  observation_about STRING(1024) NOT NULL,
  variable_measured STRING(1024) NOT NULL,
  facet_id STRING(1024) NOT NULL,
  observation_period STRING(1024),
  measurement_method STRING(1024),
  unit STRING(1024),
  scaling_factor STRING(1024),
  observations BYTES(MAX), -- changed to bytes for emulator support
  import_name STRING(1024),
  provenance_url STRING(1024),
  is_dc_aggregate BOOL,
) PRIMARY KEY(observation_about, variable_measured, facet_id)