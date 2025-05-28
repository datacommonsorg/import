BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
CREATE TABLE key_value_store (
    lookup_key varchar(255),
    value longtext
);
CREATE TABLE observations (
    entity varchar(255),
    variable varchar(255),
    date varchar(255),
    value varchar(255),
    provenance varchar(255),
    unit varchar(255),
    scaling_factor varchar(255),
    measurement_method varchar(255),
    observation_period varchar(255),
    properties TEXT
);
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
CREATE INDEX observations_variable on observations (variable);
COMMIT;
