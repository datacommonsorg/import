BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2021-03-03 00:00:00','SUCCESS','{"numVars": 2, "numObs": 2}');
CREATE TABLE key_value_store (
    lookup_key varchar(255),
    value longtext
);
INSERT INTO "key_value_store" VALUES('k0','v0');
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
INSERT INTO "observations" VALUES('e4','v4','2022','654','p4','','','','','');
INSERT INTO "observations" VALUES('e3','v1','2023','789','p1','USD','','','','{"prop1": "val1"}');
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('sub3','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('sub3','pred3','','objval3');
INSERT INTO "triples" VALUES('sub3','name','','name3');
INSERT INTO "triples" VALUES('sub2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('sub2','name','','name2');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX observations_variable_value on observations (variable, value);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
COMMIT;
