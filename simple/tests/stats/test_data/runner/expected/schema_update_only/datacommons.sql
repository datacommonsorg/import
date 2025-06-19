BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2022-02-02 00:00:00','SUCCESS','{"numVars": 1, "numObs": 3}');
CREATE TABLE key_value_store (
    lookup_key varchar(255),
    value longtext
);
INSERT INTO "key_value_store" VALUES('k1','v1');
CREATE TABLE observations (
    entity varchar(255),
    variable varchar(255),
    date varchar(255),
    value varchar(255),
    provenance varchar(255)
, unit varchar(255), scaling_factor varchar(255), measurement_method varchar(255), observation_period varchar(255), properties text);
INSERT INTO "observations" VALUES('e1','v1','2023','123','p1',NULL,NULL,NULL,NULL,NULL);
INSERT INTO "observations" VALUES('e2','v1','2023','456','p1',NULL,NULL,NULL,NULL,NULL);
INSERT INTO "observations" VALUES('e3','v1','2023','789','p1',NULL,NULL,NULL,NULL,NULL);
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('sub1','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('sub1','pred1','','objval1');
INSERT INTO "triples" VALUES('sub1','name','','name1');
INSERT INTO "triples" VALUES('sub2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('sub2','name','','name2');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX observations_variable on observations (variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
COMMIT;
