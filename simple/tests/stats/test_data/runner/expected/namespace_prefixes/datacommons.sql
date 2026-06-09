BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2025-01-23 00:00:00','SUCCESS','{"numVars": 3, "numObs": 7}');
CREATE TABLE key_value_store (
    lookup_key varchar(255),
    value longtext
);
INSERT INTO "key_value_store" VALUES('StatVarGroups','H4sIAAAAAAAC/wMAAAAAAAAAAAA=');
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
INSERT INTO "observations" VALUES('country/FAKE1','sv_female','2019','1.2','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE1','sv_male','2019','13.4','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE1','sv','2019','7.5','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE2','sv_female','2016','1.8','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE2','sv_male','2016','14.3','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE3','sv_female','2018','4.5','observations.csv','','','','','');
INSERT INTO "observations" VALUES('country/FAKE3','sv_male','2018','35.7','observations.csv','','','','','');
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('observations.csv','typeOf','Provenance','');
INSERT INTO "triples" VALUES('observations.csv','name','','observations.csv');
INSERT INTO "triples" VALUES('observations.csv','source','','');
INSERT INTO "triples" VALUES('country/FAKE1','typeOf','FakeType1','');
INSERT INTO "triples" VALUES('country/FAKE2','typeOf','FakeType2','');
INSERT INTO "triples" VALUES('country/FAKE3','typeOf','FakeType2','');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
CREATE INDEX observations_variable on observations (variable);
COMMIT;
