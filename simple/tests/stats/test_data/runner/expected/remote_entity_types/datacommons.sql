BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2025-01-23 00:00:00','SUCCESS','{"numVars": 2, "numObs": 8}');
CREATE TABLE key_value_store (
    lookup_key varchar(255),
    value longtext
);
INSERT INTO "key_value_store" VALUES('StatVarGroups','H4sIAAAAAAAC/+OS5OJMSdZP1w/Kzy8R4pHi4uKA8bjcEGwhKy4B59LikvxchbDEoszEpJzUYiEhLpayxCJDKTCpBCahYkZgMSOwmBEAlEss8mMAAAA=');
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
INSERT INTO "observations" VALUES('country/FAKE1','var1','2024','1','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/FAKE2','var1','2024','3','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/FAKE1','var2','2024','2','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/FAKE2','var2','2024','4','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/FAKE3','var1','2024','5','c/p/default','','','','','');
INSERT INTO "observations" VALUES('country/FAKE3','var2','2024','6','c/p/default','','','','','');
INSERT INTO "observations" VALUES('country/FAKE4','var1','2024','7','c/p/default','','','','','');
INSERT INTO "observations" VALUES('country/FAKE4','var2','2024','8','c/p/default','','','','','');
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('c/s/default','typeOf','Source','');
INSERT INTO "triples" VALUES('c/s/default','name','','Custom Data Commons');
INSERT INTO "triples" VALUES('c/s/1','typeOf','Source','');
INSERT INTO "triples" VALUES('c/s/1','name','','Source1 Name');
INSERT INTO "triples" VALUES('c/s/1','url','','http://source1.com');
INSERT INTO "triples" VALUES('c/s/1','domain','','source1.com');
INSERT INTO "triples" VALUES('c/p/default','typeOf','Provenance','');
INSERT INTO "triples" VALUES('c/p/default','name','','Custom Import');
INSERT INTO "triples" VALUES('c/p/default','source','c/s/default','');
INSERT INTO "triples" VALUES('c/p/default','url','','custom-import');
INSERT INTO "triples" VALUES('c/p/1','typeOf','Provenance','');
INSERT INTO "triples" VALUES('c/p/1','name','','Provenance1 Name');
INSERT INTO "triples" VALUES('c/p/1','source','c/s/1','');
INSERT INTO "triples" VALUES('c/p/1','url','','http://source1.com/provenance1');
INSERT INTO "triples" VALUES('c/g/Root','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Root','name','','Custom Variables');
INSERT INTO "triples" VALUES('c/g/Root','specializationOf','dc/g/Root','');
INSERT INTO "triples" VALUES('var1','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('var1','name','','var1');
INSERT INTO "triples" VALUES('var1','memberOf','c/g/Root','');
INSERT INTO "triples" VALUES('var1','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('var1','includedIn','c/s/1','');
INSERT INTO "triples" VALUES('var1','populationType','Thing','');
INSERT INTO "triples" VALUES('var1','statType','measuredValue','');
INSERT INTO "triples" VALUES('var1','measuredProperty','var1','');
INSERT INTO "triples" VALUES('var2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('var2','name','','var2');
INSERT INTO "triples" VALUES('var2','memberOf','c/g/Root','');
INSERT INTO "triples" VALUES('var2','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('var2','includedIn','c/s/1','');
INSERT INTO "triples" VALUES('var2','populationType','Thing','');
INSERT INTO "triples" VALUES('var2','statType','measuredValue','');
INSERT INTO "triples" VALUES('var2','measuredProperty','var2','');
INSERT INTO "triples" VALUES('country/FAKE1','typeOf','FakeType1','');
INSERT INTO "triples" VALUES('country/FAKE2','typeOf','FakeType2','');
INSERT INTO "triples" VALUES('country/FAKE3','typeOf','FakeType2','');
INSERT INTO "triples" VALUES('country/FAKE4','typeOf','FakeType1','');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
COMMIT;
