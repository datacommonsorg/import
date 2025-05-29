BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2025-01-23 00:00:00','SUCCESS','{"numVars": 2, "numObs": 4}');
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
INSERT INTO "observations" VALUES('country/IND','var1','2020','0.16','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/IND','var2','2020','53','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/CHN','var1','2020','0.23','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/CHN','var2','2020','67','c/p/1','','','','','');
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('var1','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('var1','name','','Variable1 Name');
INSERT INTO "triples" VALUES('var1','description','','Variable1 Description');
INSERT INTO "triples" VALUES('var2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('var2','name','','Variable2 Name');
INSERT INTO "triples" VALUES('var2','description','','Variable2 Description');
INSERT INTO "triples" VALUES('var2','searchDescription','','Variable2 Search Description1');
INSERT INTO "triples" VALUES('var2','searchDescription','','Variable2 Search Description2');
INSERT INTO "triples" VALUES('topic1','typeOf','Topic','');
INSERT INTO "triples" VALUES('topic1','name','','Topic1 Name');
INSERT INTO "triples" VALUES('topic1','relevantVariable','var1','');
INSERT INTO "triples" VALUES('topic2','typeOf','Topic','');
INSERT INTO "triples" VALUES('topic2','name','','Topic2 Name');
INSERT INTO "triples" VALUES('topic2','searchDescription','','Topic2 Search Description1');
INSERT INTO "triples" VALUES('topic2','searchDescription','','Topic2 Search Description2');
INSERT INTO "triples" VALUES('topic2','relevantVariableList','','var1, var2, svpg1');
INSERT INTO "triples" VALUES('svpg1','typeOf','StatVarPeerGroup','');
INSERT INTO "triples" VALUES('svpg1','name','','SVPG1 Name');
INSERT INTO "triples" VALUES('svpg1','member','var1','');
INSERT INTO "triples" VALUES('svpg1','member','var2','');
INSERT INTO "triples" VALUES('svpg1','memberList','','var3, var4');
INSERT INTO "triples" VALUES('c/s/default','typeOf','Source','');
INSERT INTO "triples" VALUES('c/s/default','name','','Custom Data Commons');
INSERT INTO "triples" VALUES('c/s/1','typeOf','Source','');
INSERT INTO "triples" VALUES('c/s/1','name','','Source1');
INSERT INTO "triples" VALUES('c/s/1','url','','http://source1.com');
INSERT INTO "triples" VALUES('c/s/1','domain','','source1.com');
INSERT INTO "triples" VALUES('c/p/default','typeOf','Provenance','');
INSERT INTO "triples" VALUES('c/p/default','name','','Custom Import');
INSERT INTO "triples" VALUES('c/p/default','source','c/s/default','');
INSERT INTO "triples" VALUES('c/p/default','url','','custom-import');
INSERT INTO "triples" VALUES('c/p/1','typeOf','Provenance','');
INSERT INTO "triples" VALUES('c/p/1','name','','Provenance1');
INSERT INTO "triples" VALUES('c/p/1','source','c/s/1','');
INSERT INTO "triples" VALUES('c/p/1','url','','http://source1.com/provenance1');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
CREATE INDEX observations_variable on observations (variable);
COMMIT;
