BEGIN TRANSACTION;
CREATE TABLE imports (
    imported_at datetime,
    status varchar(16),
    metadata text
);
INSERT INTO "imports" VALUES('2022-02-02 00:00:00','SUCCESS','{"numVars": 1, "numObs": 3}');
INSERT INTO "imports" VALUES('2025-01-23 00:00:00','SUCCESS','{"numVars": 2, "numObs": 30}');
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
    provenance varchar(255)
, unit varchar(255), scaling_factor varchar(255), measurement_method varchar(255), observation_period varchar(255), properties text);
INSERT INTO "observations" VALUES('country/AFG','var1','2023','0.19','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/YEM','var1','2023','0.21','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AGO','var1','2023','0.29','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ZMB','var1','2023','0.31','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ZWE','var1','2023','0.37','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ALB','var1','2023','0.5','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('wikidataId/Q22062741','var1','2023','0.5','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/DZA','var1','2023','0.52','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AND','var1','2023','0.76','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AFG','var2','2023','6','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/YEM','var2','2023','56','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AGO','var2','2023','6','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ZMB','var2','2023','34','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ZWE','var2','2023','76','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ALB','var2','2023','34','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('wikidataId/Q22062741','var2','2023','97','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/DZA','var2','2023','92','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AND','var2','2023','9','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ASM','var2','2023','34','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/AIA','var2','2023','42','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/WLF','var2','2023','75','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/ESH','var2','2023','65','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/IND','var1','2020','0.16','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/IND','var2','2020','53','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/CHN','var1','2020','0.23','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/CHN','var2','2020','67','c/p/1','','','','','');
INSERT INTO "observations" VALUES('country/USA','var1','2021','555','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/IND','var1','2022','321','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/USA','var2','2021','666','c/p/1','','','','','{}');
INSERT INTO "observations" VALUES('country/IND','var2','2022','123','c/p/1','','','','','{}');
CREATE TABLE triples (
    subject_id varchar(255),
    predicate varchar(255),
    object_id varchar(255),
    object_value TEXT
);
INSERT INTO "triples" VALUES('article1','typeOf','Article','');
INSERT INTO "triples" VALUES('article1','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('article1','article_id','','article1');
INSERT INTO "triples" VALUES('article1','article_title','','Article 1');
INSERT INTO "triples" VALUES('article1','article_author','author1','');
INSERT INTO "triples" VALUES('article2','typeOf','Article','');
INSERT INTO "triples" VALUES('article2','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('article2','article_id','','article2');
INSERT INTO "triples" VALUES('article2','article_title','','Article 2');
INSERT INTO "triples" VALUES('article2','article_author','author1','');
INSERT INTO "triples" VALUES('article2','article_author','author2','');
INSERT INTO "triples" VALUES('article3','typeOf','Article','');
INSERT INTO "triples" VALUES('article3','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('article3','article_id','','article3');
INSERT INTO "triples" VALUES('article3','article_title','','Article 3');
INSERT INTO "triples" VALUES('article3','article_author','author2','');
INSERT INTO "triples" VALUES('article3','article_author','author3','');
INSERT INTO "triples" VALUES('author1','typeOf','Author','');
INSERT INTO "triples" VALUES('author1','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('author1','author_id','','author1');
INSERT INTO "triples" VALUES('author1','author_name','','Jane Doe');
INSERT INTO "triples" VALUES('author1','author_country','country/USA','');
INSERT INTO "triples" VALUES('author2','typeOf','Author','');
INSERT INTO "triples" VALUES('author2','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('author2','author_id','','author2');
INSERT INTO "triples" VALUES('author2','author_name','','Joe Smith');
INSERT INTO "triples" VALUES('author2','author_country','country/CAN','');
INSERT INTO "triples" VALUES('author3','typeOf','Author','');
INSERT INTO "triples" VALUES('author3','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('author3','author_id','','author3');
INSERT INTO "triples" VALUES('author3','author_name','','Jane Smith');
INSERT INTO "triples" VALUES('author3','author_country','country/USA','');
INSERT INTO "triples" VALUES('some_var1','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('some_var1','measuredProperty','value','');
INSERT INTO "triples" VALUES('some_var1','name','','Some Variable 1 Name');
INSERT INTO "triples" VALUES('some_var1','description','','Some Variable 1 Description');
INSERT INTO "triples" VALUES('some_var2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('some_var2','measuredProperty','value','');
INSERT INTO "triples" VALUES('some_var2','name','','Some Variable 2 Name');
INSERT INTO "triples" VALUES('some_var2','description','','Some Variable 2 Description');
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
INSERT INTO "triples" VALUES('Article','typeOf','Class','');
INSERT INTO "triples" VALUES('Article','name','','Article');
INSERT INTO "triples" VALUES('Article','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('Article','includedIn','c/s/1','');
INSERT INTO "triples" VALUES('Author','typeOf','Class','');
INSERT INTO "triples" VALUES('Author','name','','Author');
INSERT INTO "triples" VALUES('Author','includedIn','c/p/1','');
INSERT INTO "triples" VALUES('Author','includedIn','c/s/1','');
INSERT INTO "triples" VALUES('article_id','typeOf','Property','');
INSERT INTO "triples" VALUES('article_id','name','','article_id');
INSERT INTO "triples" VALUES('article_title','typeOf','Property','');
INSERT INTO "triples" VALUES('article_title','name','','article_title');
INSERT INTO "triples" VALUES('article_author','typeOf','Property','');
INSERT INTO "triples" VALUES('article_author','name','','article_author');
INSERT INTO "triples" VALUES('author_id','typeOf','Property','');
INSERT INTO "triples" VALUES('author_id','name','','author_id');
INSERT INTO "triples" VALUES('author_name','typeOf','Property','');
INSERT INTO "triples" VALUES('author_name','name','','author_name');
INSERT INTO "triples" VALUES('author_country','typeOf','Property','');
INSERT INTO "triples" VALUES('author_country','name','','author_country');
INSERT INTO "triples" VALUES('country/AFG','typeOf','Country','');
INSERT INTO "triples" VALUES('country/YEM','typeOf','Country','');
INSERT INTO "triples" VALUES('country/AGO','typeOf','Country','');
INSERT INTO "triples" VALUES('country/ZMB','typeOf','Country','');
INSERT INTO "triples" VALUES('country/ZWE','typeOf','Country','');
INSERT INTO "triples" VALUES('country/ALB','typeOf','Country','');
INSERT INTO "triples" VALUES('wikidataId/Q22062741','typeOf','Country','');
INSERT INTO "triples" VALUES('country/DZA','typeOf','Country','');
INSERT INTO "triples" VALUES('country/AND','typeOf','Country','');
INSERT INTO "triples" VALUES('country/ASM','typeOf','Country','');
INSERT INTO "triples" VALUES('country/AIA','typeOf','Country','');
INSERT INTO "triples" VALUES('country/WLF','typeOf','Country','');
INSERT INTO "triples" VALUES('country/ESH','typeOf','Country','');
INSERT INTO "triples" VALUES('country/USA','typeOf','Country','');
INSERT INTO "triples" VALUES('country/IND','typeOf','Country','');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX observations_variable on observations (variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
COMMIT;
