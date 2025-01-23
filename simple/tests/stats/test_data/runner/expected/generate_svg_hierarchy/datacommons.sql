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
INSERT INTO "key_value_store" VALUES('StatVarGroups','H4sIAAAAAAAC/32RT0vDQBDFKUiwjwp1UWs2KEvBo5RG8CIepFJPLVKleivbdKyBJBs3W7+EB7+yCdFG88fLzryZYXfeb3GFnjdYD+5JJyoaqU1k5qSN78mACXSnm3BJWqgXEZOKA+IdoBjHDLuZmill2Bjd0SYxKhRzqX25DCjhPRwW0zdr+rma242v4rOF46K3uKNoRfp8TKEMiCVw8rp48s2ryJviWny3H9FOVEiLd6mH/OAhTbfLiKGYypD69VWnXL2lxNN+bPzU5WWDC3aCvVQ20rn4rdgZrDzjDvYrBpmVR9hor7ZMOxwFYXy0cFQlM8mMv8Gu5TIpUXHLVNxaKm4tFfcPlbDOxTNYdQ/e/+dHrTzy00ZvO9n5BeXclHGoAgAA');
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
INSERT INTO "triples" VALUES('some_var1','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('some_var1','measuredProperty','count','');
INSERT INTO "triples" VALUES('some_var1','name','','Some Variable 1 Name');
INSERT INTO "triples" VALUES('some_var1','description','','Some Variable 1 Description');
INSERT INTO "triples" VALUES('some_var1','populationType','Person','');
INSERT INTO "triples" VALUES('some_var1','gender','Female','');
INSERT INTO "triples" VALUES('some_var2','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('some_var2','measuredProperty','age','');
INSERT INTO "triples" VALUES('some_var2','name','','Some Variable 2 Name');
INSERT INTO "triples" VALUES('some_var2','description','','Some Variable 2 Description');
INSERT INTO "triples" VALUES('some_var2','populationType','Person','');
INSERT INTO "triples" VALUES('some_var2','gender','Male','');
INSERT INTO "triples" VALUES('PersonCountVertical','typeOf','Thing','');
INSERT INTO "triples" VALUES('PersonCountVertical','name','','Number of people');
INSERT INTO "triples" VALUES('PersonAgeVertical','typeOf','Thing','');
INSERT INTO "triples" VALUES('PersonAgeVertical','name','','Age of people');
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
INSERT INTO "triples" VALUES('c/g/Root','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Root','name','','Custom Variables');
INSERT INTO "triples" VALUES('c/g/Root','specializationOf','dc/g/Root','');
INSERT INTO "triples" VALUES('c/g/PersonAgeVertical','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/PersonAgeVertical','name','','Age of people');
INSERT INTO "triples" VALUES('c/g/PersonAgeVertical','specializationOf','c/g/Root','');
INSERT INTO "triples" VALUES('c/g/PersonCountVertical','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/PersonCountVertical','name','','Number of people');
INSERT INTO "triples" VALUES('c/g/PersonCountVertical','specializationOf','c/g/Root','');
INSERT INTO "triples" VALUES('c/g/Person','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person','name','','Person');
INSERT INTO "triples" VALUES('c/g/Person','specializationOf','c/g/PersonCountVertical','');
INSERT INTO "triples" VALUES('c/g/Person','specializationOf','c/g/PersonAgeVertical','');
INSERT INTO "triples" VALUES('c/g/Person_Gender','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_Gender','name','','Person With Gender');
INSERT INTO "triples" VALUES('c/g/Person_Gender','specializationOf','c/g/Person','');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Female','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Female','name','','Person With Gender = Female');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Female','specializationOf','c/g/Person_Gender','');
INSERT INTO "triples" VALUES('some_var1','memberOf','c/g/Person_Gender-Female','');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Male','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Male','name','','Person With Gender = Male');
INSERT INTO "triples" VALUES('c/g/Person_Gender-Male','specializationOf','c/g/Person_Gender','');
INSERT INTO "triples" VALUES('some_var2','memberOf','c/g/Person_Gender-Male','');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
COMMIT;
