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
INSERT INTO "key_value_store" VALUES('StatVarGroups','H4sIAAAAAAAC/+1YzWsTQRRnkxjso0Id1NopyphLEYylkbYH8RASqoF+0YZW8BAmm5cP2ezE3U1Lzx4rSFEE8Q/wIOJZEL140IPQ/6A3BdG74EFns5vmo0mzKRWS2Mvuzsybnd+8r3m/gZswqk7mJpfRMIUeE2XdWkPDKqhcIwxGFsvFNBpMZFkJRUlDOgxQE4cVOG23VoSwyByMxMqmJYpsjRsFntbQpKNwviYdzWH113Ss7aqw7YeJ2lhq2RAlObS1XrDy0RXkmrY1L/TcGtfKSPZ8EHbkmD3OqsJOK8oceWZPYJUZ9LUPXigefx++gwYmzOhhMkmRRNOK8bJZ0HPR1XL6PqpWIr60gUZWE5tL2QVhYDLP9cj0dCzPDa5acuH1POrzgmfknIRuiWQe49ziaW4ieaTYq7KEKeF33A5LCmavz1wAUsaFwBIszqoo2FKW2TiYC4TVkDAbCnOxMBuMFEJWhQOPFbhYp67bqGfQCM9hkWtITBiv174zyG4xdzgJQ6YoYmqDG1P03Kr83HcNNsUWeRFDrXvHm3vjaKpGoWQVpM/tBnrcgm/88E7pyiulynrJ6PSnAh/qVRzVhZVH4zAt9p4ZrrqoOysUZtqkKXIJzshm2/T3KTgAenp4Cr41uKtnvV3rP89+5oc/XVqsapSOgsdtmWN2lbCK6kwmE5klO7UjxqOl//058zx4YhjyNAC/fUcKxfo46xWb9l96IE8UCFVLltSmhJHS5OqpkotMdksYJp1orE8qeCsw97dQwWuGPAs2VTw3GiqeV3643l1okF8+mD6SI9H3PnjZL4HYk5nsa6CPFPjWD5+Vgck49LsCHwfiFCFXOlete0p9MUp2FQg6n/Re1/nCe7FMx+HsAUJIgs6bznvn7h52OAZDmf37hWEKtdsG2FbgwkFeumDTzgcw1pKVLjRx0kgzJ4205KSRlpw00pChvwQHxe8kG/ih/E8lCN05oQSzvXbrVGyVY+4CORjXNHTI/VTQedPLbXNFwH7+BZFEBAkEFQAA');
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
INSERT INTO "triples" VALUES('some_var_with_long_property_values','typeOf','StatisticalVariable','');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','measuredProperty','count','');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','name','','Some Variable With Long Property Values');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','description','','Some Variable 3 Description');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','populationType','Person','');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','propertyWithAReallyLongValue','HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','anotherPropertyWithAReallyLongValue','HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','');
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
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue','name','','Person With Another Property With A Really Long Value');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue','specializationOf','c/g/Person','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','name','','Person With Another Property With A Really Long Value = Here Is Aother Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','specializationOf','c/g/Person_AnotherPropertyWithAReallyLongValue','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue','name','','Person With Another Property With A Really Long Value = Here Is Aother Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database, Property With A Really Long Value');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue','specializationOf','c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingA-cec6dd27','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingA-cec6dd27','name','','Person With Another Property With A Really Long Value = Here Is Aother Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database, Property With A Really Long Value = Here Is A Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingA-cec6dd27','specializationOf','c/g/Person_AnotherPropertyWithAReallyLongValue_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingA-cec6dd27','specializationOf','c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue','');
INSERT INTO "triples" VALUES('some_var_with_long_property_values','memberOf','c/g/Person_AnotherPropertyWithAReallyLongValue-HereIsAotherPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingA-cec6dd27','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','name','','Person With Another Property With A Really Long Value, Property With A Really Long Value = Here Is A Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database');
INSERT INTO "triples" VALUES('c/g/Person_AnotherPropertyWithAReallyLongValue_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','specializationOf','c/g/Person_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','');
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
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue','name','','Person With Property With A Really Long Value');
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue','specializationOf','c/g/Person','');
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','typeOf','StatVarGroup','');
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','name','','Person With Property With A Really Long Value = Here Is A Property With A Really Long Value To Test Causing A Subject I D Overflow Of More Than255 Characters When Loading Into The Database');
INSERT INTO "triples" VALUES('c/g/Person_PropertyWithAReallyLongValue-HereIsAPropertyWithAReallyLongValueToTestCausingASubjectIDOverflowOfMoreThan255CharactersWhenLoadingIntoTheDatabase','specializationOf','c/g/Person_PropertyWithAReallyLongValue','');
CREATE INDEX observations_entity_variable on observations (entity, variable);
CREATE INDEX triples_subject_id on triples (subject_id);
CREATE INDEX triples_subject_id_predicate on triples (subject_id, predicate);
CREATE INDEX observations_variable on observations (variable);
COMMIT;
