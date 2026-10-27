# Events (importType: events)

Each row of an events CSV becomes an event node, and computed variables
aggregate the events into observations. Defaults: a provenance with a source
and a crime-events config whose `countryAlpha3Code` column resolves locally
(no DC API call) to `country/<code>`.

```mcf file=provenance.mcf
Node: dcid:TestSource
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:TestSource
```

```config
{
  "inputFiles": [
    {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
    {"pattern": "case.csv", "importType": "events", "eventType": "CrimeEvent",
     "entityType": "Country", "idColumn": "CASE", "provenance": "dcid:TestProvenance",
     "computedVariables": ["Count_CrimeEvent"],
     "columnsToResolve": ["countryAlpha3Code"],
     "columnMappings": {"dcid:location": "countryAlpha3Code",
                        "dcid:observationDate": "DATE OF OCCURRENCE"}}
  ],
  "events": {"CrimeEvent": {"name": "Crime Event", "description": "Crime Event description"}},
  "variables": {"Count_CrimeEvent": {"name": "Number of Crime Events", "aggregation": {"period": "day"}}}
}
```

## events and their daily counts

Note: the provenance is written once more by the node registry with the name
"TestProvenance" (its id), next to the MCF's "Test Provenance", so the loader
sees two names.

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE,PRIMARY DESCRIPTION
USA,11/08/2023 8:50:00 PM,JG497095,THEFT
BRA,11/08/2023 3:14:00 PM,JG496991,ASSAULT
USA,11/08/2023 8:25:00 AM,JG498494,ASSAULT
USA,09/17/2023 3:00:00 AM,JG427641,THEFT
```

```expect
[
  "$class_CrimeEvent",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$proxy_country_USA",
  "$statvar_Count_CrimeEvent",
  "$svg_custom_root",
  {
    "@id": "dcid:JG427641",
    "@type": "dcid:CrimeEvent",
    "dcid:PRIMARY_DESCRIPTION": "THEFT",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "09/17/2023 3:00:00 AM"
  },
  {
    "@id": "dcid:JG496991",
    "@type": "dcid:CrimeEvent",
    "dcid:PRIMARY_DESCRIPTION": "ASSAULT",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/BRA"},
    "dcid:observationDate": "11/08/2023 3:14:00 PM"
  },
  {
    "@id": "dcid:JG497095",
    "@type": "dcid:CrimeEvent",
    "dcid:PRIMARY_DESCRIPTION": "THEFT",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  },
  {
    "@id": "dcid:JG498494",
    "@type": "dcid:CrimeEvent",
    "dcid:PRIMARY_DESCRIPTION": "ASSAULT",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:25:00 AM"
  },
  {"@id": "dcid:PRIMARY_DESCRIPTION", "@type": "dcid:Property", "dcid:name": "PRIMARY DESCRIPTION"},
  {"@id": "dcid:country/BRA", "@type": "dcid:Country"},
  {
    "@id": "dcid:obs_b4994e5d7bf71d1f525e50d83e6629f68f7c0fd1a75b6f76e46b9a4f1314104c",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "2023-11-08",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  },
  {
    "@id": "dcid:obs_f7b8aa23b0efba1032697f9440f6c0f202b84ed67539ca64a7aae05aebd6ace1",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/BRA"},
    "dcid:observationDate": "2023-11-08",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  },
  {
    "@id": "dcid:obs_fa0a5910156a287c9db7bbc98d05db903b92b7de28411e5090dec20f866553aa",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "2023-09-17",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```

## yearly aggregation by default

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE
USA,11/08/2023 8:50:00 PM,JG1
USA,01/02/2024 1:00:00 AM,JG2
USA,03/04/2024 1:00:00 AM,JG3
```

```config
{"variables": {"Count_CrimeEvent": {"aggregation": null}}}
```

```expect
[
  "$class_CrimeEvent",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$proxy_country_USA",
  "$statvar_Count_CrimeEvent",
  "$svg_custom_root",
  {
    "@id": "dcid:JG1",
    "@type": "dcid:CrimeEvent",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  },
  {
    "@id": "dcid:JG2",
    "@type": "dcid:CrimeEvent",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "01/02/2024 1:00:00 AM"
  },
  {
    "@id": "dcid:JG3",
    "@type": "dcid:CrimeEvent",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "03/04/2024 1:00:00 AM"
  },
  {
    "@id": "dcid:obs_b8cfabb2b2beabc5d2e7757076ab8b7e578b3318b39426f19b215038bce6468e",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2023,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  },
  {
    "@id": "dcid:obs_bc5f1111d05da6c29bf34a3f498955504718ab3671b5731051b4caae8d6f148d",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2024,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```

## without an id column, event ids are generated

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE
USA,11/08/2023 8:50:00 PM,JG1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "importType": "events", "eventType": "CrimeEvent",
   "entityType": "Country", "provenance": "dcid:TestProvenance",
   "computedVariables": ["Count_CrimeEvent"],
   "columnsToResolve": ["countryAlpha3Code"],
   "columnMappings": {"dcid:location": "countryAlpha3Code",
                      "dcid:observationDate": "DATE OF OCCURRENCE"}}]}
```

```expect
[
  "$class_CrimeEvent",
  "$obs_USA_Count_CrimeEvent_2023-11-08_value_1",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$proxy_country_USA",
  "$statvar_Count_CrimeEvent",
  "$svg_custom_root",
  {
    "@id": "dcid:CrimeEvent_0",
    "@type": "dcid:CrimeEvent",
    "dcid:CASE": "JG1",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```

## variable metadata and groups

The id/name customizations (customIdNamespace, customSvgPrefix,
defaultCustomRootStatVarGroupName, svHierarchyPropsBlocklist) are slated for
removal and deliberately not tested.

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE
USA,11/08/2023 8:50:00 PM,JG1
```

```config
{
  "variables": {"Count_CrimeEvent": {
    "name": "Number of Crime Events",
    "description": "How many crimes were reported",
    "searchDescriptions": ["crime count", "number of crimes"],
    "group": "Public Safety/Crime",
    "properties": {"populationType": "CrimeEvent", "measuredProperty": "count"}
  }},
  "groupStatVarsByProperty": true
}
```

```expect
[
  "$class_CrimeEvent",
  "$obs_USA_Count_CrimeEvent_2023-11-08_value_1",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$proxy_country_USA",
  "$svg_custom_root",
  {
    "@id": "dcid:Count_CrimeEvent",
    "@type": "dcid:StatisticalVariable",
    "dcid:description": "How many crimes were reported",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:measuredProperty": {"@id": "dcs:count"},
    "dcid:memberOf": {"@id": "dcid:custom/g/group_2"},
    "dcid:name": "Number of Crime Events",
    "dcid:populationType": {"@id": "dcs:CrimeEvent"},
    "dcid:searchDescription": ["crime count", "number of crimes"],
    "dcid:statType": {"@id": "dcs:measuredValue"}
  },
  {
    "@id": "dcid:JG1",
    "@type": "dcid:CrimeEvent",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  },
  {
    "@id": "dcid:custom/g/group_1",
    "@type": "dcid:StatVarGroup",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:name": "Public Safety",
    "dcid:specializationOf": {"@id": "dcid:dc/g/Root"}
  },
  {
    "@id": "dcid:custom/g/group_2",
    "@type": "dcid:StatVarGroup",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:name": "Crime",
    "dcid:specializationOf": {"@id": "dcid:custom/g/group_1"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```

## unknown country code is not validated

The made-up alpha-3 code XXX becomes `country/XXX`, typed as a Country. That
is intended: codes are not validated.

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE
XXX,11/08/2023 8:50:00 PM,JG1
```

```expect
[
  "$class_CrimeEvent",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$statvar_Count_CrimeEvent",
  "$svg_custom_root",
  {
    "@id": "dcid:JG1",
    "@type": "dcid:CrimeEvent",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/XXX"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  },
  {"@id": "dcid:country/XXX", "@type": "dcid:Country"},
  {
    "@id": "dcid:obs_89a3a1a50b8c6acfb22b0aa5588aaa0992f7636b0afe27d32c44f904062f76cb",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/XXX"},
    "dcid:observationDate": "2023-11-08",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_CrimeEvent"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```

## variables in two top-level groups, and non-ASCII column names

```csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE,Catégorie
USA,11/08/2023 8:50:00 PM,JG1,vol
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "importType": "events", "eventType": "CrimeEvent",
   "entityType": "Country", "idColumn": "CASE", "provenance": "dcid:TestProvenance",
   "computedVariables": ["Count_CrimeEvent", "Count_OtherEvent"],
   "columnsToResolve": ["countryAlpha3Code"],
   "columnMappings": {"dcid:location": "countryAlpha3Code",
                      "dcid:observationDate": "DATE OF OCCURRENCE"}}],
 "variables": {"Count_CrimeEvent": {"name": "Crimes", "group": "Safety"},
               "Count_OtherEvent": {"name": "Other", "group": "Economy"}}}
```

```expect
[
  "$class_CrimeEvent",
  "$obs_USA_Count_CrimeEvent_2023-11-08_value_1",
  "$property_CASE",
  "$provenance_TestProvenance_registry_copy",
  "$proxy_country_USA",
  {
    "@id": "dcid:Count_CrimeEvent",
    "@type": "dcid:StatisticalVariable",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:measuredProperty": {"@id": "dcs:Count_CrimeEvent"},
    "dcid:memberOf": {"@id": "dcid:custom/g/group_1"},
    "dcid:name": "Crimes",
    "dcid:populationType": {"@id": "schema:Thing"},
    "dcid:statType": {"@id": "dcs:measuredValue"}
  },
  {
    "@id": "dcid:Count_OtherEvent",
    "@type": "dcid:StatisticalVariable",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:measuredProperty": {"@id": "dcs:Count_OtherEvent"},
    "dcid:memberOf": {"@id": "dcid:custom/g/group_2"},
    "dcid:name": "Other",
    "dcid:populationType": {"@id": "schema:Thing"},
    "dcid:statType": {"@id": "dcs:measuredValue"}
  },
  {
    "@id": "dcid:JG1",
    "@type": "dcid:CrimeEvent",
    "dcid:c/prop/1": "vol",
    "dcid:includedIn": {"@id": "dcid:TestProvenance"},
    "dcid:location": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "11/08/2023 8:50:00 PM"
  },
  {"@id": "dcid:c/prop/1", "@type": "dcid:Property", "dcid:name": "Catégorie"},
  {
    "@id": "dcid:custom/g/group_1",
    "@type": "dcid:StatVarGroup",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:name": "Safety",
    "dcid:specializationOf": {"@id": "dcid:dc/g/Root"}
  },
  {
    "@id": "dcid:custom/g/group_2",
    "@type": "dcid:StatVarGroup",
    "dcid:includedIn": [{"@id": "dcid:TestProvenance"}, {"@id": "dcid:TestSource"}],
    "dcid:name": "Economy",
    "dcid:specializationOf": {"@id": "dcid:dc/g/Root"}
  },
  {
    "@id": "dcid:obs_e148fc6185b03c8e2e113fcb0dbe17c73d772fbdf4918885b8eac22a4e31352f",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2023,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_OtherEvent"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance"
]
```
