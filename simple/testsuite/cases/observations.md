# Observations (variable-per-row CSV)

Each case is a CSV (plus, sometimes, config or MCF) and the exact observation
records and nodes it produces. Defaults: a provenance with a source, and a
config that maps the columns `entity, variable, date, value` of `case.csv`.
Proxy-entity lookups are off unless a case turns them on.

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
    {
      "pattern": "case.csv",
      "provenance": "dcid:TestProvenance",
      "columnMappings": {
        "dcid:observationAbout": "entity",
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value"
      }
    }
  ]
}
```

## basic rows

```csv
entity,variable,date,value
country/USA,Count_Person,2020,331000000
country/FRA,Median_Age_Person,2020,42.3
```

```expect
[
  {
    "@id": "dcid:obs_1bbdcd98141357655fd5ae85d9b536c41e30fce1268c3ca25882c63f105a47e9",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/FRA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 42.3,
    "dcid:variableMeasured": {"@id": "dcid:Median_Age_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_484468857564d12392fb9a05ec271fb3a2972c18938c44f2262cb2b571e03809",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 331000000,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## physical column names are mapped

```csv
place,stat,year,amount
country/USA,Count_Person,2021,332000000
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "place", "dcid:variableMeasured": "stat",
                      "dcid:observationDate": "year", "dcid:value": "amount"}}]}
```

```expect
[
  {
    "@id": "dcid:obs_6b27388803ef25183dff045c35d92e556170fb707e4fa78291659da60c6cd5fb",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2021,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 332000000,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## namespace prefixes in cells are normalized

```csv
entity,variable,date,value
dcid:country/USA,dcs:Count_Person,2020,1
dcs:country/FRA,dcid:Count_Person,2020,2
```

```expect
[
  "$obs_FRA_Count_Person_2020_value_2",
  "$obs_USA_Count_Person_2020_value_1"
]
```

## numeric value spellings

```csv
entity,variable,date,value
country/C1,Count_Person,2020,7
country/C2,Count_Person,2020,-3.5
country/C3,Count_Person,2020,007
country/C4,Count_Person,2020,1e5
country/C5,Count_Person,2020,2.0
```

```expect
[
  {
    "@id": "dcid:obs_14318d384f617bf47f5d9f72b4cf25e62a25aa76cbe1911ea24209456e4fce50",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C3"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 7,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_3927b40489d2c67785c36ba30f5eb5a489c4b72af32044497742fef39d4995f0",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C5"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2.0,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_42b5fd2e28bb740c4996a85c4f27b8fb7a2ea27c5bdc758d87054b8399c0884c",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C2"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": -3.5,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_7a724e7fc29e665f1e07d0d79a6e06e8b03735ab62b80e816a809ed66c3ca9be",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C1"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 7,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_984176b8a8ac7bc6e9691eb03809c3fd04890305eee97972775c2dd9795af26b",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C4"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": "1e5",
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## exponent values are emitted as numbers

> xfail: 1e5 is emitted as the JSON string "1e5" instead of the number 100000

```csv
entity,variable,date,value
country/C4,Count_Person,2020,1e5
```

```expect
[
  {
    "@id": "*",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C4"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 100000,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## non-numeric value fails the run

```csv
entity,variable,date,value
country/USA,Count_Person,2020,lots
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Invalid non-numeric value(s) found in observation values of file 'case.csv': ['lots']. If these represent missing values, please configure them under 'naValues' in config.json."
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## missing-value tokens in the value column drop the row

```csv
entity,variable,date,value
country/C1,Count_Person,2020,
country/C2,Count_Person,2020,NA
country/C3,Count_Person,2020,null
country/C4,Count_Person,2020,-
country/C5,Count_Person,2020,5
```

```expect
[
  {
    "@id": "dcid:obs_3927b40489d2c67785c36ba30f5eb5a489c4b72af32044497742fef39d4995f0",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/C5"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 5,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## NA is a real entity id

> xfail: pandas reads the entity id NA (Namibia) as a missing value, so the observation loses observationAbout

```csv
entity,variable,date,value
NA,Count_Person,2020,2540000
```

```expect
[
  {
    "@id": "*",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:NA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2540000,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## surrounding whitespace in ids is trimmed

```csv
entity,variable,date,value
 country/USA , Count_Person ,2020,1
```

```expect
[
  {
    "@id": "dcid:obs_a60d6f46da9db556d7db436c45cde62d09dfcbd1c934caf71750eb897287ecc2",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## rows with the same key keep both values

Two rows with the same entity, variable and date get the same observation id;
both records are written and the collision is only logged.

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
country/USA,Count_Person,2020,2
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1",
  {
    "@id": "dcid:obs_484468857564d12392fb9a05ec271fb3a2972c18938c44f2262cb2b571e03809",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## optional observation columns

```csv
entity,variable,date,value,unit,scaling,method,period
country/USA,Amount_Debt,2020,12.5,USDollar,100,dcs:CensusACS,P1Y
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value",
                      "dcid:unit": "unit", "dcid:scalingFactor": "scaling",
                      "dcid:measurementMethod": "method", "dcid:observationPeriod": "period"}}]}
```

```expect
[
  {
    "@id": "dcid:obs_7c8e156351b62565d5d63bf47ca785129c41e22a4143dec31b8e35106a36df59",
    "@type": "dcid:StatVarObservation",
    "dcid:measurementMethod": {"@id": "dcid:CensusACS"},
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:observationPeriod": "P1Y",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:scalingFactor": 100,
    "dcid:unit": {"@id": "dcid:USDollar"},
    "dcid:value": 12.5,
    "dcid:variableMeasured": {"@id": "dcid:Amount_Debt", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## dates of different granularity

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
country/USA,Count_Person,2020-06,2
country/USA,Count_Person,2020-06-15,3
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1",
  {
    "@id": "dcid:obs_389407622e21b697f7bfcade334370f0b103fe4da58038937523860fb56c0a40",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "2020-06",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 2,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:obs_3f445522ba32b6b49586107f38779362ab83f0434e5a99baf1adf8d28f7e3f9c",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": "2020-06-15",
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 3,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## custom entity dimension instead of observationAbout

```mcf file=dims.mcf
Node: dcid:destination
typeOf: dcs:Property
```

```csv
origin,destination,variable,date,value
country/USA,country/FRA,Count_Trip,2020,10
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "origin", "dcid:destination": "destination",
                      "dcid:variableMeasured": "variable", "dcid:observationDate": "date",
                      "dcid:value": "value"}}]}
```

```expect
[
  {"@id": "dcid:destination", "@type": "dcid:Property"},
  {
    "@id": "dcid:obs_704eac7361de795f54f4e363af4af795c8e1aa3ecf725a223b86658103453f27",
    "@type": "dcid:StatVarObservation",
    "dcid:destination": {"@id": "dcid:country/FRA"},
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 10,
    "dcid:variableMeasured": {"@id": "dcid:Count_Trip", "dcid:observationProperties": ["dcid:observationAbout", "dcid:destination"]}
  }
]
```

## ignored columns are dropped

```csv
entity,variable,date,value,notes
country/USA,Count_Person,2020,1,anything
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance", "ignoreColumns": ["notes"],
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1"
]
```

## quoted fields with commas and newlines

```csv
entity,variable,date,value,notes
country/USA,Count_Person,2020,1,"a, b"
country/FRA,Count_Person,2020,2,"line one
line ""two"""
country/DEU,Count_Person,2020,3,plain
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance", "ignoreColumns": ["notes"],
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect
[
  "$obs_FRA_Count_Person_2020_value_2",
  "$obs_USA_Count_Person_2020_value_1",
  {
    "@id": "dcid:obs_11c60a2580c0ae6e1e00b41ee0f2eecc33b697fbe0da435147462b7fec2e4397",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/DEU"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 3,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## unmapped column fails header validation

```csv
entity,variable,date,value,notes
country/USA,Count_Person,2020,1,anything
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "CSV Header Validation Failed! The following errors were found:"
  ],
  "handshake_error_types": [
    "UNMAPPED_COLUMNS"
  ]
}
```

## missing required mapping fails

```csv
entity,variable,value
country/USA,Count_Person,1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "case.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:value": "value"}}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "CSV Header Validation Failed! The following errors were found:"
  ],
  "handshake_error_types": [
    "MISSING_REQUIRED_COLUMNS"
  ]
}
```

## provenance url is copied onto observations

```mcf file=provenance.mcf
Node: dcid:TestSource
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:TestSource
url: "https://source.example.org/dataset"
```

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```expect
[
  "$provenance_TestProvenance_with_url",
  "$provenance_TestProvenance_with_url",
  {
    "@id": "dcid:obs_484468857564d12392fb9a05ec271fb3a2972c18938c44f2262cb2b571e03809",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:provenanceUrl": "https://source.example.org/dataset",
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance",
  "$provenance_TestProvenance"
]
```

## provenance url survives the production (multiprocess) path

> xfail: with use_multiprocessing=True (production), observations lose dcid:provenanceUrl
> modes: multiprocess

```mcf file=provenance.mcf
Node: dcid:TestSource
typeOf: dcs:Source
name: "Test Source"
url: "https://source.example.org"

Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:TestSource
url: "https://source.example.org/dataset"
```

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```expect
[
  {
    "@id": "*",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:country/USA"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:provenanceUrl": "https://source.example.org/dataset",
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  },
  {
    "@id": "dcid:TestProvenance",
    "@type": "dcid:Provenance",
    "dcid:name": "Test Provenance",
    "dcid:source": {"@id": "dcid:TestSource"},
    "dcid:url": "https://source.example.org/dataset"
  },
  {
    "@id": "dcid:TestProvenance",
    "@type": "dcid:Provenance",
    "dcid:name": "Test Provenance",
    "dcid:source": {"@id": "dcid:TestSource"},
    "dcid:url": "https://source.example.org/dataset"
  }
]
```

## custom namespace entity ids become dcid

```csv
entity,variable,date,value
undata:area/001,Count_Person,2020,1
```

```expect
[
  {
    "@id": "dcid:obs_d47f5f4f7f5fb44092b4f59cd71b324881d7c0e064010f7b2ad37235dd719555",
    "@type": "dcid:StatVarObservation",
    "dcid:observationAbout": {"@id": "dcid:area/001"},
    "dcid:observationDate": 2020,
    "dcid:provenance": {"@id": "dcid:TestProvenance"},
    "dcid:value": 1,
    "dcid:variableMeasured": {"@id": "dcid:Count_Person", "dcid:observationProperties": ["dcid:observationAbout"]}
  }
]
```

## byte order mark before the header

```csv file=case.csv
﻿entity,variable,date,value
country/USA,Count_Person,2020,1
country/FRA,Count_Person,2020,2
```

```expect
[
  "$obs_FRA_Count_Person_2020_value_2",
  "$obs_USA_Count_Person_2020_value_1"
]
```

