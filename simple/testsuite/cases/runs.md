# Config, input discovery and validation

How config.json selects input files, and how invalid configs and inputs fail.
Defaults: a provenance MCF and a config importing `*.mcf` and `*.csv` with
the standard column mappings. Cases add files (paths may include
directories) or override the config; a ```config``` block of `null` removes
config.json entirely.

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
      "pattern": "*.csv",
      "provenance": "dcid:TestProvenance",
      "columnMappings": {
        "dcid:observationAbout": "entity",
        "dcid:variableMeasured": "variable",
        "dcid:observationDate": "date",
        "dcid:value": "value"
      }
    }
  ],
  "importProxyEntities": false
}
```

## files in subdirectories are skipped by default

```csv file=top.csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```csv file=nested/deeper.csv
entity,variable,date,value
country/FRA,Count_Person,2020,2
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1"
]
```

## includeInputSubdirs picks up nested files

```csv file=top.csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```csv file=nested/deeper.csv
entity,variable,date,value
country/FRA,Count_Person,2020,2
```

```config
{"includeInputSubdirs": true,
 "inputFiles": [
   {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
   {"pattern": "**/*.csv", "provenance": "dcid:TestProvenance",
    "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                       "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect
[
  "$obs_FRA_Count_Person_2020_value_2",
  "$obs_USA_Count_Person_2020_value_1"
]
```

## glob patterns select files and the first matching entry wins

`countries_b.csv` matches both entries; the first one (with its column
mappings) applies. `other.txt` matches nothing and is ignored.

```csv file=countries_a.csv
place,variable,date,value
country/USA,Count_Person,2020,1
```

```csv file=countries_b.csv
place,variable,date,value
country/FRA,Count_Person,2020,2
```

```csv file=other.txt
not,a,csv
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "countries_*.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "place", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}},
  {"pattern": "*.csv", "provenance": "dcid:TestProvenance",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect
[
  "$obs_FRA_Count_Person_2020_value_2",
  "$obs_USA_Count_Person_2020_value_1"
]
```

## importProxyEntities accepts the string "false"

```csv
entity,variable,date,value
dcid:country/USA,Count_Person,2020,1
```

```config
{"importProxyEntities": "false"}
```

```dc_api
{"country/USA": {"typeOf": "Country"}}
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1"
]
```

## missing config.json fails

Note: this and the next three config errors are raised while Runner is being
constructed, before run(), so no FAILURE handshake is written and the
ingestion workflow gets no structured error.

```config
null
```

```expect-error
{
  "type": "FileNotFoundError",
  "contains": [
    "File not found: <input>/config.json"
  ]
}
```

## inputFiles given as a map fails

```config
{"inputFiles": {"*.csv": {"provenance": "dcid:TestProvenance"}}}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "'inputFiles' must be a list of objects, each with a 'pattern' and a 'provenance'. Got: dict"
  ]
}
```

## inputFiles entry without a pattern fails

```config
{"inputFiles": [{"provenance": "dcid:TestProvenance"}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Invalid entry in 'inputFiles': must specify 'pattern' or 'filename'. Got: {'provenance': 'dcid:TestProvenance'}"
  ]
}
```

## unsupported format fails

```config
{"inputFiles": [{"pattern": "*.csv", "provenance": "dcid:TestProvenance", "format": "variablePerColumn"}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Unsupported format 'variablePerColumn' for '*.csv'. The only supported format is 'variablePerRow', which is also the default."
  ]
}
```

## unsupported importType fails

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "*.csv", "provenance": "dcid:TestProvenance", "importType": "Observations"}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "CSV Header Validation Failed! The following errors were found:"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## inputFiles entry without a provenance fails

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "*.csv",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Could not determine import name: missing 'provenance' configuration for file 'case.csv'."
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## provenance without a namespace prefix fails

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "*.csv", "provenance": "TestProvenance",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Metadata Validation Failed: The 'provenance' property must be a valid DCID or URI (e.g., 'dcid:FrogCensusBureau', 'custom:WHO', or a URL). Found invalid provenance: 'TestProvenance'"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## provenance that no MCF defines fails

Note: the message talks about missing *sources*, and the handshake error type
is GENERIC_ERROR, although the validator has a MISSING_PROVENANCE type.

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "*.csv", "provenance": "dcid:UndefinedProvenance",
   "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                      "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Metadata Validation Failed: Linked sources are missing for defined provenances:"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## provenance without a source fails

```mcf file=provenance.mcf
Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
```

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Metadata Validation Failed: Linked sources are missing for defined provenances:"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## provenance linked to a source that no MCF defines is accepted

```mcf file=provenance.mcf
Node: dcid:TestProvenance
typeOf: dcs:Provenance
name: "Test Provenance"
source: dcid:ElsewhereSource
```

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```expect
[
  "$obs_USA_Count_Person_2020_value_1",
  {
    "@id": "dcid:TestProvenance",
    "@type": "dcid:Provenance",
    "dcid:name": "Test Provenance",
    "dcid:source": {"@id": "dcid:ElsewhereSource"}
  },
  {
    "@id": "dcid:TestProvenance",
    "@type": "dcid:Provenance",
    "dcid:name": "Test Provenance",
    "dcid:source": {"@id": "dcid:ElsewhereSource"}
  }
]
```

```expect-removed
[
  "$provenance_TestProvenance",
  "$provenance_TestProvenance",
  {
    "@id": "dcid:TestSource",
    "@type": "dcid:Source",
    "dcid:domain": "source.example.org",
    "dcid:name": "Test Source",
    "dcid:url": "https://source.example.org"
  },
  {"@id": "dcid:TestSource", "@type": "dcid:Source", "dcid:name": "Test Source", "dcid:url": "https://source.example.org"}
]
```

## invalid aggregation period fails

```csv file=events.csv
countryAlpha3Code,DATE OF OCCURRENCE,CASE
USA,11/08/2023 8:50:00 PM,JG1
```

```config
{"inputFiles": [
  {"pattern": "*.mcf", "provenance": "dcid:TestProvenance"},
  {"pattern": "events.csv", "importType": "events", "eventType": "CrimeEvent",
   "entityType": "Country", "idColumn": "CASE", "provenance": "dcid:TestProvenance",
   "computedVariables": ["Count_CrimeEvent"], "columnsToResolve": ["countryAlpha3Code"],
   "columnMappings": {"dcid:location": "countryAlpha3Code", "dcid:observationDate": "DATE OF OCCURRENCE"}}],
 "variables": {"Count_CrimeEvent": {"aggregation": {"period": "fortnight"}}}}
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "invalid period: fortnight"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## non-numeric values are listed, ten at most

```csv
entity,variable,date,value
country/C01,Count_Person,2020,a1
country/C02,Count_Person,2020,a2
country/C03,Count_Person,2020,a3
country/C04,Count_Person,2020,a4
country/C05,Count_Person,2020,a5
country/C06,Count_Person,2020,a6
country/C07,Count_Person,2020,a7
country/C08,Count_Person,2020,a8
country/C09,Count_Person,2020,a9
country/C10,Count_Person,2020,a10
country/C11,Count_Person,2020,a11
country/C12,Count_Person,2020,a12
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Invalid non-numeric value(s) found in observation values of file 'case.csv': ['a1', 'a2', 'a3', 'a4', 'a5', 'a6', 'a7', 'a8', 'a9', 'a10']. If these represent missing values, please configure them und"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```

## empty config imports nothing but default metadata

With an input directory, an empty config.json is accepted: no file matches,
and only the default provenance and source nodes are written. (When Runner is
driven by a config *file*, an empty config fails: no dataDownloadUrl.)

```config
null
```

```csv file=config.json
{}
```

```expect
[
  {
    "@id": "dcid:c/p/default",
    "@type": "dcid:Provenance",
    "dcid:name": "Custom Import",
    "dcid:source": {"@id": "dcid:c/s/default"},
    "dcid:url": "custom-import"
  },
  {"@id": "dcid:c/s/default", "@type": "dcid:Source", "dcid:name": "Custom Data Commons"}
]
```

```expect-removed
[
  "$provenance_TestProvenance",
  "$provenance_TestProvenance",
  {
    "@id": "dcid:TestSource",
    "@type": "dcid:Source",
    "dcid:domain": "source.example.org",
    "dcid:name": "Test Source",
    "dcid:url": "https://source.example.org"
  },
  {"@id": "dcid:TestSource", "@type": "dcid:Source", "dcid:name": "Test Source", "dcid:url": "https://source.example.org"}
]
```

## provenance and source declared in the config sources block

```mcf file=provenance.mcf
// The provenance comes from config.json instead.
```

```config
{"sources": {"Config Source": {"url": "https://config-source.example.org",
                               "provenances": {"Config Provenance": "https://config-source.example.org/p"}}},
 "inputFiles": [
   {"pattern": "*.csv", "provenance": "dcid:TestProvenance",
    "columnMappings": {"dcid:observationAbout": "entity", "dcid:variableMeasured": "variable",
                       "dcid:observationDate": "date", "dcid:value": "value"}}]}
```

```csv
entity,variable,date,value
country/USA,Count_Person,2020,1
```

```expect-error
{
  "type": "ValueError",
  "contains": [
    "Metadata Validation Failed: Linked sources are missing for defined provenances:"
  ],
  "handshake_error_types": [
    "GENERIC_ERROR"
  ]
}
```
