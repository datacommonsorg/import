# Importer Config

The config parameters for the files to be imported should be specified in a `config.json` file.

## Sample `config.json`

```json
{
    "inputFiles": {
        "countries.csv": {
            "entityType": "Country",
            "ignoreColumns": ["ignore1", "ignore2"]
        },
        "latlng.csv": {
            "entityType": "State"
        },
        "geoid.csv": {
            "entityType": ""
        }
    }
}
```

## `inputFiles`

The top-level `inputFiles` field should encode a map from input file name to parameters specific to that file.

### Input file parameters

Currently the only parameter supported for individual input files is `entityType`.

#### `entityType`

All entities in a given file must be of a specific type. This type should be specified as the value of the `entityType` field. 
The importer tries to resolve entities to dcids of that type.

#### `ignoreColumns`

The list of column names to be ignored by the importer, if any.