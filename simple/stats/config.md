# Importer Config

The config parameters for the files to be imported should be specified in a `config.json` file.

## Sample `config.json`

```json
{
  "inputFiles": {
    "countries.csv": {
      "entityType": "Country",
      "ignoreColumns": ["ignore1", "ignore2"],
      "provenance": "Provenance1 Name"
    },
    "latlng.csv": {
      "entityType": "State",
      "provenance": "Provenance1 Name"
    },
    "geoid.csv": {
      "entityType": "",
      "provenance": "Provenance2 Name"
    }
  },
  "variables": {
    "Variable 1": {"group": "Parent Group/Child Group 1"},
    "Variable 2": {"group": "Parent Group/Child Group 1"},
    "var3": {
      "name": "Var 3 Name",
      "description": "Var 3 Description",
      "nlSentences": ["Sentence 1", "Sentence 2"],
      "group": "Parent Group/Child Group 2",
      "properties": {
        "populationType": "schema:Person",
        "measuredProperty": "age",
        "statType": "medianValue",
        "gender": "Female"
      }
    },
  },
  "sources": {
    "Source1 Name": {
      "url": "http://source1.com",
      "provenances": {
        "Provenance1 Name": "http://source1.com/provenance1",
        "Provenance2 Name": "http://source1.com/provenance2"
      }
    }
  }
}
```

## `inputFiles`

The top-level `inputFiles` field should encode a map from input file name to parameters specific to that file.
Keys can be individual file names or wildcard patterns if the same config applies to multiple files.

If files match multiple wildcard patterns, the first match as specified in the config will be used.

Example:

```json
{
  "inputFiles": {
    // Applies only to "foo.csv".
    "foo.csv": {...},
    // Applies to bar.csv, bar1.csv, bar2.csv, etc.
    "bar*.csv": {...},
    // Applies to all CSVs except "foo.csv" and "bar*.csv".
    "*.csv": {...}
  }
}
```

### Input file parameters

#### `entityType`

All entities in a given file must be of a specific type. This type should be
specified as the value of the `entityType` field. The importer tries to resolve
entities to dcids of that type.

#### `ignoreColumns`

The list of column names to be ignored by the importer, if any.

#### `provenance`

The provenance (name) of this input file. 
Note that provenance details should be specified under `sources` -> `provenances` 
and this field associates one of the provenances defined there to this file.

Provenances typically map to a dataset from a source.
e.g. WorldDevelopmentIndicators provenance (or dataset) is from the WorldBank source.

## `variables`

The top-level `variables` field can be used to provide more information about variables 
in the input CSVs.

If not specified, the variable column names in the CSVs will be used as their names.

Names can be overriden and other information can be provided using the parameters described below.

### Variable parameters

#### `name`

The display name of the variable.
If not specified, the column name will be used as the display name.

#### `description`

The long form description of the variable.

The description will also be used to create NL embeddings for the variable.

#### `properties`

The properties of this variable.

These are DC property entities encoded as a dictionary.
More information on the properties that can be associated with variables can be found [here](https://github.com/datacommonsorg/data/blob/master/docs/representing_statistics.md#statisticalvariable).

#### `group`

Variables can be arranged in groups.
The group hierarchy can be specified using the `group` property.
Use "/" as a separator to specify a multi-level hierarchy.

#### `nlSentences`

An array of NL sentences to be used for creating more NL embeddings (in addition to the description)
for the variable.

## `sources`

The top-level `sources` field should encode the sources and provenances associated with the input dataset.

### Source parameters

#### `url`

The URL of the source.

#### `provenances`

The provenances under a given source should be defined using the `provenances` property as `{provenance-name}:{provenance-url}` pairs.

## `dataDownloadUrl`

The simple importer can be bootstrapped either by an input directory or by a config file.

For config driven imports, the import files are specified using a `dataDownloadUrl` field.

This is a repeated field, in that the value should be an array of download URLs. The URLs can be web urls (`http://` or `https://`), GCS directories (`gs://`) or local directories.

### Examples

Web URLs _(coming soon)_:

```json
{
  "dataDownloadUrl": [
    "http://domain/path/1.csv",
    "https://domain/path/2.csv"
  ]
}
```

GCS directory:

```json
{
  "dataDownloadUrl": ["gs://bucket/path/to/dir"]
}
```

Local directory:

```json
{
  "dataDownloadUrl": ["//local/path/to/dir"]
}
```

## `generateHierarchy`

If `true`, auto generates a variable group hierarchy based on properties of variables in the dataset. Default is `false`.

> TODO: Add more details.
