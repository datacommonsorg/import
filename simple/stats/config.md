# Importer Config

The config parameters for the files to be imported should be specified in a `config.json` file.

## Sample `config.json`

```json
{
  "inputFiles": [
    {
      "pattern": "countries.csv",
      "provenance": "dcid:Provenance1",
      "ignoreColumns": ["ignore1", "ignore2"],
      "columnMappings": {
        "dcid:variableMeasured": "variable",
        "dcid:observationAbout": "country",
        "dcid:observationDate": "year",
        "dcid:value": "value"
      }
    },
    {
      "pattern": "*.mcf",
      "provenance": "dcid:Provenance1"
    }
  ]
}
```

## `inputFiles`

The top-level `inputFiles` field is a list of objects, one per group of input
files. Each object identifies the files it applies to with a `pattern`, and
carries the parameters for those files.

If a file matches multiple patterns, the first match in list order wins.

Example:

```json
{
  "inputFiles": [
    // Applies only to "foo.csv".
    {"pattern": "foo.csv", ...},
    // Applies to bar.csv, bar1.csv, bar2.csv, etc.
    {"pattern": "bar*.csv", ...},
    // Applies to all CSVs except "foo.csv" and "bar*.csv".
    {"pattern": "*.csv", ...}
  ]
}
```

> An older format keyed `inputFiles` as a map from pattern to parameters. It is
> no longer accepted. As well as being undocumented, it silently bypassed
> provenance and source validation, because the validator iterates `inputFiles`
> and skips entries that are not objects. Over a map, that is every entry.

### Input file parameters

#### `pattern`

Required. A file name or glob pattern identifying the files this entry applies
to. Patterns are relative to the directory containing the `config.json`.

#### `provenance`

Required, for CSV and MCF files alike. The provenance DCID for this input file,
for example `dcid:Provenance1`. It must include a namespace prefix, and the
provenance node itself must be defined in one of the MCF files, with a `source`.

Provenances typically map to a dataset from a source.
e.g. WorldDevelopmentIndicators provenance (or dataset) is from the WorldBank source.

#### `columnMappings`

Required for CSV files. Maps the CSV's column headings to DCIDs. There are no
built-in default column names.

`dcid:variableMeasured`, `dcid:observationDate` and `dcid:value` are always
required, along with at least one entity mapping: either `dcid:observationAbout`
or a custom observation property you have defined in MCF.

#### `ignoreColumns`

The list of column names to be ignored by the importer, if any.

#### `importType`

Optional. Either `"observations"` (the default) or `"entities"`.

#### `observationProperties`

Optional, for observations CSVs. A map of observation properties (such as
`"unit"`, `"scalingFactor"`, `"measurementMethod"`, `"observationPeriod"`, or
custom dimension properties) applied as defaults to every row in the file.

#### `rowEntityType`

Required when `importType` is `"entities"`. The type (class DCID) assigned to
each row entity in the CSV.

#### `idColumn`

Optional, for `"entities"` imports. The column whose cell value provides each
row entity's DCID. If omitted, IDs are generated as `<rowEntityType>_<rowIndex>`.

#### `referenceColumns` _(or `columnsToResolve`)_

Optional, for `"entities"` imports. The list of columns whose cell values are
entity DCID references (or comma-separated lists of entity DCIDs) rather than
literal values.

## `entities`

Optional. Provides display `name` and `description` metadata for `Class` nodes
created by `"entities"` imports, keyed by `rowEntityType`.

## `includeInputSubdirs`

If `true`, recursively scans subdirectories of the input directory for files
matching `inputFiles` patterns. Default is `false`.

## `dataDownloadUrl`

Examples:

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

## `groupStatVarsByProperty`

If `true`, requests a hierarchy of StatVarGroups generated from the properties
of the variables in the dataset. Default is `false`.

The importer does not build the hierarchy itself. It creates the custom root
StatVarGroup and forwards the request to the ingestion pipeline as
`generateStatVarGroups` in the handshake record.
