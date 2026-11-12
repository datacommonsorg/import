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

## Validation

`config.json` is validated when the import starts, before any file is read.
Unknown keys, values of the wrong type (for example a string where a list is
expected), unsupported values and duplicate patterns are errors. Every problem
is reported at once, and the run fails with an `INVALID_CONFIGURATION` error.

## `inputFiles`

The top-level `inputFiles` field is a list of objects, one per group of input
files. Each object identifies the files it applies to with a `pattern`, and
carries the parameters for those files.

If a file matches multiple patterns, the first match in list order wins. Each
pattern can only appear once.

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
for example `dcid:Provenance1`. It must include a namespace prefix.

Provenances typically map to a dataset from a source.
e.g. WorldDevelopmentIndicators provenance (or dataset) is from the WorldBank source.

The Provenance node and its Source must be defined in an MCF file matched by an
`inputFiles` entry, like Class and Property nodes. The Provenance links to its
Source with `source`:

```
Node: dcid:Source1
typeOf: dcs:Source
name: "Source 1"
url: "https://source1.example.org"

Node: dcid:Provenance1
typeOf: dcs:Provenance
name: "Provenance 1"
url: "https://source1.example.org/dataset"
source: dcid:Source1
```

If a CSV's provenance is not defined in MCF, the import fails once every file
is imported, with one error that lists every undefined provenance and the files
that use it.

#### `columnMappings`

Required for every CSV file, for both `"observations"` and `"entities"` imports.
Maps DCIDs to the CSV's column headings. There are no built-in default column
names, and no column heading is used as a DCID on its own.

Every column in the CSV must be either mapped in `columnMappings` or listed in
`ignoreColumns`. The one exception is the `idColumn` of an `"entities"` import.
Otherwise the import fails with one error that lists all the unmapped columns.
A CSV whose entry has no `columnMappings` fails the import as well.
Each property may be mapped from only one column. Keys that differ only by
namespace name the same property, so mapping both `dcid:unit` and `unit` fails
the import too. Likewise, each column may be mapped to only one property.

For `"observations"` imports, `dcid:variableMeasured`, `dcid:observationDate`
and `dcid:value` are always required, along with one to three entity
dimensions: `dcid:observationAbout` and/or custom observation properties you
have defined in MCF.

For `"entities"` imports, each mapped column's values are written under its
mapped DCID. The `idColumn` does not need a mapping; it is only written as a
property if it is mapped.

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
each row entity in the CSV. The importer only writes a `typeOf` reference to
this type on each row; the `Class` node itself is not created. If the type is
not already in the Data Commons schema, define it (with its `name` and
`description`) in an MCF file matched by an `inputFiles` entry, for example:

```
Node: dcid:Author
typeOf: dcs:Class
name: "Author"
description: "A person who wrote an article"
```

#### `idColumn`

Optional, for `"entities"` imports. The column whose cell value provides each
row entity's DCID. If omitted, IDs are generated as `<rowEntityType>_<rowIndex>`.
Unlike other columns, it does not need a `columnMappings` entry.

#### `referenceColumns` _(or `columnsToResolve`)_

Optional, for `"entities"` imports. The list of columns whose cell values are
entity DCID references (or comma-separated lists of entity DCIDs) rather than
literal values.

## `includeInputSubdirs`

If `true`, recursively scans subdirectories of the input directory for files
matching `inputFiles` patterns. Default is `false`.

## `groupStatVarsByProperty`

If `true`, requests a hierarchy of StatVarGroups generated from the properties
of the variables in the dataset. Default is `false`.

The importer does not build the hierarchy itself. It creates the custom root
StatVarGroup and forwards the request to the ingestion pipeline as
`generateStatVarGroups` in the handshake record.
