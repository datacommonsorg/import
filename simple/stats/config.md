# Importer Config

The config parameters for the files to be imported should be specified in a `config.json` file.
Each import is a subdirectory of the input directory with its own `config.json`,
and every run names the imports it runs with `--imports`.

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
`ignoreColumns`. Otherwise the import fails with one error that lists all the
unmapped columns. A column can't be both mapped and listed in `ignoreColumns`.
A CSV whose entry has no `columnMappings` fails the import as well.
Each property may be mapped from only one column. Keys that differ only by
namespace name the same property, so mapping both `dcid:unit` and `unit` fails
the import too. Likewise, each column may be mapped to only one property.

For `"observations"` imports, `dcid:variableMeasured`, `dcid:observationDate`
and `dcid:value` are always required, along with one to three entity
dimensions: `dcid:observationAbout` and/or custom observation properties you
have defined in MCF.

For `"entities"` imports, `dcid:dcid` and `dcid:typeOf` are always required:
the column mapped to `dcid:dcid` gives each row's DCID, and the column mapped to
`dcid:typeOf` gives its type (class DCID), for example `Author`. A row with
either value empty fails the import, with an error listing those rows. Each
other mapped column's values are written under its mapped DCID. For example:

```json
"columnMappings": {
  "dcid:dcid": "author_id",
  "dcid:typeOf": "type",
  "dcid:name": "author_name"
}
```

The importer only writes a `typeOf` reference on each row; the `Class` node
itself is not created. If the type is not already in the Data Commons schema,
define it (with its `name` and `description`) in an MCF file matched by an
`inputFiles` entry, for example:

```
Node: dcid:Author
typeOf: dcs:Class
name: "Author"
description: "A person who wrote an article"
```

For `"entities"` imports, a cell value with a namespace prefix, such as
`dcid:country/USA`, is a reference to that entity rather than a literal value.
A comma-separated list of prefixed values, such as
`dcid:author1,dcid:author2`, is a list of references.

#### `ignoreColumns`

The list of column names to be ignored by the importer, if any.

#### `importType`

Optional. Either `"observations"` (the default) or `"entities"`.

#### `observationProperties`

Optional, for observations CSVs. A map of observation properties (such as
`"unit"`, `"scalingFactor"`, `"measurementMethod"`, `"observationPeriod"`, or
custom dimension properties) applied as defaults to every row in the file.

## Subdirectories

Files in subdirectories of an import's directory are matched against the
`inputFiles` patterns like any other file, except those under an `archives/`
directory, which are never imported.

`includeInputSubdirs` is deprecated and ignored: subdirectories are always
searched. A config that still sets it is accepted, with a warning in the log.
