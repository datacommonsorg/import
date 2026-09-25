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
      },
      "columnsToResolve": ["country"]
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

#### `entityType`

Events imports only. All entities in the file are assumed to be of this type,
and the importer resolves entity names to DCIDs of that type. Observations
imports ignore it: they resolve only the columns named in `columnsToResolve`,
and take entity types from Base Data Commons.

#### `columnsToResolve`

The columns holding entities. Values that are not already DCIDs (names,
wikidata ids, lat/lng pairs) are resolved against Data Commons. Values in a
column the importer knows to be pre-resolved, such as `dcid`, are taken as-is.

Listing a column here is also what makes its entities eligible for proxy nodes
in the output graph. See [`importProxyEntities`](#importproxyentities).

#### `ignoreColumns`

The list of column names to be ignored by the importer, if any.


## `importProxyEntities`

If `true` (the default), the importer looks up the entities in
`columnsToResolve` against Base Data Commons and emits a proxy node for each one
it finds a type for. Set it to `false` to skip both the lookups and the nodes;
observations still reference the entity DCIDs directly.

Can also be set per-run with the `--import_proxy_entities` flag or the
`IMPORT_PROXY_ENTITIES` environment variable.

## `variables`

Events imports only. Provides display names and other metadata for the
variables named in an events CSV.

Variables for observations imports are declared in MCF, not here.

### Variable parameters

#### `name`

The display name of the variable.
If not specified, the column name will be used as the display name.

#### `description`

The long form description of the variable.

The description is carried through to the graph and used downstream for search.

#### `properties`

The properties of this variable.

These are DC property entities encoded as a dictionary.
More information on the properties that can be associated with variables can be found [here](https://github.com/datacommonsorg/data/blob/master/docs/representing_statistics.md#statisticalvariable).

#### `group`

Variables can be arranged in groups.
The group hierarchy can be specified using the `group` property.
Use "/" as a separator to specify a multi-level hierarchy.

#### `searchDescriptions` _(formerly `nlSentences`)_

Extra phrasings to index the variable under for search.
If not specified, the variable name is used.

Note that `nlSentences` is deprecated and will be removed in the future.

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

## `groupStatVarsByProperty`

If `true`, requests a hierarchy of StatVarGroups generated from the properties
of the variables in the dataset. Default is `false`.

The importer does not build the hierarchy itself. It creates the custom root
StatVarGroup and forwards the request to the ingestion pipeline as
`generateStatVarGroups` in the handshake record.

## Hierarchy and Group Customization

These optional top-level fields customize how the StatVarGroup hierarchy is generated and labeled. All are optional; defaults match the built-in values.

- `defaultCustomRootStatVarGroupName`: Display name for the custom root StatVarGroup. Default: `"Custom Variables"`.
- `customIdNamespace`: Namespace token for generated ids for SVs and manual groups. Default: `"custom"`.
  - Generated SV ids: `<namespace>/statvar_<n>` (e.g., `custom/statvar_1`).
  - Manual group ids: `<namespace>/g/group_<n>` (e.g., `custom/g/group_1`).
- `customSvgPrefix`: String prefix for generated custom StatVarGroup ids.
  - If not set, and `customIdNamespace` is provided, it defaults to `<customIdNamespace>/g/`.
  - Otherwise defaults to `"c/g/"`.
  - Affects ids like `c/g/Person_Gender-Female`.
- `svHierarchyPropsBlocklist`: Array of additional property dcids to exclude from hierarchy generation. These are added to the internal blocklist used by Data Commons.
  - Example: `["DevelopmentFinanceRecipient", "DACCode", "CustomProperty"]`

Example fragment:

```json
{
  "groupStatVarsByProperty": true,
  "defaultCustomRootStatVarGroupName": "ONE Data",
  "customIdNamespace": "ONE"
  "customSvgPrefix": "OD/g/",
  "svHierarchyPropsBlocklist": ["DevelopmentFinanceRecipients", "DACCode"],
}