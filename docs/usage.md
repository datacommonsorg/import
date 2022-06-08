# About

The Data Commons import tool is used to analyze and debug files that are developed
in the process of importing new datasets to the Data Commons Knowledge Graph.

It can:
- debug schema (.mcf) files
- generate instance MCF from template MCF (TMCF) and corresponding CSV tables
- resolve references against the Data Commons API
- generate statistics validation reports to spot check time series data

The tool is actively used for all data imports that are included in the Data Commons Knowledge Graph.
It is under active development, including feature additions and bug fixes.

The tool is a command line application built with Java. See below for usage instructions.

# Usage

Use the import tool from the command line, like so:

  ```bash
  java -jar <path-to-jar> <mode> <list of mcf/tmcf/csv files>
  ```

Hint: it can be useful to create an alias for the jar file, such as:
  ```bash
  alias dc-import='java -jar <path-to-jar>'
  ```

This is the form that will be used in the rest of the documentation.

Hint: to access a concise explanation of usage modes and flags, run
`dc-import --help`

## Usage Modes

In `lint` mode, the import tool validates the artifacts produced for addition to Data Commons. These artifacts include [instance MCF files](https://github.com/datacommonsorg/data/blob/master/docs/mcf_format.md#instance-mcf) and pairs of [template MCF (TMCF)](https://github.com/datacommonsorg/data/blob/master/docs/mcf_format.md#instance-mcf) and corresponding CSV files.

In `genmcf` mode, the import tool produces instance MCF files from a pair of TMCF file, and its associated CSV files. This mode performs all validations that the  `lint` mode would have performed.

All flags below apply to both the modes.

### Lint Mode (`lint`)
To run the tool in lint mode, use:
  ```bash
  dc-import lint <list of mcf files>
  ```

For example;
  ```bash
  dc-import lint auto_generated_stat_vars.mcf manual_schema.mcf
  ```

### Instance MCF Generation Mode (`genmcf`)
To run the tool in genmcf mode, use:
  ```bash
  dc-import genmcf <list of csv/tmcf files>
  ```

Note that in this mode, schema file(s) can be optionally passed. This is required to resolve references to newly introduced schema nodes.

  For example;
  ```bash
  dc-import lint my_dataset_mapping.tmcf my_dataset_table.csv  auto_generated_stat_vars.mcf manual_schema.mcf
  ```


## Flags
Flags available for the tool are listed below.

You can also run `dc-import --help` to see a list of flags in your terminal.
### `-e`, `--existence-checks`
Checks DCID references to schema nodes against the KG and locally. If this flag is set, then calls will be made to the Staging API server, and instance MCFs get fully loaded into memory. 

Defaults to `true`.

### `-h`, `--help`
Shows a help message and exit.

### `-n`, `--num-threads=<numThreads>`
Specifies the number of concurrent threads used for processing CSVs.

Defaults to `1`.

### `-o`, `--output-dir=<outputDir>`
Specifies the directory to write output files.

Default is `dc_generated/` within current working directory.

### `-s`, `--stat-checks`
Checks integrity of time series by checking for holes, variance in values, etc.

Defaults to `true`.

### `-p`, `--sample-places=<samplePlaces>`
Specifies a list of place dcids to run stats check on.

This flag should only be set if `--stat-checks` is `true`. If `--stat-checks` is `true` and this flag is not set, 5 sample places are picked for roughly each distinct place type.

### `-r`, `--resolution=<resolutionMode>`
Specifies the mode of resolution to use: `NONE`, `LOCAL`, `FULL`.

- `NONE`: Does not resolve references.
- `LOCAL`: Only resolves local references and generates DCIDs
- `FULL`: Resolves external IDs (such as ISO) in DC, local references, and generated DCIDs.Note that FULL mode may be slower since it makes (batched) DC Recon API calls and performs two passes over the provided CSV files.

Defaults to `LOCAL`.

### `-sr`, `--summary-report`
Generates a summary report in html format. 

Defaults to `true`.

### `-V`, `--version`
Prints version information and exit.

### `--verbose`
Prints verbose log.

Defaults to `false`.
