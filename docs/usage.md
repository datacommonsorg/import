# About

The Data Commons import tool is used to analyze and debug files that are developed
in the process of importing new datasets to the Data Commons Knowledge Graph.

The tool:
- Operates on two types of files: instance MCF (.mcf); Template MCF (.tmcf) and corresponding CSV files
- Performs resolution, syntax and statistics validations
- Generates instance MCF from template MCF and corresponding CSV files
- Generates reports on error/warning counters, stats validation and sample time-series charts

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

In `lint` mode, the import tool validates the artifacts produced for addition to Data Commons. These artifacts include [instance MCF files](https://github.com/datacommonsorg/data/blob/master/docs/mcf_format.md#instance-mcf) and pairs of [template MCF (TMCF)](https://github.com/datacommonsorg/data/blob/master/docs/mcf_format.md#template-mcf) and corresponding CSV files.

In `genmcf` mode, the import tool produces instance MCF files from a pair of TMCF file, and its associated CSV files. This mode performs all validations that the `lint` mode would have performed.

## Output

Both modes generate two output files:
- `report.json` is a detailed log of error/warning counters and associated messages to help locate the source of the counters.
- `summary_report.html` includes a summary of the counters from `report.json`, followed by statistical summaries for sample places. It is meant to be viewed in a web browser.

If input includes statistics (CSV and TMCF files, or MCF files with [`StatVarObservation`](https://github.com/datacommonsorg/data/blob/master/docs/representing_statistics.md#statvarobservation) nodes are provided), the reports will also include information on statistics from sample places and  time-series charts. In `genmcf` node, generated instance MCF files are written to `table_mcf_nodes_{CSV_FILE_NAME}.mcf` (if there were no fatal errors).

The output files are placed under a new folder in the current working directory named `dc_generated` by default.The `--output-dir` flag (documented below) can be specified to modify the name of this output folder.

### Lint Mode (`lint`)

To run the tool in lint mode, use:
  ```bash
  dc-import lint <list of mcf files>
  ```

For example, we can use `lint` to perform syntax validation on [a test MCF included in this repository](tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf) at path `tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf` relative to the base of this repo like so:
  ```bash
  dc-import lint tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf
  ```

This will output issues found in the input file to [`dc_generated/report.json`](../tool/src/test/resources/org/datacommons/tool/lint/mcfonly/output/report.json) and [`dc_generated/summary_report.html`](../tool/src/test/resources/org/datacommons/tool/lint/mcfonly/output/summary_report.html) in the current working directory.

### Instance MCF Generation Mode (`genmcf`)

To run the tool in genmcf mode, use:
  ```bash
  dc-import genmcf <list of csv/tmcf files>
  ```

Optionally, schema file(s) may also be passed. This is required to resolve references to newly introduced schema nodes.

For example, we can use `genmcf` to perform validations, and generate instance MCF from test files about COVID-19 cases in India.

These test files are:
- [covid.csv](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv) at path `tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv` relative to the base of this repo.
- [covid.tmcf](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf) at path `tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf` relative to the base of this repo.

From the base of the repo, we issue the following command:
  ```bash
  dc-import lint tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf
  ```

This will output issues found in the input to [`dc_generated/report.json`](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/output/report.json) and [`dc_generated/summary_report.html`](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/output/summary_report.html) under the current working directory.

This will also output the instance MCFs generated from the template to [`dc_generated/table_mcf_nodes_covid.mcf`](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/output/table_mcf_nodes_covid.mcf). Note that instance MCF will not be generated if there are any fatal errors in the input files. These fatal errors will instead be logged to `report.json` and `summary_report.html`.

## Command Line Flags

Flags available to modify the behavior of the tool are listed below. All flags
apply to both usage modes (`lint` and `genmcf`).

You can also run `dc-import --help` to see a list of flags in your terminal.

### `-e`, `--existence-checks`

Checks DCID references to schema nodes against the KG and locally. If this flag is set, then calls will be made to the Staging API server, and instance MCFs get fully loaded into memory.

Suppose the CSV file has a cell value like `dcid:Count_Person` indicating a reference to a DC entity. This check will ensure that such an entity is defined either in Data Commons KG (in this case [it does](https://datacommons.org/browser/Count_Person)), or in another instance MCF given as an input.

Defaults to `true`.

### `-h`, `--help`

Shows a help message and exit.

### `-n`, `--num-threads=<numThreads>`

Specifies the number of concurrent threads used for processing CSVs.

You need multiple CSVs to take advantage of concurrent processing. TIP: In case your generated CSV is very large, you can use [the split_csv tool](https://github.com/datacommonsorg/data/tree/master/tools#csv-splitter) to shard it into multiple files.

Defaults to `1`.

### `-o`, `--output-dir=<outputDir>`

Specifies the directory to write output files.

Default is `dc_generated/` within current working directory.

### `-s`, `--stat-checks`

Checks integrity of time series by checking for holes, variance in values, etc.

A set of counters detailing the results of the checks will be logged in `report.json`. For every such counter, the tool will provide a few exemplar cases to help the user
understand and resolve the issue(s).

For example, in this test input [`covid.mcf`](../tool/src/test/resources/org/datacommons/tool/lint/statchecks/input/covid.mcf) file, the value of the `CumulativeCount_MedicalTest_ConditionCOVID_19_Positive` StatVar for place
`geoId/07` is `3.0` one day, (2020-03-02;line 49), and `7.0` on the next day (2020-03-03; line 65).
Because the fluctuation in the value is greater than 100%, the tool flags this as a
potential statistical issue (counter:  `StatsCheck_MaxPercentFluctuationGreaterThan100`). This is logged in the resulting [`report.json`](../tool/src/test/resources/org/datacommons/tool/lint/statchecks/output/report.json) as follows:
  ```json
  "statsCheckSummary": [{
      "placeDcid": "geoId/07",
      "statVarDcid": "CumulativeCount_MedicalTest_ConditionCOVID_19_Positive",
      "measurementMethod": "",
      "observationPeriod": "",
      "scalingFactor": "",
      "unit": "",
      "validationCounters": [{
        "counterKey": "StatsCheck_MaxPercentFluctuationGreaterThan100",
        "problemPoints": [{
          "date": "2020-03-02",
          "values": [{
            "value": 3.0,
            "locations": [{
              "file": "covid.mcf",
              "lineNumber": "49"
            }]
          }]
        }, {
          "date": "2020-03-03",
          "values": [{
            "value": 7.0,
            "locations": [{
              "file": "covid.mcf",
              "lineNumber": "65"
            }]
          }]
        }],
        "percentDifference": 133.33
      }]
    }
  ```
Note  that information relevant to this check (sample place, file and location of the issue,
the values involved, and the exact percent fluctuation) are conveniently provided
to assist the user in debugging issues.


Defaults to `true`.

### `-p`, `--sample-places=<samplePlaces>`

Specifies a list of place dcids to run stats check on.

This flag should only be set if `--stat-checks` is `true`. If `--stat-checks` is `true` and this flag is not set, 5 sample places are picked for roughly each distinct place type.

### `-r`, `--resolution=<resolutionMode>`

Specifies the mode of resolution to use: `NONE`, `LOCAL`, or `FULL`.

Resolution refers to the process of assigning DCIDs to every graph node in the input. For StatVarObservation nodes, new DCIDs are generated. For nodes of other types, either the DCIDs must be provided, or the tool will use the Data Commons KG to find the DCID based on an external ID.

As an example of the latter, see the MCF node below where California is referenced using the `isoCode` property. This will resolve to the dcid of California in Data Commmons ([`geoId/06`](https://datacommons.org/browser/geoId/06)) when this flag is set to `FULL`.

  ```
  Node: CANode
  typeOf: dcs:Place
  isoCode: "US-CA"
  ```

- `LOCAL`: Only resolves local references and generates DCIDs. Notably, this mode does not resolve the external IDs against the DC KG.
- `FULL`: Resolves external IDs (such as ISO) in DC, local references, and generated DCIDs. Note that FULL mode may be slower since it makes (batched) DC Recon API calls and performs two passes over the provided CSV files. You should only use this if you have to resolve location entities via external IDs.
- `NONE`: Does not resolve references. Use this only if all inputs have DCIDs defined. You rarely want to use this mode.

Defaults to `LOCAL`.

### `-sr`, `--summary-report`

Generates a summary report in HTML format. See the [output section above](#output) for more details on what is included in the summary report. 

Defaults to `true`.

### `-V`, `--version`

Prints version information and exit.

### `--verbose`

Prints verbose log.

Defaults to `false`.
