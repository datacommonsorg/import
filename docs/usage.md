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

# Prerequisites

- Make sure you've downloaded the .jar file under Assets [here](https://github.com/datacommonsorg/import/releases/). Note the path to .jar.

- Obtain an API key from [DataCommons API Portal](https://apikeys.datacommons.org/) and set environment variable DC_API_KEY as the key.

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

The tool can generate the following artifacts:

| Artifact | Generation condition | Purpose |
| --- | --- | --- |
| `report.json` | Normal processing path in both modes | Detailed processing report containing counters, exemplar messages, statistical findings, command arguments, and optional runtime metadata |
| `summary_report.html` | Both modes when `--summary-report=true` | Human-readable counter and statistical summary with sample time-series charts |
| `summary_report.csv` | Both modes when `--summary-report=true` | Machine-readable per-StatVar aggregate statistics |
| Generated `*.mcf` files | `genmcf` mode when generation succeeds | Instance graph generated from the input files |

The `--summary-report` flag defaults to `true` and controls generation of both
`summary_report.html` and `summary_report.csv`.

If input includes statistics (CSV and TMCF files, or MCF files with [`StatVarObservation`](https://github.com/datacommonsorg/data/blob/master/docs/representing_statistics.md#statvarobservation) nodes are provided), the reports will also include information on statistics from sample places and time-series charts. In `genmcf` mode, generated instance MCF files are written to `table_mcf_nodes_{CSV_FILE_NAME}.mcf` (if there were no fatal errors).

The output files are placed under a new folder in the current working directory named `dc_generated` by default.The `--output-dir` flag (documented below) can be specified to modify the name of this output folder.

### `report.json`

`report.json` is the machine-readable processing and validation log. Its
top-level fields are:

| Field | Meaning |
| --- | --- |
| `levelSummary` | Aggregate counters grouped by `LEVEL_INFO`, `LEVEL_WARNING`, `LEVEL_ERROR`, and `LEVEL_FATAL` |
| `entries` | Bounded exemplar messages with their counter, severity, source location, and related columns when available |
| `statsCheckSummary` | Detailed statistical findings grouped by place, StatVar, and observation facets |
| `commandArgs` | Effective command arguments and input context used to generate the report |
| `runtimeMetadata` | Optional tool, build, host, Java, and execution-time metadata |

Use `levelSummary` to determine the aggregate count for a counter. Use
`entries` to find example messages and source locations. The number of entries
is not the total number of occurrences because the retained messages are
bounded per counter.

Counter values use protobuf `int64` fields and are serialized as JSON strings.
Empty or optional report sections may be absent. For individual counter
meanings and suggested actions, see the [counter reference](counters.md).

### `summary_report.csv`

`summary_report.csv` contains one row per StatVar and uses these columns in
order:

| Column | Meaning |
| --- | --- |
| `StatVar` | StatisticalVariable DCID |
| `NumPlaces` | Number of distinct places represented for the StatVar |
| `NumObservations` | Number of observation points represented for the StatVar, not the number of complete time series |
| `MinValue` | Minimum numeric observation value, or `NaN` when unavailable |
| `MaxValue` | Maximum numeric observation value, or `NaN` when unavailable |
| `NumObservationsDates` | Number of distinct observation dates |
| `MinDate` | Earliest stored observation date, or empty when unavailable |
| `MaxDate` | Latest stored observation date, or empty when unavailable |
| `MeasurementMethods` | String representation of the distinct measurement-method set |
| `Units` | String representation of the distinct unit set |
| `ScalingFactors` | String representation of the distinct scaling-factor set |
| `observationPeriods` | String representation of the distinct observation-period set |

The final four columns contain set-like values rendered as strings inside CSV
cells, for example `[CensusACS5YrSurvey]` or `[]`; they are not JSON arrays.
Use a CSV parser rather than splitting records on commas.

### Lint Mode (`lint`)

To run the tool in lint mode, use:
  ```bash
  dc-import lint <list of mcf files>
  ```
  
Note that if you are importing a dataset where non-numerical StatVar Observations are expected (for example, statType is measurementResult and, therefore, the SVObs values are references), set `--allow-non-numeric-obs-values=true` in the command line invocation.

For example, we can use `lint` to perform syntax validation on [a test MCF included in this repository](tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf) at path `tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf` relative to the base of this repo like so:
  ```bash
  dc-import lint tool/src/test/resources/org/datacommons/tool/lint/mcfonly/input/McfOnly.mcf
  ```

This will output issues found in the input file to [`dc_generated/report.json`](../tool/src/test/resources/org/datacommons/tool/lint/mcfonly/output/report.json) and [`dc_generated/summary_report.html`](../tool/src/test/resources/org/datacommons/tool/lint/mcfonly/output/summary_report.html) in the current working directory. It also writes `dc_generated/summary_report.csv` when summary reporting is enabled.

### Instance MCF Generation Mode (`genmcf`)

To run the tool in genmcf mode, use:
  ```bash
  dc-import genmcf <list of csv/tmcf files>
  ```

Optionally, schema file(s) may also be passed. This is required to resolve references to newly introduced schema nodes.

Similar to when using `lint` mode above, if you are importing a dataset where non-numerical StatVar Observations are expected (for example, statType is measurementResult and, therefore, the SVObs values are references), set `--allow-non-numeric-obs-values=true` in the command line invocation.

For example, we can use `genmcf` to perform validations, and generate instance MCF from test files about COVID-19 cases in India.

These test files are:
- [covid.csv](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv) at path `tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv` relative to the base of this repo.
- [covid.tmcf](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf) at path `tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf` relative to the base of this repo.

From the base of the repo, we issue the following command:
  ```bash
  dc-import genmcf tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.csv tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/input/covid.tmcf
  ```

This will output issues found in the input to [`dc_generated/report.json`](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/output/report.json) and [`dc_generated/summary_report.html`](../tool/src/test/resources/org/datacommons/tool/genmcf/statchecks/output/summary_report.html) under the current working directory. It also writes `dc_generated/summary_report.csv` when summary reporting is enabled.

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

You need multiple CSVs to take advantage of concurrent processing.

**TIP:** In case your generated CSV is very large, you can use [the split_csv tool](https://github.com/datacommonsorg/data/tree/master/tools#csv-splitter) to shard it into multiple files.

Defaults to `1`.

### `-o`, `--output-dir=<outputDir>`

Specifies the directory to write output files.

Default is `dc_generated/` within current working directory.

### `-ep`, `--existence-checks-place`

Specifies whether to perform existence checks for places found in the `observationAbout` property in StatVarObservation nodes.

Defaults to `true`.

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
    }]
  ```
Note  that information relevant to this check (sample place, file and location of the issue,
the values involved, and the exact percent fluctuation) are conveniently provided
to assist the user in debugging issues.

Defaults to `true`.

### `--allow-non-numeric-obs-values`
Allows non-numeric (text or reference) values for StatVarObservation value field.
- When `false`, non-numeric values will log an error counter ([`Sanity_SVObs_Value_NotANumber`](counters.md#sanitysvobsvaluenotanumber))
- When `true`, these values will be allowed and relevant StatChecks might be performed
(depending on the value of --stat-checks).

Defaults to `false`.

### `--check-measurement-result`

Checks DCID references from StatVarObservation nodes if the StatisticalVariable
they are measuring has `statType: measurementResult`.

If the StatVar definition exists in the local MCF files provided, that will be used.
Otherwise, API requests to the Data Commons KG will be made synchronously per unknown StatVar.

Only nodes in sample places are subject to this check.

Defaults to `false`.

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

### `-cr`, `--coordinates-resolution`

Resolves latitude-longitude coordinates through Data Commons Recon API calls.
This option applies only in `FULL` resolution mode.

Defaults to `false`.

### `-sr`, `--summary-report`

Generates `summary_report.html` and `summary_report.csv` in the output folder.
See the [output section](#output) for the contents of each artifact.

Defaults to `true`.

### `-og`, `--optimized-graph`

Generates an optimized graph by grouping observations.

Defaults to `false`.

### `--include-runtime-metadata`

Includes system, tool version, Git commit, build, and timing metadata in
`report.json` when available.

Defaults to `true`.

### `-V`, `--version`

Prints version information and exit.

### `--verbose`

Prints verbose log.

Defaults to `false`.

## Implementation References

The primary implementation sources for the documented behavior are:

- [`Main.java`](../tool/src/main/java/org/datacommons/tool/Main.java): command-line flags and defaults.
- [`Processor.java`](../tool/src/main/java/org/datacommons/tool/Processor.java): processing and report-generation orchestration.
- [`Debug.proto`](../util/src/main/proto/Debug.proto): logical schema serialized as `report.json`.
- [`SummaryReportGenerator.java`](../util/src/main/java/org/datacommons/util/SummaryReportGenerator.java): HTML and CSV summary generation.
- [`CSVReportWriter.java`](../util/src/main/java/org/datacommons/util/CSVReportWriter.java): `summary_report.csv` columns and values.
