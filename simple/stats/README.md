# Simple Stats Importer

Outputs `observations.csv` for simple stats to be used by [RSI sqlite][sqlite].

[sqlite]: https://github.com/datacommonsorg/mixer/tree/a768446c56095aa23add8c59cf6a0630a17a726b/internal/sqlite

## Usage

```shell
python3 main.py
```

It reads inputs CSVs from the `.data/input` folder and outputs `observations.csv` in the `.data/output` folder.

The first 2 columns of input CSVs should be place names (or more generically _entity_ names) and observation periods respectively. Each subsequent column should be for each individual statvar. A sample input CSV can be found [here](sample/countries/input.csv).

The output `observations.csv` can be imported directly into sqlite. A sample output CSV can be found [here](sample/countries/observations.csv).

The program also outputs a `debug_resolve.csv` file. This is for debugging whether names were resolved to the correct DCIDs and addressed any unresolved ones. A sample CSV can be found [here](sample/countries/debug_resolve.csv).

## Other options

To see all parameters and overrides supported by the script:

```shell
python3 main.py --help
```
