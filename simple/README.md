# Simple Stats Importer

This importer imports input CSVs into a database which is subsequently used to serve Custom DCs.

[sqlite]: https://github.com/datacommonsorg/mixer/tree/a768446c56095aa23add8c59cf6a0630a17a726b/internal/sqlite

## Default usage

```shell
python3 -m stats.main
```

By default it reads inputs CSVs from the `.data/input` folder and outputs
a sqlite `datacommons.db` file in the `.data/output` folder.

To enable Data Commons API lookups, set a `DC_API_KEY` environment variable. See [API documentation](https://docs.datacommons.org/api/rest/v2/getting_started#authentication) to learn more about getting and using API keys.

## Other options

To see all parameters and overrides supported by the script:

```shell
python3 -m stats.main --help
```

## Input files

The first 2 columns of input CSVs should be place names (or more generically
_entity_ names) and observation periods respectively. Each subsequent column
should be for each individual statvar. A sample input CSV can be found
[here](sample/input/countries.csv).

## Debug files

The program also outputs a `debug_resolve.csv` file. This is for debugging
whether names were resolved to the correct DCIDs and addressed any unresolved
ones. A sample CSV can be found [here](sample/output/process/debug_resolve_countries.csv).

## Database options

As noted above, the importer by default writes to a local sqlite DB.
It can however be configured to write to a Cloud SQL DB instead as described in this section.

### Cloud SQL options

The importer writes to a Cloud SQL DB if the following environment variables are specified.

* `USE_CLOUDSQL`: To make the importer use Cloud SQL, set `USE_CLOUDSQL` to `true`.
* `DB_USER`: The DB user. e.g. `root`
* `DB_PASS`: The DB user's password.
* `DB_NAME`: [Optional] The name of the DB. Defaults to `datacommons`.

Example environment variables:

```bash
export USE_CLOUDSQL=true
export CLOUDSQL_INSTANCE=datcom-website-dev:us-central1:dc-graph
export DB_USER=root
export DB_PASS=fake
```


> Browse or create your Google SQL instances [here](https://console.cloud.google.com/sql/instances).
