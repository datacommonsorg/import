# Simple Stats Importer

This importer (also referred to as the "preprocessor") reads input CSV and MCF
files and writes the resulting graph out as sharded JSON-LD files. Those shards
are consumed by the Data Commons ingestion pipeline (the "dcpbridge" workflow).

## Default usage

```shell
python3 -m stats.main
```

By default it reads input files from the `.data/input` folder and writes output
to the `.data/output` folder.

To enable Data Commons API lookups, set a `DC_API_KEY` environment variable. See
[API documentation](https://docs.datacommons.org/api/rest/v2/getting_started#authentication)
to learn more about getting and using API keys.

## Other options

To see all parameters and overrides supported by the script:

```shell
python3 -m stats.main --help
```

## Config driven imports

The importer can be bootstrapped either by an input directory or by a config
file. Use the `--config_file` flag to use the latter:

```shell
python3 -m stats.main \
  --config_file=//path/to/config.json \
  --output_dir=//path/to/output/dir
```

For config driven imports, the import files are specified using the
[dataDownloadUrl](stats/config.md#dataDownloadUrl) field.

## Input files

The first 2 columns of input CSVs should be place names (or more generically
_entity_ names) and observation periods respectively. Each subsequent column
should be for each individual statvar.

## Output

All output is written under `--output_dir`, which can be a local path or a
`gs://` path:

* `jsonld/<import_name>_<timestamp>/<import_name>/`: the generated JSON-LD
  shards. Node shards are named `node-<index>-<uid>.jsonld` and observation
  shards are named `observation-<sanitized_input_file>-<index>.jsonld`.
* `process/report.json`: the import report, including per-file status.

There is no database output. The importer streams shards to a local temporary
directory as it runs and bulk uploads them to the output directory at the end
of the run.

## Environment variables

| Variable | Default | Description |
| --- | --- | --- |
| `DC_API_KEY` | *unset* | Data Commons API key, used for entity resolution and schema lookups. |
| `DC_API_ROOT` | `https://api.datacommons.org` | Data Commons REST API endpoint root. |
| `DC_CLIENT_DEBUG` | `false` | If true, logs DC API request details and writes debug dumps to `.data/debug`. Note that this logs the API key, so keep it off outside of local debugging. |
| `LOG_LEVEL` | `INFO` | Python logging level. |
| `WORKFLOW_EXECUTION_ID` | *unset* | Set by the ingestion workflow. When set (and the output dir is on GCS), the run writes a handshake JSON file for the workflow. |
| `TEMP_LOCATION` | *unset* | GCS path the workflow handshake file is written under. |

## Container Build & CI/CD (`datacommons-data`)

This directory is packaged into the [gcr.io/datcom-ci/datacommons-data](https://console.cloud.google.com/artifacts/docker/datcom-ci/us/gcr.io/datacommons-data) container image using `simple/Dockerfile` and `simple/cloudbuild.yaml`.

### Automated CI/CD Builds

Cloud Build automatically runs `simple/cloudbuild.yaml` when commits are pushed to `master` that modify files under `simple/**` (excluding `simple/tests/**`, `simple/sample/**`, and `**.md`). Each automated build tags and pushes:

* `gcr.io/datcom-ci/datacommons-data:<7-char-import-sha>`
* `gcr.io/datcom-ci/datacommons-data:latest`

### Manual Dev Builds

To manually build and push a custom image tag to Artifact Registry via Cloud Build (without overwriting `:latest`):

```shell
./simple/scripts/push_image.sh <image-tag> [project-id]
```

If `project-id` is omitted, it defaults to `datcom-ci` (`gcr.io/datcom-ci/datacommons-data:<image-tag>`).

### Local Docker Build & Run

The container's `run.sh` entrypoint requires `DC_API_KEY`, `INPUT_DIR`, and `OUTPUT_DIR`, and writes results under `$OUTPUT_DIR/datacommons/`. From the repo root:

```shell
# 1. Build the image locally.
docker build -t datacommons-data:local simple/

# 2a. Run it on a local data directory.
docker run --rm \
  -e DC_API_KEY="${DC_API_KEY}" \
  -e INPUT_DIR=/data/input \
  -e OUTPUT_DIR=/data/output \
  -v /path/to/local/data:/data \
  datacommons-data:local

# 2b. Or run it on Google Cloud Storage (gs://) paths, mounting your
#     Application Default Credentials file for access.
docker run --rm \
  -e DC_API_KEY="${DC_API_KEY}" \
  -e INPUT_DIR=gs://my-bucket/input \
  -e OUTPUT_DIR=gs://my-bucket/output \
  -e GOOGLE_APPLICATION_CREDENTIALS=/gcp/creds.json \
  -v "${HOME}/.config/gcloud/application_default_credentials.json:/gcp/creds.json:ro" \
  datacommons-data:local
```
