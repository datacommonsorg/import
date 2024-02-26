# Run Simple Import Docker image locally

`run_simple.sh` is an example script illustrating how the simple import docker image
can be run locally.

* In the `env.list` file, specify the `DC_API_KEY`.
* If using GCS:
  * Run `gcloud beta auth application-default login`.
  * Specify your cloud project via the `GOOGLE_CLOUD_PROJECT` variable.
* If using config file and output dirs, specify `CONFIG_FILE` and `GCS_OUTPUT_DIR` (or `OUTPUT_DIR`) respectively.
* If using local input and output dirs, specify `INPUT_DIR` and `OUTPUT_DIR` respectively.
* If using GCS input and output dirs, specify `GCS_INPUT_DIR` and `GCS_OUTPUT_DIR` respectively.
