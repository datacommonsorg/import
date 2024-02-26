# Run Simple Import Docker image locally

`run_simple.sh` is an example script illustrating how the simple import docker image
can be run locally.

* In the `env.list` file, specify the `DC_API_KEY`.
* If using GCS:
  * Run `gcloud beta auth application-default login`.
  * Specify your cloud project via the `GOOGLE_CLOUD_PROJECT` variable.
* If using config file and output dir, specify `CONFIG_FILE` and `OUTPUT_DIR` respectively.
* If using input dir and output dir, specify `INPUT_DIR` and `OUTPUT_DIR` respectively.
* Note that `CONFIG_FILE`, `INPUT_DIR` and `OUTPUT_DIR` can be GCS paths or local file system paths.
