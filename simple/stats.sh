#!/bin/bash
# Script to process stats using simple stats loader.
# Defaults
OUTPUT_DIR=".data/output"
USAGE="Script to process stats with simple importer.
Usgae: $(basename $0) [Options]
Options:
  -c <file>   Json config file for stats importer
  -k <api-key> DataCommons API Key
  -i <dir>    Input directory to process
  -o <dir>    Output folder for stats importer. Default: $OUTPUT_DIR
  -m <customdc|maindc> Mode of operation for simple importer
  -j <jar>    DC Import java jar file.
                Download latest from https://github.com/datacommonsorg/import/releases/

For more, please refer to https://github.com/datacommonsorg/import/tree/master/simple
"
TMP_DIR=${TMP_DIR:-"/tmp"}
LOG=$TMP_DIR/log-stats-$(date +%Y%m%d).log
DC_IMPORT_JAR=${DC_IMPORT_JAR:-"$TMP_DIR/dc-import-tool.jar"}
RUN_STEPS="stats,validate"

function echo_log {
  local msg="[$(date +%Y-%m-%d:%H:%M:%S)] $@"
  echo -e "$msg" >> $LOG
  [[ -z "$QUIET" ]] && echo -e "$msg" >&2
}

function echo_error {
  local msg="[$(date +%Y-%m-%d:%H:%M:%S): ERROR] $@"
  echo -e "$msg" >> $LOG
  echo -e "$msg" >&2
}

function echo_fatal {
  echo_error "$@"
  exit 1
}

# Parse command line options
function parse_options {
  while (( $# > 0 )); do
    case $1 in
      -i) shift; INPUT_DIR=$(readlink -f "$1");;
      -c) shift; CONFIG=$(readlink -f "$1");;
      -k) shift; DC_API_KEY="$1";;
      -o) shift; OUTPUT_DIR="$1";;
      -m) shift; MODE="$1";;
      -j) shift; DC_IMPORT_JAR="$1";;
      -s) shift; RUN_STEPS="$1";;
      -q) QUIET="1";;
      -h) echo "$USAGE" >&2 && exit 0;;
      -x) set -x;;
      *) echo "Unknown option '$1'" && exit 1;;
    esac
    shift
  done
}


function setup_python {
  python3 -m venv $TMP_DIR/env-stats
  source $TMP_DIR/env-stats/bin/activate
  if [[ "$PYTHON_REQUIREMENTS_INSTALLED" != "true" ]]
  then
    echo_log "Installing Python requirements from $SIMPLE_DIR/requirements.txt"
    pip3 install -r $SIMPLE_DIR/requirements.txt >> $LOG 2>&1
    PYTHON_REQUIREMENTS_INSTALLED=true
  fi
}

function setup_dc_import {
  # Get the datacommons import jar
  if [[ -f "$DC_IMPORT_JAR" ]]; then
    echo_log "Using existing dc-import jar: $DC_IMPORT_JAR"
  else
    # Download the latest jar
    echo_log "Getting latest version of dc-import jar file..."
    # Get URLthe latest release
    jar_url=$(curl -vs  "https://api.github.com/repos/datacommonsorg/import/releases/latest" | \
      grep browser_download_url | cut -d\" -f4)
    [[ -z "$jar_url" ]] && echo_fatal "Unable to get latest jar for https://github.com/datacommonsorg/import/releases.\n Please download manually and set command line option '-j'"
    jar=$(basename $jar_url)
    [[ -z "$DC_IMPORT_JAR" ]] && DC_IMPORT_JAR="$TMP_DIR/jar"
    echo_log "Downloading dc-import jar from $jar_url into $DC_IMPORT_JAR..."
    curl -Ls "$jar_url" -o $DC_IMPORT_JAR
    [[ -f "$DC_IMPORT_JAR" ]] || echo_fatal "Failed to download $jar_url"
  fi
}

function setup {
  parse_options "$@"
  # Get dir for simple
  SIMPLE_DIR=$(echo $0 | sed -e 's,/simple.*,/simple,')
  SIMPLE_DIR=$(readlink -f $SIMPLE_DIR)
  setup_python
  setup_dc_import
}

# Run the simple import on the specified config/input files.
# Geneates output into OUTPUT_DIR
function simple_import {
  if [[ "$INPUT_DIR$CONFIG" == "" ]]; then
    echo_fatal "No input directory or config. Specify one of '-i' or '-c' command line options.\n$USAGE"
  fi

  # Check for DC API key
  if [[ "$DC_API_KEY" == "" ]]; then
    echo_log "Warning: DC_API_KEY not set and may cause failures.
Set a DataCommons API key with '-k' option.
To get a key, please refer to https://docs.datacommons.org/api/rest/v2/getting_started#authentication"
  fi
  export DC_API_KEY="$DC_API_KEY"

  # Build options for simple importer
  local importer_options=""
  [[ -n "$INPUT_DIR" ]] && importer_options="$importer_options --input_dir=$INPUT_DIR"
  [[ -n "$CONFIG" ]] && importer_options="$importer_options --config_file=$CONFIG"
  [[ -n "$MODE" ]] && importer_options="$importer_options --mode=$MODE"
  [[ -n "$OUTPUT_DIR" ]] && importer_options="$importer_options --output_dir=$OUTPUT_DIR"
  mkdir -p $OUTPUT_DIR
  OUTPUT_DIR=$(readlink -f $OUTPUT_DIR)

  # Clear state from old run
  report_json=$OUTPUT_DIR/process/report.json
  rm $report_json

  # Run the simple importer
  local cwd="$PWD"
  cd "$SIMPLE_DIR"
  cmd="python -m stats.main $importer_options"
  echo_log "Running command: $cmd"
  $cmd
  cmd_status=$?
  cd "$cwd"
  if [[ "$cmd_status" != "0" ]]; then
    echo_fatal "Failed to run simple importer: $cmd.\n Logs in $LOG"
  fi

  status=$(grep '"status"' $report_json)
  echo_log "simple importer: $status"
}

# Generate tmcf for a csv file in dir
function get_or_generate_tmcf {
  set -x
  local dir="$1"; shift

  tmcf=$(ls $dir/*.tmcf | head -1)
  [[ -n "$tmcf" ]] && echo "$tmcf" && return

  # Create a new tmcf for columns in csv
  csv=$(ls $dir/*.csv | head -1)
  tmcf=$(echo "$csv" | sed -e 's/.csv$/.tmcf/')
  cat <<END_TMCF > $tmcf
Node: E:Stats->E0
typeOf: dcs:StatVarObservation
observationAbout: C:Stats->entity
observationDate: C:Stats->date
variableMeasured: C:Stats->variable
value: C:Stats->value
END_TMCF
  echo "$tmcf"
  set +x
}

# Run dc-import genmcf to validate generated csv/mcf files.
function validate_output {
  echo_log "Validating output in $OUTPUT_DIR"

  # Get the tmcf file to validate csv stats.
  tmcf=$(get_or_generate_tmcf "$OUTPUT_DIR")

  # Run dc-import genmcf
  cmd="java -jar $DC_IMPORT_JAR genmcf -n 20 -r FULL $OUTPUT_DIR/*.csv $tmcf -o $OUTPUT_DIR/dc_generated"
  echo_log "Running dc-import validation: $cmd"
  $cmd >> $LOG
  status="$?"
  [[ "$status" == "0" ]] || echo_fatal "Failed to run dc-import: $cmd"
  echo_log "Output of validiton in $OUTPUT_DIR/dc_generated/report.json"
}

# Return if being sourced
(return 0 2>/dev/null) && return

# Run a specific step
function run_step {
  local step_name="$1"; shift
  local step_fn="$1"; shift

  has_stage=$(echo "$RUN_STEPS" | grep "$step_name")
  [[ -n "$has_stage" ]] && $step_fn
}

function main {
  setup "$@"
  run_step "stats" simple_import
  run_step "validate" validate_output
}

main "$@"
