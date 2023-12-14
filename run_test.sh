#!/bin/bash

# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

set -e

# Fixes lint
function run_lint_fix {
  echo -e "#### Fixing Python code"
  python3 -m venv .env
  source .env/bin/activate
  pip3 install yapf==0.33.0 -q
  if ! command -v isort &> /dev/null
  then
    pip3 install isort -q
  fi
  yapf -r -i -p --style='{based_on_style: google, indent_width: 2}' simple/ -e=.env/*
  isort simple/ --profile google
  deactivate
}

# Lint test
function run_lint_test {
  python3 -m venv .env
  source .env/bin/activate
  pip3 install yapf==0.33.0 -q
  if ! command -v isort &> /dev/null
  then
    pip3 install isort -q
  fi
  
  echo -e "#### Checking Python style"
  if ! yapf --recursive --diff --style='{based_on_style: google, indent_width: 2}' -p simple/ -e=.env/*; then
    echo "Fix Python lint errors by running ./run_test.sh -f"
    exit 1
  fi

  echo -e "#### Checking Python import order"
  if ! isort simple/ -c --profile google; then
    echo "Fix Python import sort orders by running ./run_test.sh -f"
    exit 1
  fi
}

# Run python tests
function run_py_test {
  export TEST_MODE=test
  py_test
}

# Update goldens
function update_goldens {
  export TEST_MODE=write
  py_test
}

function py_test {
  # Clear api key to catch any spurious API calls.
  export DC_API_KEY=
  # Do not use Cloud SQL.
  export USE_CLOUDSQL=false

  python3 -m venv .env
  source .env/bin/activate
  
  cd simple
  pip3 install -r requirements.txt

  echo -e "#### Running stats tests"
  python3 -m pytest tests/stats/ -s

  deactivate
}

function run_all_tests {
  run_lint_test
  run_py_test
}

function run_sample {
  # Do not use Cloud SQL.
  export USE_CLOUDSQL=false

  python3 -m venv .env
  source .env/bin/activate
  
  cd simple
  pip3 install -r requirements.txt

  echo "Deleting existing datacommons.db file."
  rm -f sample/output/datacommons.db

  echo "Running sample."
  python3 -m stats.main --input_path=sample/input --output_dir=sample/output --freeze_time

  echo "Writing tables to CSVs."
  mkdir -p sample/output/debug
  sqlite3 -header -csv sample/output/datacommons.db "select * from observations;" > sample/output/debug/observations.csv
  sqlite3 -header -csv sample/output/datacommons.db "select * from triples;" > sample/output/debug/triples.csv
  sqlite3 -header -csv sample/output/datacommons.db "select * from imports;" > sample/output/debug/imports.csv

  deactivate
}

function help {
  echo "Usage: $0 -afhlp"
  echo "-a              Run all tests"
  echo "-f              Fix lint"
  echo "-g              Update goldens"
  echo "-h              This usage"
  echo "-l              Run lint test"
  echo "-p              Run python tests"
  echo "-s              Run sample and generate debug output"
  exit 1
}

if [ $# -eq 0 ]; then
  help
  exit 1
fi

# Always reset the variable null.
while [[ "$#" -gt 0 ]]; do
  case "$1" in
    -a)
        echo -e "### Running all tests"
        run_all_tests
        shift 1
        ;;
    -f)
        echo -e "### Fix lint errors"
        run_lint_fix
        shift 1
        ;;
    -g)
        echo -e "### Updating goldens"
        update_goldens
        shift 1
        ;;
    -h)
        help
        shift 1
        ;;
    -l)
        echo -e "### Run lint test"
        run_lint_test
        shift 1
        ;;
    -p)
        echo -e "### Running python tests"
        run_py_test
        shift 1
        ;;
    -s)
        echo -e "### Running sample"
        run_sample
        shift 1
        ;;
    *)
        help
        exit 1
        ;;
    esac
done
