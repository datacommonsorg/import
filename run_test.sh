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

# Fixes python tests
function run_py_test {
  python3 -m venv .env
  source .env/bin/activate
  
  cd simple
  pip3 install -r requirements.txt

  echo -e "#### Running stats tests"
  python3 -m pytest tests/stats/ -s

  deactivate
}

function run_all_tests {
  run_lint_fix
  run_py_test
}

function help {
  echo "Usage: $0 -afhp"
  echo "-a              Run all tests"
  echo "-f              Fix lint"
  echo "-h              This usage"
  echo "-p              Run python tests"
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
    -h)
        help
        shift 1
        ;;
    -p)
        echo -e "### Running python tests"
        run_py_test
        shift 1
        ;;
    *)
        help
        exit 1
        ;;
    esac
done
