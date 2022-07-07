#!/bin/bash

# Copyright 2022 Google LLC
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

# Tests if lint is required on client side code
function run_npm_lint_test {
  npm list eslint || npm install eslint
  if ! npm run test-lint; then
    echo "Fix lint errors by running ./run_test.sh -f"
    exit 1
  fi
}

# Fixes lint
function run_lint_fix {
  echo -e "#### Fixing client-side code"
  npm list eslint || npm install eslint
  npm run lint
}

function run_npm_test {
  npm install
  npm run build
  npm run test
}


function run_all_tests {
  run_npm_lint_test
  run_npm_test
}

function help {
  echo "Usage: $0 -lcaf"
  echo "-l       Run client lint test"
  echo "-c       Run client tests"
  echo "-a       Run all tests"
  echo "-f       Fix lint"
  exit 1
}

# Always reset the variable null.
while getopts tpwotblcsaf OPTION; do
  case $OPTION in
    l)
        echo -e "### Running lint"
        run_npm_lint_test
        ;;
    c)
        echo -e "### Running client tests"
        run_npm_test
        ;;
    f)
        echo -e "### Fix lint errors"
        run_lint_fix
        ;;
    a)
        echo -e "### Running all tests"
        run_all_tests
        ;;
    *)
        help
    esac
done

if [ $OPTIND -eq 1 ]
then
  help
fi