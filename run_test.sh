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

# Default to searching public PyPI in addition to any configured global index-url,
# to ensure standard third-party tools/packages can be resolved.
export PIP_EXTRA_INDEX_URL="${PIP_EXTRA_INDEX_URL:-https://pypi.org/simple}"


# Fixes lint
function run_lint_fix {
  echo -e "#### Fixing Python code"
  python3 -m venv .env
  source .env/bin/activate
  pip3 install yapf==0.40.2 -q
  if ! command -v isort &> /dev/null
  then
    pip3 install isort -q
  fi
  yapf -r -i -p --style='{based_on_style: google, indent_width: 2}' simple/ -e=*pb2.py -e=**/.env/**
  isort simple/ --skip-glob=*pb2.py  --skip-glob=**/.env/** --profile google
  deactivate
}

# Lint test
function run_lint_test {
  python3 -m venv .env
  source .env/bin/activate
  pip3 install yapf==0.40.2 -q
  if ! command -v isort &> /dev/null
  then
    pip3 install isort -q
  fi

  echo -e "#### Checking Python style"
  if ! yapf --recursive --diff --style='{based_on_style: google, indent_width: 2}' -p simple/ -e=*pb2.py -e=**/.env/**; then
    echo "Fix Python lint errors by running ./run_test.sh -f"
    exit 1
  fi

  echo -e "#### Checking Python import order"
  if ! isort simple/ -c --skip-glob=*pb2.py  --skip-glob=**/.env/** --profile google; then
    echo "Fix Python import sort orders by running ./run_test.sh -f"
    exit 1
  fi
}

# Run python tests
function run_py_test {
  # Clear api key to catch any spurious API calls.
  export DC_API_KEY=

  python3 -m venv .env
  source .env/bin/activate

  ROOT_DIR=$(pwd)

  cd simple
  pip3 install -r requirements_test.txt -q

  echo -e "#### Running kg_util tests"
  python3 -m pytest kg_util/ -s

  echo -e "#### Running preprocessor test suite"
  python3 -m pytest testsuite/ -n auto

  echo -e "#### Running preprocessor regression tool tests"
  python3 -m pytest tools/

  echo -e "#### Running ingestion helper tests"
  cd "${ROOT_DIR}/pipeline/workflow/ingestion-helper"
  pip3 install uv -q
  uv run --python 3.12 pytest --ignore=aggregation/e2e_tests/

  echo -e "#### Running aggregation helper unit tests"
  cd "${ROOT_DIR}/pipeline/workflow/aggregation-helper"
  # The e2e suite is too slow for CI.
  uv run --python 3.12 pytest --ignore=aggregation/e2e_tests/

  cd "${ROOT_DIR}"
  deactivate
}

# Prints the line coverage, in percent, recorded in a Cobertura coverage.xml.
function coverage_percent {
  python3 -c "import sys, xml.etree.ElementTree as E; \
print('%.1f%%' % (100 * float(E.parse(sys.argv[1]).getroot().get('line-rate'))))" "$1"
}

# Runs the preprocessor tests under coverage in the simple/ dir $1, using the
# coverage config $2, and writes $1/coverage.xml.
function coverage_run {
  local cov="--cov --cov-config=$2"
  (
    cd "$1"
    rm -f .coverage .coverage.* coverage.xml
    python3 -m pytest kg_util/ -q $cov --cov-report=
    python3 -m pytest testsuite/ -n auto -q $cov --cov-append --cov-report=
    python3 -m pytest tools/ -q $cov --cov-append \
      --cov-report=term --cov-report=xml:coverage.xml
  )
}

# Measures coverage of simple/ and of the lines changed since a base ref.
# $1 = "compare" also measures the base (the merge-base) in a temporary
# worktree, to show coverage before vs after.
# Env: COVERAGE_BASE (default upstream/master, else origin/master),
#      COVERAGE_FAIL_UNDER (fail if changed-line coverage is below it).
function run_coverage {
  export DC_API_KEY=
  python3 -m venv .env
  source .env/bin/activate
  pip3 install -r simple/requirements_test.txt -q
  pip3 install pytest-cov==7.1.0 diff-cover==11.0.0 -q

  local root base merge_base
  root=$(pwd)
  base="${COVERAGE_BASE:-}"
  if [[ -z "$base" ]]; then
    base=upstream/master
    git rev-parse -q --verify "$base" > /dev/null || base=origin/master
  fi
  merge_base=$(git merge-base "$base" HEAD)

  echo -e "#### Measuring coverage of simple/"
  coverage_run "$root/simple" "$root/simple/.coveragerc"

  if [[ "$1" == "compare" ]]; then
    echo -e "#### Measuring coverage of the base, ${merge_base:0:12}"
    local base_tree
    base_tree=$(mktemp -d)/base
    git worktree add -q --detach "$base_tree" "$merge_base"
    # The base may predate .coveragerc, so use this tree's config.
    coverage_run "$base_tree/simple" "$root/simple/.coveragerc" > /dev/null \
      || echo "Some tests failed on the base; its coverage may be low."
    echo -e "#### Coverage: base $(coverage_percent "$base_tree/simple/coverage.xml")" \
      "-> this tree $(coverage_percent "$root/simple/coverage.xml")"
    git worktree remove --force "$base_tree"
    rmdir "$(dirname "$base_tree")"
  fi

  echo -e "#### Coverage of lines changed since $base"
  diff-cover simple/coverage.xml --compare-branch="$base" \
    ${COVERAGE_FAIL_UNDER:+--fail-under="$COVERAGE_FAIL_UNDER"}
  deactivate
}

function run_all_tests {
  run_lint_test
  run_py_test
}

function help {
  echo "Usage: $0 -aCcfhlp"
  echo "-a              Run all tests"
  echo "-c              Measure coverage of simple/ and of changed lines"
  echo "-C              Like -c, and also measure the base for before vs after"
  echo "-f              Fix lint"
  echo "-h              This usage"
  echo "-l              Run lint test"
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
    -c)
        echo -e "### Measuring coverage"
        run_coverage
        shift 1
        ;;
    -C)
        echo -e "### Measuring coverage, before vs after"
        run_coverage compare
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
    *)
        help
        exit 1
        ;;
    esac
done
