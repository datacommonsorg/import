# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Runs lib/run.py in another checkout and prints the canonical output.

Used by test_baseline.py: the baseline checkout gets a copy of this testsuite
and is run in a subprocess, so its own `stats` package is the one imported.

  python -m testsuite.lib.baseline_runner REQUEST_JSON OUTPUT_JSON

REQUEST_JSON: {"input_dir", "mode", "dc_graph", "runner_kwargs", "config_file"}
"""

import json
import logging
import sys


def main() -> None:
  logging.disable(logging.WARNING)
  from testsuite.lib import run  # pylint: disable=import-outside-toplevel
  with open(sys.argv[1]) as f:
    request = json.load(f)
  result = run.run_preprocessor(request["input_dir"],
                                request["mode"],
                                dc_graph=request.get("dc_graph"),
                                runner_kwargs=request.get("runner_kwargs"),
                                config_file=request.get("config_file"))
  output = {k: v for k, v in result.output.items() if k != "_traceback"}
  with open(sys.argv[2], "w") as f:
    json.dump(output, f)


if __name__ == "__main__":
  main()
