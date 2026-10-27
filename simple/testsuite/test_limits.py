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
"""Cases that cannot be written by hand in a case file (e.g. huge values)."""

import pytest
from testsuite.lib import run
from testsuite.lib import strategies as gen

_CONFIG = {
    "inputFiles": [{
        "pattern": "*.mcf",
        "provenance": "dcid:TestProvenance"
    }],
    "importProxyEntities": False,
}


@pytest.mark.parametrize("length,fails", [(2**16 - 1, False), (2**16, True)])
def test_mcf_value_length_limit(length, fails):
  """MCF values are limited to 65535 characters (a MySQL TEXT column)."""
  mcf = f'Node: dcid:Big\ntypeOf: dcs:Thing\nname: "{"x" * length}"\n'
  output = run.run_preprocessor(
      run.write_input_dir({
          "provenance.mcf": gen.PROVENANCE_MCF,
          "big.mcf": mcf
      }, _CONFIG)).output
  if fails:
    assert output["error"]["type"] == "ValueError"
    assert "too long" in output["error"]["message"]
    assert output["handshake"]["status"] == "FAILURE"
  else:
    assert output["error"] is None
    node = output["folders"]["TestProvenance"]["nodes"]["dcid:Big"]
    assert len(node["dcid:name"]) == length
