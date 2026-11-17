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
"""What a run writes besides its records: its output dirs and failure log."""

import pytest
from testsuite.lib import builders as b

_HANDSHAKE_LOG = (
    "Diagnostic error details were written to the GCS handshake path")

# An import whose config.json fails validation.
_BAD_CONFIG_IMPORT = {
    **b.import_subdir("demo"),
    "demo/config.json": '{"unknownKey": 1}',
}


def test_a_run_writes_only_its_jsonld_and_ingestion_report():
  result = b.run_dir(b.import_subdir("demo"),
                     runner_kwargs={"import_names": ["demo"]})
  assert result.error is None, result.error
  assert result.output_entries == ["jsonld", "preprocessor_reports"]


@pytest.mark.parametrize("workflow", [True, False], ids=["workflow", "local"])
def test_a_failed_run_mentions_the_handshake_only_if_it_wrote_one(
    workflow, caplog):
  result = b.run_dir(_BAD_CONFIG_IMPORT,
                     runner_kwargs={"import_names": ["demo"]},
                     workflow=workflow)
  assert result.error is not None
  # The handshake is written only for a run in an ingestion workflow.
  assert (result.output["handshake"] is not None) == workflow
  errors = [r.getMessage() for r in caplog.records if r.levelname == "ERROR"]
  assert any(_HANDSHAKE_LOG in m for m in errors) == workflow
