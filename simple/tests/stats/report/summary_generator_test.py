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
"""Tests for stats.report.summary_generator."""

import unittest
from unittest import mock

from stats.report.model.summary import RunStatus
from stats.report.summary_generator import SummaryGenerator


class TestSummaryGenerator(unittest.TestCase):
  """Tests for SummaryGenerator."""

  def test_write_to_json_never_raises(self):
    """A failure while writing the report is logged, not raised."""
    report_dir = mock.MagicMock()
    report_dir.open_file.side_effect = OSError("disk full")

    with self.assertLogs(level="ERROR"):
      result = SummaryGenerator("wf-123").write_to_json(RunStatus.FAILURE,
                                                        "2026-10-29T00:00:00Z",
                                                        report_dir)

    self.assertIsNone(result)


if __name__ == "__main__":
  unittest.main()
