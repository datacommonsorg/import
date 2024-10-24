# Copyright 2023 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import unittest
import pandas as pd
from pandas.testing import assert_frame_equal
from differ import DatasetDiffer

module_dir = os.path.dirname(__file__)


class TestDiffer(unittest.TestCase):
  """
  Test Class to compare expected output in test/ directory to the
  output generated by DatasetDiffer class
  """

  def test_diff_analysis(self):
    groupby_columns = "variableMeasured,observationAbout,observationDate"
    value_columns = "value"

    differ = DatasetDiffer(groupby_columns, value_columns)
    current = differ.load_mcf_file(
      os.path.join(module_dir, "test", "current.mcf"))
    previous = differ.load_mcf_file(
      os.path.join(module_dir, "test", "previous.mcf"))
      
    in_data = differ.process_data(previous, current)
    summary, result = differ.point_analysis(in_data)
    result = pd.read_csv(os.path.join(module_dir, "test", "result1.csv"))
    assert_frame_equal(summary, result)

    summary, result = differ.series_analysis(in_data)
    result = pd.read_csv(os.path.join(module_dir, "test", "result2.csv"))
    assert_frame_equal(summary, result)



if __name__ == "__main__":
  unittest.main()
