# Copyright 2023 Google Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import pandas as pd
from stats.data import Triple
from util.filesystem import File


class GraphWriter:
  """Sink for the triples and observations an import produces.

  JsonLdStreamWriter is the only production implementation; it streams JSON-LD
  shards to local disk or GCS. This stays an interface because the importers
  are written against it and tests substitute an in-memory recorder.
  """

  def write_triples(self,
                    triples: list[Triple],
                    input_file: File = None,
                    provenance_dir: str = None):
    pass

  def write_observations(self, observations_df: pd.DataFrame, input_file: File):
    """Writes observations from a DataFrame.

    Args:
      observations_df: DataFrame with columns [entity, variable, date, value,
                       provenance, unit, scaling_factor, measurement_method,
                       observation_period, properties] - all transformations applied
      input_file: Source file for context
    """
    pass

  def commit(self):
    """Flushes everything written so far, leaving the writer open."""
    pass

  def commit_and_close(self):
    pass
