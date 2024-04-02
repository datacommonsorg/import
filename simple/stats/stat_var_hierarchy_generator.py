# Copyright 2024 Google Inc.
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

import logging

from stats.data import Triple
import stats.stat_var_hierarchy_constants as svh_constants


class StatVarHierarchyGenerator:
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""

  def __init__(self, triples: list[Triple]) -> None:
    # Build SV PVs.
    self.sv_pvs = StatVarPVs(triples)
    # TODO: Create SVG + SV tree.
    # TODO: Generate SVG + SV triples.


class StatVarPVs:
  """An intermediate helper object that extracts SVs from input triples
  and puts their PVs in a dict for quick lookup later.
  It also maintains a separate dict for population types since SV hierarchies
  are rooted at a group representing their population type.
  """

  def __init__(self, triples: list[Triple]) -> None:
    self.sv_id_2_population_type: dict[str, str] = {}
    self.sv_id_2_pvs: dict[str, dict[str, str]] = {}

    # Collect all triples into pop type and pv dictionaries.
    # Also collect SV DCIDs for filtering subsequently.

    # Using dict instead of set to maintain order.
    sv_ids: dict[str, bool] = {}
    dcid2poptype: dict[str, str] = {}
    dcid2pvs: dict[str, dict[str, str]] = {}

    for triple in triples:
      value = triple.object_id or triple.object_value
      if not value:
        logging.warning("Skipping, no value found for triple (%s).",
                        str(triple))
        continue

      if triple.predicate == svh_constants.PREDICATE_TYPE_OF:
        if value == svh_constants.TYPE_STATISTICAL_VARIABLE:
          sv_ids[triple.subject_id] = True
      elif triple.predicate == svh_constants.PREDICATE_POPULATION_TYPE:
        dcid2poptype[triple.subject_id] = value
      elif triple.predicate not in svh_constants.SV_HIERARCHY_PROPS_BLOCKLIST:
        pvs = dcid2pvs.setdefault(triple.subject_id, {})
        pvs[triple.predicate] = value

    # Filter SVs.
    for sv_id in sv_ids.keys():
      self.sv_id_2_population_type[sv_id] = dcid2poptype.get(
          sv_id, svh_constants.DEFAULT_POPULATION_TYPE)
      self.sv_id_2_pvs[sv_id] = dcid2pvs.get(sv_id, {})
