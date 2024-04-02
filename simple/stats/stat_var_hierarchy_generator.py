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

from dataclasses import dataclass
import json
import logging
import re

from stats import schema_constants
from stats.data import Triple


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


@dataclass(eq=True, frozen=True)
class PropVal:
  prop: str
  val: str


class SVGTree:

  def __init__(self, parent_svg_id: str, pv: PropVal) -> None:
    self.svg_id = to_svg_id(parent_svg_id, pv.prop, pv.val)
    self.svg_name = strip_namespace(pv.val) or strip_namespace(pv.prop)
    self.parent_svg_id = parent_svg_id
    self.child_svgs: dict[PropVal, SVGTree] = {}
    # Using dict instead of set to maintain insertion order.
    self.sv_ids: dict[str, bool] = {}

  def insert_sv(self, sv_id: str, pvs: dict[str, str]):
    pv_list = [PropVal(p, v) for p, v in pvs.items()]
    for pv_index in range(len(pv_list)):
      self._insert_sv(sv_id, pv_list, pv_index, 0)

  def _insert_sv(self, sv_id: str, pvs: list[PropVal], pv_index: int,
                 num_pvs_inserted: int):
    num_pvs = len(pvs)
    if num_pvs_inserted == num_pvs:
      self.sv_ids[sv_id] = True
      return

    pv = pvs[pv_index]
    child_svg = self.child_svgs.setdefault(pv, SVGTree(self.svg_id, pv))
    child_svg._insert_sv(sv_id, pvs, (pv_index + 1) % num_pvs,
                         num_pvs_inserted + 1)

  # For testing.
  def json(self) -> dict:
    json = {}
    json["svg_id"] = self.svg_id
    json["svg_name"] = self.svg_name
    json["parent_svg_id"] = self.parent_svg_id
    json["sv_ids"] = list(self.sv_ids.keys())
    json["child_svgs"] = [
        child_svg.json() for child_svg in self.child_svgs.values()
    ]
    return json

  def __str__(self) -> str:
    return json.dumps(self.json(), indent=1)


def to_svg_id(parent_svg_id: str, prop: str, val: str) -> str:
  if not prop or prop == schema_constants.PREDICATE_POPULATION_TYPE:
    svg_id = ""
  else:
    svg_id = to_dcid_token(prop)

  if val:
    val_token = to_dcid_token(val)
    if svg_id:
      svg_id = f"{svg_id}-{val_token}"
    else:
      svg_id = val_token

  assert svg_id, f"Cannot generate SVG ID: {prop or 'N/A'} = {val or 'N/A'}"

  if parent_svg_id:
    svg_id = f"{parent_svg_id}_{svg_id}"
  else:
    svg_id = f"{schema_constants.CUSTOM_SVG_PREFIX}{svg_id}"
  return svg_id


def strip_namespace(s: str) -> str:
  # Trivially strip namespaces.
  # This could strip out something like "http:" in this case but we are not expecting URLs here
  # and stripping them out may still be ok for generating IDs.
  return s[s.find(":") + 1:]


def to_dcid_token(token: str) -> str:
  # Strip namespace.
  result = strip_namespace(token)

  # Remove all non-alphanumeric characters.
  result = re.sub("[^0-9a-zA-Z]+", "", result)
  return result


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
    # Maintaining order maintains results consistency and helps with tests.
    sv_ids: dict[str, bool] = {}
    dcid2poptype: dict[str, str] = {}
    dcid2pvs: dict[str, dict[str, str]] = {}

    for triple in triples:
      value = triple.object_id or triple.object_value
      if not value:
        logging.warning("Skipping, no value found for triple (%s).",
                        str(triple))
        continue

      if triple.predicate == schema_constants.PREDICATE_TYPE_OF:
        if value == schema_constants.TYPE_STATISTICAL_VARIABLE:
          sv_ids[triple.subject_id] = True
      elif triple.predicate == schema_constants.PREDICATE_POPULATION_TYPE:
        dcid2poptype[triple.subject_id] = value
      elif triple.predicate not in schema_constants.SV_HIERARCHY_PROPS_BLOCKLIST:
        pvs = dcid2pvs.setdefault(triple.subject_id, {})
        pvs[triple.predicate] = value

    # Filter SVs.
    for sv_id in sv_ids.keys():
      self.sv_id_2_population_type[sv_id] = dcid2poptype.get(
          sv_id, schema_constants.DEFAULT_POPULATION_TYPE)
      self.sv_id_2_pvs[sv_id] = dcid2pvs.get(sv_id, {})
