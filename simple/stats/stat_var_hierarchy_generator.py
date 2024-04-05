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
from dataclasses import replace
import json
import logging
import re

from stats import schema_constants
from stats.data import Triple


def generate(triples: list[Triple]) -> list[Triple]:
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""
  return generate_internal(triples).svg_triples


# Helper functions and classes


def generate_internal(triples: list[Triple]) -> "StatVarHierarchy":
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""

  # Extract SVs.
  svs = extract_svs(triples)
  # TODO: Create SVGs.
  svgs = create_all_svgs(svs)
  # TODO: Generate SVG triples.
  return StatVarHierarchy(svgs=svgs, svg_triples=[])


@dataclass(eq=True, frozen=True)
class PropVal:
  prop: str
  val: str

  def gen_pv_id(self) -> str:
    if self.val:
      return f"{to_dcid_token(self.prop)}-{to_dcid_token(self.val)}"
    return to_dcid_token(self.prop)

  def gen_pv_name(self) -> str:
    if self.val:
      return f"{capitalize(self.prop)} = {capitalize(self.val)}"
    return capitalize(self.prop)


@dataclass
class SVPropVals:
  sv_id: str
  population_type: str
  pvs: list[PropVal]

  def gen_svg_id(self):
    svg_id = f"{schema_constants.CUSTOM_SVG_PREFIX}{to_dcid_token(self.population_type)}"
    for pv in self.pvs:
      svg_id = f"{svg_id}_{pv.gen_pv_id()}"
    return svg_id

  def gen_svg_name(self):
    svg_name = capitalize(self.population_type)
    if self.pvs:
      pvs_str = ", ".join(f"{pv.gen_pv_name()}" for pv in self.pvs)
      svg_name = f"{svg_name} With {pvs_str}"
    return svg_name

  def with_pvs(self, pvs: list[PropVal]) -> "SVPropVals":
    return replace(self, pvs=pvs)


class SVG:

  def __init__(self, svg_id: str, svg_name: str) -> None:
    self.svg_id = svg_id
    self.svg_name = svg_name

    # Using dict instead of sets below to maintain insertion order.
    self.sv_ids: dict[str, bool] = {}
    self.parent_svg_ids: dict[str, bool] = {}
    self.child_svg_ids: dict[str, bool] = {}

    self.parent_svgs_processed: bool = False
    self.has_prop_without_val: bool = False
    self.sample_sv: SVPropVals | None = None

  def from_sv(sv: SVPropVals, insert_sv_id: bool = False) -> "SVG":
    svg = SVG(svg_id=sv.gen_svg_id(), svg_name=sv.gen_svg_name())
    svg.sample_sv = sv
    if insert_sv_id:
      svg.sv_ids[sv.sv_id] = True
    return svg

  # For testing.
  def json(self) -> dict:
    return {
        "svg_id": self.svg_id,
        "svg_name": self.svg_name,
        "sv_ids": list(self.sv_ids.keys()),
        "parent_svg_ids": list(self.parent_svg_ids.keys()),
        "child_svg_ids": list(self.child_svg_ids.keys())
    }

  def __str__(self) -> str:
    return json.dumps(self.json(), indent=1)


@dataclass
class StatVarHierarchy:
  svgs: dict[str, SVG]
  svg_triples: list[Triple]


def create_all_svgs(svs: list[SVPropVals]) -> dict[str, SVG]:
  svgs = create_leaf_svgs(svs)
  for svg_id in list(svgs.keys()):
    create_parent_svgs(svg_id, svgs)
  return svgs


# Create SVGs that the SVs are directly attached to.
def create_leaf_svgs(svs: list[SVPropVals]) -> dict[str, SVG]:
  svgs: dict[str, SVG] = {}
  for sv in svs:
    svg: SVG = SVG.from_sv(sv, insert_sv_id=True)
    svgs[svg.svg_id] = svg
  return svgs


def create_parent_svg(parent_sv: SVPropVals, svg: SVG, svgs: dict[str, SVG],
                      svg_has_prop_without_val: bool):
  parent_svg_id = parent_sv.gen_svg_id()
  parent_svg = svgs.setdefault(parent_svg_id, SVG.from_sv(parent_sv))
  svg.parent_svg_ids[parent_svg_id] = True
  parent_svg.child_svg_ids[svg.svg_id] = True
  if not parent_svg.parent_svgs_processed:
    parent_svg.has_prop_without_val = svg_has_prop_without_val
    create_parent_svgs(parent_svg_id, svgs)


def create_parent_svgs(svg_id: str, svgs: dict[str, SVG]):
  svg = svgs[svg_id]
  sv = svg.sample_sv

  # If not PVs left, we've reached the top of the population type hierarchy.
  # Attach it to the DC root and return.
  if not sv.pvs:
    svg.parent_svg_ids[schema_constants.ROOT_SVG_ID] = True
    return

  # Process SVGs without a val
  if svg.has_prop_without_val:
    parent_pvs: list[PropVal] = []
    for pv in sv.pvs:
      # Skip prop without val.
      if not pv.val:
        continue
      else:
        parent_pvs.append(pv)
    create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
                      svg=svg,
                      svgs=svgs,
                      svg_has_prop_without_val=False)
  # Process SVGs with vals.
  else:
    for pv1 in sv.pvs:
      parent_pvs: list[PropVal] = []
      for pv2 in sv.pvs:
        if pv1.prop == pv2.prop:
          # Remove val of one property at a time.
          parent_pvs.append(PropVal(pv2.prop, ""))
        else:
          parent_pvs.append(pv2)
      # Create parent SVG for each combination.
      create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
                        svg=svg,
                        svgs=svgs,
                        svg_has_prop_without_val=True)

  svg.parent_svgs_processed = True


def capitalize(s: str) -> str:
  if not s:
    return s
  return s[0].upper() + s[1:]


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
  return capitalize(result)


def extract_svs(triples: list[Triple]) -> list[SVPropVals]:
  """Extracts SVs from the input triples.
  The following SV properties used for generating the SV hierarchy are extracted:
  - dcid
  - population type
  - PVs not in SV_HIERARCHY_PROPS_BLOCKLIST
  """

  # Using dict instead of set to maintain order.
  # Maintaining order maintains results consistency and helps with tests.
  sv_ids: dict[str, bool] = {}

  dcid2poptype: dict[str, str] = {}
  dcid2pvs: dict[str, dict[str, str]] = {}

  for triple in triples:
    value = triple.object_id or triple.object_value
    if not value:
      logging.warning("Skipping, no value found for triple (%s).", str(triple))
      continue

    if triple.predicate == schema_constants.PREDICATE_TYPE_OF:
      if value == schema_constants.TYPE_STATISTICAL_VARIABLE:
        sv_ids[triple.subject_id] = True
    elif triple.predicate == schema_constants.PREDICATE_POPULATION_TYPE:
      dcid2poptype[triple.subject_id] = value
    elif triple.predicate not in schema_constants.SV_HIERARCHY_PROPS_BLOCKLIST:
      pvs = dcid2pvs.setdefault(triple.subject_id, {})
      pvs[triple.predicate] = value

  svs = []
  # Filter and populate SVs.
  for sv_id in sv_ids.keys():
    pop_type = dcid2poptype.get(sv_id, schema_constants.DEFAULT_POPULATION_TYPE)
    prop_vals: list[PropVal] = []
    # Sort prop vals by keys since we use this order to generate SVG IDs later.
    for (p, v) in sorted(dcid2pvs.get(sv_id, {}).items()):
      prop_vals.append(PropVal(p, v))
    svs.append(SVPropVals(sv_id, pop_type, prop_vals))

  return svs
