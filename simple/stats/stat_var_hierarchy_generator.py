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
from typing import Self

from stats import schema_constants as sc
from stats.data import Triple


def generate(triples: list[Triple]) -> list[Triple]:
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""
  return _generate_internal(triples).svg_triples


# Helper functions and classes


# TODO: Pruning (e.g. ignore Thing).
def _generate_internal(triples: list[Triple]) -> "StatVarHierarchy":
  """Given a list of input triples (including stat vars), 
generates a SV hierarchy and returns a list of output triples
representing the hierarchy.
"""

  # Extract SVs.
  svs = _extract_svs(triples)
  # Create SVGs.
  svgs = _create_all_svgs(svs)
  # Sort by SVG ID so it's easier to follow the hierarchy.
  svgs = dict(sorted(svgs.items()))
  # Generate SVG triples.
  svg_triples = _create_all_svg_triples(svgs)
  return StatVarHierarchy(svgs=svgs, svg_triples=svg_triples)


@dataclass(eq=True, frozen=True)
class PropVal:
  prop: str
  val: str

  def gen_pv_id(self) -> str:
    if self.val:
      return f"{_to_dcid_token(self.prop)}-{_to_dcid_token(self.val)}"
    return _to_dcid_token(self.prop)

  def gen_pv_name(self) -> str:
    if self.val:
      return f"{_capitalize_and_split(self.prop)} = {_capitalize_and_split(self.val)}"
    return _capitalize_and_split(self.prop)


# TODO: DPV handling.
@dataclass
class SVPropVals:
  sv_id: str
  population_type: str
  # The PVs here are ordered by prop.
  # They are originally ordered in the extract_svs method
  # and maintain the order thereafter.
  pvs: list[PropVal]

  def gen_svg_id(self):
    svg_id = f"{sc.CUSTOM_SVG_PREFIX}{_to_dcid_token(self.population_type)}"
    for pv in self.pvs:
      svg_id = f"{svg_id}_{pv.gen_pv_id()}"
    return svg_id

  def gen_svg_name(self):
    svg_name = _capitalize_and_split(self.population_type)
    if self.pvs:
      pvs_str = ", ".join(map(lambda pv: pv.gen_pv_name(), self.pvs))
      svg_name = f"{svg_name} With {pvs_str}"
    return svg_name

  # Creates and returns a new SVPropVals object with the same fields as this object
  # except for PVs which are set to the specified list.
  def with_pvs(self, pvs: list[PropVal]) -> Self:
    return replace(self, pvs=pvs)


class SVG:

  def __init__(self, svg_id: str, svg_name: str) -> None:
    self.svg_id = svg_id
    self.svg_name = svg_name

    # Using dict instead of sets below to maintain insertion order.
    # Maintaining order maintains results consistency and helps with tests.
    self.sv_ids: dict[str, bool] = {}
    self.parent_svg_ids: dict[str, bool] = {}
    self.child_svg_ids: dict[str, bool] = {}

    self.parent_svgs_processed: bool = False
    # Only relevant for PV hierarchy.
    # Indicates whether this SVG is for PVs where
    # one of the PVs does not have a value.
    self.has_prop_without_val: bool = False
    self.sample_sv: SVPropVals | None = None

  def triples(self) -> list[Triple]:
    triples: list[Triple] = []

    # SVG info
    triples.append(
        Triple(self.svg_id,
               sc.PREDICATE_TYPE_OF,
               object_id=sc.TYPE_STATISTICAL_VARIABLE_GROUP))
    triples.append(
        Triple(self.svg_id, sc.PREDICATE_NAME, object_value=self.svg_name))

    # SVG parents ("specializationOf")
    for parent_svg_id in self.parent_svg_ids.keys():
      triples.append(
          Triple(self.svg_id,
                 sc.PREDICATE_SPECIALIZATION_OF,
                 object_id=parent_svg_id))

    # SV members ("memberOf")
    for sv_id in self.sv_ids:
      triples.append(
          Triple(sv_id, sc.PREDICATE_MEMBER_OF, object_id=self.svg_id))

    return triples

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
  # Dict from SVG dcid to SVG.
  svgs: dict[str, SVG]
  svg_triples: list[Triple]


def _get_or_create_svg(svgs: dict[str, SVG], sv: SVPropVals) -> SVG:
  svg_id = sv.gen_svg_id()
  svg = svgs.get(svg_id)
  if not svg:
    svg = SVG(svg_id=svg_id, svg_name=sv.gen_svg_name())
    svg.sample_sv = sv
    svgs[svg_id] = svg
  return svg


def _create_all_svg_triples(svgs: dict[str, SVG]):
  triples: list[Triple] = []
  for svg in svgs.values():
    triples.extend(svg.triples())
  return triples


def _create_all_svgs(svs: list[SVPropVals]) -> dict[str, SVG]:
  svgs = _create_leaf_svgs(svs)
  for svg_id in list(svgs.keys()):
    _create_parent_svgs(svg_id, svgs)
  return svgs


# Create SVGs that the SVs are directly attached to.
def _create_leaf_svgs(svs: list[SVPropVals]) -> dict[str, SVG]:
  svgs: dict[str, SVG] = {}
  for sv in svs:
    svg = _get_or_create_svg(svgs, sv)
    # Insert SV into SVG.
    svg.sv_ids[sv.sv_id] = True
  return svgs


def _create_parent_svg(parent_sv: SVPropVals, svg: SVG, svgs: dict[str, SVG],
                       svg_has_prop_without_val: bool):
  parent_svg = _get_or_create_svg(svgs, parent_sv)

  # Add parent child relationships.
  svg.parent_svg_ids[parent_svg.svg_id] = True
  parent_svg.child_svg_ids[svg.svg_id] = True

  if not parent_svg.parent_svgs_processed:
    parent_svg.has_prop_without_val = svg_has_prop_without_val
    _create_parent_svgs(parent_svg.svg_id, svgs)


def _create_parent_svgs(svg_id: str, svgs: dict[str, SVG]):
  svg = svgs[svg_id]
  sv = svg.sample_sv

  # If no PVs left, we've reached the top of the population type hierarchy.
  # Attach it to the DC root and return.
  if not sv.pvs:
    svg.parent_svg_ids[sc.ROOT_SVG_ID] = True
    return

  # Process SVGs without a val
  # e.g. The SVG c/g/Person_Gender_Race-Asian represents a SVG for
  # persons of all genders with race = Asian. In this case,
  # the prop gender does not have a val.
  if svg.has_prop_without_val:
    parent_pvs: list[PropVal] = []
    for pv in sv.pvs:
      # Skip prop without val.
      if not pv.val:
        continue
      else:
        parent_pvs.append(pv)
    _create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
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
      _create_parent_svg(parent_sv=sv.with_pvs(parent_pvs),
                         svg=svg,
                         svgs=svgs,
                         svg_has_prop_without_val=True)

  svg.parent_svgs_processed = True


def _split_camel_case(s: str) -> str:
  """Splits camel case strings into separate words with spaces.

  e.g. "CamelCaseString" => "Camel Case String"
  """
  return re.sub(r"([A-Z])", r" \1", s).strip()


# s.capitalize() turns "energySource" into "Energysource" instead of "EnergySource"
# hence this method.
def _capitalize(s: str) -> str:
  if not s:
    return s
  return s[0].upper() + s[1:]


# Capitalizes the first letter and then splits any camel case strings.
# e.g. "energySource" -> "EnergySource" -> "Energy Source"
def _capitalize_and_split(s: str) -> str:
  return _split_camel_case(_capitalize(s))


def _to_dcid_token(token: str) -> str:
  # Remove all non-alphanumeric characters.
  result = re.sub("[^0-9a-zA-Z]+", "", token)
  return _capitalize(result)


def _extract_svs(triples: list[Triple]) -> list[SVPropVals]:
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

    if triple.predicate == sc.PREDICATE_TYPE_OF:
      if value == sc.TYPE_STATISTICAL_VARIABLE:
        sv_ids[triple.subject_id] = True
    elif triple.predicate == sc.PREDICATE_POPULATION_TYPE:
      dcid2poptype[triple.subject_id] = value
    elif triple.predicate not in sc.SV_HIERARCHY_PROPS_BLOCKLIST:
      pvs = dcid2pvs.setdefault(triple.subject_id, {})
      pvs[triple.predicate] = value

  svs = []
  # Filter and populate SVs.
  for sv_id in sv_ids.keys():
    pop_type = dcid2poptype.get(sv_id, sc.DEFAULT_POPULATION_TYPE)
    prop_vals: list[PropVal] = []
    # Sort prop vals by keys since we use this order to generate SVG IDs later.
    for (p, v) in sorted(dcid2pvs.get(sv_id, {}).items()):
      prop_vals.append(PropVal(p, v))
    svs.append(SVPropVals(sv_id, pop_type, prop_vals))

  return svs
