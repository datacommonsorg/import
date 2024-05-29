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

from proto.cache_data_pb2 import StatVarGroupNode
from proto.cache_data_pb2 import StatVarGroups
from stats import schema_constants as sc
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import Triple
from stats.db import Db


def generate_svg_cache(db: Db,
                       specialized_names: ParentSVG2ChildSpecializedNames):
  # TODO: get svg and sv triples from db, generate cache and write to DB.
  pass


def _generate_svg_cache_internal(
    svg_triples: list[Triple], sv_triples: list[Triple],
    specialized_names: ParentSVG2ChildSpecializedNames) -> StatVarGroups:
  return _SVGCache(svg_triples, sv_triples, specialized_names).stat_var_groups


class _SVGCache:

  def __init__(self, svg_triples: list[Triple], sv_triples: list[Triple],
               specialized_names: ParentSVG2ChildSpecializedNames) -> None:
    self.svg_nodes: dict[str, StatVarGroupNode] = {}
    self._create_svg_nodes(svg_triples, specialized_names)
    self._attach_svs(sv_triples)
    self.stat_var_groups = self._create_cache_proto()

  def _create_svg_nodes(self, svg_triples: list[Triple],
                        specialized_names: ParentSVG2ChildSpecializedNames):
    for triple in svg_triples:
      svg_id = triple.subject_id
      svg_node = self._get_or_create_svg_node(svg_id)
      value = triple.object_id or triple.object_value
      if not value:
        continue
      predicate = triple.predicate
      if predicate == sc.PREDICATE_NAME:
        svg_node.absolute_name = value
      elif predicate == sc.PREDICATE_SPECIALIZATION_OF:
        # Skip DC root.
        if value == sc.ROOT_SVG_ID:
          continue
        parent_svg_node = self._get_or_create_svg_node(value)
        specialized_entity = specialized_names.get(value, {}).get(svg_id, "")
        parent_svg_node.child_stat_var_groups.append(
            StatVarGroupNode.ChildSVG(id=svg_id,
                                      specialized_entity=specialized_entity))

  def _attach_svs(self, sv_triples: list[Triple]):
    sv_id_2_sv: dict[str, StatVarGroupNode.ChildSV] = {}

    for triple in sv_triples:
      sv_id = triple.subject_id
      sv = sv_id_2_sv.setdefault(sv_id, StatVarGroupNode.ChildSV(id=sv_id))
      value = triple.object_id or triple.object_value
      if not value:
        continue
      predicate = triple.predicate
      if predicate == sc.PREDICATE_NAME:
        sv.display_name = value
        sv.search_names.append(value)
      elif predicate == sc.PREDICATE_DESCRIPTION:
        sv.search_names.append(value)
      elif predicate == sc.PREDICATE_MEMBER_OF:
        svg_node = self.svg_nodes.get(value)
        if not svg_node:
          logging.warning("SVG not found: %s", value)
          continue
        svg_node.child_stat_vars.append(sv)

  def _create_cache_proto(self) -> StatVarGroups:
    proto = StatVarGroups()
    for svg_id, svg_node in self.svg_nodes.items():
      proto.stat_var_groups[svg_id].CopyFrom(svg_node)
    return proto

  def _get_or_create_svg_node(self, svg_id) -> StatVarGroupNode:
    return self.svg_nodes.setdefault(svg_id, StatVarGroupNode())
