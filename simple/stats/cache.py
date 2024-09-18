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
"""
Includes logic for generating cache data.

Currently it only generates the SVG cache but any future ones will be added here as well.
(Unless the file size gets too unwieldy in which case it will be broken up.)
"""

import logging

from proto.cache_data_pb2 import StatVarGroupNode
from proto.cache_data_pb2 import StatVarGroups
from stats import schema_constants as sc
from stats.data import ParentSVG2ChildSpecializedNames
from stats.data import Triple
from stats.db import Db
from stats.util import gzip_and_base64_encode

STAT_VAR_GROUPS_CACHE_KEY = "StatVarGroups"


def generate_svg_cache(db: Db,
                       specialized_names: ParentSVG2ChildSpecializedNames):
  """Get svgs and sv triples from db, generate cache and write to DB."""
  svg_triples = db.select_triples_by_subject_type(
      sc.TYPE_STATISTICAL_VARIABLE_GROUP)
  sv_triples = db.select_triples_by_subject_type(sc.TYPE_STATISTICAL_VARIABLE)
  svgs = _generate_svg_cache_internal(svg_triples, sv_triples,
                                      specialized_names)
  db.insert_key_value(STAT_VAR_GROUPS_CACHE_KEY,
                      gzip_and_base64_encode(svgs.SerializeToString()))


# TODO: Move encode / decode methods into a util file.
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
    """
    Creates StatVarGroupNode protos from the SVG triples.
    Creates ChildSVG instances in these nodes based on specializationOf predicates.
    If specialized names are available, they are used to set the specialized_entity field in the ChildSVG instances.
    """
    for triple in svg_triples:
      svg_id = triple.subject_id
      svg_node = self._get_or_create_svg_node(svg_id)
      object_id = triple.object_id
      object_value = triple.object_value
      predicate = triple.predicate
      if predicate == sc.PREDICATE_NAME and object_value:
        svg_node.absolute_name = object_value
      elif predicate == sc.PREDICATE_SPECIALIZATION_OF and object_id:
        parent_svg_node = self._get_or_create_svg_node(object_id)
        specialized_entity = specialized_names.get(object_id,
                                                   {}).get(svg_id, "")
        parent_svg_node.child_stat_var_groups.append(
            StatVarGroupNode.ChildSVG(id=svg_id,
                                      specialized_entity=specialized_entity))

  def _attach_svs(self, sv_triples: list[Triple]):
    """
    Creates ChildSV protos from the SV triples.
    The ChildSVs are attached to the StatVarGroupNodes they are a memberOf.
    We don't compute the descendant counts here since they are computed in mixer itself.
    See: https://github.com/datacommonsorg/mixer/blob/master/internal/server/statvar/hierarchy/statvar_hierarchy_util.go#L286
    """
    sv_id_2_sv: dict[str, StatVarGroupNode.ChildSV] = {}

    for triple in sv_triples:
      sv_id = triple.subject_id
      sv = sv_id_2_sv.setdefault(sv_id, StatVarGroupNode.ChildSV(id=sv_id))
      object_id = triple.object_id
      object_value = triple.object_value
      predicate = triple.predicate
      if predicate == sc.PREDICATE_NAME and object_value:
        sv.display_name = object_value
        sv.search_names.append(object_value)
      elif predicate == sc.PREDICATE_DESCRIPTION and object_value:
        sv.search_names.append(object_value)
      elif predicate == sc.PREDICATE_MEMBER_OF and object_id:
        svg_node = self.svg_nodes.get(object_id)
        if not svg_node:
          logging.warning("SVG not found: %s", object_id)
          continue
        svg_node.child_stat_vars.append(sv)

  def _create_cache_proto(self) -> StatVarGroups:
    """
    Creates the final cached proto (StatVarGroups) from StatVarGroupNodes.
    """
    proto = StatVarGroups()
    for svg_id, svg_node in self.svg_nodes.items():
      proto.stat_var_groups[svg_id].CopyFrom(svg_node)
    return proto

  def _get_or_create_svg_node(self, svg_id) -> StatVarGroupNode:
    return self.svg_nodes.setdefault(svg_id, StatVarGroupNode())
