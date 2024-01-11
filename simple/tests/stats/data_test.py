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

import unittest

from stats.data import Event
from stats.data import Provenance
from stats.data import StatVar
from stats.data import StatVarGroup
from stats.data import Triple

SV_ID1 = "sv_id1"
SV_NAME1 = "SV Name1"
SV_DESCRIPTION1 = "SV Description1"
SV_SENTENCES1 = ["SV Sentence1", "SV SENTENCE2"]
SVG_ID1 = "svg_id1"
SVG_NAME1 = "SVG NAME1"
SVG_PARENT_ID1 = "svg_parent_id1"

EVENT_ID1 = "event1"
EVENT_TYPE1 = "EventType1"
EVENT_ENTITY_TYPE1 = "EntityType1"
EVENT_ENTITY1 = "entity1"
EVENT_DATE1 = "2024-01"
EVENT_PROVENANCE1 = "prov1"
EVENT_PROP1_TYPE = "prop1"
EVENT_PROP1_VALUE1 = "prop1_value1"
EVENT_PROP2_TYPE = "prop2"
EVENT_PROP2_VALUE1 = "prop2_value1"


class TestData(unittest.TestCase):

  def test_sv_triples_basic(self):
    sv = StatVar(SV_ID1, SV_NAME1)
    result = sv.triples()
    expected = [
        Triple(SV_ID1, "typeOf", object_id="StatisticalVariable"),
        Triple(SV_ID1, "name", object_value=SV_NAME1),
    ]
    self.assertListEqual(result, expected)

  def test_sv_triples_with_description(self):
    sv = StatVar(SV_ID1, SV_NAME1, description=SV_DESCRIPTION1)
    result = sv.triples()
    expected = [
        Triple(SV_ID1, "typeOf", object_id="StatisticalVariable"),
        Triple(SV_ID1, "name", object_value=SV_NAME1),
        Triple(SV_ID1, "description", object_value=SV_DESCRIPTION1),
    ]
    self.assertListEqual(result, expected)

  def test_sv_triples_with_nl_sentences(self):
    sv = StatVar(SV_ID1, SV_NAME1, nl_sentences=SV_SENTENCES1)
    result = sv.triples()
    expected = [
        Triple(SV_ID1, "typeOf", object_id="StatisticalVariable"),
        Triple(SV_ID1, "name", object_value=SV_NAME1),
    ]
    self.assertListEqual(result, expected)

  def test_sv_triples_with_group_id(self):
    sv = StatVar(SV_ID1, SV_NAME1, group_id=SVG_ID1)
    result = sv.triples()
    expected = [
        Triple(SV_ID1, "typeOf", object_id="StatisticalVariable"),
        Triple(SV_ID1, "name", object_value=SV_NAME1),
        Triple(SV_ID1, "memberOf", object_id=SVG_ID1),
    ]
    self.assertListEqual(result, expected)

  def test_sv_triples_with_provenances(self):
    sv = StatVar(SV_ID1, SV_NAME1)

    sv.add_provenance(Provenance("p1", "s1", ""))
    self.assertListEqual(sv.provenance_ids, ["p1"])
    self.assertListEqual(sv.source_ids, ["s1"])

    # Adding same provenance again should have no effect.
    sv.add_provenance(Provenance("p1", "s1", ""))
    self.assertListEqual(sv.provenance_ids, ["p1"])
    self.assertListEqual(sv.source_ids, ["s1"])

    # Same source, different provenance.
    sv.add_provenance(Provenance("p2", "s1", ""))
    self.assertListEqual(sv.provenance_ids, ["p1", "p2"])
    self.assertListEqual(sv.source_ids, ["s1"])

    # Another source, another provenance.
    sv.add_provenance(Provenance("p2", "s2", ""))
    self.assertListEqual(sv.provenance_ids, ["p1", "p2"])
    self.assertListEqual(sv.source_ids, ["s1", "s2"])

  def test_sv_triples_all(self):
    sv = StatVar(
        SV_ID1,
        SV_NAME1,
        description=SV_DESCRIPTION1,
        nl_sentences=SV_SENTENCES1,
        group_id=SVG_ID1,
    )
    result = sv.triples()
    expected = [
        Triple(SV_ID1, "typeOf", object_id="StatisticalVariable"),
        Triple(SV_ID1, "name", object_value=SV_NAME1),
        Triple(SV_ID1, "description", object_value=SV_DESCRIPTION1),
        Triple(SV_ID1, "memberOf", object_id=SVG_ID1),
    ]
    self.assertListEqual(result, expected)

  def test_svg_triples(self):
    svg = StatVarGroup(SVG_ID1, SVG_NAME1, SVG_PARENT_ID1)
    result = svg.triples()
    expected = [
        Triple(SVG_ID1, "typeOf", object_id="StatVarGroup"),
        Triple(SVG_ID1, "name", object_value=SVG_NAME1),
        Triple(SVG_ID1, "specializationOf", object_id=SVG_PARENT_ID1)
    ]
    self.assertListEqual(result, expected)

  def test_event_triples(self):
    event = Event(EVENT_ID1,
                  EVENT_TYPE1,
                  entity_type=EVENT_ENTITY_TYPE1,
                  entity=EVENT_ENTITY1,
                  date=EVENT_DATE1,
                  provenance_id=EVENT_PROVENANCE1,
                  properties={
                      EVENT_PROP1_TYPE: EVENT_PROP1_VALUE1,
                      EVENT_PROP2_TYPE: EVENT_PROP2_VALUE1
                  })
    result = event.triples()
    expected = [
        Triple(EVENT_ID1, "typeOf", object_id=EVENT_TYPE1),
        Triple(EVENT_ID1, EVENT_ENTITY_TYPE1, object_id=EVENT_ENTITY1),
        Triple(EVENT_ID1, "observationDate", object_value=EVENT_DATE1),
        Triple(EVENT_ID1, "includedIn", object_id=EVENT_PROVENANCE1),
        Triple(EVENT_ID1, EVENT_PROP1_TYPE, object_value=EVENT_PROP1_VALUE1),
        Triple(EVENT_ID1, EVENT_PROP2_TYPE, object_value=EVENT_PROP2_VALUE1)
    ]
    self.assertListEqual(result, expected)
