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
"""Resolvers that resolve entities by applying local functions.

Resolve methods must be of the form resolve_* and take 2 arguments:
- a list of string entities
- the entity type

The result should be a dictionary of input value to resolved entity value.
Values that could not be resolved should not be included in the result.
"""
import s2sphere
import re
import logging

_S2CELL_ENTITY_TYPE_PATTERN = r"S2CellLevel(\d+)"
_LAT_LNG_PATTERN = r"(.+)#(.+)"


# Resolves latlngs to s2cell dcids.
# e.g. "38.7#-119.4" (SF) for type "S2CellLevel10" resolves to "s2CellId/0x80982b0000000000"
def resolve_latlngs_2_s2cells(latlngs: list[str],
                              entity_type: str) -> dict[str, str]:
    matcher = re.match(_S2CELL_ENTITY_TYPE_PATTERN, entity_type)
    assert matcher is not None, f"Unsupported entity type: {entity_type}"
    level = int(matcher.group(1))
    result = {}
    for latlng in latlngs:
        s2cell = _latlng_2_s2cell_dcid(level, _parse_latlng(latlng))
        if s2cell:
            result[latlng] = s2cell
    return result


def _parse_latlng(latlng: str) -> s2sphere.LatLng:
    matcher = re.match(_LAT_LNG_PATTERN, latlng)
    if matcher is None:
        logging.warning("Cannot parse latlng: %s", latlng)
        return None
    lat, lng = matcher.group(1).strip(), matcher.group(2).strip()
    try:
        return s2sphere.LatLng.from_degrees(float(lat), float(lng))
    except Exception:
        logging.warning("Invalid latlng: %s", latlng)
        return None


def _latlng_2_s2cell_dcid(level: int, latlng: s2sphere.LatLng) -> str:
    assert level >= 0 and level <= 30

    cell = s2sphere.CellId.from_lat_lng(latlng)
    if level < 30:
        cell = cell.parent(level)
    return 's2CellId/{0:#0{1}x}'.format(cell.id(), 18)
