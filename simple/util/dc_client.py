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
""" Data Commons REST API Client."""

import asyncio
from itertools import islice
import json
import logging
import os
import re

from httpx import AsyncClient
from httpx import Limits
import requests

from .ngram_matcher import NgramMatcher
from .resolvers import resolve_latlngs_2_s2cells

# Environment variables.
_KEY_ENV = "DC_API_KEY"
_API_ROOT_ENV = "DC_API_ROOT"

# Default REST API endpoint root.
_DEFAULT_API_ROOT = "https://api.datacommons.org"

_DEBUG = True
_DEBUG_FOLDER = ".data/debug"

NGRAM_MIN_MATCH_FRACTION = 0.8

# Place types support by the resolve API.
# Reference: https://source.corp.google.com/piper///depot/google3/datacommons/import/otherids/dc_ke_recon.cc;l=123
_RESOLVE_PLACE_TYPES = set([
    "Country", "State", "County", "City", "Village", "CensusCountyDivision",
    "SchoolDistrict", "ElementarySchoolDistrict", "HighSchoolDistrict",
    "UnifiedSchoolDistrict", "CensusZipCodeTabulationArea", "EurostatNUTS1",
    "EurostatNUTS2", "EurostatNUTS3", "AdministrativeArea1",
    "AdministrativeArea2", "AdministrativeArea3", "AdministrativeArea4",
    "AdministrativeArea5", "Neighborhood", "AdministrativeArea", "Place"
])

_MAX_NODES = 10_000

# Entities of type S2CellLevel* are resolved locally by applying a mapping function.
# Make the implementation more generic if more entity types are resolved via mapping functions.
_S2CELL_ENTITY_TYPE_PATTERN = r"S2CellLevel(\d+)"

# The maximum number of entities to include in a single DC call.
_BATCH_SIZE = 500

_HTTPX_LIMITS = Limits(max_keepalive_connections=5, max_connections=10)


def get_api_key():
  return os.environ.get(_KEY_ENV, "")


def get_api_root():
  return os.environ.get(_API_ROOT_ENV, _DEFAULT_API_ROOT)


if _DEBUG:
  logging.info("DC API Root: %s", get_api_root())
  logging.info("DC API Key: %s", get_api_key())
  os.makedirs(_DEBUG_FOLDER, exist_ok=True)


def resolve_entities(entities: list[str],
                     entity_type: str = None,
                     property_name: str = "description") -> dict[str, str]:
  if entity_type and re.match(_S2CELL_ENTITY_TYPE_PATTERN, entity_type):
    return resolve_latlngs_2_s2cells(entities, entity_type)

  if not entity_type or entity_type in _RESOLVE_PLACE_TYPES:
    return resolve_place_entities(entities=entities,
                                  entity_type=entity_type,
                                  property_name=property_name)

  return resolve_non_place_entities(entities=entities, entity_type=entity_type)


# See: https://docs.datacommons.org/api/rest/v2/resolve
def resolve_place_entities(
    entities: list[str],
    entity_type: str = None,
    property_name: str = "description") -> dict[str, str]:
  return asyncio.run(
      resolve_place_entities_async(entities, entity_type, property_name))


async def resolve_place_entities_async(
    entities: list[str],
    entity_type: str = None,
    property_name: str = "description") -> dict[str, str]:

  chunks = chunked(entities, _BATCH_SIZE)

  resolved: dict[str, str] = {}
  async with AsyncClient(limits=_HTTPX_LIMITS, timeout=None) as client:
    futures: dict[str, str] = [
        _resolve_place_entities_chunk(client, chunk, entity_type, property_name)
        for chunk in chunks
    ]
    for resolved_chunk in await asyncio.gather(*futures):
      resolved.update(resolved_chunk)

  return resolved


async def _resolve_place_entities_chunk(client: AsyncClient,
                                        entities_chunk: list[str],
                                        entity_type: str,
                                        property_name: str) -> dict[str, str]:
  type_of = f"{{typeOf:{entity_type}}}" if entity_type else ""
  data = {
      "nodes": entities_chunk,
      "property": f"<-{property_name}{type_of}->dcid",
  }
  # TODO: handle pagination.
  response = await post_async(client, path="/v2/resolve", data=data)
  resolved: dict[str, str] = {}
  for entity in response.get("entities", []):
    node = entity.get("node", "")
    candidates = entity.get("candidates", [])
    dcid = ""
    for candidate in candidates:
      curr_dcid = candidate.get("dcid", "")
      if curr_dcid:
        if not dcid:
          dcid = curr_dcid
          if not entity_type:
            break
        if entity_type and entity_type == candidate.get("dominantType", ""):
          dcid = curr_dcid
          break

    if node and dcid:
      resolved[node] = dcid

  return resolved


# See: https://docs.datacommons.org/api/rest/v2/node
def resolve_non_place_entities(entities: list[str],
                               entity_type: str = None) -> dict[str, str]:
  ngrams = NgramMatcher(config={"min_match_fraction": NGRAM_MIN_MATCH_FRACTION})

  all_entities, next_token = get_entities_of_type(entity_type=entity_type)
  while True:
    ngrams.add_keys_values(all_entities)
    if ngrams.get_tuples_count() >= _MAX_NODES:
      logging.warning("Nodes fetched truncated to: %s",
                      ngrams.get_tuples_count())
      break
    if next_token:
      all_entities, next_token = get_entities_of_type(entity_type=entity_type,
                                                      next_token=next_token)
    else:
      break

  if _DEBUG:
    entities_file = os.path.join(_DEBUG_FOLDER, f"{entity_type}_entities.json")
    logging.info("Writing %s entities to %s for debugging.", entity_type,
                 entities_file)
    with open(entities_file, "w") as file:
      json.dump(ngrams.get_key_values(), file, indent=1)

  resolved: dict[str, str] = {}
  for entity in entities:
    candidates = ngrams.lookup(key=entity)
    if candidates:
      _, dcid = candidates[0]
      resolved[entity] = dcid

  return resolved


# TODO: Cache results to file and return from cache if present.
def get_entities_of_type(entity_type: str,
                         next_token: str = None) -> tuple[dict[str, str], str]:
  data = {
      "nodes": [entity_type],
      "property": "<-typeOf",
  }
  if next_token:
    data["nextToken"] = next_token

  logging.info("Fetching nodes: %s", data)
  response = post(path="/v2/node", data=data)

  result: dict[str, str] = {}
  nodes = (response.get("data", {}).get(entity_type,
                                        {}).get("arcs",
                                                {}).get("typeOf",
                                                        {}).get("nodes", []))
  for node in nodes:
    name = node.get("name", "")
    dcid = node.get("dcid", "")
    if name and dcid:
      result[name] = dcid

  return result, response.get("nextToken", "")


# Returns a common entity type that the specified entity dcids resolve to.
# Returns an empty string if there is no common entity type
def resolve_entity_type(entity_dcids: list[str]) -> str:
  data = {
      "nodes": entity_dcids,
      "property": "->typeOf",
  }

  logging.info("Fetching entity types: %s", data)
  response = post(path="/v2/node", data=data)

  results: list[(str, set[str])] = []
  for entity_dcid, entity_data in response.get("data", {}).items():
    nodes = entity_data.get("arcs", {}).get("typeOf", {}).get("nodes", [])
    entity_types = [node.get("dcid") for node in nodes if node.get("dcid")]
    if entity_types:
      results.append((entity_dcid, set(entity_types)))

  logging.debug("Entity type results: %s", results)
  if not results or len(results) != len(entity_dcids):
    return ""

  common_entity_types: set[str] = set()
  for _, entity_types in results:
    if not common_entity_types:
      common_entity_types = entity_types
    else:
      common_entity_types &= entity_types

  return common_entity_types.pop() if common_entity_types else ""


def get_property_of_entities(entities: list[str],
                             property_name: str) -> dict[str, str]:
  return asyncio.run(get_property_of_entities_async(entities, property_name))


async def get_property_of_entities_async(entities: list[str],
                                         property_name: str) -> dict[str, str]:

  chunks = chunked(entities, _BATCH_SIZE)

  result: dict[str, str] = {}
  async with AsyncClient(limits=_HTTPX_LIMITS, timeout=None) as client:
    futures: dict[str, str] = [
        _get_property_of_entities_chunk(client, chunk, property_name)
        for chunk in chunks
    ]
    for result_chunk in await asyncio.gather(*futures):
      result.update(result_chunk)

  return result


async def _get_property_of_entities_chunk(client: AsyncClient,
                                          entities_chunk: list[str],
                                          property_name: str) -> dict[str, str]:
  data = {
      "nodes": entities_chunk,
      "property": f"->{property_name}",
  }

  logging.info("Fetching nodes: %s", data)
  response = await post_async(client, path="/v2/node", data=data)

  result_chunk: dict[str, str] = {}
  for entity_dcid, entity_data in response.get("data", {}).items():
    nodes = entity_data.get("arcs", {}).get(property_name, {}).get("nodes", [])
    values = [node.get("value") for node in nodes if node.get("value")]
    if values:
      result_chunk[entity_dcid] = values[0]

  return result_chunk


def post(path: str, data={}) -> dict:
  url = get_api_root() + path
  headers = {"Content-Type": "application/json"}
  api_key = get_api_key()
  if api_key:
    headers["x-api-key"] = api_key
  logging.debug("Request: %s", json.dumps(data, indent=1))
  resp = requests.post(url, json=data, headers=headers)
  response = resp.json()
  logging.debug("Response: %s", json.dumps(response, indent=1))
  if resp.status_code != 200:
    raise Exception(
        f'{resp.status_code}: {resp.reason}\n{response["message"]}\nRequest: {path}\n{data}'
    )
  return response


async def post_async(client: AsyncClient, path: str, data={}) -> dict:
  url = get_api_root() + path
  headers = {"Content-Type": "application/json"}
  api_key = get_api_key()
  if api_key:
    headers["x-api-key"] = api_key
  logging.debug("Request: %s", json.dumps(data, indent=1))
  async with asyncio.Semaphore(_HTTPX_LIMITS.max_connections):
    resp = await client.post(url, json=data, headers=headers)
  response = resp.json()
  logging.debug("Response: %s", json.dumps(response, indent=1))
  if resp.status_code != 200:
    raise Exception(
        f'{resp.status_code}: {resp.reason_phrase}\n{response["message"]}\nRequest: {path}\n{data}'
    )
  return response


def chunked(elements: list, chunk_size: int) -> list[list]:
  iterator = iter(elements)
  chunks = list()
  while chunk := list(islice(iterator, chunk_size)):
    chunks.append(chunk)
  return chunks
