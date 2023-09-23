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
""" Data Commons REST API Client.
"""

import os
import requests
from absl import logging
import json

from .ngram_matcher import NgramMatcher

# Environment variables.
_KEY_ENV = "DC_API_KEY"
_API_ROOT_ENV = "DC_API_ROOT"

# Default REST API endpoint root.
_DEFAULT_API_ROOT = "https://api.datacommons.org"


def get_api_key():
    return os.environ.get(_KEY_ENV, "")


def get_api_root():
    return os.environ.get(_API_ROOT_ENV, _DEFAULT_API_ROOT)


_DEBUG = True
_DEBUG_FOLDER = ".data/debug"

NGRAM_MIN_MATCH_FRACTION = 0.8

# Place types support by the resolve API.
_RESOLVE_PLACE_TYPES = set(
    ["Place", "Continent", "Country", "State", "Province", "City"])

_MAX_NODES = 10_000

if _DEBUG:
    logging.info("DC API Root: %s", get_api_root())
    logging.info("DC API Key: %s", get_api_key())
    os.makedirs(_DEBUG_FOLDER, exist_ok=True)


# See: https://docs.datacommons.org/api/rest/v2/resolve
def resolve_entities(entities: list[str],
                     entity_type: str = None) -> dict[str, str]:
    if not entity_type or entity_type in _RESOLVE_PLACE_TYPES:
        return resolve_place_entities(entities=entities,
                                      entity_type=entity_type)

    return resolve_non_place_entities(entities=entities,
                                      entity_type=entity_type)


# See: https://docs.datacommons.org/api/rest/v2/resolve
def resolve_place_entities(entities: list[str],
                           entity_type: str = None) -> dict[str, str]:
    type_of = f"{{typeOf:{entity_type}}}" if entity_type else ""
    data = {
        "nodes": entities,
        "property": f"<-description{type_of}->dcid",
    }
    response = post(path="/v2/resolve", data=data)

    resolved: dict[str, str] = {}
    for entity in response.get("entities", []):
        node = entity.get("node", "")
        candidates = entity.get("candidates", [])
        dcid = candidates[0].get("dcid", "") if candidates else ""
        if node and dcid:
            resolved[node] = dcid

    return resolved


# See: https://docs.datacommons.org/api/rest/v2/node
def resolve_non_place_entities(entities: list[str],
                               entity_type: str = None) -> dict[str, str]:
    ngrams = NgramMatcher(
        config={"min_match_fraction": NGRAM_MIN_MATCH_FRACTION})

    all_entities, next_token = get_entities_of_type(entity_type=entity_type)
    while True:
        ngrams.add_keys_values(all_entities)
        if ngrams.get_tuples_count() >= _MAX_NODES:
            logging.warning("Nodes fetched truncated to: %s",
                            ngrams.get_tuples_count())
            break
        if next_token:
            all_entities, next_token = get_entities_of_type(
                entity_type=entity_type, next_token=next_token)
        else:
            break

    if _DEBUG:
        entities_file = os.path.join(_DEBUG_FOLDER,
                                     f"{entity_type}_entities.json")
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
                         next_token: str = None) -> (dict[str, str], str):
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


def post(path: str, data={}) -> dict:
    url = get_api_root() + path
    headers = {"Content-Type": "application/json"}
    api_key = get_api_key()
    if api_key:
        headers["x-api-key"] = api_key
    resp = requests.post(url, json=data, headers=headers)
    if resp.status_code != 200:
        raise Exception(
            f'{resp.status_code}: {resp.reason}\n{resp.json()["message"]}')
    return resp.json()
