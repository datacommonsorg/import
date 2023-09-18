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

# Environment variable for API key.
_KEY_ENV = "DC_API_KEY"


def get_api_key():
    return os.environ.get(_KEY_ENV, "")


# REST API endpoint root
_API_ROOT = "https://api.datacommons.org"


# See: https://docs.datacommons.org/api/rest/v2/resolve
def resolve_entities(entities: list[str],
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


def post(path: str, data={}) -> dict:
    url = _API_ROOT + path
    headers = {"Content-Type": "application/json"}
    api_key = get_api_key()
    if api_key:
        headers["x-api-key"] = api_key
    resp = requests.post(url, json=data, headers=headers)
    if resp.status_code != 200:
        raise Exception(
            f'{resp.status_code}: {resp.reason}\n{resp.json()["message"]}')
    return resp.json()
