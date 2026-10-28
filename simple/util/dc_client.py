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
"""Data Commons REST API Client."""

import asyncio
from itertools import islice
import json
import logging
import os
import time

from httpx import AsyncClient
from httpx import Limits
import requests

# Environment variables.
_KEY_ENV = "DC_API_KEY"
_API_ROOT_ENV = "DC_API_ROOT"

# Default REST API endpoint root.
_DEFAULT_API_ROOT = "https://api.datacommons.org"

_DEBUG_ENV = "DC_CLIENT_DEBUG"

# Debug mode is off by default. It logs the DC API key and writes debug dumps
# to disk, so it must only be enabled explicitly for local troubleshooting.
_DEBUG = os.environ.get(_DEBUG_ENV, "").lower() in ("true", "1", "yes")
_DEBUG_FOLDER = ".data/debug"

_HTTPX_LIMITS = Limits(max_keepalive_connections=5, max_connections=10)


def get_api_key():
  return os.environ.get(_KEY_ENV, "")


def get_api_root():
  return os.environ.get(_API_ROOT_ENV, _DEFAULT_API_ROOT)


if _DEBUG:
  logging.info("DC API Root: %s", get_api_root())
  logging.info("DC API Key: %s", get_api_key())
  os.makedirs(_DEBUG_FOLDER, exist_ok=True)


def post(path: str, data={}) -> dict:
  url = get_api_root() + path
  headers = {"Content-Type": "application/json"}
  api_key = get_api_key()
  if api_key:
    headers["x-api-key"] = api_key
  logging.debug("Request: %s", json.dumps(data, indent=1))

  max_retries = 3
  base_delay = 1
  resp = None

  for attempt in range(max_retries):
    try:
      resp = requests.post(url, json=data, headers=headers)

      if resp.status_code == 200:
        return resp.json()

      if resp.status_code in [502, 503, 504]:
        logging.warning("Received %d from %s. Attempt %d/%d.", resp.status_code,
                        path, attempt + 1, max_retries)
      else:
        # Non-transient error, don't retry
        break

    except Exception as e:
      if attempt == max_retries - 1:
        raise
      logging.warning("Error calling %s: %s. Attempt %d/%d.", path, e,
                      attempt + 1, max_retries)

    delay = base_delay * (2**attempt)
    logging.info("Waiting %d seconds before retrying %s...", delay, path)
    time.sleep(delay)

  if resp is not None and resp.status_code != 200:
    try:
      response = resp.json()
      message = response.get("message", "")
    except Exception:
      message = resp.text
    raise Exception(
        f'{resp.status_code}: {resp.reason}\n{message}\nRequest: {path}')

  raise Exception(f"Failed to call {path} after {max_retries} attempts.")


async def post_async(client: AsyncClient, path: str, data={}) -> dict:
  url = get_api_root() + path
  headers = {"Content-Type": "application/json"}
  api_key = get_api_key()
  if api_key:
    headers["x-api-key"] = api_key
  logging.debug("Request: %s", json.dumps(data, indent=1))

  max_retries = 3
  base_delay = 1
  resp = None

  for attempt in range(max_retries):
    try:
      async with asyncio.Semaphore(_HTTPX_LIMITS.max_connections):
        resp = await client.post(url, json=data, headers=headers)

      if resp.status_code == 200:
        return resp.json()

      if resp.status_code in [502, 503, 504]:
        logging.warning("Received %d from %s. Attempt %d/%d.", resp.status_code,
                        path, attempt + 1, max_retries)
      else:
        # Non-transient error, don't retry
        break

    except Exception as e:
      if attempt == max_retries - 1:
        raise
      logging.warning("Error calling %s: %s. Attempt %d/%d.", path, e,
                      attempt + 1, max_retries)

    delay = base_delay * (2**attempt)
    logging.info("Waiting %d seconds before retrying %s...", delay, path)
    await asyncio.sleep(delay)

  if resp is not None and resp.status_code != 200:
    try:
      response = resp.json()
      message = response.get("message", "")
    except Exception:
      message = resp.text
    raise Exception(
        f'{resp.status_code}: {resp.reason_phrase}\n{message}\nRequest: {path}')

  raise Exception(f"Failed to call {path} after {max_retries} attempts.")


def chunked(elements: list, chunk_size: int) -> list[list]:
  iterator = iter(elements)
  chunks = list()
  while chunk := list(islice(iterator, chunk_size)):
    chunks.append(chunk)
  return chunks
