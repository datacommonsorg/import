# Copyright 2026 Google Inc.
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
"""Fast batched node-existence verification against a Data Commons Mixer.

Verifies that referenced DCIDs not defined locally in the current import run
exist in a Data Commons / DCP Mixer instance via `/v2/node` (or DCP's
`/core/api/v2/node`) using `property="->typeOf"`.

Designed for low latency at billion-observation scale:
- Only distinct unresolved DCIDs are checked (e.g. ~95k for 1.064B Ozone rows).
- Concurrent HTTP requests over a pooled `requests.Session` (default 16 workers,
  batch size 500) with retry/backoff on 429 and 5xx.
- Optional persistent positive-only cache (local file or `gs://` object):
  known-existing DCIDs skip Mixer entirely on warm runs; missing DCIDs are
  never cached so newly-ingested nodes are always picked up.
"""

from concurrent.futures import ThreadPoolExecutor
from dataclasses import dataclass
from dataclasses import field
import logging
import os
import time
from typing import Iterable

import requests
from requests.adapters import HTTPAdapter

DEFAULT_BATCH_SIZE = 500
DEFAULT_CONCURRENCY = 16
DEFAULT_TIMEOUT_S = 30.0
DEFAULT_MAX_RETRIES = 3

_RETRYABLE_STATUS = frozenset({429, 500, 502, 503, 504})


def normalize_node_url(mixer_url: str) -> str:
  """Normalizes a Mixer base URL or endpoint URL to the v2 node endpoint."""
  url = mixer_url.strip().rstrip("/")
  if url.endswith("/v2/node"):
    return url
  if "api.datacommons.org" in url:
    return f"{url}/v2/node"
  return f"{url}/core/api/v2/node"


def _split_gcs(gs_path: str) -> tuple[str, str]:
  bucket, _, blob = gs_path[len("gs://"):].partition("/")
  return bucket, blob


def load_positive_cache(cache_path: str) -> set[str]:
  """Loads newline-delimited known-existing DCIDs from local disk or GCS."""
  if not cache_path:
    return set()
  try:
    if cache_path.startswith("gs://"):
      from google.cloud import storage  # pylint: disable=g-import-not-at-top
      bucket_name, blob_name = _split_gcs(cache_path)
      blob = storage.Client().bucket(bucket_name).blob(blob_name)
      if not blob.exists():
        return set()
      text = blob.download_as_text()
      return {line for line in text.splitlines() if line}
    if not os.path.exists(cache_path):
      return set()
    with open(cache_path, "r", encoding="utf-8") as f:
      return {line.strip() for line in f if line.strip()}
  except Exception as e:  # pylint: disable=broad-except
    logging.warning("Failed to load node cache from %s: %s", cache_path, e)
    return set()


def save_positive_cache(cache_path: str, dcids: set[str]) -> None:
  """Persists newline-delimited known-existing DCIDs to local disk or GCS."""
  if not cache_path or not dcids:
    return
  payload = "\n".join(sorted(dcids)) + "\n"
  try:
    if cache_path.startswith("gs://"):
      from google.cloud import storage  # pylint: disable=g-import-not-at-top
      bucket_name, blob_name = _split_gcs(cache_path)
      storage.Client().bucket(bucket_name).blob(blob_name).upload_from_string(
          payload, content_type="text/plain")
      return
    parent = os.path.dirname(os.path.abspath(cache_path))
    if parent:
      os.makedirs(parent, exist_ok=True)
    tmp_path = f"{cache_path}.tmp.{os.getpid()}"
    with open(tmp_path, "w", encoding="utf-8") as f:
      f.write(payload)
    os.replace(tmp_path, cache_path)
  except Exception as e:  # pylint: disable=broad-except
    logging.warning("Failed to save node cache to %s: %s", cache_path, e)


@dataclass
class NodeResolutionResult:
  """Outcome and timing breakdown of a Mixer node-existence check."""
  mixer_url: str
  total_unresolved: int = 0
  cache_hits: int = 0
  mixer_queried: int = 0
  mixer_found: int = 0
  mixer_missing: int = 0
  mixer_calls: int = 0
  mixer_errors: int = 0
  missing_dcids: list[str] = field(default_factory=list)
  cache_load_seconds: float = 0.0
  mixer_seconds: float = 0.0
  cache_save_seconds: float = 0.0

  @property
  def total_seconds(self) -> float:
    return self.cache_load_seconds + self.mixer_seconds + self.cache_save_seconds

  def to_dict(self) -> dict:
    return {
        "mixer_url": self.mixer_url,
        "total_unresolved": self.total_unresolved,
        "cache_hits": self.cache_hits,
        "mixer_queried": self.mixer_queried,
        "mixer_found": self.mixer_found,
        "mixer_missing": self.mixer_missing,
        "mixer_calls": self.mixer_calls,
        "mixer_errors": self.mixer_errors,
        "cache_load_seconds": round(self.cache_load_seconds, 3),
        "mixer_seconds": round(self.mixer_seconds, 3),
        "cache_save_seconds": round(self.cache_save_seconds, 3),
        "total_seconds": round(self.total_seconds, 3),
    }


class NodeExistenceResolver:
  """Verifies DCID existence against a Mixer `/v2/node` endpoint."""

  def __init__(self,
               mixer_url: str,
               api_key: str = "",
               cache_path: str = "",
               batch_size: int = DEFAULT_BATCH_SIZE,
               concurrency: int = DEFAULT_CONCURRENCY,
               timeout_s: float = DEFAULT_TIMEOUT_S,
               max_retries: int = DEFAULT_MAX_RETRIES) -> None:
    self.node_url = normalize_node_url(mixer_url)
    self.api_key = api_key
    self.cache_path = cache_path
    self.batch_size = max(1, batch_size)
    self.concurrency = max(1, concurrency)
    self.timeout_s = timeout_s
    self.max_retries = max(1, max_retries)

  @classmethod
  def from_env(cls) -> "NodeExistenceResolver | None":
    """Constructs a resolver from environment variables, or None if unset."""
    mixer_url = os.environ.get("DC_MIXER_URL", "").strip()
    if not mixer_url:
      return None
    return cls(
        mixer_url=mixer_url,
        api_key=os.environ.get("DC_API_KEY", "").strip(),
        cache_path=os.environ.get("PP_NODE_CACHE_PATH", "").strip(),
        batch_size=int(
            os.environ.get("PP_NODE_RESOLVER_BATCH_SIZE", DEFAULT_BATCH_SIZE)),
        concurrency=int(
            os.environ.get("PP_NODE_RESOLVER_CONCURRENCY",
                           DEFAULT_CONCURRENCY)),
    )

  def _make_session(self) -> requests.Session:
    session = requests.Session()
    adapter = HTTPAdapter(pool_connections=self.concurrency,
                          pool_maxsize=self.concurrency)
    session.mount("https://", adapter)
    session.mount("http://", adapter)
    headers = {"Content-Type": "application/json"}
    if self.api_key:
      headers["X-API-Key"] = self.api_key
    session.headers.update(headers)
    return session

  def _query_batch(self, session: requests.Session,
                   batch: list[str]) -> tuple[set[str], set[str], bool]:
    """Queries one batch of DCIDs. Returns (found, missing, had_error)."""
    payload = {"nodes": batch, "property": "->typeOf"}
    last_err: Exception | None = None
    for attempt in range(self.max_retries):
      try:
        resp = session.post(self.node_url, json=payload, timeout=self.timeout_s)
        if resp.status_code in _RETRYABLE_STATUS:
          last_err = RuntimeError(f"HTTP {resp.status_code}: {resp.text[:200]}")
          time.sleep(min(2.0**attempt * 0.25, 4.0))
          continue
        resp.raise_for_status()
        data = resp.json().get("data", {})
        found = set()
        missing = set()
        for dcid in batch:
          entry = data.get(dcid)
          if entry and entry.get("arcs", {}).get("typeOf", {}).get("nodes"):
            found.add(dcid)
          else:
            missing.add(dcid)
        return found, missing, False
      except (requests.RequestException, ValueError) as e:
        last_err = e
        if attempt + 1 < self.max_retries:
          time.sleep(min(2.0**attempt * 0.25, 4.0))
    logging.warning("Mixer batch lookup failed after %d attempts (%d dcids): %s",
                    self.max_retries, len(batch), last_err)
    return set(), set(), True

  def verify(self, unresolved_dcids: Iterable[str]) -> NodeResolutionResult:
    """Checks `unresolved_dcids` against cache + Mixer."""
    unresolved = set(unresolved_dcids)
    res = NodeResolutionResult(mixer_url=self.node_url,
                               total_unresolved=len(unresolved))
    if not unresolved:
      return res

    t0 = time.perf_counter()
    cached = load_positive_cache(self.cache_path) if self.cache_path else set()
    res.cache_load_seconds = time.perf_counter() - t0

    to_query = sorted(unresolved - cached)
    res.cache_hits = len(unresolved) - len(to_query)
    res.mixer_queried = len(to_query)
    if not to_query:
      return res

    batches = [
        to_query[i:i + self.batch_size]
        for i in range(0, len(to_query), self.batch_size)
    ]
    res.mixer_calls = len(batches)

    t1 = time.perf_counter()
    newly_found: set[str] = set()
    missing: list[str] = []
    session = self._make_session()
    try:
      if self.concurrency == 1 or len(batches) == 1:
        results = [self._query_batch(session, b) for b in batches]
      else:
        with ThreadPoolExecutor(
            max_workers=min(self.concurrency, len(batches))) as ex:
          results = list(ex.map(lambda b: self._query_batch(session, b),
                                batches))
    finally:
      session.close()
    res.mixer_seconds = time.perf_counter() - t1

    for found_b, missing_b, had_err in results:
      if had_err:
        res.mixer_errors += 1
      newly_found.update(found_b)
      missing.extend(missing_b)

    missing.sort()
    res.mixer_found = len(newly_found)
    res.mixer_missing = len(missing)
    res.missing_dcids = missing

    if self.cache_path and newly_found:
      t2 = time.perf_counter()
      cached.update(newly_found)
      save_positive_cache(self.cache_path, cached)
      res.cache_save_seconds = time.perf_counter() - t2

    return res
