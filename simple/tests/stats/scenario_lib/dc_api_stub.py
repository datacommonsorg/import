# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""A local Data Commons REST API stub for scenario tests.

Why a real HTTP server instead of mock.patch:
- util/dc_client.py runs for real (request building, response parsing,
  retries), so scenarios test the code production runs.
- dc_client reads DC_API_ROOT at call time and spawned worker processes inherit
  environment variables, so the stub also serves the multiprocessing path,
  which module-level mocks cannot reach.

Fixtures come from the [dc_api] table of a scenario.toml:

  [dc_api.resolve]            # /v2/resolve: name -> dcid (any property)
  "California" = "geoId/06"

  [dc_api.types]              # /v2/node ->typeOf: dcid -> type
  "geoId/06" = "State"

  [dc_api.entities_of_type.Author]   # /v2/node <-typeOf: name -> dcid
  "Jane Doe" = "author/jane"

  [dc_api.faults]             # status codes returned before succeeding
  "/v2/node" = [503, "ok"]

Response shapes mirror what dc_client parses. Requests the stub does not
understand are recorded as unexpected so tests fail loudly instead of silently
receiving empty data.
"""

import http.server
import json
import re
import threading
from typing import Any

_RESOLVE_PROPERTY_RE = re.compile(
    r"^<-(?P<prop>[A-Za-z0-9_#]+)(\{typeOf:(?P<type>[A-Za-z0-9_]+)\})?->dcid$")
_OUT_ARC_RE = re.compile(r"^->(?P<prop>[A-Za-z0-9_]+)$")

ALLOWED_FIXTURE_KEYS = {"resolve", "types", "entities_of_type", "faults"}


class DcApiStub:
  """Serves /v2/resolve and /v2/node from scenario fixtures on localhost.

  Use as a context manager; `root` is the value for DC_API_ROOT.
  """

  def __init__(self, fixtures: dict[str, Any] | None = None) -> None:
    fixtures = fixtures or {}
    unknown = set(fixtures) - ALLOWED_FIXTURE_KEYS
    if unknown:
      raise ValueError(f"Unknown [dc_api] keys: {sorted(unknown)}")
    self._resolve: dict[str, str] = dict(fixtures.get("resolve", {}))
    self._types: dict[str, str] = dict(fixtures.get("types", {}))
    self._entities_of_type: dict[str, dict[str, str]] = {
        k: dict(v) for k, v in fixtures.get("entities_of_type", {}).items()
    }
    self._faults: dict[str, list] = {
        k: list(v) for k, v in fixtures.get("faults", {}).items()
    }
    self._lock = threading.Lock()
    self.requests: list[dict[str, Any]] = []
    self.unexpected: list[str] = []
    self._server: http.server.ThreadingHTTPServer | None = None
    self._thread: threading.Thread | None = None

  @property
  def root(self) -> str:
    assert self._server is not None, "stub is not running"
    host, port = self._server.server_address[:2]
    return f"http://{host}:{port}"

  def __enter__(self) -> "DcApiStub":
    stub = self

    class Handler(http.server.BaseHTTPRequestHandler):

      def do_POST(self):  # pylint: disable=invalid-name
        length = int(self.headers.get("Content-Length", 0))
        raw = self.rfile.read(length) if length else b"{}"
        try:
          body = json.loads(raw or b"{}")
        except json.JSONDecodeError:
          body = {}
        status, payload = stub.handle(self.path, body)
        data = json.dumps(payload).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

      def do_GET(self):  # pylint: disable=invalid-name
        stub.record_unexpected(f"GET {self.path}")
        self.send_response(404)
        self.end_headers()

      def log_message(self, *args):  # Keep test output quiet.
        pass

    self._server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    self._server.daemon_threads = True
    self._thread = threading.Thread(target=self._server.serve_forever,
                                    daemon=True)
    self._thread.start()
    return self

  def __exit__(self, *exc_info) -> None:
    if self._server:
      self._server.shutdown()
      self._server.server_close()
    if self._thread:
      self._thread.join(timeout=5)

  def record_unexpected(self, message: str) -> None:
    with self._lock:
      self.unexpected.append(message)

  def handle(self, path: str, body: dict[str, Any]) -> tuple[int, dict]:
    """Returns (status, payload) for one request. Public for unit tests."""
    with self._lock:
      self.requests.append({"path": path, "body": body})
      queue = self._faults.get(path)
      fault = queue.pop(0) if queue else "ok"
    if fault != "ok":
      return int(fault), {"message": f"injected fault {fault}"}
    if path == "/v2/resolve":
      return self._handle_resolve(body)
    if path == "/v2/node":
      return self._handle_node(body)
    self.record_unexpected(f"POST {path} {body}")
    return 404, {"message": f"stub does not serve {path}"}

  def _handle_resolve(self, body: dict) -> tuple[int, dict]:
    match = _RESOLVE_PROPERTY_RE.match(str(body.get("property", "")))
    if not match:
      self.record_unexpected(f"/v2/resolve property {body.get('property')}")
      return 400, {"message": "unsupported resolve property"}
    requested_type = match.group("type") or ""
    entities = []
    for node in body.get("nodes", []):
      dcid = self._resolve.get(node, "")
      candidates = []
      if dcid:
        candidates.append({
            "dcid": dcid,
            "dominantType": self._types.get(dcid, requested_type)
        })
      entities.append({"node": node, "candidates": candidates})
    return 200, {"entities": entities}

  def _handle_node(self, body: dict) -> tuple[int, dict]:
    prop = str(body.get("property", ""))
    nodes = body.get("nodes", [])
    if prop == "<-typeOf":
      data = {}
      for entity_type in nodes:
        members = self._entities_of_type.get(entity_type, {})
        data[entity_type] = {
            "arcs": {
                "typeOf": {
                    "nodes": [{
                        "dcid": dcid,
                        "name": name
                    } for name, dcid in members.items()]
                }
            }
        }
      return 200, {"data": data, "nextToken": ""}
    match = _OUT_ARC_RE.match(prop)
    if not match or match.group("prop") != "typeOf":
      self.record_unexpected(f"/v2/node property {prop}")
      return 400, {"message": "unsupported node property"}
    data = {}
    for dcid in nodes:
      entity_type = self._types.get(dcid)
      if entity_type:
        data[dcid] = {
            "arcs": {
                "typeOf": {
                    "nodes": [{
                        "dcid": entity_type,
                        "name": entity_type
                    }]
                }
            }
        }
      else:
        # The real API returns an empty object for unknown nodes.
        data[dcid] = {}
    return 200, {"data": data}
