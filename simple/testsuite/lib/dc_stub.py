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
"""A local fake of the Data Commons REST API (v2 node + resolve).

The real util/dc_client.py talks to it over HTTP (via DC_API_ROOT), so the
client code, including from forked worker processes, runs unmodified.

The fake is backed by a tiny knowledge graph:

    {"country/USA": {"typeOf": ["Country"], "name": ["United States"]}}

Supported requests:
  /v2/node    property "->P"        values of P for each node
  /v2/node    property "<-typeOf"   all nodes of a type (with names)
  /v2/resolve property "<-P{typeOf:T}->dcid"  match by name/description/dcid
Anything else answers with an empty result and is recorded in `unexpected`.
"""

from http.server import BaseHTTPRequestHandler
from http.server import ThreadingHTTPServer
import json
import re
import threading
from typing import Any

_RESOLVE_PROP_RE = re.compile(r"^<-(\w+)(?:\{typeOf:([^}]+)\})?->dcid$")


class DcApiStub:

  def __init__(self, graph: dict[str, dict[str, Any]] | None = None) -> None:
    self.graph = {
        dcid: {
            p: v if isinstance(v, list) else [v] for p, v in props.items()
        } for dcid, props in (graph or {}).items()
    }
    self.requests: list[dict] = []
    self.unexpected: list[dict] = []
    self._server: ThreadingHTTPServer | None = None
    self._thread: threading.Thread | None = None

  @property
  def root(self) -> str:
    host, port = self._server.server_address
    return f"http://{host}:{port}"

  def __enter__(self) -> "DcApiStub":
    stub = self

    class Handler(BaseHTTPRequestHandler):

      def do_POST(self):  # pylint: disable=invalid-name
        length = int(self.headers.get("Content-Length", 0))
        body = json.loads(self.rfile.read(length) or b"{}")
        stub.requests.append({"path": self.path, "body": body})
        response = stub.answer(self.path, body)
        payload = json.dumps(response).encode()
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

      def log_message(self, *args):  # Silence the default stderr logging.
        pass

    self._server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    self._thread = threading.Thread(target=self._server.serve_forever,
                                    daemon=True)
    self._thread.start()
    return self

  def __exit__(self, *exc) -> None:
    self._server.shutdown()
    self._server.server_close()

  def answer(self, path: str, body: dict) -> dict:
    path = path.split("?", 1)[0]
    nodes = body.get("nodes", [])
    prop = body.get("property", "")
    if path == "/v2/node" and prop.startswith("->"):
      return self._out_arcs(nodes, prop[2:])
    if path == "/v2/node" and prop == "<-typeOf":
      return self._nodes_of_type(nodes)
    if path == "/v2/resolve":
      match = _RESOLVE_PROP_RE.match(prop)
      if match:
        return self._resolve(nodes, match.group(1), match.group(2))
    self.unexpected.append({"path": path, "body": body})
    return {}

  def _out_arcs(self, nodes: list[str], prop: str) -> dict:
    data = {}
    for dcid in nodes:
      values = self.graph.get(dcid, {}).get(prop, [])
      if values:
        data[dcid] = {
            "arcs": {
                prop: {
                    "nodes": [{
                        "dcid": v
                    } if prop == "typeOf" else {
                        "value": v
                    } for v in values]
                }
            }
        }
    return {"data": data}

  def _nodes_of_type(self, types: list[str]) -> dict:
    data = {}
    for entity_type in types:
      members = [{
          "dcid": dcid,
          "name": (props.get("name") or [dcid])[0]
      }
                 for dcid, props in self.graph.items()
                 if entity_type in props.get("typeOf", [])]
      data[entity_type] = {"arcs": {"typeOf": {"nodes": members}}}
    return {"data": data}

  def _resolve(self, names: list[str], prop: str, entity_type: str | None):
    entities = []
    for name in names:
      candidates = []
      for dcid, props in self.graph.items():
        types = props.get("typeOf", [])
        if entity_type and entity_type not in types:
          continue
        if name == dcid or name in props.get(prop, []) or name in props.get(
            "name", []):
          candidates.append({
              "dcid": dcid,
              "dominantType": types[0] if types else ""
          })
      entities.append({"node": name, "candidates": candidates})
    return {"entities": entities}
