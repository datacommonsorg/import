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
"""
Records the GCS requests that a preprocessor run sends to the fake GCS.

Each request is tagged with the Runner phase (a PHASES value) that sent it.

  with gcs_requests.record() as requests:
    b.run_dir(files, mode="gcs_input", runner_kwargs=...)
  requests.of("find_input_files")
"""

import contextlib
import dataclasses
import re
from typing import Iterator
from unittest import mock
from urllib.parse import unquote

import requests as http

# Runner methods whose requests are tagged with a phase. A request that
# none of them sends (e.g. the test staging its input) isn't recorded.
PHASES = {
    "_load_config": "load_config",
    "_find_and_filter_input_files": "find_input_files",
    "_validate_all_headers": "validate_headers",
}


@dataclasses.dataclass(frozen=True)
class Request:
  phase: str  # A PHASES value.
  # "list", "get" (metadata), "download" (of a whole object), "range" (a
  # ranged download), "upload" or the HTTP method.
  kind: str
  url: str  # Percent-decoded.


class Requests:
  """The requests of a run."""

  def __init__(self) -> None:
    self.all: list[Request] = []

  def of(self, phase: str, kind: str | None = None) -> list[Request]:
    return [
        r for r in self.all
        if r.phase == phase and (kind is None or r.kind == kind)
    ]


def _kind(method: str, url: str, headers: dict) -> str:
  if "/upload/" in url:
    return "upload"
  if "alt=media" in url:
    ranged = any(name.lower() == "range" for name in headers)
    return "range" if ranged else "download"
  if re.search(r"/b/[^/]+/o(\?|$)", url):
    return "list"
  if method == "GET":
    return "get"
  return method.lower()


@contextlib.contextmanager
def record() -> Iterator[Requests]:
  """Records the GCS requests that Runner sends in each of PHASES."""
  from stats.runner import Runner  # pylint: disable=import-outside-toplevel

  result = Requests()
  phase = {"name": None}
  real_request = http.Session.request

  def recording_request(session, method, url, *args, **kwargs):
    if phase["name"]:
      decoded = unquote(str(url))
      kind = _kind(method, decoded, kwargs.get("headers") or {})
      result.all.append(Request(phase["name"], kind, decoded))
    return real_request(session, method, url, *args, **kwargs)

  def in_phase(name, method):

    def wrapper(*args, **kwargs):
      previous, phase["name"] = phase["name"], name
      try:
        return method(*args, **kwargs)
      finally:
        phase["name"] = previous

    return wrapper

  with contextlib.ExitStack() as stack:
    stack.enter_context(
        mock.patch.object(http.Session, "request", recording_request))
    for method_name, name in PHASES.items():
      stack.enter_context(
          mock.patch.object(Runner, method_name,
                            in_phase(name, getattr(Runner, method_name))))
    yield result
