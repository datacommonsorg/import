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

Request-count budgets keep per-file costs (e.g. a metadata request per input
file) from creeping back. Every request is tagged with the Runner phase that
sent it, or "worker" for worker processes, which inherit the patch when
forked; workers append to a shared log file.

  with gcs_requests.record() as requests:
    b.run_dir(files, mode="gcs_input", runner_kwargs=...)
  requests.of("find_input_files")
"""

import contextlib
import dataclasses
import os
import re
import tempfile
from typing import Iterator
from unittest import mock
from urllib.parse import unquote

import requests as http

# Runner methods whose requests are tagged with a phase. A request that
# none of them sends (e.g. the test staging its input) isn't recorded.
PHASES = {
    "__init__": "init",
    "_load_config": "load_config",
    "_find_and_filter_input_files": "find_input_files",
    "_validate_all_headers": "validate_headers",
    "_run_multiprocess_imports": "imports",
}
WORKER = "worker"


@dataclasses.dataclass(frozen=True)
class Request:
  phase: str  # A PHASES value, or WORKER.
  kind: str  # "list", "get" (metadata), "download", "upload" or the method.
  url: str  # Percent-decoded.


class Requests:
  """The requests of a run; filled in when `record()`'s block exits."""

  def __init__(self) -> None:
    self.all: list[Request] = []

  def of(self, phase: str, kind: str | None = None) -> list[Request]:
    return [
        r for r in self.all
        if r.phase == phase and (kind is None or r.kind == kind)
    ]


def _kind(method: str, url: str) -> str:
  if "/upload/" in url:
    return "upload"
  if "alt=media" in url:
    return "download"
  if re.search(r"/b/[^/]+/o(\?|$)", url):
    return "list"
  if method == "GET":
    return "get"
  return method.lower()


@contextlib.contextmanager
def record() -> Iterator[Requests]:
  """Records the GCS requests sent by Runner and its workers."""
  from stats.runner import Runner  # pylint: disable=import-outside-toplevel

  fd, log_path = tempfile.mkstemp(prefix="gcs-requests-", suffix=".tsv")
  os.close(fd)
  coordinator_pid = os.getpid()
  phase = {"name": None}
  real_request = http.Session.request

  def recording_request(session, method, url, *args, **kwargs):
    name = phase["name"] if os.getpid() == coordinator_pid else WORKER
    if name:
      decoded = unquote(str(url))
      with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{name}\t{_kind(method, decoded)}\t{decoded}\n")
    return real_request(session, method, url, *args, **kwargs)

  def in_phase(name, method):

    def wrapper(*args, **kwargs):
      previous, phase["name"] = phase["name"], name
      try:
        return method(*args, **kwargs)
      finally:
        phase["name"] = previous

    return wrapper

  result = Requests()
  try:
    with contextlib.ExitStack() as stack:
      stack.enter_context(
          mock.patch.object(http.Session, "request", recording_request))
      for method_name, name in PHASES.items():
        stack.enter_context(
            mock.patch.object(Runner, method_name,
                              in_phase(name, getattr(Runner, method_name))))
      yield result
    with open(log_path, encoding="utf-8") as f:
      result.all = [Request(*line.rstrip("\n").split("\t")) for line in f]
  finally:
    os.remove(log_path)
