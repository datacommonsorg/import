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

from datetime import datetime
from enum import auto
from enum import Enum
import json

from util.filesystem import File


class Status(Enum):
  NOT_STARTED = auto()
  STARTED = auto()
  SUCCESS = auto()
  FAILURE = auto()


class ImportReporter:
  """Writes the run's status to `report_file` (report.json in the process
  directory) each time it changes."""

  def __init__(self, report_file: File) -> None:
    self.report_file = report_file
    self.status = Status.NOT_STARTED
    self.error: str | None = None
    self.start_time = None
    self.last_update = datetime.now()

  def report_started(self):
    self.status = Status.STARTED
    self.start_time = datetime.now()
    self._save()

  def report_done(self):
    self.status = Status.SUCCESS
    self._save()

  def report_failure(self, error: str):
    self.status = Status.FAILURE
    self.error = error
    self._save()

  def json(self) -> dict:
    report = {"status": self.status.name}
    if self.error:
      report["error"] = self.error
    if self.start_time:
      report["startTime"] = str(self.start_time)
      report["lastUpdate"] = str(self.last_update)
    return report

  def _save(self) -> None:
    self.last_update = datetime.now()
    self.report_file.write(json.dumps(self.json(), indent=2))
