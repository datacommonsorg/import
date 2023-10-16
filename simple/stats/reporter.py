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
from functools import wraps
import json
import os
import sys

# For importing util
_CODEDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, os.path.join(_CODEDIR, "../"))

from util.filehandler import FileHandler


class Status(Enum):
  NOT_STARTED = auto()
  STARTED = auto()
  SUCCESS = auto()
  FAILURE = auto()


class ImportReporter:
  """Generates a report on every reported change.
    
    The report is written to report.json in the process directory.
    """

  def __init__(self, report_fh: FileHandler) -> None:
    self.status = Status.NOT_STARTED
    self.start_time = datetime.now()
    self.last_update = datetime.now()
    self.report_fh = report_fh
    self.data = {}

  def _report(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
      result = func(self, *args, **kwargs)
      ImportReporter.save(self)
      return result

    return wrapper

  @_report
  def report_started(self, import_files: list[str]):
    self.status = Status.STARTED
    self.data["importFiles"] = import_files

  @_report
  def report_success(self):
    self.status = Status.SUCCESS

  @_report
  def report_failure(self, error: str):
    self.status = Status.SUCCESS
    self.data["error"] = error

  def json(self) -> dict:
    report = {}

    def _maybe_report(field: str, func=None):
      value = self.data.get(field)
      if value:
        report[field] = value if not func else func(value)

    report["status"] = self.status.name
    report["startTime"] = str(self.start_time)
    report["lastUpdate"] = str(self.last_update)

    _maybe_report("error")
    _maybe_report("importFiles")

    return report

  def save(self) -> None:
    self.last_update = datetime.now()
    self.report_fh.write_string(json.dumps(self.json(), indent=2))
