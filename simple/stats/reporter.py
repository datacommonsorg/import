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
import time

from util.filehandler import FileHandler

# Minimum interval before a report should be saved to disk or cloud.
# This keeps it from reporting too frequently and running into GCS rate limit issues.
_REPORT_SAVE_INTERVAL_SECONDS = 10.0


class Status(Enum):
  NOT_STARTED = auto()
  STARTED = auto()
  SUCCESS = auto()
  FAILURE = auto()


def _is_done_status(status: Status) -> bool:
  return status == Status.SUCCESS or status == Status.FAILURE


class ImportReporter:
  """Generates a report on every reported change.

    The report is written to report.json in the process directory.
    """

  def __init__(self, report_fh: FileHandler) -> None:
    self.status = Status.NOT_STARTED
    self.start_time = None
    self.last_update = datetime.now()
    self.last_reported: float | None = None
    self.report_fh = report_fh
    self.data = {}
    self.import_files: dict[str, FileImportReporter] = {}

  # Functions decorated with @_report will result in the report being saved
  # upon function execution.
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
    self.start_time = datetime.now()
    for import_file in import_files:
      self.import_files[import_file] = FileImportReporter(import_file, self)

  @_report
  def report_done(self):
    self._compute_all_done()
    self.status = Status.SUCCESS

  @_report
  def report_failure(self, error: str):
    self.status = Status.FAILURE
    self.data["error"] = error

  def import_file(self, import_file: str):
    return self.import_files[import_file]

  def import_file_update(self, import_file: str):
    self._compute_all_done()
    self.save()

  def _compute_all_done(self):
    if self._all_file_imports(Status.SUCCESS):
      self.status = Status.SUCCESS
    elif self._all_file_imports(Status.FAILURE):
      self.status = Status.FAILURE

  def _all_file_imports(self, status: Status) -> bool:
    return all(
        reporter.status == status for reporter in self.import_files.values())

  def json(self) -> dict:
    report = {}

    def _maybe_report(field: str, func=None):
      value = self.data.get(field)
      if value:
        report[field] = value if not func else func(value)

    report["status"] = self.status.name
    _maybe_report("error")

    if self.start_time:
      report["startTime"] = str(self.start_time)
      report["lastUpdate"] = str(self.last_update)

    import_files = {}
    for import_file, import_file_reporter in self.import_files.items():
      import_files[import_file] = import_file_reporter.json()

    report["importFiles"] = import_files

    return report

  def save(self) -> None:
    self.last_update = datetime.now()
    report = not self.last_reported or time.time(
    ) - self.last_reported >= _REPORT_SAVE_INTERVAL_SECONDS or _is_done_status(
        self.status)
    if report:
      self.last_reported = time.time()
      self.report_fh.write_string(json.dumps(self.json(), indent=2))


class FileImportReporter:
  """Generates a report on every reported change for a single file import.
    """

  def __init__(self, import_file: str, parent: ImportReporter) -> None:
    self.status = Status.NOT_STARTED
    self.start_time = None
    self.last_update = datetime.now()
    self.import_file = import_file
    self.parent = parent
    self.data = {}

  # Functions decorated with @_report will result in this report being reported to the parent reporter
  # upon function execution.
  def _report(func):

    @wraps(func)
    def wrapper(self, *args, **kwargs):
      result = func(self, *args, **kwargs)
      FileImportReporter.report(self)
      return result

    return wrapper

  @_report
  def report_started(self):
    self.status = Status.STARTED
    self.start_time = datetime.now()

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
    _maybe_report("error")

    if self.start_time:
      report["startTime"] = str(self.start_time)
      report["lastUpdate"] = str(self.last_update)

    return report

  def report(self) -> None:
    self.last_update = datetime.now()
    self.parent.import_file_update(self.import_file)
