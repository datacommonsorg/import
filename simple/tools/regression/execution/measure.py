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
Runs a command and measures its time, CPU, memory and log health.

CPU time and max RSS come from wait4(), like /usr/bin/time -v: they cover the
process and every child it waited for (the multiprocessing pool workers).
max_rss_kb is the largest single process. peak_tree_rss_kb is the largest sum
of RSS over the whole process tree, sampled from /proc; pages shared by forked
workers are counted once per process, so it is an upper bound.
"""

import dataclasses
import os
import re
import resource
import signal
import subprocess  # nosec B404: runs the preprocessor argv, no shell.
import threading
import time

_SAMPLE_INTERVAL_SECONDS = 0.5
_PAGE_KB = resource.getpagesize() // 1024
_ERROR_LINE = re.compile(r"^(?:\[[^\]]* )?(?:ERROR|CRITICAL)[ :]")
_WARNING_LINE = re.compile(r"^(?:\[[^\]]* )?WARNING[ :]")
_TRACEBACK_LINE = "Traceback (most recent call last):"


@dataclasses.dataclass(frozen=True)
class RunMetrics:
  exit_code: int  # Negative: killed by that signal.
  timed_out: bool
  wall_seconds: float
  user_seconds: float
  sys_seconds: float
  max_rss_kb: int
  peak_tree_rss_kb: int
  error_lines: int  # ERROR and CRITICAL log lines, plus tracebacks.
  warning_lines: int

  @property
  def cpu_seconds(self) -> float:
    return self.user_seconds + self.sys_seconds


def _rss_kb(pid: int) -> int:
  try:
    with open(f"/proc/{pid}/statm") as f:
      return int(f.read().split()[1]) * _PAGE_KB
  except (OSError, IndexError):  # Gone, or /proc not readable here.
    return 0


def _children(pid: int) -> list[int]:
  children = []
  try:
    for task in os.listdir(f"/proc/{pid}/task"):
      with open(f"/proc/{pid}/task/{task}/children") as f:
        children.extend(int(c) for c in f.read().split())
  except OSError:  # Gone, or /proc not readable here.
    pass
  return children


def _tree_rss_kb(root: int) -> int:
  total, pending = 0, [root]
  while pending:
    pid = pending.pop()
    total += _rss_kb(pid)
    pending.extend(_children(pid))
  return total


class _TreeRssSampler(threading.Thread):
  """Samples the summed RSS of a process tree until stopped."""

  def __init__(self, root: int) -> None:
    super().__init__(daemon=True)
    self._root = root
    self._stopped = threading.Event()
    self.peak_kb = 0

  def run(self) -> None:
    while True:
      self.peak_kb = max(self.peak_kb, _tree_rss_kb(self._root))
      if self._stopped.wait(_SAMPLE_INTERVAL_SECONDS):
        return

  def stop(self) -> None:
    self._stopped.set()
    self.join()


def count_log_lines(log_path: str) -> tuple[int, int]:
  """Returns (error lines including tracebacks, warning lines) of a log."""
  errors = warnings = 0
  with open(log_path, errors="replace") as f:
    for line in f:
      if _ERROR_LINE.match(line) or line.startswith(_TRACEBACK_LINE):
        errors += 1
      elif _WARNING_LINE.match(line):
        warnings += 1
  return errors, warnings


def run_measured(command: list[str], cwd: str, env: dict[str, str],
                 log_path: str, timeout_seconds: float | None) -> RunMetrics:
  """
  Runs command with stdout and stderr in log_path, and measures it.

  On timeout the whole process group is killed with SIGKILL. reap_lock guards
  against killing a recycled PID: once wait4 has reaped the child, its PID
  (and process group id) may be reused, so kill() must become a no-op.
  """
  timed_out = threading.Event()
  reap_lock = threading.Lock()
  reaped = False
  with open(log_path, "wb") as log:
    start = time.monotonic()
    process = subprocess.Popen(  # nosec B603: caller-built argv, no shell.
        command,
        cwd=cwd,
        env=env,
        stdout=log,
        stderr=subprocess.STDOUT,
        start_new_session=True)

    def kill() -> None:
      with reap_lock:
        if reaped:
          return
        timed_out.set()
        os.killpg(process.pid, signal.SIGKILL)

    timer = threading.Timer(timeout_seconds, kill) if timeout_seconds else None
    if timer:
      timer.start()
    sampler = _TreeRssSampler(process.pid)
    sampler.start()
    _, status, usage = os.wait4(process.pid, 0)
    with reap_lock:
      reaped = True
    wall = time.monotonic() - start
    sampler.stop()
    if timer:
      timer.cancel()
    # wait4 reaped the child; tell Popen so that it does not wait again.
    process.returncode = os.waitstatus_to_exitcode(status)
  errors, warnings = count_log_lines(log_path)
  return RunMetrics(exit_code=process.returncode,
                    timed_out=timed_out.is_set(),
                    wall_seconds=wall,
                    user_seconds=usage.ru_utime,
                    sys_seconds=usage.ru_stime,
                    max_rss_kb=usage.ru_maxrss,
                    peak_tree_rss_kb=sampler.peak_kb,
                    error_lines=errors,
                    warning_lines=warnings)
