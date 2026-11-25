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
"""Multiprocess worker pool orchestration, part fallback, and cross-file collisions."""

import concurrent.futures
from dataclasses import dataclass
import logging
import os
import tempfile
import threading
import time
import traceback
from typing import Callable

from stats.config.config import ImportConfig
from stats.import_worker import ImportProcResult
from stats.import_worker import ImportTask
from stats.import_worker import run_import_task
from stats.import_worker import WorkerArgs
from stats.importers.file_split import FilePart
from stats.writer.obs_id_collisions import CrossFileCollisions
from stats.writer.obs_id_collisions import find_cross_file_collisions
from util.filesystem import File

# Default worker processes per CPU for runs that read or write GCS, and the
# count above which no extra workers are added (see default_max_workers).
_GCS_WORKERS_PER_CPU = 2
_MAX_GCS_WORKERS = 32


def default_max_workers(uses_gcs: bool) -> int:
  """Returns the default number of worker processes that import files.

  Each worker is a separate process that holds its own copy of the parsed data
  for the file it imports, so for local runs the default is the number of CPUs
  this process may run on: more workers add memory without adding throughput.
  Workers of a run that reads or writes GCS (`uses_gcs`) also wait on it, so
  that default is `_GCS_WORKERS_PER_CPU` per CPU, up to `_MAX_GCS_WORKERS`, and
  never fewer than the CPUs.

  `os.sched_getaffinity` is preferred over `os.cpu_count` because it honors the
  CPU affinity mask and cpusets (e.g. a container or Cloud Batch task pinned to
  a subset of the host's CPUs), whereas `os.cpu_count` reports every CPU on the
  host. It is not available on every platform (e.g. macOS), hence the fallback.
  """
  try:
    cpus = len(os.sched_getaffinity(0))
  except AttributeError:
    cpus = os.cpu_count() or 1
  if not uses_gcs:
    return cpus
  return max(cpus, min(_GCS_WORKERS_PER_CPU * cpus, _MAX_GCS_WORKERS))


def num_import_workers(max_workers: int, num_tasks: int) -> int:
  """Returns how many worker processes to start: never more than the tasks."""
  return min(max_workers, num_tasks)


@dataclass
class FileFailure:
  """A file whose import or merge raised, with the traceback text.

  `traceback` includes the worker process's remote traceback.
  """
  file_path: str
  error: Exception
  traceback: str


class FileImportsFailedError(Exception):
  """Raised once every file has run if several of them failed to import."""

  def __init__(self, failures: list[FileFailure]) -> None:
    self.failures = failures
    details = "\n".join(f"  {f.file_path}: {type(f.error).__name__}: {f.error}"
                        for f in failures)
    super().__init__(f"{len(failures)} files failed to import:\n{details}")


def _finish_imports(
    files: list[File],
    futures_by_file: list[list[concurrent.futures.Future]],
    merge: Callable[[File, list[ImportProcResult]], None],
    import_whole: Callable[[File, list[ImportProcResult]],
                           concurrent.futures.Future] | None = None,
) -> list[FileFailure]:
  """Waits for every file in order, falling back to whole-file import on part errors."""
  failures: list[FileFailure] = []
  imported = 0
  for file, futures in zip(files, futures_by_file, strict=True):
    try:
      merge(file, _file_results(file, futures, import_whole))
      imported += 1
      logging.info("[%d/%d] Imported file: %s", imported, len(files), file.path)
    except Exception as e:
      failures.append(
          FileFailure(file.path, e, "".join(traceback.format_exception(e))))
  return failures


def _file_results(
    file: File,
    futures: list[concurrent.futures.Future],
    import_whole: Callable[[File, list[ImportProcResult]],
                           concurrent.futures.Future] | None,
) -> list[ImportProcResult]:
  """Returns the results of a file's tasks, or retries whole if any part failed."""
  if len(futures) == 1:
    return [futures[0].result()]
  results, errors = [], []
  for future in futures:
    try:
      results.append(future.result())
    except Exception as e:  # pylint: disable=broad-except
      errors.append(e)
  if not errors:
    return results
  if import_whole is None:
    raise errors[0]
  logging.warning(
      "%d of the %d parts of %s failed to import, so the file is imported"
      " whole. The first error: %s: %s", len(errors), len(futures), file.path,
      type(errors[0]).__name__, errors[0])
  return [import_whole(file, results).result()]


def raise_for_failures(failures: list[FileFailure]) -> None:
  """Logs each failure's traceback, then raises.

  A single failure is re-raised as is, so its type and message reach the
  caller unchanged. Several are raised together as one `FileImportsFailedError`
  that names every failed file.
  """
  for failure in failures:
    logging.error("Failed to import %s:\n%s", failure.file_path,
                  failure.traceback)
  if len(failures) == 1:
    raise failures[0].error
  raise FileImportsFailedError(failures)


def _check_cross_file_collisions(
    obs_ids_files: list[tuple[str, str]]) -> CrossFileCollisions | None:
  """Finds collisions between `obs_ids_files`, or returns None if <2 files."""
  if len(obs_ids_files) < 2:
    return None
  logging.info("Checking observation ids of %d files for collisions...",
               len({path for path, _ in obs_ids_files}))
  collisions = find_cross_file_collisions(obs_ids_files)
  if collisions.count:
    logging.warning(
        "Found %d observation @id collisions between files, or between parts"
        " of a split file: they produce identical observation ids.",
        collisions.count)
  return collisions


def _make_progress_callback(
    num_tasks: int) -> Callable[[concurrent.futures.Future], None]:
  """Returns a Future done-callback that logs task progress periodically."""
  lock = threading.Lock()
  completed = 0
  last_logged_count = 0
  last_logged_time = time.monotonic()
  step = max(1, num_tasks // 20)

  def _on_task_done(future: concurrent.futures.Future) -> None:
    nonlocal completed, last_logged_count, last_logged_time
    if future.cancelled() or num_tasks <= 1:
      return
    with lock:
      completed += 1
      now = time.monotonic()
      if (completed == num_tasks or completed - last_logged_count >= step or
          now - last_logged_time >= 30.0):
        last_logged_count = completed
        last_logged_time = now
        logging.info("Completed %d/%d tasks (%.0f%%)", completed, num_tasks,
                     100.0 * completed / num_tasks)

  return _on_task_done


def run_multiprocess_imports(
    files: list[File],
    parts_by_path: dict[str, list[FilePart]],
    *,
    max_workers: int,
    output_dir_path: str,
    import_names: list[str],
    config_model: ImportConfig,
    jsonld_dir_name: str,
    merge_file: Callable[[File, list[ImportProcResult]], None],
    discard_part_outputs: Callable[[File, list[ImportProcResult]], None],
) -> tuple[CrossFileCollisions | None, list[FileFailure]]:
  """Imports `files` in worker processes, merges results, and checks collisions.

  A file in `parts_by_path` is imported in those parts, in parallel. Tasks are
  submitted largest first. On part failure, `discard_part_outputs` cleans up
  partial shards and the file is retried whole. Returns `(collisions, failures)`
  so the caller can merge collisions or record/raise failures.
  """
  tasks_by_file = [
      [ImportTask.of(file, part)
       for part in parts_by_path.get(file.path, [])] or [ImportTask.of(file)]
      for file in files
  ]
  num_tasks = sum(len(t) for t in tasks_by_file)
  num_workers = num_import_workers(max_workers, num_tasks)
  logging.info("Importing %d files in %d tasks with %d worker processes",
               len(files), num_tasks, num_workers)
  # Workers run on this machine, so they can save ids to a local directory.
  # A failed run doesn't wait for the files in progress, which may still save
  # ids while the directory is removed.
  with tempfile.TemporaryDirectory(prefix="obs-ids-",
                                   ignore_cleanup_errors=True) as obs_ids_dir:
    worker_args = WorkerArgs(
        output_dir_path=output_dir_path,
        import_names=import_names,
        config=config_model,
        jsonld_dir_name=jsonld_dir_name,
        obs_ids_dir=obs_ids_dir,
    )
    # (input file path, ids path) of every task with observations, in file
    # and then part order.
    obs_ids_files: list[tuple[str, str]] = []

    def merge(file: File, results: list[ImportProcResult]) -> None:
      merge_file(file, results)
      obs_ids_files.extend(
          (file.path, res.obs_ids_path) for res in results if res.obs_ids_path)

    executor = concurrent.futures.ProcessPoolExecutor(max_workers=num_workers)

    def import_whole(
        file: File,
        part_results: list[ImportProcResult]) -> concurrent.futures.Future:
      discard_part_outputs(file, part_results)
      return executor.submit(run_import_task, ImportTask.of(file), worker_args)

    on_task_done = _make_progress_callback(num_tasks)

    try:
      # Submit largest tasks first so a huge file doesn't run alone at the end.
      indexed_tasks = [(fi, ti, task)
                       for fi, file_tasks in enumerate(tasks_by_file)
                       for ti, task in enumerate(file_tasks)]
      futures_by_file: list[list[concurrent.futures.Future | None]] = [
          [None] * len(file_tasks) for file_tasks in tasks_by_file
      ]
      for fi, ti, task in sorted(indexed_tasks,
                                 key=lambda x: x[2].size,
                                 reverse=True):
        fut = executor.submit(run_import_task, task, worker_args)
        fut.add_done_callback(on_task_done)
        futures_by_file[fi][ti] = fut
      failures = _finish_imports(files,
                                 futures_by_file,
                                 merge=merge,
                                 import_whole=import_whole)
    except BaseException:
      # E.g. Ctrl-C: files not yet started are cancelled, and the run fails
      # without waiting for the ones in progress. (Not a `with` block: its
      # exit would wait for them, and reset cancel_futures.)
      executor.shutdown(wait=False, cancel_futures=True)
      raise
    executor.shutdown()
    if failures:
      return None, failures
    return _check_cross_file_collisions(obs_ids_files), []
