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

import logging
import os

from absl import app
from absl import flags
from freezegun import freeze_time
import requests.adapters
from stats import constants
from stats.logger import initialize_logger
from stats.runner import Runner

FLAGS = flags.FLAGS

flags.DEFINE_string("config_file", None, "The config file.")
flags.DEFINE_string("input_dir", constants.DEFAULT_INPUT_DIR,
                    "The input directory.")
flags.DEFINE_string("output_dir", constants.DEFAULT_OUTPUT_DIR,
                    "The output directory.")
flags.DEFINE_list("imports", [],
                  "The names of the imports (subdirectories under input_dir).")
flags.DEFINE_string(
    "mode",
    "",
    "Deprecated and ignored. The importer only runs the dcpbridge workflow "
    "now. Any value is accepted so existing callers keep working, but it has "
    "no effect. The flag will be removed once callers stop passing it.",
)
flags.DEFINE_bool(
    "freeze_time",
    False,
    "Freeze time in generated reports. Useful for sample and test runs.",
)
flags.DEFINE_string(
    "frozen_time",
    constants.DEFAULT_FROZEN_TIME,
    "If freeze_time is True, the time that the run is frozen at.",
)
flags.DEFINE_bool(
    "import_proxy_entities",
    os.getenv("IMPORT_PROXY_ENTITIES", "true").lower() == "true",
    "Whether to generate proxy entity nodes in the graph for entities resolved from Base Data Commons (default: True).",
)

# If running with time frozen, the packages to be ignored.
# i.e. packages where time should not be frozen if it leads to errant behavior.
_FREEZE_TIME_IGNORE_LIST = ["transformers"]

# Values --mode used to accept. Passing one of these now does nothing, so we
# warn rather than silently proceeding as if the caller got what it asked for.
_REMOVED_RUN_MODES = frozenset(["customdc", "maindc", "schemaupdate"])


def _warn_if_mode_is_set():
  """Logs a deprecation warning for --mode, which is accepted but ignored."""
  mode = (FLAGS.mode or "").strip()
  if not mode or mode == "dcpbridge":
    return
  if mode in _REMOVED_RUN_MODES:
    logging.warning(
        "--mode=%s is no longer supported and is being ignored. This importer "
        "only runs the dcpbridge workflow. The run will continue as dcpbridge, "
        "which does NOT do what %s used to do. Remove the flag from the "
        "caller.", mode, mode)
  else:
    logging.warning(
        "Unrecognized --mode=%s. The flag is deprecated and "
        "ignored; running the dcpbridge workflow.", mode)


def _run():
  # Configure requests adapter default pool size to support parallel GCS uploads
  requests.adapters.DEFAULT_POOLSIZE = 32

  initialize_logger()

  _warn_if_mode_is_set()

  logging.info("Starting stats data importer job.")

  Runner(
      config_file_path=FLAGS.config_file,
      input_dir_path=FLAGS.input_dir,
      output_dir_path=FLAGS.output_dir,
      import_names=FLAGS.imports,
      import_proxy_entities=FLAGS.import_proxy_entities,
  ).run()
  logging.info("Runner finished successfully.")


def main(_):
  if FLAGS.freeze_time:
    logging.info("Running with time frozen at: %s", FLAGS.frozen_time)
    with freeze_time(FLAGS.frozen_time, ignore=_FREEZE_TIME_IGNORE_LIST):
      _run()
  else:
    _run()


if __name__ == "__main__":
  app.run(main)
