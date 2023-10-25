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
import sys

from absl import app
from absl import flags
from freezegun import freeze_time
from stats import constants
from stats.runner import Runner

FLAGS = flags.FLAGS

flags.DEFINE_string(
    "entity_type",
    None,
    "The type of entities in the CSV (e.g. 'City', 'Country', 'Company', etc.).",
)
flags.DEFINE_string("input_path", constants.DEFAULT_INPUT_PATH,
                    "The input directory or file.")
flags.DEFINE_string("output_dir", constants.DEFAULT_OUTPUT_DIR,
                    "The output directory.")
flags.DEFINE_list("ignore_columns", [], "List of input columns to be ignored.")
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

# If running with time frozen, the packages to be ignored.
# i.e. packages where time should not be frozen if it leads to errant behavior.
_FREEZE_TIME_IGNORE_LIST = ["transformers"]


def _init_logger():
  # Log to stdout for easy redirect of the output text.
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)
  handler = logging.StreamHandler(sys.stdout)
  handler.setLevel(logging.INFO)
  logger.addHandler(handler)


def _run():
  Runner(
      input_path=FLAGS.input_path,
      output_dir=FLAGS.output_dir,
      entity_type=FLAGS.entity_type,
      ignore_columns=FLAGS.ignore_columns,
  ).run()


def main(_):
  _init_logger()

  if FLAGS.freeze_time:
    logging.info("Running with time frozen at: %s", FLAGS.frozen_time)
    with freeze_time(FLAGS.frozen_time, ignore=_FREEZE_TIME_IGNORE_LIST):
      _run()
  else:
    _run()


if __name__ == "__main__":
  app.run(main)
