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

from absl import app
from absl import flags
from stats import constants
from stats.logger import initialize_logger
from stats.runner import Runner

FLAGS = flags.FLAGS

flags.DEFINE_string("input_dir", constants.DEFAULT_INPUT_DIR,
                    "The input directory.")
flags.DEFINE_string("output_dir", constants.DEFAULT_OUTPUT_DIR,
                    "The output directory.")
flags.DEFINE_list(
    "imports", [],
    "Required. The imports to run: one or more comma-separated subdirectories"
    f" of input_dir, each with its own config.json, or {constants.ALL_IMPORTS}"
    " for every subdirectory that has one.")
flags.DEFINE_integer(
    "max_workers",
    None,
    "The maximum number of worker processes that import files concurrently. "
    "Defaults to the number of CPUs available to this process, or twice that "
    "(at most 32) when the input or output is on GCS; see "
    "runner.default_max_workers. Lower it to reduce peak memory, since each "
    "worker holds the data of the file it is importing.",
    lower_bound=1,
)
flags.DEFINE_integer(
    "split_csv_bytes",
    None,
    "Observation CSVs larger than this many bytes are imported in parts, in "
    "parallel: up to one part per worker, each at least a quarter of this "
    "size. Defaults to 256 MiB; see csv_split.num_parts.",
    lower_bound=1,
)


def main(_):
  initialize_logger()
  logging.info("Starting stats data importer job.")

  Runner(
      input_dir_path=FLAGS.input_dir,
      output_dir_path=FLAGS.output_dir,
      import_names=FLAGS.imports,
      max_workers=FLAGS.max_workers,
      split_csv_bytes=FLAGS.split_csv_bytes,
  ).run()
  logging.info("Runner finished successfully.")


if __name__ == "__main__":
  app.run(main)
