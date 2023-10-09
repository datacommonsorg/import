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

import os
import sys
from absl import flags
from absl import app
import logging
import constants
from runner import Runner

# For importing util
_CODEDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, os.path.join(_CODEDIR, "../"))

from util import dc_client as dc

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


def main(_):
    logging.getLogger().setLevel(logging.INFO)
    runner = Runner(
        input_path=FLAGS.input_path,
        output_dir=FLAGS.output_dir,
        entity_type=FLAGS.entity_type,
        ignore_columns=FLAGS.ignore_columns,
    )
    runner.run()


if __name__ == "__main__":
    app.run(main)
