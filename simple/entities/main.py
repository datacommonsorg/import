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
import constants
from importer import SimpleEntitiesImporter

# For importing util
_CODEDIR = os.path.dirname(os.path.realpath(__file__))
sys.path.insert(1, os.path.join(_CODEDIR, "../"))

from util import dc_client as dc

FLAGS = flags.FLAGS

flags.DEFINE_enum("entity_type", None, ["Country", "City"],
                  "The type of entities in the CSV.")
flags.DEFINE_string("input_dir", constants.DEFAULT_INPUT_DIR,
                    "The input directory.")
flags.DEFINE_string("output_dir", constants.DEFAULT_OUTPUT_DIR,
                    "The output directory.")


def main(_):
    importer = SimpleEntitiesImporter(
        input_dir=FLAGS.input_dir,
        output_dir=FLAGS.output_dir,
        entity_type=FLAGS.entity_type,
    )
    importer.do_import()


if __name__ == "__main__":
    app.run(main)
