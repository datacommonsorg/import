# Copyright 2024 Google Inc.
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


def initialize_logger():
  """
    Initialize the root logger with a standard configuration.

    The logger is configured to:
    - Log to stdout
    - Use INFO level
    - Format messages with timestamp, level, filename, line number and message
    - Remove any existing handlers first

    """
  # Remove handlers from root logger
  for handler in logging.root.handlers:
    logging.root.removeHandler(handler)

  # Initialize logging
  logger = logging.getLogger()
  logger.setLevel(logging.INFO)
  handler = logging.StreamHandler(sys.stdout)
  formatter = logging.Formatter(
      "[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] %(message)s")
  handler.setFormatter(formatter)
  logger.addHandler(handler)
