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
import os
import sys


def initialize_logger():
  """Logs to stdout, replacing any existing handlers.

  The level is $LOG_LEVEL (INFO by default), and each message is prefixed with
  its time, level, file and line.
  """
  logging.basicConfig(
      level=os.environ.get("LOG_LEVEL", "INFO").upper(),
      format="[%(asctime)s %(levelname)s %(filename)s:%(lineno)d] %(message)s",
      stream=sys.stdout,
      force=True)
