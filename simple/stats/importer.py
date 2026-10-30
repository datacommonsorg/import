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

from stats.data import FileValidationError
from stats.data import strip_namespace


class Importer:
  """The base class for all importers."""

  def do_import(self) -> None:
    pass

  def validate_headers(self) -> list[FileValidationError]:
    """Validates CSV headers and returns a list of validation errors (empty if valid)."""
    return []

  def get_column_mappings(self) -> dict[str, str]:
    """Returns the column mappings for the input file."""
    if not hasattr(self, "config") or not hasattr(self, "input_file"):
      return {}
    return self.config.column_mappings(self.input_file)

  def get_reverse_column_mappings(self) -> dict[str, str]:
    """Returns a mapping from physical CSV column names to stripped property DCIDs."""
    return {v: strip_namespace(k) for k, v in self.get_column_mappings().items()}
