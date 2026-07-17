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


class Importer:
  """The base class for all importers."""

  def do_import(self) -> None:
    pass

  def check_and_report_unresolved_entities(self, unresolved_entities: set[str]) -> None:
    """Checks if there are any unresolved entities, reports them and raises ValueError."""
    if not unresolved_entities:
      return

    unresolved_list = sorted(list(unresolved_entities))
    self.reporter.report_unresolved_entities(unresolved_list)

    if hasattr(self, "_write_debug_csvs"):
      getattr(self, "_write_debug_csvs")()

    raise ValueError(
        f"Entity resolution failed for {len(unresolved_list)} entities in file '{self.input_file.path}': "
        f"{unresolved_list[:50]}... "
        f"Please check the debug resolution CSV file for the complete list."
    )
