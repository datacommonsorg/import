# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the 'License');
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#         https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an 'AS IS' BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
"""Class for dictionary of named counters."""

from typing import Union


class Counters:
    """A dictionary of named counters for tracking metrics."""

    def __init__(self, counters_dict: dict = None, prefix: str = ''):
        self._counters = counters_dict if counters_dict is not None else {}
        self._prefix = prefix

    def _get_counter_name(self, name: str, debug_context: str = None) -> str:
        full_name = f'{self._prefix}{name}'
        if debug_context:
            full_name = f'{full_name}_{debug_context}'
        return full_name

    def add_counter(self,
                    counter_name: str,
                    value: Union[int, float] = 1,
                    debug_context: str = None):
        """Increment a named counter by the given value."""
        name = self._get_counter_name(counter_name)
        self._counters[name] = self._counters.get(name, 0) + value
        return self

    def set_counter(self,
                    name: str,
                    value: Union[int, float],
                    debug_context: str = None):
        """Set the value of a counter, overwriting any previous value."""
        self._counters[self._get_counter_name(name)] = value
        if debug_context:
            self._counters[self._get_counter_name(name, debug_context)] = value
        return self

    def get_counters(self) -> dict:
        """Return the dictionary of all counter names and their values."""
        return self._counters

    def get_counter(self, name: str) -> Union[int, float]:
        """Return the value of a named counter (0 if not present)."""
        return self._counters.get(self._get_counter_name(name), 0)
