# Copyright 2026 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

from enum import Enum
from typing import Optional
from pydantic import BaseModel, Field

class ResponseStatus(str, Enum):
    OK = "OK"
    SUCCESS = "SUCCESS"
    SUBMITTED = "SUBMITTED"
    FAILED = "FAILED"
    ERROR = "ERROR"
    SKIPPED = "SKIPPED"
    DONE = "DONE"
    RUNNING = "RUNNING"

    @classmethod
    def from_str(cls, value: str, default: ResponseStatus = None) -> ResponseStatus:
        """Safely parses a string into a ResponseStatus, returning the default (ERROR) if invalid."""
        try:
            return cls(value)
        except ValueError:
            return default or cls.ERROR

class BaseResponse(BaseModel):
    status: ResponseStatus = Field(description="The execution status of the request")
    message: Optional[str] = Field(default=None, description="Optional informational message")
