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


class LockAlertLevel(str, Enum):
    OK = "OK"
    WARNING_50PCT = "WARNING_50PCT"
    WARNING_90PCT = "WARNING_90PCT"
    CRITICAL_100PCT = "CRITICAL_100PCT"


class BaseResponse(BaseModel):
    status: ResponseStatus = Field(description="The execution status of the request")
    message: Optional[str] = Field(default=None, description="Optional informational message")


class LockStatusResponse(BaseModel):
    status: ResponseStatus = Field(default=ResponseStatus.OK, description="The execution status of the request")
    locked: bool = Field(default=False, description="Whether the global ingestion lock is currently held")
    lockOwner: Optional[str] = Field(default=None, description="Workflow ID currently holding the lock")
    acquiredTimestamp: Optional[str] = Field(default=None, description="ISO-8601 formatted UTC timestamp when acquired")
    ageSeconds: Optional[float] = Field(default=None, description="Duration in seconds the lock has been held")
    alertLevel: LockAlertLevel = Field(default=LockAlertLevel.OK, description="Retention SLA alert level based on Spanner PITR horizon")
