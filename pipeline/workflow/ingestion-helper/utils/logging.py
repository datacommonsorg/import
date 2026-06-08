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

import time
import logging
import uuid
from datetime import datetime
from fastapi import Request

def log_start(func):
    """Decorator to flag a FastAPI endpoint for START request logging.
    
    When applied, the HTTP middleware will log a correlated 'START' entry 
    as soon as the request is received, allowing developers to track 
    heavy or long-running operations before they complete.
    """
    func.log_start = True
    return func

async def log_requests(request: Request, call_next):
    """Centralized HTTP Request/Response Logging Middleware.

    Design Rationale:
    1. Single Log Entry (Default): For fast, lightweight transactional routes (like /cache/clear), 
       we only log at the end of the request. This cuts log volume by 50%, reducing ingestion costs 
       and keeping the logs highly readable by storing status, path, and latency in one line.

    2. Correlated START/END Log (Optional): For heavy, long-running operations (like /embeddings/ingest), 
       logging only at the end is risky because a server hang or connection reset would leave no trace. 
       For these, we decorate the route with `@log_start`. The middleware will log a "START" entry 
       immediately, and correlate it to the completion log using a unique, short `request_id` (e.g. `req-1234abcd`).
    """
    # Generate a unique short request correlation ID
    request_id = f"req-{uuid.uuid4().hex[:8]}"
    request.state.request_id = request_id
    
    timestamp = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    
    # Inspect the matched FastAPI route to check if the endpoint is flagged for START logging
    route = request.scope.get("route")
    if route and hasattr(route, "endpoint"):
        if getattr(route.endpoint, "log_start", False):
            logging.info(
                f"[{timestamp}] [{request_id}] START {request.method} {request.url.path}"
            )

    # Process request and measure duration
    start_time = time.perf_counter()
    response = await call_next(request)
    duration = (time.perf_counter() - start_time) * 1000
    
    # Log the completion with the correlated request ID
    logging.info(
        f"[{timestamp}] [{request_id}] {request.method} {request.url.path} - {response.status_code} (took {duration:.2f}ms)"
    )
    return response
