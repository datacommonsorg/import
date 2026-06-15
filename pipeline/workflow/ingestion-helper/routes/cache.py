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

import logging
import config
import redis
from fastapi import APIRouter, HTTPException
from routes.models import BaseResponse, ResponseStatus

router = APIRouter(prefix="/cache", tags=["cache"])

@router.post("/clear", response_model=BaseResponse)
def clear_redis_cache():
    """Clears the Redis cache."""
    redis_host = config.REDIS_HOST
    redis_port = config.REDIS_PORT
    if not redis_host:
        logging.warning("REDIS_HOST not set, skipping cache flush.")
        return BaseResponse(status=ResponseStatus.SKIPPED, message="REDIS_HOST not set")
    try:
        r = redis.Redis(host=redis_host, port=int(redis_port))
        r.flushall(asynchronous=True)
        logging.info(f"Redis cache at {redis_host}:{redis_port} flushed successfully (async).")
        return BaseResponse(status=ResponseStatus.SUCCESS, message="Cache cleared")
    except Exception as e:
        logging.error(f"Failed to flush Redis cache: {e}")
        raise HTTPException(status_code=500, detail=f"Failed to flush Redis cache: {e}")
