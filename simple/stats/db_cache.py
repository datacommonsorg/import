# Copyright 2025 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os

import redis

ENV_REDIS_HOST = "REDIS_HOST"
ENV_REDIS_PORT = "REDIS_PORT"
DEFAULT_REDIS_PORT = 6739


class DbCache:
  """Abstract class for a database cache layer.

  Provides only the interface needed by the data loading process.
  """

  def clear(self):
    pass


class RedisDbCache(DbCache):
  """Connects to a Redis instance used as a database cache and provides methods to interact with it."""

  def __init__(self, host: str, port: int):
    self.connection = redis.Redis(host=host, port=port, decode_responses=True)
    self.connection.ping()

  def clear(self):
    """Flushes all data from the Redis instance used as a database cache."""
    self.connection.flushall(asynchronous=False)


def get_db_cache_from_env() -> DbCache | None:
  """Returns an appropriate database cache interface based on environment variables."""
  redis_host = os.getenv(ENV_REDIS_HOST)
  if not redis_host:
    return None
  redis_port = DEFAULT_REDIS_PORT
  redis_port_str = os.getenv(ENV_REDIS_PORT)
  if redis_port_str:
    try:
      redis_port = int(redis_port_str)
    except ValueError:
      raise ValueError(f"Invalid REDIS_PORT: {redis_port_str}")
  return RedisDbCache(host=redis_host, port=redis_port)
