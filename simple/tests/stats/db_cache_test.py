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
import unittest
from unittest.mock import patch

from fakeredis import FakeRedis
from fakeredis import FakeServer
from redis.exceptions import ConnectionError
from stats.db_cache import DEFAULT_REDIS_PORT
from stats.db_cache import ENV_REDIS_HOST
from stats.db_cache import ENV_REDIS_PORT
from stats.db_cache import get_db_cache_from_env
from stats.db_cache import RedisDbCache


class TestDbCache(unittest.TestCase):

  @patch.dict(os.environ, {ENV_REDIS_HOST: "localhost"})
  def test_db_cache_redis_default_port(self):
    with patch("redis.Redis", return_value=FakeRedis()) as redis_constructor:
      cache = get_db_cache_from_env()
      self.assertIsInstance(cache, RedisDbCache)
      redis_constructor.assert_called_with(host="localhost",
                                           port=DEFAULT_REDIS_PORT,
                                           decode_responses=True)

  @patch.dict(os.environ, {ENV_REDIS_HOST: "localhost", ENV_REDIS_PORT: "6740"})
  def test_db_cache_redis_custom_port(self):
    with patch("redis.Redis", return_value=FakeRedis()) as redis_constructor:
      cache = get_db_cache_from_env()
      self.assertIsInstance(cache, RedisDbCache)
      redis_constructor.assert_called_with(host="localhost",
                                           port=6740,
                                           decode_responses=True)

  @patch.dict(os.environ, {
      ENV_REDIS_HOST: "localhost",
      ENV_REDIS_PORT: "invalid"
  })
  def test_db_cache_redis_invalid_port(self):
    with patch("redis.Redis"):  # Mock redis.Redis to avoid actual connection
      with self.assertRaises(ValueError):
        get_db_cache_from_env()

  @patch.dict(os.environ, {ENV_REDIS_PORT: "unused"})
  def test_get_db_cache_redis_no_host(self):
    with patch("redis.Redis"):  # Mock redis.Redis to avoid actual connection
      cache = get_db_cache_from_env()
      self.assertIsNone(cache)

  @patch.dict(os.environ, {ENV_REDIS_HOST: "localhost"})
  def test_get_db_cache_redis_no_connection(self):
    server = FakeServer()
    server.connected = False
    fake_redis = FakeRedis(server=server)
    with patch("redis.Redis", return_value=fake_redis):
      self.assertRaises(ConnectionError, get_db_cache_from_env)

  @patch.dict(os.environ, {ENV_REDIS_HOST: "localhost"})
  def test_get_db_cache_redis_clear(self):
    fake_redis = FakeRedis()
    fake_redis.set("somekey", "somevalue")
    self.assertEqual(1, len(fake_redis.keys("*")))
    with patch("redis.Redis", return_value=fake_redis):
      cache = get_db_cache_from_env()
      self.assertIsInstance(cache, RedisDbCache)
      cache.clear()
      self.assertEqual(0, len(fake_redis.keys("*")))
