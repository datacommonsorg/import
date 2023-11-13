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

import os
import sqlite3
import tempfile
import unittest

from stats.data import Observation
from stats.data import Triple
from stats.db import Db
from stats.db import to_observation_tuple
from stats.db import to_triple_tuple

_TRIPLES = [
    Triple("sub1", "pred1", object_id="objid1"),
    Triple("sub2", "pred2", object_value="objval1")
]

_OBSERVATIONS = [
    Observation("e1", "v1", "2023", "123", "p1"),
    Observation("e2", "v1", "2023", "456", "p1")
]


class TestDb(unittest.TestCase):

  def test_db(self):
    with tempfile.NamedTemporaryFile() as temp_file:
      db = Db(temp_file.name)
      db.insert_triples(_TRIPLES)
      db.insert_observations(_OBSERVATIONS)
      db.close()

      sqldb = sqlite3.connect(temp_file.name)

      triples = sqldb.execute("select * from triples").fetchall()
      self.assertListEqual(triples,
                           list(map(lambda x: to_triple_tuple(x), _TRIPLES)))

      observations = sqldb.execute("select * from observations").fetchall()
      self.assertListEqual(
          observations,
          list(map(lambda x: to_observation_tuple(x), _OBSERVATIONS)))
