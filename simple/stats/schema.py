# Copyright 2024 Google Inc.
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

# Functions to fetch schema info from custom dc db and remote dc.

from stats import schema_constants as sc
from stats.db import Db

from util import dc_client


# Gets names of the specified dcids first from db and any remaining
# ones from remote dc.
def get_schema_names(dcids: list[str], db: Db) -> dict[str, str]:
  db_dcid2name = db.select_entity_names(dcids)
  dcids = list(filter(lambda x: x not in db_dcid2name, dcids))
  remote_dcid2name = {}
  if dcids:
    remote_dcid2name = dc_client.get_property_of_entities(
        dcids, sc.PREDICATE_NAME)
  return remote_dcid2name | db_dcid2name
