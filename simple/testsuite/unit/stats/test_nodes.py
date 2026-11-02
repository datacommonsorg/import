# Copyright 2026 Google Inc.
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

from stats.config import Config
from stats.data import Triple
from stats.nodes import Nodes


def test_merge_metadata_keeps_every_field():
  worker = Nodes(Config({}))
  worker.register_source(id="dcid:MySource",
                         name="My Source",
                         url="https://example.com",
                         custom_triples=[
                             Triple(subject_id="MySource",
                                    predicate="license",
                                    object_value="CC-BY")
                         ],
                         import_folder="MyProvenance")
  worker.register_provenance(id="dcid:MyProvenance",
                             name="My Provenance",
                             url="https://example.com/data",
                             source_id="dcid:MySource",
                             custom_triples=[
                                 Triple(subject_id="MyProvenance",
                                        predicate="isPartOf",
                                        object_id="dcid:Other")
                             ])

  coordinator = Nodes(Config({}))
  coordinator.merge_metadata(worker.provenances, worker.sources)

  assert coordinator.sources["MySource"] == worker.sources["MySource"]
  assert (coordinator.provenances["MyProvenance"] ==
          worker.provenances["MyProvenance"])
