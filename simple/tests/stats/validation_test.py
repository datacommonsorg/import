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

import unittest
from unittest import mock

from stats.config import Config
from stats.data import Triple
from stats.db import Db
from stats.validation import MetadataValidator


class TestMetadataValidator(unittest.TestCase):

  def test_validation_success(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [
            {
                "pattern": "data.csv",
                "provenance": "dcid:MyProvenance"
            }
        ]
    }

    # Setup mock database with matching MCF definitions
    mock_db = mock.MagicMock(spec=Db)
    mock_db._global_triples = [
        # Define Source
        Triple("dcid:MySource", "typeOf", object_id="Source"),
        # Define Provenance
        Triple("dcid:MyProvenance", "typeOf", object_id="Provenance"),
        # Link Provenance to Source
        Triple("dcid:MyProvenance", "sourceLink", object_id="dcid:MySource"),
    ]
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    # Should complete without raising an error
    validator.validate()

  def test_validation_missing_provenance(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [
            {
                "pattern": "data.csv",
                "provenance": "dcid:MissingProvenance"
            }
        ]
    }

    # Setup mock database missing the provenance definition
    mock_db = mock.MagicMock(spec=Db)
    mock_db._global_triples = [
        Triple("dcid:MySource", "typeOf", object_id="Source"),
    ]
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("referenced provenances are not defined in your MCF files",
                  str(context.exception))
    self.assertIn("dcid:MissingProvenance", str(context.exception))

  def test_validation_missing_source(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [
            {
                "pattern": "data.csv",
                "provenance": "dcid:MyProvenance"
            }
        ]
    }

    # Setup mock database where Provenance is defined but points to a missing Source
    mock_db = mock.MagicMock(spec=Db)
    mock_db._global_triples = [
        Triple("dcid:MyProvenance", "typeOf", object_id="Provenance"),
        Triple("dcid:MyProvenance", "sourceLink", object_id="dcid:MissingSource"),
    ]
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("Linked sources are missing for defined provenances",
                  str(context.exception))
    self.assertIn("points to missing/empty Source 'dcid:MissingSource'",
                  str(context.exception))

  def test_validation_no_referenced_provenances(self):
    # Setup mock config with no referenced provenances
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [
            {
                "pattern": "data.csv"
            }
        ]
    }

    mock_db = mock.MagicMock(spec=Db)
    mock_db._global_triples = []
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    # Should bypass validation immediately and pass
    validator.validate()
