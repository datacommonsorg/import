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
from stats.data import ValidationErrorType
from stats.db import Db
from stats.validation import MetadataValidator


class TestMetadataValidator(unittest.TestCase):

  def test_validation_success(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    }

    # Setup mock database with matching MCF definitions
    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {
        "_global": [
            # Define Source
            Triple("dcid:MySource", "typeOf", object_id="Source"),
            # Define Provenance
            Triple("dcid:MyProvenance", "typeOf", object_id="Provenance"),
            # Link Provenance to Source
            Triple("dcid:MyProvenance", "source", object_id="dcid:MySource"),
        ]
    }

    validator = MetadataValidator(mock_config, mock_db)
    # Should complete without raising an error
    validator.validate()

  def test_validation_missing_provenance(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MissingProvenance"
        }]
    }

    # Setup mock database missing the provenance definition
    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {
        "_global": [Triple("dcid:MySource", "typeOf", object_id="Source"),]
    }

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("referenced provenances are not defined in your MCF files",
                  str(context.exception))
    self.assertIn("dcid:MissingProvenance", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.MISSING_PROVENANCE)

  def test_validation_missing_source_link(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    }

    # Setup mock database where Provenance is defined but completely lacks a linked source
    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {
        "_global": [
            Triple("dcid:MyProvenance", "typeOf", object_id="Provenance"),
        ]
    }

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("Linked sources are missing for defined provenances",
                  str(context.exception))
    self.assertIn("has no linked Source", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.MISSING_SOURCE)

  def test_validation_undefined_source_node_passes(self):
    # Setup mock config with referenced provenance
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    }

    # Setup mock database where Provenance is defined and points to a source,
    # but the Source node itself is NOT defined in the database triples
    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {
        "_global": [
            Triple("dcid:MyProvenance", "typeOf", object_id="Provenance"),
            Triple("dcid:MyProvenance", "source", object_id="dcid:MySource"),
        ]
    }

    validator = MetadataValidator(mock_config, mock_db)
    # This should now succeed without raising any exceptions!
    validator.validate()

  def test_validation_missing_provenance_key(self):
    # Setup mock config with missing provenance property
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {"inputFiles": [{"pattern": "data.csv"}]}

    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("must have a 'provenance' property", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.INVALID_CONFIGURATION)

  def test_validation_invalid_provenance_format(self):
    # Setup mock config with provenance that doesn't start with 'dcid:'
    mock_config = mock.MagicMock(spec=Config)
    mock_config.data = {
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "InvalidProvenanceName"
        }]
    }

    mock_db = mock.MagicMock(spec=Db)
    mock_db._triples = {}

    validator = MetadataValidator(mock_config, mock_db)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("must be a valid DCID or URI", str(context.exception))
    self.assertIn("InvalidProvenanceName", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.INVALID_CONFIGURATION)
