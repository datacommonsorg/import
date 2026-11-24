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

from stats.config.config import Config
from stats.graph.nodes import Nodes
from stats.report.model.summary import ValidationErrorType
from stats.validation.validation import MetadataValidator


class TestMetadataValidator(unittest.TestCase):

  def test_validation_success(self):
    # Setup mock config with referenced provenance
    config = Config({
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    })

    # MCF files define the provenance and its source
    nodes = Nodes(config)
    nodes.register_source("dcid:MySource")
    nodes.register_provenance("dcid:MyProvenance", source_id="dcid:MySource")

    validator = MetadataValidator(config, nodes)
    # Should complete without raising an error
    validator.validate()

  def test_validation_missing_provenance(self):
    # Setup mock config with referenced provenance
    config = Config({
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MissingProvenance"
        }]
    })

    # MCF files define only a source
    nodes = Nodes(config)
    nodes.register_source("dcid:MySource")

    validator = MetadataValidator(config, nodes)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("referenced provenances are not defined in your MCF files",
                  str(context.exception))
    self.assertIn("dcid:MissingProvenance", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.MISSING_PROVENANCE)

  def test_validation_missing_source_link(self):
    # Setup mock config with referenced provenance
    config = Config({
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    })

    # The provenance is defined but has no linked source
    nodes = Nodes(config)
    nodes.register_provenance("dcid:MyProvenance")

    validator = MetadataValidator(config, nodes)
    with self.assertRaises(ValueError) as context:
      validator.validate()

    self.assertIn("Linked sources are missing for defined provenances",
                  str(context.exception))
    self.assertIn("has no linked Source", str(context.exception))
    self.assertEqual(context.exception.error_type,
                     ValidationErrorType.MISSING_SOURCE)

  def test_validation_undefined_source_node_passes(self):
    # Setup mock config with referenced provenance
    config = Config({
        "inputFiles": [{
            "pattern": "data.csv",
            "provenance": "dcid:MyProvenance"
        }]
    })

    # The provenance points to a source, but no MCF file defines that source
    nodes = Nodes(config)
    nodes.register_provenance("dcid:MyProvenance", source_id="dcid:MySource")

    validator = MetadataValidator(config, nodes)
    # This should now succeed without raising any exceptions!
    validator.validate()
