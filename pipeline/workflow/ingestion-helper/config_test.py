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

import os
import tempfile
import unittest
import importlib
import yaml

import config

class TestConfig(unittest.TestCase):

    def setUp(self):
        self.original_env = os.environ.copy()

    def tearDown(self):
        os.environ.clear()
        os.environ.update(self.original_env)
        importlib.reload(config)

    def test_default_embedding_specs(self):
        if 'EMBEDDING_SPEC_PATH' in os.environ:
            del os.environ['EMBEDDING_SPEC_PATH']
        importlib.reload(config)
        self.assertEqual(config.EMBEDDING_SPECS, config._DEFAULT_EMBEDDING_SPECS)

    def test_valid_yaml_specs(self):
        valid_specs = [
            {
                "embedding_label": "custom_embedding",
                "model_name": "CustomModel",
                "task_type": "CUSTOM_TASK",
                "node_types": ["StatVar"],
                "node_filter_type": "NoFilter"
            },
            {
                "embedding_label": "another_embedding",
                "model_name": "AnotherModel",
                "task_type": "ANOTHER_TASK",
                "node_types": ["StatisticalVariable"],
                "node_filter_type": "NLStatisticalVariable"
            }
        ]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(valid_specs, f)
            temp_path = f.name

        try:
            os.environ['EMBEDDING_SPEC_PATH'] = temp_path
            importlib.reload(config)
            expected = [config.EmbeddingSpec(**s) for s in valid_specs]
            self.assertEqual(config.EMBEDDING_SPECS, expected)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_single_yaml_spec(self):
        single_spec = {
            "embedding_label": "single_embedding",
            "model_name": "SingleModel",
            "task_type": "SINGLE_TASK",
            "node_types": ["StatVar"],
            "node_filter_type": "NoFilter"
        }
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(single_spec, f)
            temp_path = f.name

        try:
            os.environ['EMBEDDING_SPEC_PATH'] = temp_path
            importlib.reload(config)
            expected = [config.EmbeddingSpec(**single_spec)]
            self.assertEqual(config.EMBEDDING_SPECS, expected)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_invalid_yaml_specs_format(self):
        # Missing required keys
        invalid_specs = [
            {
                "embedding_label": "custom_embedding",
                "model_name": "CustomModel"
            }
        ]
        with tempfile.NamedTemporaryFile(mode='w', suffix='.yaml', delete=False) as f:
            yaml.dump(invalid_specs, f)
            temp_path = f.name

        try:
            os.environ['EMBEDDING_SPEC_PATH'] = temp_path
            importlib.reload(config)
            self.assertEqual(config.EMBEDDING_SPECS, config._DEFAULT_EMBEDDING_SPECS)
        finally:
            if os.path.exists(temp_path):
                os.remove(temp_path)

    def test_spanner_embedding_settings_dev_yaml(self):
        os.environ['EMBEDDING_SPEC_PATH'] = 'spanner_embedding_settings_dev.yaml'
        importlib.reload(config)
        expected = [
            config.EmbeddingSpec(
                embedding_label="base_text_embedding",
                model_name="NodeEmbeddingModel",
                task_type="RETRIEVAL_QUERY",
                node_types=["StatisticalVariable", "Topic"],
                node_filter_type="NLStatisticalVariable"
            )
        ]
        self.assertEqual(config.EMBEDDING_SPECS, expected)

    def test_nonexistent_yaml_file(self):
        os.environ['EMBEDDING_SPEC_PATH'] = '/nonexistent/path/to/spec.yaml'
        importlib.reload(config)
        self.assertEqual(config.EMBEDDING_SPECS, config._DEFAULT_EMBEDDING_SPECS)

if __name__ == '__main__':
    unittest.main()
