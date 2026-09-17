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
"""Unit tests for standalone validation-helper in import repo."""

import json
import unittest
from unittest import mock

import main as validation_helper_main


class ValidationHelperTest(unittest.TestCase):

    @mock.patch('main.ValidationRunner')
    @mock.patch('main.bigquery_differ.run_bigquery_differ')
    @mock.patch('main.file_util.file_copy')
    @mock.patch('main.file_util.file_get_matching')
    @mock.patch('main.storage.Client')
    def test_run_validation_job_staging(self, mock_storage_cls, mock_get_match,
                                        mock_copy, mock_bq_differ,
                                        mock_runner_cls):
        mock_gcs = mock_storage_cls.return_value
        mock_bucket = mock_gcs.bucket.return_value

        # Mock blobs for staging_version.txt, latest_version.txt, import_summary.json
        def mock_blob_side_effect(blob_path):
            b = mock.MagicMock()
            b.name = blob_path
            if blob_path.endswith('staging_version.txt'):
                b.exists.return_value = True
                b.download_as_text.return_value = '2026_09_17_00_00'
            elif blob_path.endswith('latest_version.txt'):
                b.exists.return_value = True
                b.download_as_text.return_value = '2026_09_10_00_00'
            elif blob_path.endswith('import_summary.json'):
                b.exists.return_value = True
                b.download_as_text.return_value = json.dumps({
                    'import_name': 'USFed_ConstantMaturityRates',
                    'execution_time': 120,
                    'data_volume': 5000,
                    'import_stats': {
                        'source_data_size': 2000,
                        'mcf_data_size': 3000
                    }
                })
            else:
                b.exists.return_value = False
            return b

        mock_bucket.blob.side_effect = mock_blob_side_effect
        mock_gcs.list_blobs.return_value = []

        mock_get_match.return_value = ['gs://bucket/path/file.mcf']
        mock_bq_differ.return_value = {
            'obs_diff_count': 10,
            'schema_diff_count': 0
        }

        mock_runner = mock_runner_cls.return_value
        mock_runner.run_validations.return_value = (True, [])

        rc = validation_helper_main.run_validation_job(
            absolute_import_name=
            'scripts/us_fed/treasury_constant_maturity_rates:USFed_ConstantMaturityRates',
            import_config_str='{}',
            version_override='',
            bucket_name='test-bucket',
            bq_dataset='test_dataset')

        self.assertEqual(rc, 0)
        mock_bq_differ.assert_called_once()
        mock_runner.run_validations.assert_called_once()


if __name__ == '__main__':
    unittest.main()
