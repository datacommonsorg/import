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

import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock

from aggregation_spec_utils import AggregationRunner
from aggregation_spec_utils import AggregationSpec
from aggregation_spec_utils import LocalJsonlSink
from aggregation_spec_utils import LocalJsonlSource
from aggregation_spec_utils import SpannerDmlMode
from aggregation_spec_utils import SpannerDmlSink
from aggregation_spec_utils import SpannerSqlSource
from aggregation_spec_utils import SpannerTableSink


class MockField:

    def __init__(self, name):
        self.name = name


class MockResults:

    def __init__(self, rows, field_names):
        self.rows = rows
        self.fields = [MockField(name) for name in field_names]

    def __iter__(self):
        return iter(self.rows)


class DelayedFieldResults:

    def __init__(self, rows, field_names):
        self.rows = rows
        self.field_names = field_names
        self._iteration_started = False

    @property
    def fields(self):
        if not self._iteration_started:
            return []
        return [MockField(name) for name in self.field_names]

    def __iter__(self):
        self._iteration_started = True
        return iter(self.rows)


class NoneFieldResults:

    def __init__(self, rows):
        self.rows = rows
        self.fields = None

    def __iter__(self):
        return iter(self.rows)


class NoBatchSizeSink:
    def __init__(self):
        self.batches = []
        self.started = False
        self.closed = False

    def start(self):
        self.started = True

    def write_many(self, rows):
        self.batches.append(rows)

    def close(self):
        self.closed = True


class TestAggregationSpecUtils(unittest.TestCase):

    def test_local_jsonl_source_to_local_jsonl_sink(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / 'input.jsonl'
            output_path = Path(temp_dir) / 'output.jsonl'
            self._write_jsonl(input_path, [{
                'subject_id': 'dc/1'
            }, {
                'subject_id': 'dc/2'
            }])

            count = AggregationRunner().run(
                AggregationSpec(
                    name='copy',
                    source=LocalJsonlSource(str(input_path)),
                    sink=LocalJsonlSink(str(output_path), batch_size=1),
                ))

            self.assertEqual(count, 2)
            self.assertEqual(self._read_jsonl(output_path), [{
                'subject_id': 'dc/1'
            }, {
                'subject_id': 'dc/2'
            }])

    def test_mapper_transforms_row_shape(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / 'input.jsonl'
            output_path = Path(temp_dir) / 'output.jsonl'
            self._write_jsonl(input_path, [{'source': 'dc/1'}])

            count = AggregationRunner().run(
                AggregationSpec(
                    name='mapped',
                    source=LocalJsonlSource(str(input_path)),
                    sink=LocalJsonlSink(str(output_path)),
                    mapper=lambda row: {'subject_id': row['source']},
                ))

            self.assertEqual(count, 1)
            self.assertEqual(self._read_jsonl(output_path),
                             [{'subject_id': 'dc/1'}])

    def test_spanner_source_streams_named_rows_from_snapshot(self):
        database = MagicMock()
        snapshot = database.snapshot.return_value.__enter__.return_value
        snapshot.execute_sql.return_value = MockResults(
            rows=[('dc/1', 'Node 1'), ('dc/2', 'Node 2')],
            field_names=['subject_id', 'name'],
        )
        source = SpannerSqlSource(database,
                                  'SELECT subject_id, name FROM Node',
                                  params={'limit': 2},
                                  param_types={'limit': 'INT64'})

        rows = list(source.read())

        self.assertEqual(rows, [{
            'subject_id': 'dc/1',
            'name': 'Node 1'
        }, {
            'subject_id': 'dc/2',
            'name': 'Node 2'
        }])
        snapshot.execute_sql.assert_called_once_with(
            'SELECT subject_id, name FROM Node',
            params={'limit': 2},
            param_types={'limit': 'INT64'},
        )

    def test_spanner_source_reads_fields_after_stream_starts(self):
        database = MagicMock()
        snapshot = database.snapshot.return_value.__enter__.return_value
        snapshot.execute_sql.return_value = DelayedFieldResults(
            rows=[('dc/1', 'Node 1')],
            field_names=['subject_id', 'name'],
        )

        rows = list(
            SpannerSqlSource(database,
                             'SELECT subject_id, name FROM Node').read())

        self.assertEqual(rows, [{'subject_id': 'dc/1', 'name': 'Node 1'}])

    def test_spanner_source_handles_none_fields(self):
        database = MagicMock()
        snapshot = database.snapshot.return_value.__enter__.return_value
        snapshot.execute_sql.return_value = NoneFieldResults(rows=[("dc/1", "Node 1")])

        rows = list(
            SpannerSqlSource(database, "SELECT subject_id, name FROM Node").read()
        )

        self.assertEqual(rows, [{"value": ("dc/1", "Node 1")}])

    def test_spanner_sink_writes_multiple_batches(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / 'input.jsonl'
            self._write_jsonl(input_path, [{
                'subject_id': f'dc/{idx}'
            } for idx in range(5)])

            database = MagicMock()
            batches = []

            def make_batch_context():
                context = MagicMock()
                batch = MagicMock()
                context.__enter__.return_value = batch
                batches.append(batch)
                return context

            database.batch.side_effect = make_batch_context
            sink = SpannerTableSink(database,
                                    table='Node',
                                    columns=['subject_id'],
                                    batch_size=2)

            count = AggregationRunner().run(
                AggregationSpec(
                    name='write_nodes',
                    source=LocalJsonlSource(str(input_path)),
                    sink=sink,
                ))

            self.assertEqual(count, 5)
            self.assertEqual(database.batch.call_count, 3)
            self.assertEqual(
                [call.kwargs['values'] for batch in batches
                 for call in batch.insert_or_update.call_args_list],
                [[['dc/0'], ['dc/1']], [['dc/2'], ['dc/3']], [['dc/4']]],
            )

    def test_empty_source_writes_nothing(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / 'input.jsonl'
            input_path.write_text('', encoding='utf-8')
            database = MagicMock()

            count = AggregationRunner().run(
                AggregationSpec(
                    name='empty',
                    source=LocalJsonlSource(str(input_path)),
                    sink=SpannerTableSink(database,
                                          table='Node',
                                          columns=['subject_id'],
                                          batch_size=2),
                ))

            self.assertEqual(count, 0)
            database.batch.assert_not_called()

    def test_runner_uses_default_batch_size_when_sink_omits_it(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            input_path = Path(temp_dir) / "input.jsonl"
            self._write_jsonl(
                input_path, [{"subject_id": "dc/1"}, {"subject_id": "dc/2"}]
            )
            sink = NoBatchSizeSink()

            count = AggregationRunner().run(
                AggregationSpec(
                    name="default_batch_size",
                    source=LocalJsonlSource(str(input_path)),
                    sink=sink,
                )
            )

            self.assertEqual(count, 2)
            self.assertTrue(sink.started)
            self.assertTrue(sink.closed)
            self.assertEqual(
                sink.batches, [[{"subject_id": "dc/1"}, {"subject_id": "dc/2"}]]
            )

    def test_runner_runs_transactional_dml_sink_without_source(self):
        database = MagicMock()
        transaction = MagicMock()
        transaction.execute_update.return_value = 3
        sql = "INSERT INTO Target (id) SELECT id FROM Source"

        def run_in_transaction_side_effect(callback):
            return callback(transaction)

        database.run_in_transaction.side_effect = run_in_transaction_side_effect

        count = AggregationRunner().run(
            AggregationSpec(
                name="insert_select",
                source=None,
                sink=SpannerDmlSink(
                    database=database,
                    sql=sql,
                    params={"import_name": "dc/test"},
                    param_types={"import_name": "STRING"},
                    timeout=120,
                ),
            ))

        self.assertEqual(count, 3)
        database.run_in_transaction.assert_called_once()
        transaction.execute_update.assert_called_once_with(
            sql,
            params={"import_name": "dc/test"},
            param_types={"import_name": "STRING"},
            timeout=120,
        )

    def test_runner_runs_partitioned_dml_sink_without_source(self):
        database = MagicMock()
        database.execute_partitioned_dml.return_value = 4
        sql = "DELETE FROM Target WHERE import_name = @import_name"

        count = AggregationRunner().run(
            AggregationSpec(
                name="delete_target",
                source=None,
                sink=SpannerDmlSink(
                    database=database,
                    sql=sql,
                    params={"import_name": "dc/test"},
                    param_types={"import_name": "STRING"},
                    mode=SpannerDmlMode.PARTITIONED,
                ),
            ))

        self.assertEqual(count, 4)
        database.execute_partitioned_dml.assert_called_once_with(
            sql,
            params={"import_name": "dc/test"},
            param_types={"import_name": "STRING"},
        )

    def test_partitioned_dml_rejects_timeout(self):
        sink = SpannerDmlSink(
            database=MagicMock(),
            sql="DELETE FROM Target WHERE import_name = @import_name",
            mode=SpannerDmlMode.PARTITIONED,
            timeout=120,
        )

        with self.assertRaisesRegex(ValueError, "does not support timeout"):
            sink.run()

    def _write_jsonl(self, path, rows):
        path.write_text(''.join(json.dumps(row) + '\n' for row in rows),
                        encoding='utf-8')

    def _read_jsonl(self, path):
        return [
            json.loads(line)
            for line in path.read_text(encoding='utf-8').splitlines()
        ]


if __name__ == '__main__':
    unittest.main()
