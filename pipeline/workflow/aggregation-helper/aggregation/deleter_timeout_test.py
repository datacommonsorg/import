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

"""Tests that AggregationDeleter's timeout reaches its Partitioned DML RPCs."""

import functools
import types
import unittest
from unittest.mock import MagicMock, patch

from google.auth.credentials import AnonymousCredentials
from google.cloud import spanner
from google.cloud.spanner_v1 import (
    PartialResultSet,
    ResultSetMetadata,
    ResultSetStats,
    Session,
    Transaction,
)

from aggregation.deleter import DEFAULT_DELETION_TIMEOUT_SECONDS, AggregationDeleter

# A real Spanner client in emulator mode. Nothing is dialed: the tests replace
# the gRPC stubs underneath the generated client before any RPC is sent.
_OfflineSpannerClient = functools.partial(
    spanner.Client,
    credentials=AnonymousCredentials(),
    client_options={"api_endpoint": "localhost:9010"},
)


@patch("aggregation.deleter.spanner.Client", new=_OfflineSpannerClient)
class TestDeletionTimeout(unittest.TestCase):
    """Runs the real Database.execute_partitioned_dml() over faked gRPC stubs.

    AggregationDeleter._configure_streaming_timeout() depends on
    google-cloud-spanner internals; these tests pin it to the locked version.
    """

    def _run_delete(self, deleter):
        """Runs one Partitioned DML delete.

        Returns:
            The row count and a dict of RPC name -> deadline it was sent with.
        """
        database = deleter.spanner_database
        transport = database.spanner_api._transport
        deadlines = {}

        def fake_rpc(name, response):
            def rpc(request, timeout=None, **kwargs):
                deadlines[name] = timeout
                return response

            return rpc

        stream = iter(
            [
                PartialResultSet(
                    metadata=ResultSetMetadata(),
                    stats=ResultSetStats(row_count_lower_bound=7),
                )
            ]
        )
        fakes = {
            transport.create_session: fake_rpc(
                "create_session", Session(name=f"{database.name}/sessions/s1")
            ),
            transport.begin_transaction: fake_rpc(
                "begin_transaction", Transaction(id=b"txn")
            ),
            transport.execute_streaming_sql: fake_rpc("execute_streaming_sql", stream),
        }
        for stub, fake in fakes.items():
            transport._wrapped_methods[stub]._target = fake

        rows = database.execute_partitioned_dml(
            "DELETE FROM Edge WHERE provenance = 'dc/base/Import'"
        )
        return rows, deadlines

    def test_partitioned_dml_stream_uses_deletion_timeout(self):
        rows, deadlines = self._run_delete(
            AggregationDeleter("proj", "inst", "db", timeout=1234.0)
        )

        self.assertEqual(rows, 7)
        # Without the override this is the generated client's 3600s default.
        self.assertAlmostEqual(deadlines["execute_streaming_sql"], 1234.0, delta=1)

    def test_default_deletion_timeout(self):
        _, deadlines = self._run_delete(AggregationDeleter("proj", "inst", "db"))

        self.assertAlmostEqual(
            deadlines["execute_streaming_sql"],
            DEFAULT_DELETION_TIMEOUT_SECONDS,
            delta=1,
        )

    def test_fails_loudly_if_library_internals_change(self):
        transport = MagicMock()
        # Simulates a library version whose wrapper no longer has `_timeout`.
        transport._wrapped_methods = {
            transport.execute_streaming_sql: types.SimpleNamespace()
        }
        database = MagicMock()
        database.spanner_api._transport = transport

        with self.assertRaisesRegex(RuntimeError, "internals changed"):
            AggregationDeleter("proj", "inst", "db")._configure_streaming_timeout(
                database
            )

    def test_database_not_cached_if_deadline_cannot_be_set(self):
        deleter = AggregationDeleter("proj", "inst", "db")
        with patch.object(
            AggregationDeleter, "_configure_streaming_timeout", side_effect=RuntimeError
        ):
            with self.assertRaises(RuntimeError):
                _ = deleter.spanner_database

        # The next access must fail again rather than reuse a 1-hour client.
        self.assertIsNone(deleter._spanner_database)


if __name__ == "__main__":
    unittest.main()
