"""Integration test for issue #1274: background indexing time must not be
attributed to the triggering write command in SLOWLOG."""

import time
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseDebugMode
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from utils import IndexingTestHelper, run_in_thread, pausepoint_hit


class TestSlowlogIngestAttribution(ValkeySearchTestCaseDebugMode):

    def append_startup_args(self, args: dict[str, str]) -> dict[str, str]:
        args = super().append_startup_args(args)
        args["search.writer-threads"] = "1"
        return args

    def test_write_slowlog_excludes_background_indexing(self):
        """A write blocked while its background indexing is paused must not have
        that paused duration recorded against it in SLOWLOG."""
        client: Valkey = self.server.get_new_client()

        client.execute_command(
            "FT.CREATE", "idx", "ON", "HASH", "PREFIX", "1", "doc:",
            "SCHEMA", "content", "TEXT"
        )
        client.execute_command("HSET", "doc:1", "content", "hello world")
        IndexingTestHelper.is_indexing_complete_on_node(client, "idx")

        # Hold the mutation in-flight so the writing client stays blocked.
        client.execute_command("FT._DEBUG PAUSEPOINT SET mutation_processing")
        client.execute_command("SLOWLOG", "RESET")
        client.execute_command("CONFIG", "SET", "slowlog-log-slower-than", "0")

        hset_thread, _, hset_err = run_in_thread(
            lambda: self.server.get_new_client().execute_command(
                "HSET", "doc:1", "content", "updated"
            )
        )
        waiters.wait_for_true(lambda: pausepoint_hit(client, "mutation_processing"))

        hold_seconds = 2.0
        time.sleep(hold_seconds)

        client.execute_command("FT._DEBUG PAUSEPOINT RESET mutation_processing")
        hset_thread.join()
        assert hset_err[0] is None

        durations = [
            entry[2]
            for entry in client.execute_command("SLOWLOG", "GET", 128)
            if entry[3][0] == b"HSET"
        ]
        assert durations, "expected the blocked HSET to be recorded in SLOWLOG"
        # The paused background window (~hold_seconds) must not be attributed to
        # the write. Main-thread time for a single HSET is far below this bound.
        assert max(durations) < hold_seconds * 1_000_000 / 2
