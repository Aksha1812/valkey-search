from valkey_search_test_case import ValkeySearchTestCaseBase
import pytest
import valkey
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters
from utils import IndexingTestHelper


def _wait_backfill_done(client, index_name):
    waiters.wait_for_true(
        lambda: IndexingTestHelper.is_backfill_complete_on_node(
            client, index_name
        )
    )


class TestFTAlter(ValkeySearchTestCaseBase):
    def test_add_numeric_field(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        assert c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "f2", "NUMERIC") == b"OK"

        info = c.execute_command("FT.INFO", "idx")
        info_str = str(info)
        assert "f2" in info_str

    def test_add_tag_field(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "t1", "TAG")

        info = c.execute_command("FT.INFO", "idx")
        assert b"t1" in info

    def test_add_text_field(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "body", "TEXT")

        info = c.execute_command("FT.INFO", "idx")
        assert b"body" in info

    def test_existing_data_searchable_after_alter(self):
        c = self.get_primary_connection()
        c.hset("doc:1", mapping={"price": "100", "color": "red"})
        c.hset("doc:2", mapping={"price": "200", "color": "blue"})

        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "price", "NUMERIC")
        _wait_backfill_done(c, "idx")

        c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "color", "TAG")
        _wait_backfill_done(c, "idx")

        # old field still works
        res = c.execute_command("FT.SEARCH", "idx", "@price:[100 100]")
        assert res[0] == 1

        # new field indexes existing docs via backfill
        res = c.execute_command("FT.SEARCH", "idx", "@color:{red}")
        assert res[0] == 1

    def test_skipinitialscan_skips_existing_data(self):
        c = self.get_primary_connection()
        c.hset("doc:1", mapping={"price": "100", "color": "red"})

        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "price", "NUMERIC")
        _wait_backfill_done(c, "idx")

        c.execute_command(
            "FT.ALTER", "idx", "SKIPINITIALSCAN", "SCHEMA", "ADD", "color", "TAG"
        )

        # existing doc should NOT appear (no backfill)
        res = c.execute_command("FT.SEARCH", "idx", "@color:{red}")
        assert res[0] == 0

        # new doc written after ALTER should be indexed
        c.hset("doc:2", mapping={"price": "200", "color": "red"})
        _wait_backfill_done(c, "idx")
        res = c.execute_command("FT.SEARCH", "idx", "@color:{red}")
        assert res[0] == 1

    def test_duplicate_field_rejected(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        with pytest.raises(valkey.exceptions.ResponseError) as exc:
            c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "f1", "TAG")
        assert "already exists" in str(exc.value)

    def test_index_not_found(self):
        c = self.get_primary_connection()
        with pytest.raises(valkey.exceptions.ResponseError) as exc:
            c.execute_command("FT.ALTER", "no_such", "SCHEMA", "ADD", "f1", "NUMERIC")
        assert "not found" in str(exc.value)

    def test_missing_schema_keyword(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        with pytest.raises(valkey.exceptions.ResponseError):
            c.execute_command("FT.ALTER", "idx", "ADD", "f2", "NUMERIC")

    def test_missing_add_keyword(self):
        c = self.get_primary_connection()
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "f1", "NUMERIC")
        with pytest.raises(valkey.exceptions.ResponseError):
            c.execute_command("FT.ALTER", "idx", "SCHEMA", "f2", "NUMERIC")

    def test_saverestore_preserves_altered_schema(self):
        c = self.get_primary_connection()
        c.hset("doc:1", mapping={"price": "10", "color": "red"})
        c.execute_command("FT.CREATE", "idx", "ON", "HASH", "SCHEMA", "price", "NUMERIC")
        _wait_backfill_done(c, "idx")
        c.execute_command("FT.ALTER", "idx", "SCHEMA", "ADD", "color", "TAG")
        _wait_backfill_done(c, "idx")

        c.execute_command("SAVE")
        self.server.restart(remove_rdb=False)
        c = self.get_primary_connection()

        # both fields survive RDB round-trip
        res = c.execute_command("FT.SEARCH", "idx", "@price:[10 10]")
        assert res[0] == 1
        res = c.execute_command("FT.SEARCH", "idx", "@color:{red}")
        assert res[0] == 1
