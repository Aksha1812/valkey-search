"""
Tests for FT.TAGVALS: the distinct set of values indexed in a TAG field.

Values come back normalized the way the index stores them -- lowercased and
whitespace-stripped unless the field is CASESENSITIVE -- deduplicated across
documents, and a value disappears once its last document is gone.
"""

import pytest
import valkey
from valkey.client import Valkey
from valkey_search_test_case import ValkeySearchTestCaseBase
from valkeytestframework.conftest import resource_port_tracker
from valkeytestframework.util import waiters


def tagvals(client, index, field):
    return sorted(
        v.decode() for v in client.execute_command("FT.TAGVALS", index, field)
    )


class TestFTTagVals(ValkeySearchTestCaseBase):

    def _create(self, client, index="idx", *extra):
        client.execute_command(
            "FT.CREATE", index, "ON", "HASH",
            "PREFIX", "1", "d:",
            "SCHEMA",
            "brand", "TAG", *extra,
            "title", "TEXT",
            "price", "NUMERIC",
        )

    def _wait_indexed(self, client, index, num_docs):
        waiters.wait_for_true(
            lambda: client.execute_command("FT.SEARCH", index, "*", "LIMIT", "0", "0")[0]
            == num_docs
        )

    def test_distinct_normalized_values(self):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        client.hset("d:1", mapping={"brand": "Nike", "title": "a", "price": "1"})
        client.hset("d:2", mapping={"brand": "NIKE", "title": "b", "price": "2"})
        client.hset("d:3", mapping={"brand": " Puma , Adidas ", "title": "c", "price": "3"})
        self._wait_indexed(client, "idx", 3)

        assert tagvals(client, "idx", "brand") == ["adidas", "nike", "puma"]

    def test_empty_index_returns_empty(self):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        assert tagvals(client, "idx", "brand") == []

    def test_value_drops_when_last_document_removed(self):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        client.hset("d:1", mapping={"brand": "nike", "title": "a", "price": "1"})
        client.hset("d:2", mapping={"brand": "puma", "title": "b", "price": "2"})
        self._wait_indexed(client, "idx", 2)
        assert tagvals(client, "idx", "brand") == ["nike", "puma"]

        client.delete("d:2")
        waiters.wait_for_true(
            lambda: tagvals(client, "idx", "brand") == ["nike"]
        )

    def test_case_sensitive_field_keeps_case(self):
        client: Valkey = self.server.get_new_client()
        self._create(client, "csidx", "CASESENSITIVE")
        client.hset("d:1", mapping={"brand": "Nike", "title": "a", "price": "1"})
        client.hset("d:2", mapping={"brand": "NIKE", "title": "b", "price": "2"})
        self._wait_indexed(client, "csidx", 2)

        assert tagvals(client, "csidx", "brand") == ["NIKE", "Nike"]

    def test_empty_tag_value_is_not_indexed(self):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        client.hset("d:1", mapping={"brand": "", "title": "a", "price": "1"})
        client.hset("d:2", mapping={"brand": "nike", "title": "b", "price": "2"})
        self._wait_indexed(client, "idx", 2)

        assert tagvals(client, "idx", "brand") == ["nike"]

    @pytest.mark.parametrize("field", ["title", "price"])
    def test_non_tag_field_rejected(self, field):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command("FT.TAGVALS", "idx", field)
        assert "Not a tag field" in str(e.value)

    def test_unknown_field_rejected(self):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        with pytest.raises(valkey.exceptions.ResponseError):
            client.execute_command("FT.TAGVALS", "idx", "nosuchfield")

    def test_unknown_index_rejected(self):
        client: Valkey = self.server.get_new_client()
        with pytest.raises(valkey.exceptions.ResponseError):
            client.execute_command("FT.TAGVALS", "nosuchindex", "brand")

    @pytest.mark.parametrize("args", [[], ["idx"], ["idx", "brand", "extra"]])
    def test_wrong_arity(self, args):
        client: Valkey = self.server.get_new_client()
        self._create(client)
        with pytest.raises(valkey.exceptions.ResponseError) as e:
            client.execute_command("FT.TAGVALS", *args)
        assert "wrong number of arguments" in str(e.value)
