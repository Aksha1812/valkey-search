import struct, threading, time
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkey.exceptions import ConnectionError, ResponseError
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector, Tag, Numeric


def _f32(xs):
    return b"".join(struct.pack("<f", x) for x in xs)


class TestFanoutRaceDet(ValkeySearchClusterTestCaseDebugMode):
    """Deterministic reproduction of the index_key_info_ data race (ELMO-120313).

    CrashReproRaceGateMs gates background mutation workers to park right after
    writing index_key_info_ (write lock held); the coordinator FT.SEARCH on the
    main thread then performs the UNLOCKED GetIndexKeyInfoSize() read while the
    worker holds. Under TSAN this reports the race. Requires the repro build.
    """

    CLUSTER_SIZE = 3

    def test_race_det(self):
        client: ValkeyCluster = self.new_cluster_client()
        idx = Index("cd", [Vector("v", 3, type="HNSW"), Tag("t"), Numeric("n")])
        idx.create(client)
        idx.load_data(client, 2000)
        waiters.wait_for_true(
            lambda: sum(idx.info(self.client_for_primary(i)).num_docs
                        for i in range(len(self.replication_groups))) >= 1900,
            timeout=40)

        for node in (self.client_for_primary(i) for i in range(self.CLUSTER_SIZE)):
            node.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "set",
                                  "CrashReproRaceGateMs", "1500")

        stop = threading.Event()

        def mutator(seed):
            cl = self.new_cluster_client()
            i = seed * 100000
            while not stop.is_set():
                i += 1
                try:
                    cl.execute_command("HSET", f":cd:{(i % 2000):08d}", "v",
                                      _f32([i % 5, 2.0, 3.0]), "t", f"t{i%6}",
                                      "n", str(i % 100))
                except (ResponseError, ConnectionError):
                    pass

        def searcher(seed):
            for _ in range(200):
                if stop.is_set():
                    break
                try:
                    c = self.get_primary(seed % self.CLUSTER_SIZE).get_new_client()
                    c.execute_command(
                        "FT.SEARCH", "cd", "*=>[KNN 20 @v $BLOB]",
                        "RETURN", "1", "t", "LIMIT", "0", "10", "DIALECT", "2",
                        "PARAMS", "2", "BLOB", _f32([seed % 5, 1.0, 1.0]))
                except (ConnectionError, ResponseError):
                    pass

        threads = ([threading.Thread(target=mutator, args=(i,)) for i in range(4)]
                   + [threading.Thread(target=searcher, args=(i,)) for i in range(8)])
        for t in threads:
            t.start()
        for t in threads[4:]:
            t.join(timeout=120)
        stop.set()
        for t in threads[:4]:
            t.join(timeout=20)

        for node in (self.client_for_primary(i) for i in range(self.CLUSTER_SIZE)):
            node.execute_command("FT._DEBUG", "CONTROLLED_VARIABLE", "set",
                                  "CrashReproRaceGateMs", "0")
        assert self.client_for_primary(0).ping()
