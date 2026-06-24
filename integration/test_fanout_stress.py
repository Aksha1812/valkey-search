# Copyright (c) 2025, valkey-search contributors
# SPDX-License-Identifier: BSD 3-Clause
#
# [REPRO/STRESS] Concurrent cluster-mode FT.SEARCH raced against HSET/DEL
# mutations. Reproduces the data race on IndexSchema::index_key_info_: the
# fanout query path reads GetIndexKeyInfoSize() without the time-sliced mutex
# while background mutation workers insert/erase (and rehash) the same map.
# Run under ThreadSanitizer:
#   ./build.sh --tsan --run-integration-tests=test_fanout_stress

import struct
import threading
import pytest
from valkey_search_test_case import ValkeySearchClusterTestCaseDebugMode
from valkey.cluster import ValkeyCluster
from valkeytestframework.util import waiters
from valkeytestframework.conftest import resource_port_tracker
from indexes import Index, Vector, Numeric, Tag


def _f32(xs):
    return b"".join(struct.pack("<f", x) for x in xs)


class TestFanoutStress(ValkeySearchClusterTestCaseDebugMode):
    CLUSTER_SIZE = 3

    def test_concurrent_fanout_content_trim(self):
        client: ValkeyCluster = self.new_cluster_client()
        idx = Index("stress",
                    [Vector("v", 3, type="HNSW"), Tag("t"), Numeric("n")])
        idx.create(client)
        idx.load_data(client, 4000)
        waiters.wait_for_true(
            lambda: sum(idx.info(self.client_for_primary(i)).num_docs
                        for i in range(len(self.replication_groups))) >= 3900,
            timeout=40)

        stop = threading.Event()
        errors = []

        def searcher(seed):
            try:
                c = self.get_primary(seed % self.CLUSTER_SIZE).get_new_client()
                for _ in range(600):
                    if stop.is_set():
                        break
                    c.execute_command(
                        "FT.SEARCH", "stress",
                        "*=>[KNN 60 @v $BLOB]",
                        "RETURN", "2", "t", "n",
                        "LIMIT", "5", "20",
                        "DIALECT", "2",
                        "PARAMS", "2", "BLOB", _f32([seed % 7, 1.0, 2.0]),
                    )
            except Exception as e:  # noqa
                errors.append(("search", repr(e)))
                stop.set()

        def mutator(seed):
            # Concurrent HSET/DEL invalidates content mid-search -> exercises the
            # kContentionCheckRequired path and content-resolution/free race.
            try:
                cl: ValkeyCluster = self.new_cluster_client()
                i = seed * 100000
                while not stop.is_set():
                    i += 1
                    k = f":stress:{(i % 4000):08d}"
                    cl.execute_command("HSET", k, "v",
                                       _f32([i % 5, 2.0, 3.0]), "t", f"t{i%6}",
                                       "n", str(i % 100))
                    if i % 3 == 0:
                        cl.execute_command("DEL", f":stress:{(i*7 % 4000):08d}")
            except Exception as e:  # noqa
                errors.append(("mutate", repr(e)))

        threads = ([threading.Thread(target=searcher, args=(i,)) for i in range(12)]
                   + [threading.Thread(target=mutator, args=(i,)) for i in range(4)])
        for t in threads:
            t.start()
        # Let searchers finish; then stop mutators.
        for t in threads[:12]:
            t.join(timeout=180)
        stop.set()
        for t in threads[12:]:
            t.join(timeout=30)
        print("errors:", errors[:5])
