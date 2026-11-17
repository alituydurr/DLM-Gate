"""Faz 3 Verification Suite: Alpine Docker Config, Kafka Event Bus (LOCAL_FALLBACK), and Side-Channel Audit Analytics."""

from __future__ import annotations
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.spanning_tree import run_bcast_convergecast
from src.mutex.ricart_agrawala import (
    run_ricart_agrawala_simulation,
    run_unprotected_race_simulation,
)
from src.mutex.ricart_agrawala import default_concurrent_requests
from src.stream.gate_worker import DistributedGateWorker, request_from_event, request_to_event
from src.stream.kafka_bus import TOPIC_AUDIT, TOPIC_MUTEX, TOPIC_TREE, KafkaEventBus
from src.stream.spark_audit import analyze_audit_stream, build_kafka_structured_streaming_query


def test_docker_alpine_and_kraft_compose_files() -> None:
    dockerfile = (ROOT_DIR / "Dockerfile").read_text(encoding="utf-8")
    compose = (ROOT_DIR / "docker" / "docker-compose.yml").read_text(encoding="utf-8")

    assert "FROM python:3.12-alpine" in dockerfile
    assert "apache/kafka:3.8.1" in compose
    assert "KAFKA_PROCESS_ROLES: broker,controller" in compose, "Must use ZooKeeper-less KRaft mode"
    for svc in ("gate-1:", "gate-2:", "gate-3:", "dashboard:"):
        assert svc in compose


def test_kafka_bus_and_side_channel_audit_analytics() -> None:
    bus = KafkaEventBus(local_fallback=True)
    budgets = {"A": 4.0, "B": 8.0, "C": 8.0}

    # 1. Publish Spanning Tree Bcast_ST + Ccast_ST run (6 messages)
    tree_res = run_bcast_convergecast(inject_on_b=True)
    tree_count = bus.publish_tree_run(tree_res)
    assert tree_count == 6
    assert len(bus.read_topic(TOPIC_TREE)) == 6

    # 2. Window 1: Protected by Ricart-Agrawala + Piggybacked G-Counter (12 mutex messages, 3 audit events)
    ra_res = run_ricart_agrawala_simulation(budgets=budgets)
    mutex_cnt, audit_cnt = bus.publish_mutex_run(ra_res, window_id="win-protected")
    assert mutex_cnt == 12
    assert audit_cnt == 3
    assert len(bus.read_topic(TOPIC_MUTEX)) == 12

    # 3. Window 2: Unprotected Race Condition (0 mutex messages, 3 audit events with double-spending)
    race_res = run_unprotected_race_simulation(budgets=budgets)
    bus.publish_mutex_run(race_res, window_id="win-race")
    assert len(bus.read_topic(TOPIC_AUDIT)) == 6

    # 4. Run Side-Channel Audit Analytics across both windows
    summaries = {
        s.window_id: s
        for s in analyze_audit_stream(bus.read_topic(TOPIC_AUDIT), budgets=budgets, enable_spark=False)
    }

    w_prot = summaries["win-protected"]
    assert w_prot.total_events == 3
    assert w_prot.approved_cloud_count == 2
    assert w_prot.rejected_local_count == 1
    assert w_prot.leaked_bits_per_owner == {"A": 4.0, "B": 4.0, "C": 4.0}
    assert w_prot.total_tree_messages == 6
    assert w_prot.total_mutex_messages == 12
    assert w_prot.invariant_preserved is True

    w_race = summaries["win-race"]
    assert w_race.approved_cloud_count == 3
    assert w_race.rejected_local_count == 0
    assert w_race.leaked_bits_per_owner["A"] == 6.0
    assert w_race.total_mutex_messages == 0
    assert w_race.invariant_preserved is False, "Audit analytics must detect B_A budget breach under race condition"

    assert callable(build_kafka_structured_streaming_query)


def test_gate_workers_run_ricart_agrawala_over_event_bus() -> None:
    """3 independent workers (as in Docker) coordinate only via mutex events; B_A = 4 lets exactly 2 queries pass."""
    ids = ("gate-1", "gate-2", "gate-3")
    workers = {
        rid: DistributedGateWorker(replica_id=rid, peers=tuple(p for p in ids if p != rid), local_fallback=True)
        for rid in ids
    }
    for rid, req in zip(ids, default_concurrent_requests()):
        assert workers[rid].submit(request_from_event(request_to_event(rid, req))) is None  # must wait for OKAYs

    # Deliver every published mutex event to all workers (a Kafka topic with one consumer group per gate)
    cursor = {rid: 0 for rid in ids}
    decisions = []
    progressed = True
    while progressed:
        progressed = False
        for rid, w in workers.items():
            events = w.bus.read_topic(TOPIC_MUTEX)[cursor[rid]:]
            cursor[rid] += len(events)
            for ev in events:
                progressed = True
                for target in workers.values():
                    d = target.handle_mutex_event(ev)
                    if d is not None:
                        decisions.append(d)

    assert [d.query_id for d in decisions] == ["q-1", "q-2", "q-3"]
    assert [d.approved for d in decisions] == [True, True, False]
    assert decisions[2].reason == "THEOREM1_BUDGET_EXCEEDED:A"
    assert sum(cursor.values()) == 12, "3 entries x 2(N-1) = 12 REQUEST/OKAY messages"
    assert all(w.replica.deferred_queue == [] and not w.replica.requesting_cs for w in workers.values())


if __name__ == "__main__":
    test_docker_alpine_and_kraft_compose_files()
    test_kafka_bus_and_side_channel_audit_analytics()
    test_gate_workers_run_ricart_agrawala_over_event_bus()
    print("[PASS] Faz 3 Alpine Docker, Kafka Event Bus ve Yan Kanal Audit Analitigi dogrulandi.")
