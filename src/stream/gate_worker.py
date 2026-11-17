"""Distributed Gate Replica Worker for Docker Compose (gate-1..3): real Ricart-Agrawala over Kafka topics.

Each container owns one GateReplica (same state machine as the in-process simulation) and talks to its peers
only through the dlm-mutex topic; queries arrive on dlm-query. Watch it live with:
    docker compose -f docker/docker-compose.yml logs -f gate-1 gate-2 gate-3
and fire 3 concurrent queries with:
    docker compose -f docker/docker-compose.yml exec gate-1 python -m src.stream.gate_worker submit
"""

from __future__ import annotations
import json
import os
import sys
from src.core.dlm import DLMLabel
from src.core.gate_fsm import DeclassDecision, DeclassRequest, GateFSM
from src.mutex.ricart_agrawala import GateReplica, default_concurrent_requests
from src.stream.kafka_bus import TOPIC_MUTEX, TOPIC_QUERY, KafkaEventBus

DEFAULT_PEERS = "gate-1,gate-2,gate-3"


def request_to_event(replica: str, req: DeclassRequest) -> dict:
    return {
        "replica": replica,
        "query_id": req.query_id,
        "schema_name": req.schema_name,
        "candidate_value": req.candidate_value,
        "plan_integrity": req.plan_integrity,
        "target_reader": req.target_reader,
        "endorsed": req.endorsed,
        "label": {
            "policies": {o: sorted(r) for o, r in req.label.policies.items()},
            "integrity": req.label.integrity,
        },
    }


def request_from_event(event: dict) -> DeclassRequest:
    label = DLMLabel(
        policies={o: frozenset(r) for o, r in event["label"]["policies"].items()},
        integrity=event["label"]["integrity"],
    )
    return DeclassRequest(
        query_id=event["query_id"],
        label=label,
        schema_name=event["schema_name"],
        candidate_value=event["candidate_value"],
        plan_integrity=event.get("plan_integrity", "HIGH"),
        target_reader=event.get("target_reader", "PUBLIC"),
        endorsed=bool(event.get("endorsed", False)),
    )


class DistributedGateWorker:
    """Stateful gate container: queues queries, runs Ricart-Agrawala with peers over Kafka, decides inside the CS."""

    def __init__(
        self,
        replica_id: str | None = None,
        peers: tuple[str, ...] | None = None,
        bootstrap_servers: str | None = None,
        local_fallback: bool | None = None,
    ) -> None:
        self.replica_id = replica_id or os.getenv("REPLICA_ID", "gate-1")
        if peers is None:
            peers = tuple(p for p in os.getenv("PEERS", DEFAULT_PEERS).split(",") if p and p != self.replica_id)
        self.replica = GateReplica(
            replica_id=self.replica_id,
            fsm=GateFSM(budgets={"A": 4.0, "B": 8.0, "C": 8.0}, replica_id=self.replica_id),
            peers=peers,
        )
        self.bus = KafkaEventBus(
            bootstrap_servers=bootstrap_servers or os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092"),
            local_fallback=local_fallback if local_fallback is not None else (os.getenv("LOCAL_FALLBACK", "false").lower() == "true"),
        )
        self.pending: list[DeclassRequest] = []
        self.current: DeclassRequest | None = None
        self.verbose = False

    @property
    def fsm(self) -> GateFSM:
        return self.replica.fsm

    def _log(self, text: str) -> None:
        if self.verbose:
            print(f"[{self.replica_id}] {text}", flush=True)

    def _send(self, msg_type: str, receiver: str, req_ts: int, ledger: dict[str, dict[str, float]] | None) -> None:
        self._log(f"-> {receiver:6s} {msg_type:7s} ts={req_ts}" + (f" ledger={ledger}" if ledger else ""))
        self.bus.publish(
            TOPIC_MUTEX,
            {
                "msg_type": msg_type,
                "sender": self.replica_id,
                "receiver": receiver,
                "req_ts": req_ts,
                "piggybacked_ledger": ledger,
            },
        )

    def submit(self, req: DeclassRequest) -> DeclassDecision | None:
        self.pending.append(req)
        return self._maybe_request()

    def _maybe_request(self) -> DeclassDecision | None:
        if self.current is not None or not self.pending:
            return None
        self.current = self.pending.pop(0)
        self.replica.begin_request()
        self._log(f"{self.current.query_id} icin kritik bolge istegi (Lamport ts={self.replica.req_ts})")
        self.replica.broadcast_request(self._send)
        return self._enter_cs() if not self.replica.peers else None

    def handle_mutex_event(self, event: dict) -> DeclassDecision | None:
        """Process a peer REQUEST/OKAY; returns the decision if this event let us enter the CS."""
        if event.get("receiver") != self.replica_id:
            return None
        ready = self.replica.on_message(
            event["msg_type"],
            event["sender"],
            int(event.get("req_ts", 0)),
            event.get("piggybacked_ledger"),
            self._send,
        )
        return self._enter_cs() if ready else None

    def handle_query_event(self, event: dict) -> DeclassDecision | None:
        if event.get("replica") != self.replica_id:
            return None
        return self.submit(request_from_event(event))

    def _enter_cs(self) -> DeclassDecision:
        assert self.current is not None
        self.replica.in_cs = True
        decision = self.fsm.step(self.current)
        self._log(
            f"KRITIK BOLGE: {decision.query_id} -> {decision.route} dL={decision.delta_bits}b "
            f"kalan={decision.remaining_budgets} ({decision.reason})"
        )
        self.bus.publish_audit_event(decision, mutex_msgs=2 * len(self.replica.peers), window_id=f"worker-{self.replica_id}")
        self.replica.release_cs(self._send)
        self.current = None
        self._maybe_request()
        return decision

    def run_forever(self) -> None:
        from kafka import KafkaConsumer  # type: ignore

        if self.bus.local_fallback:
            raise SystemExit(f"[{self.replica_id}] Kafka'ya baglanilamadi ({self.bus.bootstrap_servers}); yeniden denenecek.")
        self.verbose = True
        self._log(f"Gate worker basladi (backend: {self.bus.active_backend}, peers: {self.replica.peers})")
        consumer = KafkaConsumer(
            TOPIC_MUTEX,
            TOPIC_QUERY,
            bootstrap_servers=self.bus.bootstrap_servers,
            value_deserializer=lambda m: json.loads(m.decode("utf-8")),
            auto_offset_reset="earliest",
            group_id=f"group-{self.replica_id}",
        )
        for record in consumer:
            if record.topic == TOPIC_QUERY:
                self.handle_query_event(record.value)
            else:
                self.handle_mutex_event(record.value)
            self.bus.flush()


def submit_demo_queries() -> None:
    """Publish 3 concurrent 2-bit queries (one per gate) on B_A = 4 bits: exactly 2 must pass."""
    bus = KafkaEventBus(local_fallback=False)
    if bus.local_fallback:
        raise SystemExit(f"Kafka'ya baglanilamadi ({bus.bootstrap_servers}).")
    peers = os.getenv("PEERS", DEFAULT_PEERS).split(",")
    for replica, req in zip(peers, default_concurrent_requests()):
        bus.publish(TOPIC_QUERY, request_to_event(replica, req))
    bus.flush()
    print(f"{len(peers)} sorgu gonderildi ({bus.active_backend}).")


if __name__ == "__main__":
    if sys.argv[1:] == ["submit"]:
        submit_demo_queries()
    else:
        DistributedGateWorker().run_forever()
