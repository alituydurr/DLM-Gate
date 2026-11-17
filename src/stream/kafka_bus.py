"""Kafka Event Bus for Tree (dlm-tree), Mutex (dlm-mutex), and Side-Channel Audit (declass-audit) with LOCAL_FALLBACK."""

from __future__ import annotations
from dataclasses import dataclass, field
import json
import os
import time
from typing import Any
from src.core.gate_fsm import DeclassDecision
from src.core.spanning_tree import SpanningTreeResult
from src.mutex.ricart_agrawala import ConcurrentRunResult

TOPIC_TREE = "dlm-tree"
TOPIC_MUTEX = "dlm-mutex"
TOPIC_AUDIT = "declass-audit"
TOPIC_QUERY = "dlm-query"


@dataclass
class KafkaEventBus:
    """Dual-mode event bus: connects to KRaft Kafka when available or runs in-memory under LOCAL_FALLBACK."""

    bootstrap_servers: str = field(default_factory=lambda: os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092"))
    local_fallback: bool = field(
        default_factory=lambda: os.getenv("LOCAL_FALLBACK", "true").lower() in ("1", "true", "yes")
    )
    _topics: dict[str, list[dict[str, Any]]] = field(default_factory=lambda: {
        TOPIC_TREE: [],
        TOPIC_MUTEX: [],
        TOPIC_AUDIT: [],
    })
    _producer: Any = field(default=None, init=False, repr=False)
    active_backend: str = field(default="LOCAL_MEMORY", init=False)

    def __post_init__(self) -> None:
        if not self.local_fallback:
            try:
                from kafka import KafkaProducer  # type: ignore
                self._producer = KafkaProducer(
                    bootstrap_servers=self.bootstrap_servers,
                    value_serializer=lambda v: json.dumps(v, ensure_ascii=False).encode("utf-8"),
                    request_timeout_ms=1500,
                )
                self.active_backend = f"KAFKA({self.bootstrap_servers})"
            except Exception:
                # Fall back to in-memory topics when the Kafka container is offline
                self.local_fallback = True
                self.active_backend = "LOCAL_MEMORY_FALLBACK"

    def publish(self, topic: str, event: dict[str, Any]) -> dict[str, Any]:
        record = {"topic": topic, "timestamp": time.time(), **event}
        self._topics.setdefault(topic, []).append(record)
        if self._producer is not None:
            self._producer.send(topic, record)
        return record

    def publish_tree_run(self, tree_result: SpanningTreeResult) -> int:
        """Publish all 2n-2 = 6 Bcast_ST / Ccast_ST messages to dlm-tree topic."""
        for msg in tree_result.messages:
            self.publish(
                TOPIC_TREE,
                {
                    "step": msg.step,
                    "msg_type": msg.msg_type,
                    "sender": msg.sender,
                    "receiver": msg.receiver,
                    "payload": msg.payload,
                    "label": msg.label.format_short() if msg.label else None,
                    "vc": dict(msg.vc_snapshot),
                },
            )
        return len(tree_result.messages)

    def publish_mutex_run(self, mutex_result: ConcurrentRunResult, window_id: str = "win-1") -> tuple[int, int]:
        """Publish 12 Ricart-Agrawala messages to dlm-mutex and decision records to declass-audit."""
        for msg in mutex_result.messages:
            self.publish(
                TOPIC_MUTEX,
                {
                    "step": msg.step,
                    "msg_type": msg.msg_type,
                    "sender": msg.sender,
                    "receiver": msg.receiver,
                    "req_ts": msg.req_ts,
                    "piggybacked_ledger": msg.piggybacked_ledger,
                },
            )
        per_entry_msgs = mutex_result.messages_per_entry
        for dec in mutex_result.decisions:
            self.publish_audit_event(dec, tree_msgs=6, mutex_msgs=per_entry_msgs, window_id=window_id)
        return len(mutex_result.messages), len(mutex_result.decisions)

    def publish_audit_event(
        self,
        decision: DeclassDecision,
        tree_msgs: int = 6,
        mutex_msgs: int = 4,
        window_id: str = "win-1",
    ) -> dict[str, Any]:
        """Emit an asynchronous audit event to the non-blocking declass-audit side channel."""
        return self.publish(
            TOPIC_AUDIT,
            {
                "window_id": window_id,
                "query_id": decision.query_id,
                "approved": decision.approved,
                "route": decision.route,
                "reason": decision.reason,
                "schema_name": decision.schema_name,
                "emitted_value": decision.emitted_value,
                "delta_bits": decision.delta_bits,
                "bit_complexity": decision.bit_complexity,
                "charged_owners": list(decision.charged_owners),
                "remaining_budgets": dict(decision.remaining_budgets),
                "tree_msg_count": tree_msgs,
                "mutex_msg_count": mutex_msgs,
            },
        )

    def flush(self) -> None:
        if self._producer is not None:
            self._producer.flush()

    def read_topic(self, topic: str) -> list[dict[str, Any]]:
        return list(self._topics.get(topic, []))

    def clear(self) -> None:
        for k in self._topics:
            self._topics[k].clear()
