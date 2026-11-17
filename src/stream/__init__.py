"""Streaming & side-channel analytics layer (Kafka Event Bus, PySpark Audit Aggregator). Gate daemon: src.stream.gate_worker."""

from src.stream.kafka_bus import TOPIC_AUDIT, TOPIC_MUTEX, TOPIC_TREE, KafkaEventBus
from src.stream.spark_audit import (
    WindowAuditSummary,
    analyze_audit_stream,
    build_kafka_structured_streaming_query,
)

__all__ = [
    "TOPIC_TREE",
    "TOPIC_MUTEX",
    "TOPIC_AUDIT",
    "KafkaEventBus",
    "WindowAuditSummary",
    "analyze_audit_stream",
    "build_kafka_structured_streaming_query",
]
