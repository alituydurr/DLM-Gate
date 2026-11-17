"""Asynchronous Side-Channel Audit Analytics on declass-audit (PySpark Structured Streaming & DataFrame Windowing)."""

from __future__ import annotations
from dataclasses import dataclass
import os
from typing import Any


@dataclass(frozen=True)
class WindowAuditSummary:
    """Windowed leakage & message-complexity summary computed off the critical TCB path."""

    window_id: str
    engine: str  # "PYSPARK_SQL_WINDOW" | "STDLIB_STREAM_AGGREGATOR"
    total_events: int
    approved_cloud_count: int
    rejected_local_count: int
    leaked_bits_per_owner: dict[str, float]
    total_tree_messages: int
    total_mutex_messages: int
    max_single_leak_bits: float
    invariant_preserved: bool


def build_kafka_structured_streaming_query(
    spark: Any,
    bootstrap_servers: str = "kafka:9092",
    topic: str = "declass-audit",
    window_duration: str = "10 seconds",
    slide_duration: str = "5 seconds",
) -> Any:
    """Construct the canonical PySpark Structured Streaming pipeline reading from Kafka topic declass-audit."""
    from pyspark.sql import functions as F  # type: ignore
    from pyspark.sql.types import (  # type: ignore
        ArrayType,
        BooleanType,
        DoubleType,
        IntegerType,
        StringType,
        StructField,
        StructType,
    )

    audit_schema = StructType(
        [
            StructField("window_id", StringType(), False),
            StructField("timestamp", DoubleType(), False),
            StructField("query_id", StringType(), False),
            StructField("approved", BooleanType(), False),
            StructField("route", StringType(), False),
            StructField("reason", StringType(), False),
            StructField("schema_name", StringType(), False),
            StructField("delta_bits", DoubleType(), False),
            StructField("bit_complexity", IntegerType(), False),
            StructField("charged_owners", ArrayType(StringType()), False),
            StructField("tree_msg_count", IntegerType(), False),
            StructField("mutex_msg_count", IntegerType(), False),
        ]
    )

    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", bootstrap_servers)
        .option("subscribe", topic)
        .option("startingOffsets", "latest")
        .load()
    )

    parsed = (
        raw_stream.selectExpr("CAST(value AS STRING) AS json_payload")
        .select(F.from_json(F.col("json_payload"), audit_schema).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp(F.from_unixtime(F.col("timestamp"))))
    )

    windowed_agg = (
        parsed.withWatermark("event_time", "15 seconds")
        .groupBy(
            F.window(F.col("event_time"), window_duration, slide_duration),
            F.col("window_id"),
        )
        .agg(
            F.count("*").alias("total_events"),
            F.sum(F.when(F.col("approved") == True, 1).otherwise(0)).alias("approved_cloud_count"),  # noqa: E712
            F.sum(F.when(F.col("approved") == False, 1).otherwise(0)).alias("rejected_local_count"),  # noqa: E712
            F.sum(
                F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "A"), F.col("delta_bits")).otherwise(0.0)
            ).alias("leaked_A"),
            F.sum(
                F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "B"), F.col("delta_bits")).otherwise(0.0)
            ).alias("leaked_B"),
            F.sum(
                F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "C"), F.col("delta_bits")).otherwise(0.0)
            ).alias("leaked_C"),
            F.max("tree_msg_count").alias("total_tree_messages"),
            F.sum("mutex_msg_count").alias("total_mutex_messages"),
            F.max(F.when(F.col("approved") == True, F.col("delta_bits")).otherwise(0.0)).alias("max_single_leak_bits"),  # noqa: E712
        )
    )
    return windowed_agg


def _run_pyspark_dataframe_aggregation(
    audit_events: list[dict[str, Any]],
    owner_budgets: dict[str, float],
) -> list[WindowAuditSummary]:
    """Execute real PySpark SQL DataFrame aggregation over collected audit events."""
    from pyspark.sql import SparkSession, functions as F  # type: ignore

    spark = (
        SparkSession.builder.master("local[*]")
        .appName("DLMGateDeclassAudit")
        .config("spark.ui.enabled", "false")
        .getOrCreate()
    )
    df = spark.createDataFrame(audit_events)
    agg_df = df.groupBy("window_id").agg(
        F.count("*").alias("total_events"),
        F.sum(F.when(F.col("approved") == True, 1).otherwise(0)).alias("approved_cloud_count"),  # noqa: E712
        F.sum(F.when(F.col("approved") == False, 1).otherwise(0)).alias("rejected_local_count"),  # noqa: E712
        F.sum(
            F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "A"), F.col("delta_bits")).otherwise(0.0)
        ).alias("leaked_A"),
        F.sum(
            F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "B"), F.col("delta_bits")).otherwise(0.0)
        ).alias("leaked_B"),
        F.sum(
            F.when(F.col("approved") & F.array_contains(F.col("charged_owners"), "C"), F.col("delta_bits")).otherwise(0.0)
        ).alias("leaked_C"),
        F.max("tree_msg_count").alias("total_tree_messages"),
        F.sum("mutex_msg_count").alias("total_mutex_messages"),
        F.max(F.when(F.col("approved") == True, F.col("delta_bits")).otherwise(0.0)).alias("max_single_leak_bits"),  # noqa: E712
    )
    rows = agg_df.collect()
    summaries: list[WindowAuditSummary] = []
    for r in rows:
        leaked = {
            "A": float(r["leaked_A"] or 0.0),
            "B": float(r["leaked_B"] or 0.0),
            "C": float(r["leaked_C"] or 0.0),
        }
        invariant_ok = all(leaked.get(o, 0.0) <= owner_budgets[o] for o in owner_budgets)
        summaries.append(
            WindowAuditSummary(
                window_id=str(r["window_id"]),
                engine="PYSPARK_SQL_WINDOW",
                total_events=int(r["total_events"]),
                approved_cloud_count=int(r["approved_cloud_count"]),
                rejected_local_count=int(r["rejected_local_count"]),
                leaked_bits_per_owner=leaked,
                total_tree_messages=int(r["total_tree_messages"] or 0),
                total_mutex_messages=int(r["total_mutex_messages"] or 0),
                max_single_leak_bits=float(r["max_single_leak_bits"] or 0.0),
                invariant_preserved=invariant_ok,
            )
        )
    return summaries


def analyze_audit_stream(
    audit_events: list[dict[str, Any]],
    budgets: dict[str, float] | None = None,
    enable_spark: bool | None = None,
) -> list[WindowAuditSummary]:
    """Aggregate declass-audit events per window using PySpark SQL/Streaming or deterministic stream aggregator."""
    if not audit_events:
        return []
    use_spark = (
        enable_spark
        if enable_spark is not None
        else os.getenv("ENABLE_SPARK", "false").lower() in ("1", "true", "yes")
    )
    owner_budgets = dict(budgets or {"A": 4.0, "B": 8.0, "C": 8.0})

    if use_spark:
        try:
            return _run_pyspark_dataframe_aggregation(audit_events, owner_budgets)
        except Exception:
            pass

    windows: dict[str, list[dict[str, Any]]] = {}
    for ev in audit_events:
        wid = str(ev.get("window_id", "win-1"))
        windows.setdefault(wid, []).append(ev)

    summaries: list[WindowAuditSummary] = []
    for wid, events in windows.items():
        leaked: dict[str, float] = {o: 0.0 for o in sorted(owner_budgets)}
        approved_cnt = 0
        rejected_cnt = 0
        mutex_msgs = 0
        max_leak = 0.0

        for ev in events:
            delta = float(ev.get("delta_bits", 0.0))
            if ev.get("approved"):
                approved_cnt += 1
                if delta > max_leak:
                    max_leak = delta
                for owner in ev.get("charged_owners", []):
                    leaked[owner] = leaked.get(owner, 0.0) + delta
            else:
                rejected_cnt += 1
            mutex_msgs += int(ev.get("mutex_msg_count", 0))

        tree_msgs = int(events[0].get("tree_msg_count", 6)) if events else 0
        invariant_ok = all(leaked.get(o, 0.0) <= owner_budgets[o] for o in owner_budgets)

        summaries.append(
            WindowAuditSummary(
                window_id=wid,
                engine="STDLIB_STREAM_AGGREGATOR",
                total_events=len(events),
                approved_cloud_count=approved_cnt,
                rejected_local_count=rejected_cnt,
                leaked_bits_per_owner=leaked,
                total_tree_messages=tree_msgs,
                total_mutex_messages=mutex_msgs,
                max_single_leak_bits=max_leak,
                invariant_preserved=invariant_ok,
            )
        )
    return summaries
