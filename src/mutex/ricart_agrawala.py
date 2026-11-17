"""Mod A: Ricart-Agrawala Distributed Mutual Exclusion with Piggybacked G-Counter CRDT (Mutex.pdf p.9, 50-55)."""

from __future__ import annotations
from dataclasses import dataclass, field
import queue
import threading
from typing import Callable
from src.core.clocks import GCounter
from src.core.gate_fsm import DeclassDecision, DeclassRequest, GateFSM
from src.core.spanning_tree import run_bcast_convergecast


@dataclass(frozen=True)
class MutexMessage:
    """Peer-to-peer message exchanged between stateful gate containers (gate-1..3)."""

    step: int
    msg_type: str  # "REQUEST" | "OKAY"
    sender: str
    receiver: str
    req_ts: int
    piggybacked_ledger: dict[str, dict[str, float]] | None = None


# send(msg_type, receiver, req_ts, piggybacked_ledger) — transport-agnostic (thread mailbox or Kafka topic)
Send = Callable[[str, str, int, "dict[str, dict[str, float]] | None"], None]


@dataclass
class GateReplica:
    """Stateful gate holding a local GateFSM and the Ricart-Agrawala state machine (shared by threads & Kafka workers)."""

    replica_id: str
    fsm: GateFSM
    peers: tuple[str, ...] = ()
    lamport_ts: int = 0
    requesting_cs: bool = False
    in_cs: bool = False
    req_ts: int = 0
    okay_received: set[str] = field(default_factory=set)
    deferred_queue: list[tuple[str, int]] = field(default_factory=list)
    mailbox: queue.Queue[MutexMessage] = field(default_factory=queue.Queue)
    state_lock: threading.Lock = field(default_factory=threading.Lock)
    cs_ready: threading.Event = field(default_factory=threading.Event)

    def snapshot_ledger(self) -> dict[str, dict[str, float]]:
        return {owner: counter.to_dict() for owner, counter in self.fsm.spent_counters.items()}

    def merge_piggybacked_ledger(self, payload: dict[str, dict[str, float]] | None) -> None:
        if not payload:
            return
        for owner, counts in payload.items():
            self.fsm.spent_counters.setdefault(owner, GCounter()).merge(GCounter(dict(counts)))

    def begin_request(self) -> None:
        """Lamport tick: the CS request is timestamped with the replica's own logical clock."""
        self.lamport_ts += 1
        self.req_ts = self.lamport_ts
        self.requesting_cs = True
        self.okay_received.clear()

    def broadcast_request(self, send: Send) -> None:
        for peer in self.peers:
            send("REQUEST", peer, self.req_ts, None)

    def on_message(
        self,
        msg_type: str,
        sender: str,
        ts: int,
        ledger: dict[str, dict[str, float]] | None,
        send: Send,
    ) -> bool:
        """Handle REQUEST/OKAY; return True when all N-1 OKAYs are collected (CS may be entered)."""
        self.lamport_ts = max(self.lamport_ts, ts) + 1
        if msg_type == "REQUEST":
            # Ricart-Agrawala total order (req_ts, replica_id): defer if I am in CS or my request has priority
            if self.in_cs or (self.requesting_cs and (self.req_ts, self.replica_id) < (ts, sender)):
                self.deferred_queue.append((sender, ts))
            else:
                send("OKAY", sender, ts, self.snapshot_ledger())
            return False
        self.merge_piggybacked_ledger(ledger)
        self.okay_received.add(sender)
        return self.requesting_cs and len(self.okay_received) >= len(self.peers)

    def release_cs(self, send: Send) -> None:
        """Exit CS and release deferred OKAYs carrying the freshly updated G-Counter ledger."""
        self.in_cs = False
        self.requesting_cs = False
        ledger = self.snapshot_ledger()
        while self.deferred_queue:
            peer, ts = self.deferred_queue.pop(0)
            send("OKAY", peer, ts, ledger)


def default_concurrent_requests() -> list[DeclassRequest]:
    """3 concurrent 2-bit queries on the root label (LOW integrity via B, so the plan explicitly endorses)."""
    root_label = run_bcast_convergecast(inject_on_b=False).root_label
    return [
        DeclassRequest(
            query_id=f"q-{i+1}",
            label=root_label,
            schema_name="risk_level",
            candidate_value=val,
            endorsed=True,
        )
        for i, val in enumerate(("Düşük", "Orta", "Yüksek"))
    ]


@dataclass
class ConcurrentRunResult:
    """Result of running concurrent declassification requests across N gate replicas."""

    mutex_enabled: bool
    replica_count: int
    decisions: list[DeclassDecision]
    messages: list[MutexMessage]
    initial_budgets: dict[str, float]
    converged_spent: dict[str, float]
    converged_remaining: dict[str, float]
    budget_breached: bool

    @property
    def request_msg_count(self) -> int:
        return sum(1 for m in self.messages if m.msg_type == "REQUEST")

    @property
    def okay_msg_count(self) -> int:
        return sum(1 for m in self.messages if m.msg_type == "OKAY")

    @property
    def total_messages(self) -> int:
        return len(self.messages)

    @property
    def messages_per_entry(self) -> int:
        """Theoretical Ricart-Agrawala complexity per CS entry: 2(N - 1)."""
        return 2 * (self.replica_count - 1) if self.mutex_enabled else 0


def _build_replicas(replica_ids: tuple[str, ...], budgets: dict[str, float]) -> dict[str, GateReplica]:
    return {
        rid: GateReplica(
            replica_id=rid,
            fsm=GateFSM(budgets=dict(budgets), replica_id=rid),
            peers=tuple(p for p in replica_ids if p != rid),
        )
        for rid in replica_ids
    }


def _converge_all(replicas: dict[str, GateReplica]) -> tuple[dict[str, float], dict[str, float]]:
    """Merge G-Counters across all replicas to observe final eventual state."""
    for r1 in replicas.values():
        snap = r1.snapshot_ledger()
        for r2 in replicas.values():
            r2.merge_piggybacked_ledger(snap)
    sample = next(iter(replicas.values())).fsm
    spent = {o: sample.spent(o) for o in sorted(sample.budgets)}
    rem = {o: sample.budgets[o] - spent[o] for o in sorted(sample.budgets)}
    return spent, rem


def run_ricart_agrawala_simulation(
    requests: list[DeclassRequest] | None = None,
    budgets: dict[str, float] | None = None,
    replica_ids: tuple[str, ...] = ("gate-1", "gate-2", "gate-3"),
) -> ConcurrentRunResult:
    """Execute N concurrent replica threads coordinating via asynchronous REQUEST/OKAY mailboxes (2(N-1) msgs/entry)."""
    init_budgets = dict(budgets or {"A": 4.0, "B": 8.0, "C": 8.0})
    replicas = _build_replicas(replica_ids, init_budgets)
    if requests is None:
        requests = default_concurrent_requests()

    messages: list[MutexMessage] = []
    decisions: list[DeclassDecision] = []
    log_lock = threading.Lock()
    start_barrier = threading.Barrier(len(replica_ids))
    all_done = threading.Event()

    def sender_for(rid: str) -> Send:
        def send(msg_type: str, receiver: str, req_ts: int, ledger: dict[str, dict[str, float]] | None) -> None:
            with log_lock:
                msg = MutexMessage(
                    step=len(messages) + 1,
                    msg_type=msg_type,
                    sender=rid,
                    receiver=receiver,
                    req_ts=req_ts,
                    piggybacked_ledger=ledger,
                )
                messages.append(msg)
            replicas[receiver].mailbox.put(msg)

        return send

    def deliver(rep: GateReplica, send: Send) -> bool:
        try:
            msg = rep.mailbox.get(timeout=0.005)
        except queue.Empty:
            return False
        with rep.state_lock:
            return rep.on_message(msg.msg_type, msg.sender, msg.req_ts, msg.piggybacked_ledger, send)

    def replica_worker(rid: str, req: DeclassRequest) -> None:
        rep = replicas[rid]
        send = sender_for(rid)
        with rep.state_lock:
            rep.begin_request()

        # All replicas timestamp their request first, then broadcast concurrently
        start_barrier.wait()
        with rep.state_lock:
            rep.broadcast_request(send)

        while not deliver(rep, send):
            pass

        # CRITICAL SECTION: read merged G-Counter, check <= B_o invariant, deduct budget
        with rep.state_lock:
            rep.in_cs = True
            decision = rep.fsm.step(req)
            with log_lock:
                decisions.append(decision)
                if len(decisions) == len(replica_ids):
                    all_done.set()
            rep.release_cs(send)

        # Keep answering peers' late REQUESTs until every replica has left its CS
        while not all_done.is_set():
            deliver(rep, send)

    threads = [
        threading.Thread(target=replica_worker, args=(rid, req), name=f"worker-{rid}")
        for rid, req in zip(replica_ids, requests)
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=5.0)

    spent, rem = _converge_all(replicas)
    breached = any(spent[o] > init_budgets[o] for o in init_budgets)
    return ConcurrentRunResult(
        mutex_enabled=True,
        replica_count=len(replica_ids),
        decisions=decisions,
        messages=messages,
        initial_budgets=init_budgets,
        converged_spent=spent,
        converged_remaining=rem,
        budget_breached=breached,
    )


def run_unprotected_race_simulation(
    requests: list[DeclassRequest] | None = None,
    budgets: dict[str, float] | None = None,
    replica_ids: tuple[str, ...] = ("gate-1", "gate-2", "gate-3"),
) -> ConcurrentRunResult:
    """Uncoordinated replicas (no Mutex): each gate decides on its own stale ledger copy, then G-Counters merge.

    Distributed form of the race condition in Mutex.pdf p.9: check-then-spend on B_o is not atomic across
    replicas, so every replica sees the full budget and the merged ledger reveals double-spending.
    """
    init_budgets = dict(budgets or {"A": 4.0, "B": 8.0, "C": 8.0})
    replicas = _build_replicas(replica_ids, init_budgets)
    if requests is None:
        requests = default_concurrent_requests()

    decisions_by_idx: dict[int, DeclassDecision] = {}
    barrier = threading.Barrier(len(replica_ids))

    def worker(idx: int, rid: str, req: DeclassRequest) -> None:
        barrier.wait()
        decisions_by_idx[idx] = replicas[rid].fsm.step(req)

    threads = [
        threading.Thread(target=worker, args=(i, rid, req))
        for i, (rid, req) in enumerate(zip(replica_ids, requests))
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    ordered_decisions = [decisions_by_idx[i] for i in range(len(replica_ids))]
    spent, rem = _converge_all(replicas)
    breached = any(spent[o] > init_budgets[o] for o in init_budgets)
    return ConcurrentRunResult(
        mutex_enabled=False,
        replica_count=len(replica_ids),
        decisions=ordered_decisions,
        messages=[],
        initial_budgets=init_budgets,
        converged_spent=spent,
        converged_remaining=rem,
        budget_breached=breached,
    )
