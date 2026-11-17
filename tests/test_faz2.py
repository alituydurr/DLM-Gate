"""Faz 2 Verification Suite: Ricart-Agrawala + Piggybacked G-Counter (12 msgs) vs. Unprotected Race Condition."""

from __future__ import annotations
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.mutex.ricart_agrawala import (
    run_ricart_agrawala_simulation,
    run_unprotected_race_simulation,
)


def test_unprotected_race_condition_breaches_budget() -> None:
    """Verify that without Mutex, 3 concurrent 2-bit queries double-spend B_A = 4.0 bits -> 6.0 bits leaked."""
    res = run_unprotected_race_simulation(budgets={"A": 4.0, "B": 8.0, "C": 8.0})
    assert res.mutex_enabled is False
    assert res.total_messages == 0

    # All 3 replicas saw B_A = 4.0 locally and approved their 2-bit query -> 3 approvals!
    approved_count = sum(1 for d in res.decisions if d.approved)
    assert approved_count == 3, "Without Mutex, all 3 concurrent replicas must erroneously approve"
    assert res.converged_spent["A"] == 6.0, "Eventual G-Counter merge reveals 6.0 bits spent > B_A = 4.0"
    assert res.converged_remaining["A"] == -2.0
    assert res.budget_breached is True


def test_ricart_agrawala_piggybacked_gcounter_protects_budget() -> None:
    """Verify Ricart-Agrawala exchanges exactly 3 * 2(N-1) = 12 messages and prevents budget breach."""
    res = run_ricart_agrawala_simulation(budgets={"A": 4.0, "B": 8.0, "C": 8.0})
    assert res.mutex_enabled is True
    assert res.replica_count == 3

    # Message complexity (Mutex.pdf p.55): 2(N-1) = 4 per entry, 3 * 4 = 12 total (6 REQUEST + 6 OKAY)
    assert res.messages_per_entry == 4
    assert res.request_msg_count == 6
    assert res.okay_msg_count == 6
    assert res.total_messages == 12

    # First 2 queries approved (CLOUD_GEMINI, 4.0 bits total), 3rd query denied and routed to LOCAL_ENCLAVE
    assert [d.approved for d in res.decisions] == [True, True, False]
    assert [d.route for d in res.decisions] == ["CLOUD_GEMINI", "CLOUD_GEMINI", "LOCAL_ENCLAVE"]
    assert res.decisions[2].reason == "THEOREM1_BUDGET_EXCEEDED:A"

    # Invariant preserved: spent(A) == 4.0 <= B_A == 4.0
    assert res.converged_spent == {"A": 4.0, "B": 4.0, "C": 4.0}
    assert res.converged_remaining == {"A": 0.0, "B": 4.0, "C": 4.0}
    assert res.budget_breached is False

    # Real Lamport timestamps: all 3 requests are concurrent (ts=1), ties broken by replica id
    assert {m.req_ts for m in res.messages if m.msg_type == "REQUEST"} == {1}


if __name__ == "__main__":
    test_unprotected_race_condition_breaches_budget()
    test_ricart_agrawala_piggybacked_gcounter_protects_budget()
    print("[PASS] Faz 2 Dagitik Mutex (Ricart-Agrawala 12 mesaj & Race Condition onleme) dogrulandi.")
