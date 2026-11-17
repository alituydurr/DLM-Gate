"""Faz 1 Verification Suite: Core Theoretical Engine (DLM, Clocks/CRDT, Spanning Tree, Mealy FSM Gate)."""

from __future__ import annotations
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.core.dlm import DLMLabel
from src.core.clocks import VectorClock, GCounter
from src.core.spanning_tree import run_bcast_convergecast
from src.core.gate_fsm import DeclassRequest, GateFSM


def test_dlm_lattice() -> None:
    la = DLMLabel({"A": frozenset({"A", "B", "C"})}, integrity="HIGH")
    lb = DLMLabel({"B": frozenset({"A", "B"})}, integrity="LOW")
    lc = DLMLabel({"C": frozenset({"A", "C"})}, integrity="HIGH")

    lac = la | lc
    assert lac.owners == frozenset({"A", "C"})
    assert lac.effective_readers() == frozenset({"A", "C"})
    assert lac.integrity == "HIGH"

    lroot = lac | lb
    assert lroot.owners == frozenset({"A", "B", "C"})
    assert lroot.effective_readers() == frozenset({"A"})
    assert lroot.integrity == "LOW"
    assert lroot.owners_requiring_declass("PUBLIC") == ("A", "B", "C")

    declassified = lroot.declassify_for("PUBLIC", endorse=True)
    assert declassified.can_read("PUBLIC")
    assert declassified.integrity == "HIGH"


def test_vector_clock_and_gcounter() -> None:
    # Potential causality demonstration (4.Time.pdf p.22-25):
    # Agent reads secret doc at event s, then emits an unrelated status message at event t -> VC(s) < VC(t)
    vc_s = VectorClock({"A": 0, "B": 0}).tick("A").copy()
    vc_t = vc_s.copy().tick("A")
    assert vc_s < vc_t, "VC(s) < VC(t) must hold (potential causality / source of label creep)"

    vc_b = VectorClock({"A": 0, "B": 0}).tick("B")
    assert vc_s.concurrent_with(vc_b)

    # State-based G-Counter CRDT (idempotent & commutative pairwise-max merge)
    g1 = GCounter()
    g2 = GCounter()
    g1.increment("gate-1", 2.0)
    g2.increment("gate-2", 4.0)
    g1.merge(g2)
    g1.merge(g2)  # idempotent re-merge
    assert g1.value == 6.0


def test_spanning_tree_complexity_and_convergecast() -> None:
    res = run_bcast_convergecast(inject_on_b=True)
    assert res.node_count == 4
    assert res.bcast_count == 3
    assert res.convcast_count == 3
    assert res.total_messages == res.theoretical_bound == 6, "Theorem 4.5 requires 2n - 2 = 6 messages"

    # Intermediate Node A join: L_A ⊔ L_C
    assert res.intermediate_joins["A"].effective_readers() == frozenset({"A", "C"})
    # Root join: (L_A ⊔ L_C) ⊔ L_B
    assert res.root_label.effective_readers() == frozenset({"A"})
    assert res.root_label.integrity == "LOW"

    # Causal ordering along Root -> A -> C -> A -> Root path
    vc_bcast_a = VectorClock(res.messages[0].vc_snapshot)  # ROOT -> A
    vc_bcast_c = VectorClock(res.messages[2].vc_snapshot)  # A -> C
    vc_conv_c = VectorClock(res.messages[3].vc_snapshot)   # C -> A
    vc_conv_a = VectorClock(res.messages[4].vc_snapshot)   # A -> ROOT
    assert vc_bcast_a < vc_bcast_c < vc_conv_c < vc_conv_a


def test_gate_fsm_theorems_1_and_2() -> None:
    tree_res = run_bcast_convergecast(inject_on_b=True)
    gate = GateFSM(budgets={"A": 4.0, "B": 8.0, "C": 8.0})

    # 1. Valid high-integrity plan request with 4-option schema (log2(4) = 2.0 bits)
    d1 = gate.step(
        DeclassRequest(
            query_id="q-1",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Yüksek",
            plan_integrity="HIGH",
            target_reader="PUBLIC",
            endorsed=True,
        )
    )
    assert d1.approved is True
    assert d1.route == "CLOUD_GEMINI"
    assert d1.delta_bits == 2.0
    assert d1.delta_bits <= d1.bit_complexity
    assert d1.charged_owners == ("A", "B", "C")
    assert d1.remaining_budgets == {"A": 2.0, "B": 6.0, "C": 6.0}

    # 2. Prompt Injection Attack #1: Untrusted plan tries to trigger declassification (Theorem 2)
    d_untrusted = gate.step(
        DeclassRequest(
            query_id="q-inj-plan",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Kritik",
            plan_integrity="LOW",
        )
    )
    assert d_untrusted.approved is False
    assert d_untrusted.route == "LOCAL_ENCLAVE"
    assert d_untrusted.delta_bits == 0.0
    assert d_untrusted.reason == "THEOREM2_UNTRUSTED_PLAN_DECISION"

    # 3. Prompt Injection Attack #2: Unapproved free_text schema requested
    d_schema = gate.step(
        DeclassRequest(
            query_id="q-inj-schema",
            label=tree_res.root_label,
            schema_name="free_text",
            candidate_value="TUM_GIZLI_VERILER",
        )
    )
    assert d_schema.approved is False
    assert d_schema.reason == "THEOREM2_UNAPPROVED_SCHEMA:free_text"

    # 4. Prompt Injection Attack #3: Unauthorized external recipient requested
    d_target = gate.step(
        DeclassRequest(
            query_id="q-inj-target",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Kritik",
            target_reader="ATTACKER_EXT",
        )
    )
    assert d_target.approved is False
    assert d_target.reason == "THEOREM2_UNAUTHORIZED_TARGET:ATTACKER_EXT"

    # 5. Secure default: LOW-integrity root label without explicit plan endorsement is rejected
    d_unendorsed = gate.step(
        DeclassRequest(
            query_id="q-unendorsed",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Kritik",
        )
    )
    assert d_unendorsed.approved is False
    assert d_unendorsed.reason == "THEOREM2_UNENDORSED_LOW_INTEGRITY"

    # 6. Byzantine LLM Output: arbitrary string outside finite schema O_i
    d_byz = gate.step(
        DeclassRequest(
            query_id="q-byz",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="SIZINTI_METNI_12345",
            endorsed=True,
        )
    )
    assert d_byz.approved is False
    assert d_byz.reason == "BYZANTINE_INVALID_SCHEMA_VALUE"

    # 7. Second valid query consumes remaining 2.0 bits of Owner A (B_A becomes 0.0)
    d2 = gate.step(
        DeclassRequest(
            query_id="q-2",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Kritik",
            endorsed=True,
        )
    )
    assert d2.approved is True
    assert d2.remaining_budgets == {"A": 0.0, "B": 4.0, "C": 4.0}

    # 8. Third query exceeds Owner A's budget (B_A = 0.0 < 2.0) -> fallback to LOCAL_ENCLAVE (Theorem 1)
    d3 = gate.step(
        DeclassRequest(
            query_id="q-3",
            label=tree_res.root_label,
            schema_name="risk_level",
            candidate_value="Orta",
            endorsed=True,
        )
    )
    assert d3.approved is False
    assert d3.route == "LOCAL_ENCLAVE"
    assert d3.reason == "THEOREM1_BUDGET_EXCEEDED:A"
    assert d3.remaining_budgets == {"A": 0.0, "B": 4.0, "C": 4.0}


if __name__ == "__main__":
    test_dlm_lattice()
    test_vector_clock_and_gcounter()
    test_spanning_tree_complexity_and_convergecast()
    test_gate_fsm_theorems_1_and_2()
    print("[PASS] Faz 1 Cekirdek Teorik Motor dogrulamasi basariyla tamamlandi (6 mesaj, Teorem 1 & 2).")
