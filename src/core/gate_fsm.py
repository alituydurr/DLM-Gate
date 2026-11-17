"""Deterministic Declassification Gate as a Mealy FSM (2- Comp Model.pdf p.14, Theorem 1 & Theorem 2)."""

from __future__ import annotations
from dataclasses import dataclass, field
import math
from src.core.dlm import DLMLabel
from src.core.clocks import GCounter


DEFAULT_SCHEMAS: dict[str, tuple[str, ...]] = {
    "risk_level": ("Düşük", "Orta", "Yüksek", "Kritik"),  # |O_i| = 4 -> log2(4) = 2.0 bits
    "binary_alert": ("Normal", "Alarm"),  # |O_i| = 2 -> log2(2) = 1.0 bit
}


@dataclass(frozen=True)
class DeclassRequest:
    """Input symbol I to the Mealy FSM declassification gate.

    Trust boundary: every field except candidate_value is fixed by the trusted planner before any
    untrusted document is read; the LLM only fills candidate_value. plan_integrity must be derived
    from the label of what the planner had read (e.g. root_label.integrity after re-planning).
    """

    query_id: str
    label: DLMLabel
    schema_name: str
    candidate_value: str
    plan_integrity: str = "HIGH"  # Must originate from HIGH-integrity user plan (Theorem 2)
    target_reader: str = "PUBLIC"
    endorsed: bool = False  # Secure default: LOW-integrity data passes only with explicit plan endorsement


@dataclass(frozen=True)
class DeclassDecision:
    """Output symbol O emitted by the Mealy FSM transition function delta(S, I) -> (S', O)."""

    query_id: str
    approved: bool
    route: str  # "CLOUD_GEMINI" | "LOCAL_ENCLAVE"
    reason: str
    schema_name: str
    emitted_value: str | None
    delta_bits: float
    bit_complexity: int
    charged_owners: tuple[str, ...]
    remaining_budgets: dict[str, float]
    output_label: DLMLabel


@dataclass
class GateFSM:
    """Mealy FSM enforcing Theorem 1 (QIF Per-Owner Leakage Budget) & Theorem 2 (Qualified Robustness)."""

    budgets: dict[str, float] = field(default_factory=lambda: {"A": 8.0, "B": 8.0, "C": 8.0})
    acts_for: set[str] = field(default_factory=lambda: {"A", "B", "C"})
    allowed_schemas: dict[str, tuple[str, ...]] = field(default_factory=lambda: dict(DEFAULT_SCHEMAS))
    authorized_targets: set[str] = field(default_factory=lambda: {"PUBLIC", "CLOUD"})
    replica_id: str = "gate-1"
    spent_counters: dict[str, GCounter] = field(default_factory=dict)

    def __post_init__(self) -> None:
        for owner in self.budgets:
            self.spent_counters.setdefault(owner, GCounter())

    def spent(self, owner: str) -> float:
        counter = self.spent_counters.get(owner)
        return counter.value if counter else 0.0

    def remaining(self, owner: str) -> float:
        return max(0.0, self.budgets.get(owner, 0.0) - self.spent(owner))

    def remaining_map(self) -> dict[str, float]:
        return {o: self.remaining(o) for o in sorted(self.budgets)}

    @staticmethod
    def schema_leakage_bits(domain: tuple[str, ...]) -> float:
        """Theorem 1 (Smith 2009 QIF): max min-entropy leakage of finite schema O_i is log2(|O_i|)."""
        if len(domain) <= 1:
            return 0.0
        return math.log2(len(domain))

    @staticmethod
    def message_bit_complexity(value: str) -> int:
        """Raw UTF-8 wire bit length: verifies L_i <= log2|O_i| <= Bit Complexity (2- Comp Model.pdf p.35)."""
        return len(value.encode("utf-8")) * 8

    def step(self, req: DeclassRequest) -> DeclassDecision:
        """Mealy FSM deterministic transition delta(S, req) -> (S', DeclassDecision)."""
        bit_len = self.message_bit_complexity(req.candidate_value)

        # 1. Theorem 2 (Robust Declassification): decision cannot be driven by LOW-integrity untrusted input
        if req.plan_integrity != "HIGH":
            return self._deny(req, "THEOREM2_UNTRUSTED_PLAN_DECISION", bit_len)

        # 2. Theorem 2 (Where/Who dimension): target reader must be an authorized declassification sink
        if req.target_reader not in self.authorized_targets:
            return self._deny(req, f"THEOREM2_UNAUTHORIZED_TARGET:{req.target_reader}", bit_len)

        # 3. Theorem 2 (What dimension): only pre-approved finite schemas from the HIGH-integrity plan
        if req.schema_name not in self.allowed_schemas:
            return self._deny(req, f"THEOREM2_UNAPPROVED_SCHEMA:{req.schema_name}", bit_len)

        domain = self.allowed_schemas[req.schema_name]

        # 4. Theorem 2 (Qualified Robustness - Myers, Sabelfeld & Zdancewic 2006):
        #    Low-integrity data requires explicit plan endorsement to pass through a bounded schema
        if req.label.integrity == "LOW" and not req.endorsed:
            return self._deny(req, "THEOREM2_UNENDORSED_LOW_INTEGRITY", bit_len)

        # 5. Byzantine Containment: LLM output must strictly match a valid symbol in finite schema O_i
        if req.candidate_value not in domain:
            return self._deny(req, "BYZANTINE_INVALID_SCHEMA_VALUE", bit_len)

        # 6. Theorem 1 (Quantitative Information Flow & Multi-Owner DLM Acts-For + Budget Check)
        delta_bits = self.schema_leakage_bits(domain)
        affected_owners = req.label.owners_requiring_declass(req.target_reader)

        for owner in affected_owners:
            if owner not in self.acts_for:
                return self._deny(req, f"DLM_MISSING_ACTS_FOR:{owner}", bit_len, affected_owners)
            if self.spent(owner) + delta_bits > self.budgets.get(owner, 0.0):
                return self._deny(req, f"THEOREM1_BUDGET_EXCEEDED:{owner}", bit_len, affected_owners)

        # Commit state transition: deduct delta_bits from every contributing owner (D_o in Theorem 1)
        for owner in affected_owners:
            self.spent_counters.setdefault(owner, GCounter()).increment(self.replica_id, delta_bits)

        out_label = req.label.declassify_for(req.target_reader, endorse=True)
        return DeclassDecision(
            query_id=req.query_id,
            approved=True,
            route="CLOUD_GEMINI",
            reason="APPROVED_ENDORSED_SCHEMA",
            schema_name=req.schema_name,
            emitted_value=req.candidate_value,
            delta_bits=delta_bits,
            bit_complexity=bit_len,
            charged_owners=affected_owners,
            remaining_budgets=self.remaining_map(),
            output_label=out_label,
        )

    def _deny(
        self,
        req: DeclassRequest,
        reason: str,
        bit_len: int,
        charged_owners: tuple[str, ...] = (),
    ) -> DeclassDecision:
        return DeclassDecision(
            query_id=req.query_id,
            approved=False,
            route="LOCAL_ENCLAVE",
            reason=reason,
            schema_name=req.schema_name,
            emitted_value=None,
            delta_bits=0.0,
            bit_complexity=bit_len,
            charged_owners=charged_owners,
            remaining_budgets=self.remaining_map(),
            output_label=req.label,
        )
