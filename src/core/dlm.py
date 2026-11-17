"""DLM (Decentralized Label Model) lattice and multi-owner budget ledger (Myers & Liskov 1997, 2000)."""

from __future__ import annotations
from dataclasses import dataclass, field

Integrity = str  # "HIGH" | "LOW"


@dataclass(frozen=True)
class DLMLabel:
    """Multi-owner confidentiality policy map {owner: allowed_readers} paired with integrity level."""

    policies: dict[str, frozenset[str]] = field(default_factory=dict)
    integrity: Integrity = "HIGH"

    @property
    def owners(self) -> frozenset[str]:
        return frozenset(self.policies.keys())

    def effective_readers(self, universe: set[str] | frozenset[str] | None = None) -> frozenset[str]:
        """effectiveReaders(L) = intersection of R_o across all owners o in L."""
        if not self.policies:
            return frozenset(universe) if universe else frozenset()
        it = iter(self.policies.values())
        acc = set(next(it))
        for readers in it:
            acc &= readers
        return frozenset(acc)

    def can_read(self, principal: str) -> bool:
        return bool(self.policies) and all(principal in readers for readers in self.policies.values())

    def join(self, other: DLMLabel) -> DLMLabel:
        """Lattice join (L1 ⊔ L2): union of owners, intersection of readers per shared owner, LOW integrity wins."""
        merged: dict[str, frozenset[str]] = dict(self.policies)
        for owner, readers in other.policies.items():
            merged[owner] = (merged[owner] & readers) if owner in merged else readers
        joint_integrity = "LOW" if (self.integrity == "LOW" or other.integrity == "LOW") else "HIGH"
        return DLMLabel(policies=merged, integrity=joint_integrity)

    def __or__(self, other: DLMLabel) -> DLMLabel:
        return self.join(other)

    def owners_requiring_declass(self, target_reader: str) -> tuple[str, ...]:
        """Return sorted owners whose reader set does not yet include target_reader."""
        return tuple(sorted(o for o, readers in self.policies.items() if target_reader not in readers))

    def declassify_for(self, target_reader: str, endorse: bool = False) -> DLMLabel:
        """Add target_reader to every owner's reader set and optionally endorse integrity to HIGH."""
        updated = {o: (readers | {target_reader}) for o, readers in self.policies.items()}
        return DLMLabel(policies=updated, integrity="HIGH" if endorse else self.integrity)

    def format_short(self) -> str:
        parts = [f"{o}:{{{','.join(sorted(r))}}}" for o, r in sorted(self.policies.items())]
        return f"{{{'; '.join(parts)}}} [Int:{self.integrity}]"
