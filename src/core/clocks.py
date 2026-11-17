"""Vector Clocks (Happened-Before & Potential Causality) and State-based G-Counter CRDT (4.Time.pdf)."""

from __future__ import annotations
from dataclasses import dataclass, field


@dataclass
class VectorClock:
    """Vector Clock capturing happened-before (s -> t <=> VC(s) < VC(t)) and potential causality."""

    clock: dict[str, int] = field(default_factory=dict)

    def copy(self) -> VectorClock:
        return VectorClock(dict(self.clock))

    def tick(self, node_id: str) -> VectorClock:
        self.clock[node_id] = self.clock.get(node_id, 0) + 1
        return self

    def merge(self, other: VectorClock) -> VectorClock:
        """Pairwise maximum across all known node entries."""
        for node_id, ts in other.clock.items():
            if ts > self.clock.get(node_id, 0):
                self.clock[node_id] = ts
        return self

    def happened_before(self, other: VectorClock) -> bool:
        """Return True iff self < other (every component <= and at least one strictly <)."""
        keys = set(self.clock.keys()) | set(other.clock.keys())
        all_le = all(self.clock.get(k, 0) <= other.clock.get(k, 0) for k in keys)
        any_lt = any(self.clock.get(k, 0) < other.clock.get(k, 0) for k in keys)
        return all_le and any_lt

    def __lt__(self, other: VectorClock) -> bool:
        return self.happened_before(other)

    def concurrent_with(self, other: VectorClock) -> bool:
        return not (self < other) and not (other < self) and self.clock != other.clock


@dataclass
class GCounter:
    """State-based Grow-Only Counter CRDT (monotone join-semilattice via pairwise-max)."""

    counts: dict[str, float] = field(default_factory=dict)

    def increment(self, replica_id: str, amount: float) -> None:
        if amount < 0:
            raise ValueError("GCounter only supports non-negative increments")
        self.counts[replica_id] = self.counts.get(replica_id, 0.0) + amount

    def merge(self, other: GCounter) -> GCounter:
        for replica_id, val in other.counts.items():
            if val > self.counts.get(replica_id, 0.0):
                self.counts[replica_id] = val
        return self

    @property
    def value(self) -> float:
        return sum(self.counts.values())

    def to_dict(self) -> dict[str, float]:
        return dict(self.counts)
