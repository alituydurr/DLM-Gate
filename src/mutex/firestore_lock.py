"""Mod B: Stateless Cloud Run replicas + versioned budget ledger (real Firestore when GCP_PROJECT_ID is set, else local) & Hybrid Gemini Router."""

from __future__ import annotations
from dataclasses import dataclass, field
import json
import os
import threading
import time
import urllib.request

from src.core.gate_fsm import DeclassDecision, DeclassRequest, GateFSM
from src.mutex.ricart_agrawala import default_concurrent_requests


@dataclass(frozen=True)
class FirestoreTxEvent:
    """Audit trace of a single Firestore atomic transaction attempt (including OCC retries)."""

    step: int
    instance_id: str
    query_id: str
    tx_attempt: int
    status: str  # "COMMITTED" | "ABORTED_RETRY" | "DENIED_BUDGET" | "DENIED_POLICY"
    read_version: int
    committed_version: int
    delta_bits: float
    remaining_budgets: dict[str, float]


@dataclass
class ModBCloudRunResult:
    """Result of running concurrent requests across stateless Cloud Run instances using Firestore Transactions."""

    backend_mode: str  # "GCP_FIRESTORE" | "LOCAL_DOCKER_FIRESTORE_TX" | "LOCAL_FALLBACK (...)"
    instance_count: int
    decisions: list[DeclassDecision]
    tx_events: list[FirestoreTxEvent]
    llm_responses: dict[str, dict[str, str]]
    initial_budgets: dict[str, float]
    final_spent: dict[str, float]
    final_remaining: dict[str, float]
    budget_breached: bool

    @property
    def total_tx_attempts(self) -> int:
        return len(self.tx_events)

    @property
    def occ_retry_count(self) -> int:
        return sum(1 for e in self.tx_events if e.status == "ABORTED_RETRY")


LedgerState = dict  # {"version": int, "budgets": {owner: bits}, "spent": {owner: bits}}


def initial_ledger(budgets: dict[str, float]) -> LedgerState:
    return {"version": 1, "budgets": dict(budgets), "spent": {o: 0.0 for o in budgets}}


def remaining_bits(state: LedgerState) -> dict[str, float]:
    return {o: state["budgets"][o] - state["spent"].get(o, 0.0) for o in sorted(state["budgets"])}


def apply_versioned_spend(
    state: LedgerState,
    expected_version: int,
    charged_owners: tuple[str, ...],
    delta_bits: float,
) -> tuple[bool, LedgerState]:
    """Compare-and-swap on the ledger version: spend only if nobody wrote since we read (OCC)."""
    if state["version"] != expected_version:
        return False, state
    spent = dict(state["spent"])
    for owner in charged_owners:
        spent[owner] = spent.get(owner, 0.0) + delta_bits
    return True, {"version": state["version"] + 1, "budgets": dict(state["budgets"]), "spent": spent}


class LocalFirestoreTransactionStore:
    """In-process ledger with the same version-checked commit as the Firestore store (tests and offline demo)."""

    backend_mode = "LOCAL_DOCKER_FIRESTORE_TX"

    def __init__(self, initial_budgets: dict[str, float]) -> None:
        self._lock = threading.Lock()
        self._state = initial_ledger(initial_budgets)

    def read_snapshot(self) -> tuple[int, dict[str, float], dict[str, float]]:
        with self._lock:
            return self._state["version"], dict(self._state["budgets"]), dict(self._state["spent"])

    def try_commit(
        self,
        expected_version: int,
        charged_owners: tuple[str, ...],
        delta_bits: float,
    ) -> tuple[bool, int, dict[str, float]]:
        with self._lock:
            ok, self._state = apply_versioned_spend(self._state, expected_version, charged_owners, delta_bits)
            return ok, self._state["version"], remaining_bits(self._state)

    def state_summary(self) -> tuple[dict[str, float], dict[str, float]]:
        with self._lock:
            return dict(self._state["spent"]), remaining_bits(self._state)


class FirestoreTransactionStore:
    """Real Google Cloud Firestore ledger: one document, version-checked inside a Firestore transaction."""

    backend_mode = "GCP_FIRESTORE"

    def __init__(
        self,
        initial_budgets: dict[str, float],
        project_id: str,
        collection: str = "dlm_gate",
        doc_id: str = "budget_ledger",
    ) -> None:
        from google.cloud import firestore  # type: ignore

        self._firestore = firestore
        self._client = firestore.Client(project=project_id)
        self._doc = self._client.collection(collection).document(doc_id)
        # Each demo run starts from a fresh ledger so results are reproducible
        self._doc.set(initial_ledger(initial_budgets))

    def _read(self, transaction=None) -> LedgerState:
        return self._doc.get(transaction=transaction).to_dict()

    def read_snapshot(self) -> tuple[int, dict[str, float], dict[str, float]]:
        state = self._read()
        return state["version"], dict(state["budgets"]), dict(state["spent"])

    def try_commit(
        self,
        expected_version: int,
        charged_owners: tuple[str, ...],
        delta_bits: float,
    ) -> tuple[bool, int, dict[str, float]]:
        @self._firestore.transactional
        def commit(transaction) -> tuple[bool, LedgerState]:
            ok, new_state = apply_versioned_spend(self._read(transaction), expected_version, charged_owners, delta_bits)
            if ok:
                transaction.set(self._doc, new_state)
            return ok, new_state

        ok, state = commit(self._client.transaction())
        return ok, state["version"], remaining_bits(state)

    def state_summary(self) -> tuple[dict[str, float], dict[str, float]]:
        state = self._read()
        return dict(state["spent"]), remaining_bits(state)


def make_ledger_store(budgets: dict[str, float]):
    """Real Firestore when GCP_PROJECT_ID is set; local ledger otherwise or if Firestore is unreachable."""
    project_id = os.getenv("GCP_PROJECT_ID", "")
    if not project_id:
        return LocalFirestoreTransactionStore(budgets)
    try:
        return FirestoreTransactionStore(budgets, project_id)
    except Exception as exc:  # keep the live demo running, but make the fallback visible on the panel
        print(f"[firestore] bağlanılamadı, yerel deftere dönülüyor: {exc}", flush=True)
        store = LocalFirestoreTransactionStore(budgets)
        store.backend_mode = f"LOCAL_FALLBACK ({type(exc).__name__})"
        return store


class HybridLLMRouter:
    """Routes approved schema outputs to Cloud Gemini 2.5 Flash and denied requests to Local Enclave."""

    def __init__(self, api_key: str | None = None) -> None:
        self.api_key = api_key if api_key is not None else os.getenv("GEMINI_API_KEY", "")

    def execute(self, decision: DeclassDecision) -> dict[str, str]:
        if not decision.approved or decision.route == "LOCAL_ENCLAVE":
            return {
                "target": "LOCAL_ENCLAVE (Confidential VM / Yerel Yalıtılmış Model)",
                "mode": "OFFLINE_ISOLATED",
                "output": (
                    f"[LOCAL_ENCLAVE] Sorgu '{decision.query_id}' buluta gönderilmedi "
                    f"(Sebep: {decision.reason}). Gizli veri kurum içi yalıtılmış modelde işlendi (Sızıntı = 0.0 bit)."
                ),
            }

        # Approved for CLOUD_GEMINI: only the finite schema value (e.g. 2 bits) crosses the boundary
        if self.api_key:
            try:
                url = (
                    "https://generativelanguage.googleapis.com/v1beta/models/"
                    f"gemini-2.5-flash:generateContent?key={self.api_key}"
                )
                body = json.dumps(
                    {
                        "contents": [
                            {
                                "parts": [
                                    {
                                        "text": (
                                            f"Kısıtlı şema çıktısı ({decision.schema_name}={decision.emitted_value}). "
                                            "Tek cümlelik kurumsal aksiyon özeti üret."
                                        )
                                    }
                                ]
                            }
                        ]
                    }
                ).encode("utf-8")
                req = urllib.request.Request(url, data=body, headers={"Content-Type": "application/json"}, method="POST")
                with urllib.request.urlopen(req, timeout=4) as resp:
                    data = json.loads(resp.read().decode("utf-8"))
                    text = data["candidates"][0]["content"]["parts"][0]["text"].strip()
                    return {
                        "target": "CLOUD_GEMINI (gemini-2.5-flash Live API)",
                        "mode": "GCP_LIVE",
                        "output": text,
                    }
            except Exception:
                pass

        return {
            "target": "CLOUD_GEMINI (Simüle Bulut Modeli — Yalnızca Kısıtlı Şema Çıktısı)",
            "mode": "LOCAL_DOCKER_SIM",
            "output": (
                f"[CLOUD_GEMINI] İndirilen şema çıktısı alındı: {decision.schema_name}='{decision.emitted_value}' "
                f"(Harcanan sızıntı bütçesi: ΔL={decision.delta_bits} bit). Ham gizli belgelere erişim yok."
            ),
        }


def run_mod_b_firestore_simulation(
    requests: list[DeclassRequest] | None = None,
    budgets: dict[str, float] | None = None,
    instance_ids: tuple[str, ...] = ("cloud-run-1", "cloud-run-2", "cloud-run-3"),
) -> ModBCloudRunResult:
    """Run 3 concurrent stateless Cloud Run instances coordinating via atomic Firestore OCC transactions."""
    init_budgets = dict(budgets or {"A": 4.0, "B": 8.0, "C": 8.0})
    store = make_ledger_store(init_budgets)
    router = HybridLLMRouter()

    if requests is None:
        requests = default_concurrent_requests()

    tx_events: list[FirestoreTxEvent] = []
    decisions_by_idx: dict[int, DeclassDecision] = {}
    llm_responses: dict[str, dict[str, str]] = {}
    log_lock = threading.Lock()
    start_barrier = threading.Barrier(len(instance_ids))

    def record_tx(
        instance_id: str,
        query_id: str,
        attempt: int,
        status: str,
        r_ver: int,
        c_ver: int,
        delta: float,
        rem: dict[str, float],
    ) -> None:
        with log_lock:
            tx_events.append(
                FirestoreTxEvent(
                    step=len(tx_events) + 1,
                    instance_id=instance_id,
                    query_id=query_id,
                    tx_attempt=attempt,
                    status=status,
                    read_version=r_ver,
                    committed_version=c_ver,
                    delta_bits=delta,
                    remaining_budgets=dict(rem),
                )
            )

    def stateless_cloud_run_worker(idx: int, instance_id: str, req: DeclassRequest) -> None:
        start_barrier.wait()
        attempt = 0
        while True:
            attempt += 1
            read_ver, cur_budgets, cur_spent = store.read_snapshot()
            # Simulate concurrent network overlap so OCC conflict detection & retry is exercised
            time.sleep(0.005 * (idx + 1))

            # Construct ephemeral stateless FSM hydrated from the Firestore snapshot
            fsm = GateFSM(budgets=cur_budgets, replica_id=instance_id)
            for owner, val in cur_spent.items():
                fsm.spent_counters[owner].increment("firestore", val)

            trial_decision = fsm.step(req)
            if not trial_decision.approved:
                # Budget exceeded or policy rejected: no write needed, transaction finishes cleanly
                rem_map = {o: cur_budgets[o] - cur_spent[o] for o in sorted(cur_budgets)}
                status = "DENIED_BUDGET" if trial_decision.reason.startswith("THEOREM1") else "DENIED_POLICY"
                record_tx(instance_id, req.query_id, attempt, status, read_ver, read_ver, 0.0, rem_map)
                decisions_by_idx[idx] = trial_decision
                with log_lock:
                    llm_responses[req.query_id] = router.execute(trial_decision)
                return

            # Attempt atomic compare-and-commit on Firestore ledger document
            committed, new_ver, rem_after = store.try_commit(
                expected_version=read_ver,
                charged_owners=trial_decision.charged_owners,
                delta_bits=trial_decision.delta_bits,
            )
            if committed:
                final_dec = DeclassDecision(
                    query_id=trial_decision.query_id,
                    approved=True,
                    route=trial_decision.route,
                    reason=trial_decision.reason,
                    schema_name=trial_decision.schema_name,
                    emitted_value=trial_decision.emitted_value,
                    delta_bits=trial_decision.delta_bits,
                    bit_complexity=trial_decision.bit_complexity,
                    charged_owners=trial_decision.charged_owners,
                    remaining_budgets=rem_after,
                    output_label=trial_decision.output_label,
                )
                record_tx(
                    instance_id,
                    req.query_id,
                    attempt,
                    "COMMITTED",
                    read_ver,
                    new_ver,
                    trial_decision.delta_bits,
                    rem_after,
                )
                decisions_by_idx[idx] = final_dec
                with log_lock:
                    llm_responses[req.query_id] = router.execute(final_dec)
                return
            else:
                record_tx(instance_id, req.query_id, attempt, "ABORTED_RETRY", read_ver, new_ver, 0.0, rem_after)

    threads = [
        threading.Thread(target=stateless_cloud_run_worker, args=(i, iid, req), name=f"worker-{iid}")
        for i, (iid, req) in enumerate(zip(instance_ids, requests))
    ]
    for t in threads:
        t.start()
    for t in threads:
        t.join(timeout=30.0)  # real Firestore round-trips are network calls

    ordered_decisions = [decisions_by_idx[i] for i in range(len(instance_ids))]
    spent, rem = store.state_summary()
    breached = any(spent[o] > init_budgets[o] for o in init_budgets)
    return ModBCloudRunResult(
        backend_mode=store.backend_mode,
        instance_count=len(instance_ids),
        decisions=ordered_decisions,
        tx_events=tx_events,
        llm_responses=llm_responses,
        initial_budgets=init_budgets,
        final_spent=spent,
        final_remaining=rem,
        budget_breached=breached,
    )
