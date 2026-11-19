"""Faz 5 Verification Suite: Mod B (Stateless Cloud Run + Firestore Atomic OCC Transactions & Hybrid LLM Router)."""

from __future__ import annotations
import copy
import os
import pathlib
import sys
import threading
import types

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.mutex.firestore_lock import apply_versioned_spend, initial_ledger, run_mod_b_firestore_simulation
from src.web.app import DashboardRuntime


def _assert_two_commits_one_denial(res) -> None:
    assert res.instance_count == 3
    assert res.budget_breached is False

    # Exactly 2 concurrent instances commit (4.0 bits total), 1 instance is denied and routed to LOCAL_ENCLAVE
    approved = [d for d in res.decisions if d.approved]
    denied = [d for d in res.decisions if not d.approved]
    assert len(approved) == 2
    assert len(denied) == 1
    assert denied[0].route == "LOCAL_ENCLAVE"
    assert denied[0].reason == "THEOREM1_BUDGET_EXCEEDED:A"

    # Verify OCC transaction log captured versioned commits and automatic conflict retries
    committed_events = [e for e in res.tx_events if e.status == "COMMITTED"]
    assert len(committed_events) == 2
    assert res.occ_retry_count >= 1, "Concurrent stateless workers must trigger at least 1 OCC retry"
    assert res.final_spent == {"A": 4.0, "B": 4.0, "C": 4.0}
    assert res.final_remaining == {"A": 0.0, "B": 4.0, "C": 4.0}

    # Verify Hybrid LLM Router outputs
    for d in approved:
        assert "CLOUD_GEMINI" in res.llm_responses[d.query_id]["target"]
    for d in denied:
        assert "LOCAL_ENCLAVE" in res.llm_responses[d.query_id]["target"]


def test_mod_b_firestore_atomic_transactions_and_occ_retries() -> None:
    res = run_mod_b_firestore_simulation(budgets={"A": 4.0, "B": 8.0, "C": 8.0})
    assert res.backend_mode == "LOCAL_DOCKER_FIRESTORE_TX"
    _assert_two_commits_one_denial(res)


def test_versioned_spend_rejects_stale_writes() -> None:
    state = initial_ledger({"A": 4.0})
    ok, state = apply_versioned_spend(state, 1, ("A",), 2.0)
    assert ok and state["version"] == 2 and state["spent"]["A"] == 2.0
    stale_ok, unchanged = apply_versioned_spend(state, 1, ("A",), 2.0)  # read v1, but v2 is current
    assert stale_ok is False and unchanged is state


def _fake_firestore_module() -> types.ModuleType:
    """Minimal stand-in for google.cloud.firestore: one document, serializable transactions."""
    serial = threading.Lock()

    class Snapshot:
        def __init__(self, data: dict) -> None:
            self._data = copy.deepcopy(data)

        def to_dict(self) -> dict:
            return copy.deepcopy(self._data)

    class Document:
        def __init__(self) -> None:
            self.data: dict = {}
            self.writes = 0

        def get(self, transaction=None) -> Snapshot:
            return Snapshot(self.data)

        def set(self, data: dict) -> None:
            self.data = copy.deepcopy(data)
            self.writes += 1

    class Transaction:
        def __init__(self) -> None:
            self.pending: list[tuple[Document, dict]] = []

        def set(self, ref: Document, data: dict) -> None:
            self.pending.append((ref, data))

    class Client:
        document_ref = Document()

        def __init__(self, project: str) -> None:
            assert project == "test-project"

        def collection(self, name: str) -> "Client":
            return self

        def document(self, name: str) -> Document:
            return Client.document_ref

        def transaction(self) -> Transaction:
            return Transaction()

    def transactional(fn):
        def run(transaction: Transaction):
            with serial:  # Firestore serializes conflicting transactions on one document
                result = fn(transaction)
                for ref, data in transaction.pending:
                    ref.set(data)
                return result

        return run

    module = types.ModuleType("google.cloud.firestore")
    module.Client = Client
    module.transactional = transactional
    return module


def test_real_firestore_store_path_with_fake_client() -> None:
    """GCP_PROJECT_ID switches Mod B to FirestoreTransactionStore; same guarantees, ledger lives in the document."""
    fake = _fake_firestore_module()
    google, cloud = types.ModuleType("google"), types.ModuleType("google.cloud")
    google.cloud, cloud.firestore = cloud, fake
    saved = {k: sys.modules.get(k) for k in ("google", "google.cloud", "google.cloud.firestore")}
    sys.modules.update({"google": google, "google.cloud": cloud, "google.cloud.firestore": fake})
    os.environ["GCP_PROJECT_ID"] = "test-project"
    try:
        res = run_mod_b_firestore_simulation(budgets={"A": 4.0, "B": 8.0, "C": 8.0})
    finally:
        os.environ.pop("GCP_PROJECT_ID")
        for k, v in saved.items():
            if v is None:
                sys.modules.pop(k, None)
            else:
                sys.modules[k] = v

    assert res.backend_mode == "GCP_FIRESTORE"
    _assert_two_commits_one_denial(res)
    doc = fake.Client.document_ref
    assert doc.data["version"] == 3 and doc.data["spent"] == {"A": 4.0, "B": 4.0, "C": 4.0}
    assert doc.writes == 3, "1 reset + 2 committed transactions; aborted attempts must not write"


def test_unreachable_firestore_fails_closed() -> None:
    """Firestore beklenirken ulaşılamazsa yerel deftere düşülmez: tüm sorgular reddedilir, 0 bit harcanır."""
    saved = sys.modules.get("google.cloud.firestore")
    sys.modules["google.cloud.firestore"] = None  # import -> ImportError
    os.environ["GCP_PROJECT_ID"] = "test-project"
    try:
        res = run_mod_b_firestore_simulation(budgets={"A": 4.0, "B": 8.0, "C": 8.0})
    finally:
        os.environ.pop("GCP_PROJECT_ID")
        if saved is None:
            sys.modules.pop("google.cloud.firestore", None)
        else:
            sys.modules["google.cloud.firestore"] = saved

    assert res.backend_mode.startswith("FAIL_CLOSED")
    assert all(not d.approved and d.reason == "LEDGER_UNAVAILABLE_FAIL_CLOSED" for d in res.decisions)
    assert all(d.emitted_value is None for d in res.decisions)
    assert all("LOCAL_ENCLAVE" in r["target"] for r in res.llm_responses.values())
    assert res.final_spent == {"A": 0.0, "B": 0.0, "C": 0.0}
    assert res.budget_breached is False


def test_dashboard_mod_b_and_cloud_run_script() -> None:
    rt = DashboardRuntime()
    state = rt.run_scenario_3_firestore()
    assert state["scenario"] == "SCENARIO_3_MOD_B_FIRESTORE_ATOMIC_TX"
    assert state["firestore"]["budget_breached"] is False
    assert state["budgets"]["A"]["spent"] == 4.0
    assert state["budgets"]["A"]["remaining"] == 0.0

    deploy_script = (ROOT_DIR / "docker" / "deploy_cloud_run.sh").read_text(encoding="utf-8")
    assert "--min-instances=0" in deploy_script
    assert "--max-instances=1" in deploy_script
    assert "firestore.googleapis.com" in deploy_script
    assert "GCP_PROJECT_ID=${PROJECT_ID}" in deploy_script
    dockerfile = (ROOT_DIR / "Dockerfile").read_text(encoding="utf-8")
    assert "${PORT:-8000}" in dockerfile, "Cloud Run injects PORT"
    assert "google-cloud-firestore" in dockerfile


if __name__ == "__main__":
    test_mod_b_firestore_atomic_transactions_and_occ_retries()
    test_versioned_spend_rejects_stale_writes()
    test_real_firestore_store_path_with_fake_client()
    test_unreachable_firestore_fails_closed()
    test_dashboard_mod_b_and_cloud_run_script()
    print("[PASS] Faz 5 Mod B (Cloud Run + Firestore Atomic OCC Transaction & Hybrid Router) yerel ve Firestore yolunda dogrulandi.")
