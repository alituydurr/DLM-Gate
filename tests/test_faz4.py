"""Faz 4 Verification Suite: Live Visual Web Dashboard (3 Demo Scenarios + HTTP API Verification)."""

from __future__ import annotations
from http.server import ThreadingHTTPServer
import json
import pathlib
import sys
import threading
import urllib.request

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.web.app import DashboardRuntime, _StdlibDashboardHandler


def test_dashboard_runtime_scenarios() -> None:
    rt = DashboardRuntime()

    # Scenario 1: Spanning Tree (6 msgs) + Multi-Owner Budget Deduction (8.0 -> 6.0)
    s1 = rt.run_scenario_1()
    assert s1["tree"]["total_messages"] == 6
    assert s1["tree"]["intermediate_a_label"]["effective_readers"] == ["A", "C"]
    assert s1["tree"]["root_label"]["effective_readers"] == ["A"]
    assert s1["budgets"]["A"]["remaining"] == 6.0
    assert s1["budgets"]["B"]["remaining"] == 6.0
    assert s1["budgets"]["C"]["remaining"] == 6.0

    # Scenario 2: Prompt Injection Containment (5 attacks blocked with 0 bits, 1 endorsed schema = 2.0 bits)
    s2 = rt.run_scenario_2()
    assert [d["reason"] for d in s2["decisions"]] == [
        "BYZANTINE_INVALID_SCHEMA_VALUE",
        "THEOREM2_UNTRUSTED_PLAN_DECISION",
        "THEOREM2_UNAPPROVED_SCHEMA:free_text",
        "THEOREM2_UNAUTHORIZED_TARGET:ATTACKER_EXT",
        "THEOREM2_UNENDORSED_LOW_INTEGRITY",
        "APPROVED_ENDORSED_SCHEMA",
    ]
    assert s2["budgets"]["A"]["spent"] == 2.0
    assert s2["budgets"]["A"]["remaining"] == 6.0

    # Scenario 3a: Uncoordinated replicas double-spend (6.0 > 4.0 bit breach on Owner A)
    s3_race = rt.run_scenario_3(mutex_enabled=False)
    assert s3_race["mutex"]["total_messages"] == 0
    assert s3_race["budgets"]["A"]["spent"] == 6.0
    assert s3_race["budgets"]["A"]["breached"] is True

    # Scenario 3b: Ricart-Agrawala + Piggybacked G-Counter (12 messages, 4.0 <= 4.0 bit invariant preserved)
    s3_mutex = rt.run_scenario_3(mutex_enabled=True)
    assert s3_mutex["mutex"]["total_messages"] == 12
    assert s3_mutex["budgets"]["A"]["spent"] == 4.0
    assert s3_mutex["budgets"]["A"]["breached"] is False
    assert [d["route"] for d in s3_mutex["decisions"]] == ["CLOUD_GEMINI", "CLOUD_GEMINI", "LOCAL_ENCLAVE"]


def test_live_http_endpoints() -> None:
    server = ThreadingHTTPServer(("127.0.0.1", 0), _StdlibDashboardHandler)
    port = server.server_address[1]
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    try:
        # 1. Verify HTML UI loads with all 3 scenario controls
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/", timeout=3) as resp:
            html = resp.read().decode("utf-8")
            assert resp.status == 200
            assert "DLM-Gate" in html
            assert "/api/scenario/1" in html
            assert "/api/scenario/2" in html
            assert "/api/scenario/3/mutex" in html

        # 2. Trigger Scenario 3b over HTTP POST and verify JSON state
        req = urllib.request.Request(f"http://127.0.0.1:{port}/api/scenario/3/mutex", method="POST")
        with urllib.request.urlopen(req, timeout=3) as resp:
            payload = json.loads(resp.read().decode("utf-8"))
            assert resp.status == 200
            assert payload["tree"]["total_messages"] == 6
            assert payload["mutex"]["total_messages"] == 12
            assert payload["budgets"]["A"]["breached"] is False
    finally:
        server.shutdown()
        server.server_close()


if __name__ == "__main__":
    test_dashboard_runtime_scenarios()
    test_live_http_endpoints()
    print("[PASS] Faz 4 Canli Web Kontrol Paneli (3 Demo Senaryosu & HTTP API) dogrulandi.")
