"""DLM-Gate 12-Minute Live & Backup Classroom Demo Runner (Generates YEDEK_DEMO_CIKTI_KAYDI.txt)."""

from __future__ import annotations
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from src.web.app import DashboardRuntime

TRANSCRIPT_PATH = pathlib.Path(__file__).resolve().parent / "YEDEK_DEMO_CIKTI_KAYDI.txt"


def generate_demo_transcript() -> str:
    rt = DashboardRuntime()
    lines: list[str] = []

    def log(text: str = "") -> None:
        lines.append(text)

    log("================================================================================")
    log("DLM-GATE: CANLI SINIF DEMOSU VE YEDEK DOGRULAMA KAYDI (3 ADIM - 12 DAKIKA)")
    log("================================================================================")
    log()

    # ADIM 1
    s1 = rt.run_scenario_1()
    t1 = s1["tree"]
    d1 = s1["decisions"][0]
    log("[ADIM 1] Derinligi 2 Olan Agacta Bcast_ST / Ccast_ST, Vector Clock ve DLM Butce Dusumu")
    log("-" * 80)
    log(f"• Agac Topolojisi: KOK -> A -> C ve KOK -> B (n={t1['node_count']} dugum, Derinlik=2)")
    log(f"• Theorem 4.5 Mesaj Sayisi: {t1['bcast_count']} BCAST + {t1['convcast_count']} CONVCAST = {t1['total_messages']} / {t1['theoretical_bound']} mesaj")
    for m in t1["messages"]:
        log(f"  [{m['step']}] {m['msg_type']:8s} | {m['sender']:4s} -> {m['receiver']:4s} | Etiket: {m['label']:32s} | VC: {m['vc']}")
    log(f"• Ara Dugum A Birlesimi (L_A ⊔ L_C): effectiveReaders = {t1['intermediate_a_label']['effective_readers']}")
    log(f"• Kok Birlesimi (L_root): effectiveReaders = {t1['root_label']['effective_readers']} | Integrity = {t1['root_label']['integrity']}")
    log(f"• Indirme Karari: {d1['query_id']} -> {d1['route']} | ΔL = {d1['delta_bits']} bit <= {d1['bit_complexity']} bit (Wire)")
    log(f"• Kalan Butceler (Her 3 kurumdan 2'ser bit dustu): A={s1['budgets']['A']['remaining']}b, B={s1['budgets']['B']['remaining']}b, C={s1['budgets']['C']['remaining']}b")
    log()

    # ADIM 2
    s2 = rt.run_scenario_2()
    log("[ADIM 2] Dolayli Komut Enjeksiyonu Altinda Teorem 1 ve Teorem 2 (Dayanikli Indirme)")
    log("-" * 80)
    for d in s2["decisions"]:
        status = "ONAY" if d["approved"] else "RED "
        log(f"  [{status}] {d['query_id']:22s} | Sema: {d['schema_name']:10s} | Hedef: {d['route']:13s} | ΔL={d['delta_bits']}b | Gerekce: {d['reason']}")
    rejected = sum(1 for d in s2["decisions"] if not d["approved"])
    log(f"• Sonuc: {rejected} saldiri/izinsiz adim 0.0 bit ile LOCAL_ENCLAVE'de tutuldu; onayli 4 secenekli sema en fazla 2.0 bit harcadi.")
    log()

    # ADIM 3A (RACE CONDITION)
    s3_race = rt.run_scenario_3(mutex_enabled=False)
    log("[ADIM 3A] Mutex Kapali: Koordinasyonsuz Kopyalar Ayni Butceyi Gorur -> Cift Harcama (B_A = 4.0 bit)")
    log("-" * 80)
    log(f"• Koordinasyon Mesaji: {s3_race['mutex']['total_messages']} mesaj")
    log(f"• Harcanan Bitler: A={s3_race['budgets']['A']['spent']}b / {s3_race['budgets']['A']['max']}b -> BUTCE IHLALI (Breached={s3_race['budgets']['A']['breached']})!")
    log()

    # ADIM 3B (MOD A: RICART-AGRAWALA + PIGGYBACKED G-COUNTER)
    s3_ra = rt.run_scenario_3(mutex_enabled=True)
    log("[ADIM 3B] Mod A: Ricart-Agrawala + Piggybacked G-Counter CRDT Acikken (N=3 Kapi: gate-1..3)")
    log("-" * 80)
    log(f"• Ricart-Agrawala Mesaj Sayisi: {s3_ra['mutex']['request_msg_count']} REQUEST + {s3_ra['mutex']['okay_msg_count']} OKAY = {s3_ra['mutex']['total_messages']} mesaj (Giris basi 2(N-1)=4)")
    for m in s3_ra["mutex"]["messages"]:
        ledger = f" | defter A={m['piggybacked_ledger'].get('A', {})}" if m["piggybacked_ledger"] else ""
        log(f"  ({m['step']:2d}) {m['msg_type']:7s} {m['sender']} -> {m['receiver']} | ts={m['req_ts']}{ledger}")
    for d in s3_ra["decisions"]:
        log(f"  • {d['query_id']} -> {d['route']:13s} | ΔL={d['delta_bits']}b | Kalan A={d['remaining_budgets']['A']}b | Gerekce: {d['reason']}")
    log(f"• Sonuc: Ilk 2 sorgu CLOUD_GEMINI'ye onaylandi (4.0 bit), 3. sorgu LOCAL_ENCLAVE'e yonlendirildi (Breached={s3_ra['budgets']['A']['breached']}).")
    log()

    # ADIM 3C (MOD B: CLOUD RUN + FIRESTORE ATOMIC OCC TRANSACTION)
    s3_fb = rt.run_scenario_3_firestore()
    log(f"[ADIM 3C] Mod B: Durumsuz Kopyalar + Firestore Atomik OCC Transaction (defter: {s3_fb['firestore']['backend_mode']})")
    log("-" * 80)
    log(f"• Toplam Transaction Denemesi: {s3_fb['firestore']['total_tx_attempts']} (OCC Otomatik Retry: {s3_fb['firestore']['occ_retry_count']})")
    for e in s3_fb["firestore"]["tx_events"]:
        log(f"  [{e['step']}] {e['instance_id']} ({e['query_id']}) | Deneme #{e['tx_attempt']} | v{e['read_version']}->v{e['committed_version']} | {e['status']:14s} | Kalan A={e['remaining_budgets']['A']}b")
    log("================================================================================")

    full_text = "\n".join(lines) + "\n"
    TRANSCRIPT_PATH.write_text(full_text, encoding="utf-8")
    return full_text


if __name__ == "__main__":
    sys.stdout.reconfigure(encoding="utf-8")  # Windows cmd (cp1254) cannot print the lattice/delta symbols otherwise
    output = generate_demo_transcript()
    print(output)
