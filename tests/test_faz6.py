"""Faz 6 Verification Suite: command-line demo runner and its backup transcript."""

from __future__ import annotations
import pathlib
import sys

ROOT_DIR = pathlib.Path(__file__).resolve().parents[1]
if str(ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(ROOT_DIR))

from presentation.CANLI_VE_YEDEK_DEMO_SENARYOSU import TRANSCRIPT_PATH, generate_demo_transcript


def test_backup_demo_runner_and_transcript() -> None:
    transcript = generate_demo_transcript()
    assert TRANSCRIPT_PATH.exists()
    assert "[ADIM 1]" in transcript
    assert "[ADIM 2]" in transcript
    assert "[ADIM 3A]" in transcript
    assert "[ADIM 3B]" in transcript
    assert "[ADIM 3C]" in transcript
    assert "3 BCAST + 3 CONVCAST = 6 / 6 mesaj" in transcript
    assert "6 REQUEST + 6 OKAY = 12 mesaj" in transcript


if __name__ == "__main__":
    test_backup_demo_runner_and_transcript()
    print("[PASS] Faz 6 Komut satiri demosu ve yedek cikti kaydi dogrulandi.")
