from __future__ import annotations

import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def test_fetch_reference_script_wrapper_runs_help() -> None:
    result = subprocess.run(
        [sys.executable, str(ROOT / "scripts" / "fetch_reference.py"), "--help"],
        check=True,
        capture_output=True,
        text=True,
    )

    assert "offline-first cache" in result.stdout
