from __future__ import annotations

import hashlib
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT / "src"))

from risk_assessment_list.synonym_database import build_synonym_database

MANIFEST_PATH = ROOT / "reference/manifest.json"
SOURCE_DB = ROOT / "src/risk_assessment_list/data/ra.sqlite3"
OUTPUT_DB = ROOT / "reference/generated_synonyms.sqlite3"


def main() -> None:
    json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    manifest_sha = hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest()
    build_synonym_database(
        SOURCE_DB,
        OUTPUT_DB,
        source_manifest_sha=manifest_sha,
    )


if __name__ == "__main__":
    main()
