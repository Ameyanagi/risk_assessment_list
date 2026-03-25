from __future__ import annotations

import hashlib
import json
from pathlib import Path

ManifestValue = str | bool
ManifestEntry = dict[str, ManifestValue]

PACKAGE_ROOT = Path(__file__).resolve().parents[1]
SRC_ROOT = PACKAGE_ROOT.parent
PROJECT_ROOT = SRC_ROOT.parent
REFERENCE_DIR = PROJECT_ROOT / "reference"
MANIFEST_PATH = REFERENCE_DIR / "manifest.json"
BUILD_DIR = PROJECT_ROOT / "build"
OUTPUT_DB = PACKAGE_ROOT / "data/ra.sqlite3"
SYNONYM_DB_OUTPUT = REFERENCE_DIR / "generated_synonyms.sqlite3"
REVIEWED_SYNONYM_CSV = REFERENCE_DIR / "reviewed_synonyms.csv"
TEMP_DB = BUILD_DIR / "ra.sqlite3.tmp"
USER_AGENT = "risk-assessment-list/0.1.0"
SCHEMA_VERSION = 2

LEGAL_SOURCE_FILES = ["001168179.xlsx", "001474394.xlsx"]
GHS_SOURCE_FILE = "list_nite_all.xlsx"
SOURCES: tuple[dict[str, str], ...] = (
    {
        "key": "johas_step_list",
        "url": "https://cheminfo.johas.go.jp/step/list.html",
        "filename": "johas_step_list.html",
    },
    {
        "key": "mhlw_001168179",
        "url": "https://www.mhlw.go.jp/content/11300000/001168179.xlsx",
        "filename": "001168179.xlsx",
    },
    {
        "key": "mhlw_001474394",
        "url": "https://www.mhlw.go.jp/content/11300000/001474394.xlsx",
        "filename": "001474394.xlsx",
    },
    {
        "key": "nite_list_nite_all",
        "url": "https://www.chem-info.nite.go.jp/chem/ghs/files/list_nite_all.xlsx",
        "filename": "list_nite_all.xlsx",
    },
)
SNAPSHOT_BASELINES = {
    "8ddba2f69b917a4c5386d37cc062bec2a6792e9f4f8faef3c3a08dd749b9d3ca": {
        "raw_legal_rows_total": 4953,
        "raw_legal_rows_data": 4889,
        "raw_ghs_rows_total": 3418,
        "raw_ghs_rows_data": 3417,
        "legal_obligations": 4889,
        "legal_obligation_cas": 4951,
        "ghs_entries": 3417,
        "ghs_entry_cas": 3453,
        "ghs_hazard_classes": 35,
    },
}


def load_manifest_sources() -> list[ManifestEntry]:
    payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return payload.get("sources", [])


def manifest_sha256() -> str:
    return hashlib.sha256(MANIFEST_PATH.read_bytes()).hexdigest()
