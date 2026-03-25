from __future__ import annotations

from risk_assessment_list.synonym_database import build_synonym_database

from .config import OUTPUT_DB
from .config import SYNONYM_DB_OUTPUT
from .config import load_manifest_sources
from .config import manifest_sha256


def main() -> None:
    load_manifest_sources()
    build_synonym_database(
        OUTPUT_DB,
        SYNONYM_DB_OUTPUT,
        source_manifest_sha=manifest_sha256(),
    )
