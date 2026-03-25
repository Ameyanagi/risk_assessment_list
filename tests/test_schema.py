from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = (
    Path(__file__).resolve().parents[1]
    / "src"
    / "risk_assessment_list"
    / "data"
    / "ra.sqlite3"
)


def test_packaged_database_uses_normalized_schema() -> None:
    connection = sqlite3.connect(DB_PATH)
    try:
        cursor = connection.cursor()
        tables = {
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type IN ('table', 'view')"
            )
        }

        assert "build_meta" in tables
        assert "raw_legal_rows" in tables
        assert "raw_ghs_rows" in tables
        assert "substances" in tables
        assert "substance_identifiers" in tables
        assert "substance_aliases" in tables
        assert "legal_obligations" in tables
        assert "ghs_classifications" in tables
        assert "ghs_hazard_classes" in tables
        assert "substance_alias_fts" in tables

        schema_version = cursor.execute(
            "SELECT schema_version FROM build_meta"
        ).fetchone()[0]
        assert schema_version >= 2
        user_version = cursor.execute("PRAGMA user_version").fetchone()[0]
        assert user_version == schema_version

        hazard_class_count = cursor.execute(
            "SELECT COUNT(*) FROM ghs_hazard_classes"
        ).fetchone()[0]
        ghs_entry_count = cursor.execute("SELECT COUNT(*) FROM ghs_entries").fetchone()[
            0
        ]
        ghs_classification_count = cursor.execute(
            "SELECT COUNT(*) FROM ghs_classifications"
        ).fetchone()[0]
        assert hazard_class_count == 35
        assert ghs_entry_count > 0
        assert ghs_classification_count == ghs_entry_count * hazard_class_count
    finally:
        connection.close()
