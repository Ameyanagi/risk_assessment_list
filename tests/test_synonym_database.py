from __future__ import annotations

import sqlite3
from pathlib import Path

DB_PATH = (
    Path(__file__).resolve().parents[1] / "reference" / "generated_synonyms.sqlite3"
)


def test_generated_synonym_database_covers_hazard_population() -> None:
    connection = sqlite3.connect(DB_PATH)
    try:
        cursor = connection.cursor()
        tables = {
            row[0]
            for row in cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%'"
            )
        }
        assert "build_meta" in tables
        assert "synonym_groups" in tables
        assert "synonym_identifiers" in tables
        assert "synonym_aliases" in tables

        group_count = cursor.execute("SELECT COUNT(*) FROM synonym_groups").fetchone()[
            0
        ]
        hazard_group_count = cursor.execute(
            "SELECT COUNT(*) FROM synonym_groups WHERE has_ghs_match = 1"
        ).fetchone()[0]
        alias_count = cursor.execute("SELECT COUNT(*) FROM synonym_aliases").fetchone()[
            0
        ]

        assert group_count >= 3000
        assert hazard_group_count >= 3000
        assert alias_count >= group_count
    finally:
        connection.close()
