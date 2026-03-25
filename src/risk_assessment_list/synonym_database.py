from __future__ import annotations

import os
import sqlite3
from datetime import datetime, timezone
from pathlib import Path

SCHEMA_VERSION = 1


def build_synonym_database(
    source_db_path: Path,
    output_path: Path,
    *,
    source_manifest_sha: str | None = None,
) -> None:
    source_db_path = Path(source_db_path)
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)
    temp_path = output_path.with_suffix(output_path.suffix + ".tmp")
    if temp_path.exists():
        temp_path.unlink()

    source = sqlite3.connect(source_db_path)
    source.row_factory = sqlite3.Row
    target = sqlite3.connect(temp_path)
    target.execute("pragma foreign_keys = on")

    try:
        create_schema(target)
        export_groups(source, target)
        export_identifiers(source, target)
        export_aliases(source, target)
        write_build_meta(target, source_manifest_sha=source_manifest_sha)
        create_indexes(target)
        target.commit()
        target.execute("vacuum")
    finally:
        source.close()
        target.close()

    os.replace(temp_path, output_path)


def create_schema(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        create table build_meta (
            id integer primary key check (id = 1),
            schema_version integer not null,
            built_at text not null,
            source_manifest_sha256 text,
            group_count integer not null,
            identifier_count integer not null,
            alias_count integer not null
        );

        create table synonym_groups (
            id integer primary key,
            substance_id integer not null unique,
            canonical_name text not null,
            canonical_name_normalized text not null,
            canonical_english_name text,
            canonical_english_name_normalized text,
            canonical_cas text,
            substance_kind text not null,
            has_legal_match integer not null,
            has_ghs_match integer not null
        );

        create table synonym_identifiers (
            id integer primary key,
            group_id integer not null references synonym_groups(id) on delete cascade,
            identifier_type text not null,
            value_raw text not null,
            value_normalized text not null,
            is_primary integer not null
        );

        create table synonym_aliases (
            id integer primary key,
            group_id integer not null references synonym_groups(id) on delete cascade,
            alias_raw text not null,
            alias_normalized text not null,
            alias_type text not null,
            confidence text not null,
            exact_match_allowed integer not null
        );
        """
    )


def export_groups(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    rows = source.execute(
        """
        select
            s.id as substance_id,
            s.canonical_name,
            s.canonical_name_normalized,
            s.canonical_english_name,
            s.canonical_english_name_normalized,
            s.canonical_cas,
            s.substance_kind,
            exists(
                select 1 from legal_obligations lo where lo.substance_id = s.id
            ) as has_legal_match,
            exists(
                select 1 from ghs_entries ge where ge.substance_id = s.id
            ) as has_ghs_match
        from substances s
        order by s.id
        """
    )
    for row in rows:
        target.execute(
            """
            insert into synonym_groups (
                substance_id,
                canonical_name,
                canonical_name_normalized,
                canonical_english_name,
                canonical_english_name_normalized,
                canonical_cas,
                substance_kind,
                has_legal_match,
                has_ghs_match
            ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                row["substance_id"],
                row["canonical_name"],
                row["canonical_name_normalized"],
                row["canonical_english_name"],
                row["canonical_english_name_normalized"],
                row["canonical_cas"],
                row["substance_kind"],
                row["has_legal_match"],
                row["has_ghs_match"],
            ),
        )


def export_identifiers(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    rows = source.execute(
        """
        select
            substance_id,
            identifier_type,
            value_raw,
            value_normalized,
            is_primary
        from substance_identifiers
        order by substance_id, identifier_type, value_normalized
        """
    )
    for row in rows:
        target.execute(
            """
            insert into synonym_identifiers (
                group_id,
                identifier_type,
                value_raw,
                value_normalized,
                is_primary
            ) values (?, ?, ?, ?, ?)
            """,
            (
                row["substance_id"],
                row["identifier_type"],
                row["value_raw"],
                row["value_normalized"],
                row["is_primary"],
            ),
        )


def export_aliases(source: sqlite3.Connection, target: sqlite3.Connection) -> None:
    rows = source.execute(
        """
        select
            substance_id,
            alias_raw,
            alias_normalized,
            alias_type,
            confidence,
            exact_match_allowed
        from substance_aliases
        order by substance_id, alias_type, alias_normalized
        """
    )
    for row in rows:
        target.execute(
            """
            insert into synonym_aliases (
                group_id,
                alias_raw,
                alias_normalized,
                alias_type,
                confidence,
                exact_match_allowed
            ) values (?, ?, ?, ?, ?, ?)
            """,
            (
                row["substance_id"],
                row["alias_raw"],
                row["alias_normalized"],
                row["alias_type"],
                row["confidence"],
                row["exact_match_allowed"],
            ),
        )


def write_build_meta(
    connection: sqlite3.Connection, *, source_manifest_sha: str | None
) -> None:
    connection.execute(
        """
        insert into build_meta (
            id,
            schema_version,
            built_at,
            source_manifest_sha256,
            group_count,
            identifier_count,
            alias_count
        ) values (?, ?, ?, ?, ?, ?, ?)
        """,
        (
            1,
            SCHEMA_VERSION,
            datetime.now(timezone.utc).isoformat(),
            source_manifest_sha,
            scalar(connection, "select count(*) from synonym_groups"),
            scalar(connection, "select count(*) from synonym_identifiers"),
            scalar(connection, "select count(*) from synonym_aliases"),
        ),
    )


def create_indexes(connection: sqlite3.Connection) -> None:
    connection.executescript(
        """
        create index idx_synonym_groups_canonical_name on synonym_groups(canonical_name_normalized);
        create index idx_synonym_groups_canonical_cas on synonym_groups(canonical_cas);
        create index idx_synonym_identifiers_lookup on synonym_identifiers(value_normalized, identifier_type);
        create index idx_synonym_aliases_lookup on synonym_aliases(alias_normalized);
        """
    )


def scalar(connection: sqlite3.Connection, query: str) -> int:
    return int(connection.execute(query).fetchone()[0])
