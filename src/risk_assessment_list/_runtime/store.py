from __future__ import annotations

import sqlite3
from functools import lru_cache

from ..db import connect
from ..models import GHSMatch, LegalMatch
from ..normalize import normalize_cas, normalize_text


class RuntimeStore:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path

    @property
    def connection(self) -> sqlite3.Connection:
        return connect(self.db_path)

    @lru_cache(maxsize=1)
    def substance_catalog(self) -> dict[int, dict]:
        rows = self.connection.execute(
            """
            select
                s.id,
                s.canonical_name,
                s.canonical_english_name,
                s.canonical_cas,
                exists(
                    select 1
                    from legal_obligations lo
                    where lo.substance_id = s.id
                ) as legal_match_available,
                exists(
                    select 1
                    from ghs_entries ge
                    where ge.substance_id = s.id
                ) as ghs_match_available
            from substances s
            order by s.canonical_name, s.canonical_cas
            """
        ).fetchall()

        catalog = {
            int(row["id"]): {
                "display_name": row["canonical_name"],
                "english_name": row["canonical_english_name"],
                "primary_cas": row["canonical_cas"],
                "cas_rns": [],
                "alias_records": [],
                "legal_match_available": row["legal_match_available"],
                "ghs_match_available": row["ghs_match_available"],
            }
            for row in rows
        }

        for row in self.connection.execute(
            """
            select substance_id, value_raw
            from substance_identifiers
            where identifier_type = 'cas'
            order by substance_id, is_primary desc, value_raw
            """
        ):
            substance = catalog[int(row["substance_id"])]
            substance["cas_rns"].append(row["value_raw"])

        for row in self.connection.execute(
            """
            select
                substance_id,
                alias_normalized,
                alias_type,
                confidence,
                exact_match_allowed
            from substance_aliases
            """
        ):
            catalog[int(row["substance_id"])]["alias_records"].append(
                (
                    row["alias_normalized"],
                    row["alias_type"],
                    row["confidence"],
                    bool(row["exact_match_allowed"]),
                )
            )

        for substance in catalog.values():
            substance["cas_rns"] = tuple(substance["cas_rns"])
            substance["alias_records"] = tuple(
                sorted(
                    substance["alias_records"],
                    key=lambda item: _alias_sort_key(
                        item[1],
                        item[2],
                        item[3],
                        item[0],
                    ),
                )
            )

        return catalog

    def exact_candidate_substance_ids(
        self,
        *,
        normalized_query: str,
        normalized_name: str,
        normalized_cas: str,
    ) -> tuple[int, ...]:
        substance_ids: set[int] = set()

        if normalized_cas:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_identifiers
                where identifier_type = 'cas' and value_normalized = ?
                """,
                (normalized_cas,),
            ):
                substance_ids.add(int(row["substance_id"]))

        if normalized_name:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_identifiers
                where exact_match_allowed = 1 and value_normalized = ?
                """,
                (normalized_name,),
            ):
                substance_ids.add(int(row["substance_id"]))

        if normalized_query:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_aliases
                where alias_normalized = ?
                """,
                (normalized_query,),
            ):
                substance_ids.add(int(row["substance_id"]))

        return tuple(sorted(substance_ids))

    def candidate_substance_ids(
        self,
        *,
        normalized_query: str,
        normalized_cas: str,
    ) -> tuple[int, ...]:
        substance_ids: set[int] = set()

        if normalized_cas:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_identifiers
                where identifier_type = 'cas' and value_normalized = ?
                """,
                (normalized_cas,),
            ):
                substance_ids.add(int(row["substance_id"]))

        if normalized_query:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_aliases
                where alias_normalized = ?
                """,
                (normalized_query,),
            ):
                substance_ids.add(int(row["substance_id"]))

            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_aliases
                where alias_normalized like ?
                limit 200
                """,
                (f"{normalized_query}%",),
            ):
                substance_ids.add(int(row["substance_id"]))

            if self.fts_enabled():
                match_query = _fts_prefix_query(normalized_query)
                if match_query:
                    try:
                        for row in self.connection.execute(
                            """
                            select distinct substance_id
                            from substance_alias_fts
                            where alias_normalized match ?
                            limit 200
                            """,
                            (match_query,),
                        ):
                            substance_ids.add(int(row["substance_id"]))
                    except sqlite3.OperationalError:
                        pass

        return tuple(sorted(substance_ids))

    def resolve_substance_ids(self, identifier: str) -> tuple[int, ...]:
        normalized_cas = normalize_cas(identifier)
        normalized_name = normalize_text(identifier)
        substance_ids: set[int] = set()

        if normalized_cas:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_identifiers
                where identifier_type = 'cas' and value_normalized = ?
                """,
                (normalized_cas,),
            ):
                substance_ids.add(int(row["substance_id"]))

        if normalized_name:
            for row in self.connection.execute(
                """
                select distinct substance_id
                from substance_identifiers
                where exact_match_allowed = 1 and value_normalized = ?
                """,
                (normalized_name,),
            ):
                substance_ids.add(int(row["substance_id"]))

        return tuple(sorted(substance_ids))

    @lru_cache(maxsize=1)
    def fts_enabled(self) -> bool:
        row = self.connection.execute(
            """
            select fts_enabled
            from build_meta
            where id = 1
            """
        ).fetchone()
        return bool(row and row["fts_enabled"])

    def load_legal_matches(self, substance_ids: tuple[int, ...]) -> list[LegalMatch]:
        if not substance_ids:
            return []
        placeholders = ",".join("?" for _ in substance_ids)
        rows = self.connection.execute(
            f"""
            select
                lo.id,
                lo.substance_name,
                lo.english_name,
                lo.cas_text,
                lo.section_title,
                lo.section_number,
                lo.list_index,
                lo.label_threshold,
                lo.sds_threshold,
                lo.remarks,
                lo.source_sheet,
                lo.source_row,
                lo.source_list_effective_date,
                lo.raw_effective_date,
                sf.filename as source_file
            from legal_obligations lo
            join source_files sf on sf.id = lo.source_file_id
            where lo.substance_id in ({placeholders})
            order by sf.filename, lo.source_sheet, lo.source_row
            """,
            substance_ids,
        ).fetchall()
        cas_map = self.child_values(
            table="legal_obligation_cas",
            id_column="legal_obligation_id",
            value_column="cas_rn",
            parent_ids=[int(row["id"]) for row in rows],
        )
        return [
            LegalMatch(
                substance_name=row["substance_name"],
                english_name=row["english_name"] or None,
                cas_text=row["cas_text"] or "",
                cas_rns=cas_map.get(int(row["id"]), tuple()),
                section_title=row["section_title"] or "",
                section_number=row["section_number"] or None,
                list_index=row["list_index"] or None,
                label_threshold_weight_percent=row["label_threshold"],
                sds_threshold_weight_percent=row["sds_threshold"],
                remarks=row["remarks"] or None,
                source_file=row["source_file"],
                source_sheet=row["source_sheet"],
                source_row=row["source_row"],
                source_list_effective_date=row["source_list_effective_date"] or None,
                raw_effective_date=row["raw_effective_date"] or None,
            )
            for row in rows
        ]

    def load_ghs_matches(self, substance_ids: tuple[int, ...]) -> list[GHSMatch]:
        if not substance_ids:
            return []
        placeholders = ",".join("?" for _ in substance_ids)
        rows = self.connection.execute(
            f"""
            select
                ge.id,
                ge.substance_name,
                ge.cas_text,
                ge.ghs_result_id,
                ge.model_label_url,
                ge.model_sds_url
            from ghs_entries ge
            where ge.substance_id in ({placeholders})
            order by ge.substance_name, ge.id
            """,
            substance_ids,
        ).fetchall()

        entry_ids = [int(row["id"]) for row in rows]
        cas_map = self.child_values(
            table="ghs_entry_cas",
            id_column="ghs_entry_id",
            value_column="cas_rn",
            parent_ids=entry_ids,
        )
        class_map: dict[int, dict[str, str]] = {entry_id: {} for entry_id in entry_ids}
        if entry_ids:
            placeholders = ",".join("?" for _ in entry_ids)
            for row in self.connection.execute(
                f"""
                select
                    gc.ghs_entry_id,
                    ghc.label_ja,
                    gc.raw_result
                from ghs_classifications gc
                join ghs_hazard_classes ghc on ghc.id = gc.hazard_class_id
                where gc.ghs_entry_id in ({placeholders}) and gc.is_assigned = 1
                order by gc.ghs_entry_id, ghc.sort_order
                """,
                entry_ids,
            ):
                class_map[int(row["ghs_entry_id"])][row["label_ja"]] = row["raw_result"]

        pictogram_map: dict[int, tuple[str, ...]] = {
            entry_id: tuple()
            for entry_id in entry_ids
        }
        if entry_ids:
            placeholders = ",".join("?" for _ in entry_ids)
            grouped: dict[int, list[str]] = {entry_id: [] for entry_id in entry_ids}
            for row in self.connection.execute(
                f"""
                select ghs_entry_id, pictogram_code
                from ghs_pictograms
                where ghs_entry_id in ({placeholders})
                order by ghs_entry_id, pictogram_code
                """,
                entry_ids,
            ):
                grouped[int(row["ghs_entry_id"])].append(row["pictogram_code"])
            pictogram_map = {
                entry_id: tuple(values)
                for entry_id, values in grouped.items()
            }

        return [
            GHSMatch(
                substance_name=row["substance_name"],
                cas_text=row["cas_text"] or "",
                cas_rns=cas_map.get(int(row["id"]), tuple()),
                ghs_result_id=row["ghs_result_id"],
                active_hazard_classes=class_map.get(int(row["id"]), {}),
                pictograms=pictogram_map.get(int(row["id"]), tuple()),
                model_label_url=row["model_label_url"] or None,
                model_sds_url=row["model_sds_url"] or None,
            )
            for row in rows
        ]

    def child_values(
        self,
        *,
        table: str,
        id_column: str,
        value_column: str,
        parent_ids: list[int],
    ) -> dict[int, tuple[str, ...]]:
        if not parent_ids:
            return {}
        placeholders = ",".join("?" for _ in parent_ids)
        rows = self.connection.execute(
            f"""
            select {id_column} as parent_id, {value_column} as child_value
            from {table}
            where {id_column} in ({placeholders})
            order by {id_column}, {value_column}
            """,
            parent_ids,
        ).fetchall()
        mapping: dict[int, list[str]] = {parent_id: [] for parent_id in parent_ids}
        for row in rows:
            mapping[int(row["parent_id"])].append(row["child_value"])
        return {
            parent_id: tuple(values)
            for parent_id, values in mapping.items()
        }


def _alias_type_priority(alias_type: str) -> int:
    priorities = {
        "cas": 0,
        "canonical_name": 1,
        "canonical_english_name": 1,
        "common_name": 2,
        "explicit_alias": 2,
        "alias": 3,
        "abbreviation": 3,
        "explicit_abbreviation": 3,
        "generated_synonym": 4,
    }
    return priorities.get(alias_type, 5)


def _confidence_priority(confidence: str) -> int:
    return {
        "high": 0,
        "medium": 1,
        "low": 2,
    }.get(confidence, 3)


def _alias_sort_key(
    alias_type: str,
    confidence: str,
    exact_match_allowed: bool,
    alias_normalized: str,
) -> tuple[int, int, int, str]:
    return (
        _alias_type_priority(alias_type),
        _confidence_priority(confidence),
        0 if exact_match_allowed else 1,
        alias_normalized,
    )


def _fts_prefix_query(normalized_query: str) -> str | None:
    query = normalized_query.replace('"', "").strip()
    if not query:
        return None
    return f"{query}*"
