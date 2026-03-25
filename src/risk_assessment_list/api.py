from __future__ import annotations

import sqlite3
from functools import lru_cache
from typing import Iterable

from rapidfuzz import fuzz

from .db import connect
from .models import (
    GHSMatch,
    LegalMatch,
    MixtureComponent,
    MixtureComponentResult,
    MixtureResult,
    SubstanceCandidate,
    SubstanceResult,
)
from .normalize import normalize_cas, normalize_text
from .synonyms import normalize_synonym_text


class RiskAssessmentList:
    def __init__(self, db_path: str | None = None) -> None:
        self.db_path = db_path

    @property
    def connection(self) -> sqlite3.Connection:
        return connect(self.db_path)

    def search_substances(
        self,
        query: str,
        limit: int = 10,
        mode: str = "balanced",
    ) -> list[SubstanceCandidate]:
        mode = _normalize_search_mode(mode)
        normalized_query = normalize_synonym_text(query)
        normalized_name = normalize_text(query)
        normalized_cas = normalize_cas(query)
        if not normalized_query and not normalized_cas:
            return []
        catalog = self._substance_catalog()

        exact_ids = self._exact_candidate_substance_ids(
            normalized_query=normalized_query,
            normalized_name=normalized_name,
            normalized_cas=normalized_cas,
        )
        if mode == "balanced" and exact_ids:
            return self._rank_candidates(
                exact_ids,
                catalog=catalog,
                normalized_query=normalized_query,
                normalized_cas=normalized_cas,
                mode=mode,
                min_score=0.0,
            )[:limit]

        candidate_ids = set(
            self._candidate_substance_ids(
                normalized_query=normalized_query,
                normalized_cas=normalized_cas,
            )
        )
        candidate_ids.update(exact_ids)

        if not candidate_ids and mode == "balanced":
            return self._rank_candidates(
                tuple(catalog.keys()),
                catalog=catalog,
                normalized_query=normalized_query,
                normalized_cas=normalized_cas,
                mode=mode,
                min_score=90.0,
                fallback_only=True,
            )[:limit]

        if not candidate_ids:
            candidate_ids = set(catalog.keys())

        min_score = 85.0 if mode == "balanced" else 70.0
        return self._rank_candidates(
            tuple(sorted(candidate_ids)),
            catalog=catalog,
            normalized_query=normalized_query,
            normalized_cas=normalized_cas,
            mode=mode,
            min_score=min_score,
        )[:limit]

    def evaluate_substance(self, identifier: str) -> SubstanceResult:
        substance_ids = self._resolve_substance_ids(identifier)
        legal_matches = self._load_legal_matches(substance_ids)
        ghs_matches = self._load_ghs_matches(substance_ids)
        legal_ra_required = bool(legal_matches)
        ghs_notice_required = bool(ghs_matches)
        pictograms = tuple(
            sorted({p for match in ghs_matches for p in match.pictograms})
        )
        model_label_url = next(
            (match.model_label_url for match in ghs_matches if match.model_label_url),
            None,
        )
        model_sds_url = next(
            (match.model_sds_url for match in ghs_matches if match.model_sds_url), None
        )
        status, notice_summary = _summarize_status(
            legal_ra_required=legal_ra_required,
            ghs_notice_required=ghs_notice_required,
        )
        return SubstanceResult(
            query=identifier,
            exact_match=bool(substance_ids),
            legal_ra_required=legal_ra_required,
            ghs_notice_required=ghs_notice_required,
            status=status,
            notice_summary=notice_summary,
            legal_matches=tuple(legal_matches),
            ghs_matches=tuple(ghs_matches),
            ghs_pictograms=pictograms,
            model_label_url=model_label_url,
            model_sds_url=model_sds_url,
        )

    def evaluate_mixture(self, components: Iterable[MixtureComponent]) -> MixtureResult:
        component_results = []
        for component in components:
            substance_result = self.evaluate_substance(component.identifier)
            legal_triggered = any(
                _threshold_met(component.weight_percent, match)
                for match in substance_result.legal_matches
            )
            component_results.append(
                MixtureComponentResult(
                    identifier=component.identifier,
                    weight_percent=component.weight_percent,
                    legal_triggered=legal_triggered,
                    result=substance_result,
                )
            )
        triggering_components = tuple(
            result for result in component_results if result.legal_triggered
        )
        legal_ra_required = bool(triggering_components)
        ghs_notice_required = any(
            component.result.ghs_notice_required for component in component_results
        )
        pictograms = tuple(
            sorted(
                {
                    pictogram
                    for component in component_results
                    for pictogram in component.result.ghs_pictograms
                }
            )
        )
        status, notice_summary = _summarize_status(
            legal_ra_required=legal_ra_required,
            ghs_notice_required=ghs_notice_required,
        )
        return MixtureResult(
            legal_ra_required=legal_ra_required,
            ghs_notice_required=ghs_notice_required,
            status=status,
            notice_summary=notice_summary,
            component_results=tuple(component_results),
            triggering_components=triggering_components,
            ghs_pictograms=pictograms,
        )

    @lru_cache(maxsize=1)
    def _substance_catalog(self) -> dict[int, dict]:
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

    def _exact_candidate_substance_ids(
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

    def _candidate_substance_ids(
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

            if self._fts_enabled():
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

    def _rank_candidates(
        self,
        substance_ids: Iterable[int],
        *,
        catalog: dict[int, dict],
        normalized_query: str,
        normalized_cas: str,
        mode: str,
        min_score: float,
        fallback_only: bool = False,
    ) -> list[SubstanceCandidate]:
        ranked: list[tuple[tuple[int, int, float], dict]] = []
        seen: set[int] = set()

        for substance_id in substance_ids:
            if substance_id in seen:
                continue
            seen.add(substance_id)
            substance = catalog.get(int(substance_id))
            if substance is None:
                continue
            match = _score_substance(
                substance=substance,
                normalized_query=normalized_query,
                normalized_cas=normalized_cas,
                mode=mode,
                fallback_only=fallback_only,
            )
            if match is None:
                continue
            if match[2] < min_score:
                continue
            ranked.append((match, substance))

        ranked.sort(
            key=lambda item: (
                item[0][0],
                item[0][1],
                -item[0][2],
                -int(bool(item[1]["legal_match_available"])),
                -int(bool(item[1]["ghs_match_available"])),
                item[1]["display_name"] or "",
                item[1]["primary_cas"] or "",
            )
        )

        return [
            SubstanceCandidate(
                display_name=substance["display_name"],
                english_name=substance["english_name"],
                primary_cas_rn=substance["primary_cas"],
                cas_rns=substance["cas_rns"],
                score=round(match[2], 2),
                confidence_band=_confidence_band(match[2]),
                legal_match_available=bool(substance["legal_match_available"]),
                ghs_match_available=bool(substance["ghs_match_available"]),
            )
            for match, substance in ranked
        ]

    def _resolve_substance_ids(self, identifier: str) -> tuple[int, ...]:
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
    def _fts_enabled(self) -> bool:
        row = self.connection.execute(
            """
            select fts_enabled
            from build_meta
            where id = 1
            """
        ).fetchone()
        return bool(row and row["fts_enabled"])

    def _load_legal_matches(self, substance_ids: tuple[int, ...]) -> list[LegalMatch]:
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
        cas_map = self._child_values(
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

    def _load_ghs_matches(self, substance_ids: tuple[int, ...]) -> list[GHSMatch]:
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
        cas_map = self._child_values(
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
            entry_id: tuple() for entry_id in entry_ids
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
                entry_id: tuple(values) for entry_id, values in grouped.items()
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

    def _child_values(
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
        return {parent_id: tuple(values) for parent_id, values in mapping.items()}


def _threshold_met(weight_percent: float, match: LegalMatch) -> bool:
    thresholds = [
        threshold
        for threshold in (
            match.label_threshold_weight_percent,
            match.sds_threshold_weight_percent,
        )
        if threshold is not None
    ]
    return any(weight_percent >= threshold for threshold in thresholds)


def _confidence_band(score: float) -> str:
    if score >= 95:
        return "high"
    if score >= 80:
        return "medium"
    return "low"


def _normalize_search_mode(mode: str) -> str:
    normalized = (mode or "balanced").strip().lower()
    if normalized not in {"balanced", "fuzzy"}:
        raise ValueError("mode must be 'balanced' or 'fuzzy'")
    return normalized


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


def _scoring_alias_records(
    alias_records: Iterable[tuple[str, str, str, bool]],
    *,
    mode: str,
    fallback_only: bool,
) -> tuple[tuple[str, str, str, bool], ...]:
    if not fallback_only or mode != "balanced":
        return tuple(alias_records)

    high_signal_alias_types = {
        "cas",
        "canonical_name",
        "canonical_english_name",
        "common_name",
        "explicit_alias",
        "alias",
        "abbreviation",
        "explicit_abbreviation",
    }
    return tuple(
        record
        for record in alias_records
        if record[3] or record[1] in high_signal_alias_types
    )


def _score_substance(
    *,
    substance: dict,
    normalized_query: str,
    normalized_cas: str,
    mode: str,
    fallback_only: bool,
) -> tuple[int, int, float] | None:
    if normalized_cas and normalized_cas in substance["cas_rns"]:
        return (0, 0, 100.0)

    best: tuple[int, int, float] | None = None
    for (
        alias_normalized,
        alias_type,
        confidence,
        exact_match_allowed,
    ) in _scoring_alias_records(
        substance["alias_records"],
        mode=mode,
        fallback_only=fallback_only,
    ):
        match = _score_alias(
            normalized_query=normalized_query,
            alias_normalized=alias_normalized,
            alias_type=alias_type,
            confidence=confidence,
            exact_match_allowed=exact_match_allowed,
            mode=mode,
        )
        if match is None:
            continue
        if best is None or _match_sort_key(match) < _match_sort_key(best):
            best = match
    return best


def _score_alias(
    *,
    normalized_query: str,
    alias_normalized: str,
    alias_type: str,
    confidence: str,
    exact_match_allowed: bool,
    mode: str,
) -> tuple[int, int, float] | None:
    if not normalized_query or not alias_normalized:
        return None

    signal_rank = _alias_sort_key(
        alias_type,
        confidence,
        exact_match_allowed,
        alias_normalized,
    )[:3]
    ranking_signal = signal_rank[0] * 10 + signal_rank[1] * 2 + signal_rank[2]

    if normalized_query == alias_normalized:
        exact_tier = 0 if alias_type == "cas" else 1
        return (exact_tier, ranking_signal, 100.0)

    if len(normalized_query) >= 2 and alias_normalized.startswith(normalized_query):
        prefix_penalty = min(max(len(alias_normalized) - len(normalized_query), 0), 12)
        score = max(86.0, 96.0 - (prefix_penalty * 1.25))
        return (2, ranking_signal, score)

    wratio = float(fuzz.WRatio(normalized_query, alias_normalized))
    score = wratio

    if mode == "fuzzy":
        partial = float(fuzz.partial_ratio(normalized_query, alias_normalized))
        score = max(
            score, partial * _length_similarity(normalized_query, alias_normalized)
        )
        if len(normalized_query) >= 3 and normalized_query in alias_normalized:
            substring_penalty = min(
                max(len(alias_normalized) - len(normalized_query), 0), 24
            )
            score = max(score, 82.0 - (substring_penalty * 0.5))

    if score <= 0:
        return None

    tier = 3 if score >= 90 else 4
    return (tier, ranking_signal, score)


def _length_similarity(left: str, right: str) -> float:
    if not left or not right:
        return 0.0
    return min(len(left), len(right)) / max(len(left), len(right))


def _match_sort_key(match: tuple[int, int, float]) -> tuple[int, int, float]:
    return (match[0], match[1], -match[2])


def _fts_prefix_query(normalized_query: str) -> str | None:
    query = normalized_query.replace('"', "").strip()
    if not query:
        return None
    return f"{query}*"


def _summarize_status(
    *, legal_ra_required: bool, ghs_notice_required: bool
) -> tuple[str, str]:
    if legal_ra_required and ghs_notice_required:
        return (
            "legal_obligation",
            "The substance matches the published MHLW obligation lists and also has GHS-classified hazards.",
        )
    if legal_ra_required:
        return (
            "legal_obligation",
            "The substance matches the published MHLW obligation lists.",
        )
    if ghs_notice_required:
        return (
            "ghs_notice",
            "The substance is not matched to the published MHLW obligation lists, but NITE GHS classifications indicate hazards that should be reviewed.",
        )
    return (
        "no_match",
        "No published MHLW obligation match or assigned NITE GHS classification was found for the identifier.",
    )


_default = RiskAssessmentList()


def search_substances(
    query: str,
    limit: int = 10,
    mode: str = "balanced",
) -> list[SubstanceCandidate]:
    return _default.search_substances(query=query, limit=limit, mode=mode)


def evaluate_substance(identifier: str) -> SubstanceResult:
    return _default.evaluate_substance(identifier=identifier)


def evaluate_mixture(components: Iterable[MixtureComponent]) -> MixtureResult:
    return _default.evaluate_mixture(components=components)
