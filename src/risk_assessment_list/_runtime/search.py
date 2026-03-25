from __future__ import annotations

from typing import Iterable

from rapidfuzz import fuzz

from ..models import SubstanceCandidate
from ..normalize import normalize_cas, normalize_text
from ..synonyms import normalize_synonym_text
from .store import RuntimeStore


def search_substances(
    store: RuntimeStore,
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
    catalog = store.substance_catalog()

    exact_ids = store.exact_candidate_substance_ids(
        normalized_query=normalized_query,
        normalized_name=normalized_name,
        normalized_cas=normalized_cas,
    )
    if mode == "balanced" and exact_ids:
        return _rank_candidates(
            exact_ids,
            catalog=catalog,
            normalized_query=normalized_query,
            normalized_cas=normalized_cas,
            mode=mode,
            min_score=0.0,
        )[:limit]

    candidate_ids = set(
        store.candidate_substance_ids(
            normalized_query=normalized_query,
            normalized_cas=normalized_cas,
        )
    )
    candidate_ids.update(exact_ids)

    if not candidate_ids and mode == "balanced":
        return _rank_candidates(
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
    return _rank_candidates(
        tuple(sorted(candidate_ids)),
        catalog=catalog,
        normalized_query=normalized_query,
        normalized_cas=normalized_cas,
        mode=mode,
        min_score=min_score,
    )[:limit]


def _rank_candidates(
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

    score = float(fuzz.WRatio(normalized_query, alias_normalized))

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
