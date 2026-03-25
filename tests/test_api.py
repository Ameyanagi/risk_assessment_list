from __future__ import annotations

from risk_assessment_list import MixtureComponent, RiskAssessmentList


def test_exact_cas_lookup_returns_legal_and_ghs_matches() -> None:
    library = RiskAssessmentList()
    result = library.evaluate_substance("50-00-0")

    assert result.exact_match is True
    assert result.legal_ra_required is True
    assert result.ghs_notice_required is True
    assert "health_hazard" in result.ghs_pictograms
    assert result.model_label_url is not None


def test_ghs_only_lookup_returns_notice_without_legal_obligation() -> None:
    library = RiskAssessmentList()
    result = library.evaluate_substance("64-69-7")

    assert result.legal_ra_required is False
    assert result.ghs_notice_required is True
    assert result.status == "ghs_notice"


def test_fuzzy_search_returns_candidates_with_scores() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("ホルムアル", limit=5)

    assert candidates
    assert any(candidate.score > 0 for candidate in candidates)
    assert any("ホルムアルデヒド" == candidate.display_name for candidate in candidates)


def test_synonym_search_hits_curated_alias() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("PCB", limit=5)

    assert candidates
    assert "PCB" in (candidates[0].english_name or candidates[0].display_name)


def test_synonym_search_hits_explicit_common_name() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("Aspirin", limit=5)

    assert candidates
    assert candidates[0].display_name == "アセチルサリチル酸（別名アスピリン）"


def test_balanced_search_returns_no_candidates_for_garbage_input() -> None:
    library = RiskAssessmentList()

    assert library.search_substances("not-a-real-substance", mode="balanced") == []
    assert library.search_substances("asdfghjk", mode="balanced") == []


def test_balanced_search_prefers_exact_name_and_suppresses_derivatives() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("マレイン酸", limit=5, mode="balanced")

    assert candidates
    assert [candidate.display_name for candidate in candidates] == ["マレイン酸"]


def test_balanced_search_prefers_exact_alias_hit() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("HCFC-22", limit=5, mode="balanced")

    assert candidates
    assert candidates[0].display_name == "クロロジフルオロメタン（別名ＨＣＦＣ－２２）"
    assert all(
        candidate.display_name != "クロロジフルオロメタン（別名ＨＣＦＣ－２２５）"
        for candidate in candidates
    )


def test_fuzzy_mode_keeps_exact_hit_first_but_allows_broader_matches() -> None:
    library = RiskAssessmentList()
    candidates = library.search_substances("マレイン酸", limit=5, mode="fuzzy")

    assert candidates
    assert candidates[0].display_name == "マレイン酸"
    assert any(candidate.display_name != "マレイン酸" for candidate in candidates[1:])


def test_fuzzy_mode_handles_typo_queries() -> None:
    library = RiskAssessmentList()

    maleik = library.search_substances("maleik acid", limit=5, mode="fuzzy")
    aspiriin = library.search_substances("aspiriin", limit=5, mode="fuzzy")
    formaldehyd = library.search_substances("formaldehyd", limit=5, mode="fuzzy")

    assert maleik
    assert maleik[0].display_name == "マレイン酸"

    assert aspiriin
    assert aspiriin[0].display_name == "アセチルサリチル酸（別名アスピリン）"

    assert formaldehyd
    assert formaldehyd[0].display_name == "ホルムアルデヒド"


def test_mixture_uses_label_or_sds_threshold() -> None:
    library = RiskAssessmentList()
    result = library.evaluate_mixture(
        [
            MixtureComponent(identifier="119-93-7", weight_percent=0.2),
        ]
    )

    assert result.legal_ra_required is True
    assert result.triggering_components


def test_unknown_identifier_returns_no_match() -> None:
    library = RiskAssessmentList()
    result = library.evaluate_substance("not-a-real-substance")

    assert result.exact_match is False
    assert result.legal_ra_required is False
    assert result.ghs_notice_required is False
