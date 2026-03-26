from __future__ import annotations

from typing import Iterable

from ..models import (
    LegalMatch,
    MixtureComponent,
    MixtureComponentResult,
    MixtureResult,
    SubstanceResult,
)
from .store import RuntimeStore


def evaluate_substance(store: RuntimeStore, identifier: str) -> SubstanceResult:
    substance_ids = store.resolve_substance_ids(identifier)
    legal_matches = store.load_legal_matches(substance_ids)
    ghs_matches = store.load_ghs_matches(substance_ids)
    legal_ra_required = bool(legal_matches)
    ghs_notice_required = bool(ghs_matches)
    pictograms = tuple(sorted({p for match in ghs_matches for p in match.pictograms}))
    nite_chrip_urls = _distinct_urls(
        url
        for match in legal_matches
        for url in match.nite_chrip_urls
    )
    nite_chrip_url = next(iter(nite_chrip_urls), None)
    model_label_url = next(
        (match.model_label_url for match in ghs_matches if match.model_label_url), None
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
        nite_chrip_urls=nite_chrip_urls,
        nite_chrip_url=nite_chrip_url,
        model_label_url=model_label_url,
        model_sds_url=model_sds_url,
    )


def evaluate_mixture(
    store: RuntimeStore,
    components: Iterable[MixtureComponent],
) -> MixtureResult:
    component_results = []
    for component in components:
        substance_result = evaluate_substance(store, component.identifier)
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


def _distinct_urls(urls: Iterable[str]) -> tuple[str, ...]:
    seen: set[str] = set()
    ordered_urls = []
    for url in urls:
        if not url or url in seen:
            continue
        seen.add(url)
        ordered_urls.append(url)
    return tuple(ordered_urls)
