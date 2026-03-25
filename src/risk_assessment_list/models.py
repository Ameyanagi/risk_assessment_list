from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional


@dataclass(frozen=True)
class SubstanceCandidate:
    display_name: str
    english_name: Optional[str]
    primary_cas_rn: Optional[str]
    cas_rns: tuple[str, ...]
    score: float
    confidence_band: str
    legal_match_available: bool
    ghs_match_available: bool


@dataclass(frozen=True)
class LegalMatch:
    substance_name: str
    english_name: Optional[str]
    cas_text: str
    cas_rns: tuple[str, ...]
    section_title: str
    section_number: Optional[str]
    list_index: Optional[str]
    label_threshold_weight_percent: Optional[float]
    sds_threshold_weight_percent: Optional[float]
    remarks: Optional[str]
    source_file: str
    source_sheet: str
    source_row: int
    source_list_effective_date: Optional[str]
    raw_effective_date: Optional[str]


@dataclass(frozen=True)
class GHSMatch:
    substance_name: str
    cas_text: str
    cas_rns: tuple[str, ...]
    ghs_result_id: str
    active_hazard_classes: dict[str, str]
    pictograms: tuple[str, ...]
    model_label_url: Optional[str]
    model_sds_url: Optional[str]


@dataclass(frozen=True)
class SubstanceResult:
    query: str
    exact_match: bool
    legal_ra_required: bool
    ghs_notice_required: bool
    status: str
    notice_summary: str
    legal_matches: tuple[LegalMatch, ...] = field(default_factory=tuple)
    ghs_matches: tuple[GHSMatch, ...] = field(default_factory=tuple)
    ghs_pictograms: tuple[str, ...] = field(default_factory=tuple)
    model_label_url: Optional[str] = None
    model_sds_url: Optional[str] = None


@dataclass(frozen=True)
class MixtureComponent:
    identifier: str
    weight_percent: float


@dataclass(frozen=True)
class MixtureComponentResult:
    identifier: str
    weight_percent: float
    legal_triggered: bool
    result: SubstanceResult


@dataclass(frozen=True)
class MixtureResult:
    legal_ra_required: bool
    ghs_notice_required: bool
    status: str
    notice_summary: str
    component_results: tuple[MixtureComponentResult, ...]
    triggering_components: tuple[MixtureComponentResult, ...]
    ghs_pictograms: tuple[str, ...]
