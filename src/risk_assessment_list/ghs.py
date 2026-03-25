from __future__ import annotations

import re
from typing import Iterable

GHS_CLASS_SPECS = (
    ("explosives", "爆発物"),
    ("flammable_gases", "可燃性ガス"),
    ("aerosols", "エアゾール"),
    ("oxidizing_gases", "酸化性ガス"),
    ("gases_under_pressure", "高圧ガス"),
    ("flammable_liquids", "引火性液体"),
    ("flammable_solids", "可燃性固体"),
    ("self_reactive_substances", "自己反応性化学品"),
    ("pyrophoric_liquids", "自然発火性液体"),
    ("pyrophoric_solids", "自然発火性固体"),
    ("self_heating_substances", "自己発熱性化学品"),
    (
        "substances_which_in_contact_with_water_emit_flammable_gases",
        "水反応可燃性化学品",
    ),
    ("oxidizing_liquids", "酸化性液体"),
    ("oxidizing_solids", "酸化性固体"),
    ("organic_peroxides", "有機過酸化物"),
    ("corrosive_to_metals", "金属腐食性化学品"),
    ("desensitized_explosives", "鈍性化爆発物"),
    ("acute_toxicity_oral", "急性毒性（経口）"),
    ("acute_toxicity_dermal", "急性毒性（経皮）"),
    ("acute_toxicity_inhalation_gas", "急性毒性（吸入：ガス）"),
    ("acute_toxicity_inhalation_vapor", "急性毒性（吸入：蒸気）"),
    ("acute_toxicity_inhalation_dust_mist", "急性毒性（吸入：粉塵、ミスト）"),
    ("skin_corrosion_irritation", "皮膚腐食性／刺激性"),
    (
        "serious_eye_damage_eye_irritation",
        "眼に対する重篤な損傷性／眼刺激性",
    ),
    ("respiratory_sensitization", "呼吸器感作性"),
    ("skin_sensitization", "皮膚感作性"),
    ("germ_cell_mutagenicity", "生殖細胞変異原性"),
    ("carcinogenicity", "発がん性"),
    ("reproductive_toxicity", "生殖毒性"),
    ("stot_single_exposure", "特定標的臓器毒性（単回暴露）"),
    ("stot_repeated_exposure", "特定標的臓器毒性（反復暴露）"),
    ("aspiration_hazard", "誤えん有害性"),
    ("aquatic_acute", "水生環境有害性　短期（急性）"),
    ("aquatic_chronic", "水生環境有害性　長期（慢性）"),
    ("ozone_hazard", "オゾン層への有害性"),
)

ACTIVE_NEGATIONS = (
    "区分に該当しない",
    "分類できない",
    "not classified",
    "-",
)

FLAME_COLUMNS = {
    "可燃性ガス",
    "エアゾール",
    "引火性液体",
    "可燃性固体",
    "自己反応性化学品",
    "自然発火性液体",
    "自然発火性固体",
    "自己発熱性化学品",
    "水反応可燃性化学品",
    "有機過酸化物",
}
OXIDIZER_COLUMNS = {"酸化性ガス", "酸化性液体", "酸化性固体"}
EXPLOSIVE_COLUMNS = {"爆発物", "鈍性化爆発物"}
ACUTE_TOXICITY_COLUMNS = {
    "急性毒性（経口）",
    "急性毒性（経皮）",
    "急性毒性（吸入：ガス）",
    "急性毒性（吸入：蒸気）",
    "急性毒性（吸入：粉塵、ミスト）",
}
AQUATIC_COLUMNS = {
    "水生環境有害性　短期（急性）",
    "水生環境有害性　長期（慢性）",
}
SERIOUS_HEALTH_COLUMNS = {
    "呼吸器感作性",
    "生殖細胞変異原性",
    "発がん性",
    "生殖毒性",
    "誤えん有害性",
}
STOT_COLUMNS = {
    "特定標的臓器毒性（単回暴露）",
    "特定標的臓器毒性（反復暴露）",
}


def ghs_class_labels() -> tuple[str, ...]:
    return tuple(label for _, label in GHS_CLASS_SPECS)


def is_assigned_class(value: str) -> bool:
    text = (value or "").strip()
    if not text:
        return False
    lowered = text.lower()
    if lowered in {"-", "ー", "―"}:
        return False
    return not any(token in lowered for token in ACTIVE_NEGATIONS)


def classification_state(value: str) -> str:
    text = (value or "").strip()
    if not text:
        return "missing"
    lowered = text.lower()
    if lowered in {"-", "ー", "―"}:
        return "placeholder"
    if "分類できない" in text:
        return "cannot_classify"
    if "分類対象外" in text:
        return "not_applicable"
    if "区分に該当しない" in text:
        return "not_classified"
    return "assigned"


def extract_category_codes(value: str) -> str | None:
    tokens = _category_tokens(value)
    if not tokens:
        return None
    return ",".join(sorted(tokens))


def active_hazard_classes(classes: dict[str, str]) -> dict[str, str]:
    return {name: value for name, value in classes.items() if is_assigned_class(value)}


def derive_pictograms(classes: dict[str, str]) -> tuple[str, ...]:
    pictograms = set()
    active = active_hazard_classes(classes)
    for name, value in active.items():
        if name in EXPLOSIVE_COLUMNS:
            pictograms.add("exploding_bomb")
            continue
        if name in FLAME_COLUMNS:
            pictograms.add("flame")
            continue
        if name in OXIDIZER_COLUMNS:
            pictograms.add("flame_over_circle")
            continue
        if name == "高圧ガス":
            pictograms.add("gas_cylinder")
            continue
        if name == "金属腐食性化学品":
            pictograms.add("corrosion")
            continue
        if name in ACUTE_TOXICITY_COLUMNS:
            if _is_skull_toxicity(value):
                pictograms.add("skull_and_crossbones")
            else:
                pictograms.add("exclamation_mark")
            continue
        if name == "皮膚腐食性／刺激性":
            if _has_category(value, {"1", "1a", "1b", "1c"}):
                pictograms.add("corrosion")
            else:
                pictograms.add("exclamation_mark")
            continue
        if name == "眼に対する重篤な損傷性／眼刺激性":
            if _has_category(value, {"1"}):
                pictograms.add("corrosion")
            else:
                pictograms.add("exclamation_mark")
            continue
        if name == "皮膚感作性":
            pictograms.add("exclamation_mark")
            continue
        if name in SERIOUS_HEALTH_COLUMNS:
            pictograms.add("health_hazard")
            continue
        if name in STOT_COLUMNS:
            if _is_stot_exclamation_only(value):
                pictograms.add("exclamation_mark")
            else:
                pictograms.add("health_hazard")
            continue
        if name in AQUATIC_COLUMNS:
            pictograms.add("environment")
            continue
    return tuple(sorted(pictograms))


def _category_tokens(value: str) -> set[str]:
    normalized = (value or "").lower().replace(" ", "")
    return set(re.findall(r"区分([0-9]+[a-z]?)", normalized))


def _has_category(value: str, categories: Iterable[str]) -> bool:
    tokens = _category_tokens(value)
    return any(category.lower() in tokens for category in categories)


def _is_skull_toxicity(value: str) -> bool:
    return _has_category(value, {"1", "2", "3"})


def _is_stot_exclamation_only(value: str) -> bool:
    tokens = _category_tokens(value)
    return bool(tokens) and tokens.issubset({"3"})
