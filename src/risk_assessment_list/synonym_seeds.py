from __future__ import annotations

# Curated alias groups for discovery-only fuzzy search.
# Keep this list small and conservative: these are common industry abbreviations
# and stable alternate spellings, not speculative equivalence classes.

CURATED_ALIAS_MAP = {
    "ポリ塩化ビフェニル": (
        "PCB",
        "polychlorinated biphenyl",
        "polychlorinated biphenyls",
        "ポリ塩素化ビフェニル",
    ),
    "ジクロロメタン": (
        "DCM",
        "methylene chloride",
    ),
    "トリクロロエチレン": (
        "TCE",
        "trichloroethylene",
        "trichloroethene",
    ),
    "テトラクロロエチレン": (
        "PCE",
        "perchloroethylene",
        "tetrachloroethylene",
    ),
    "1,1,1-トリクロロ-2,2-ビス(4-クロロフェニル)エタン": (
        "DDT",
        "dichlorodiphenyltrichloroethane",
        "1,1,1-trichloro-2,2-bis(4-chlorophenyl)ethane",
    ),
}


GREEK_VARIANTS = {
    "α": ("alpha", "アルファ"),
    "β": ("beta", "ベータ"),
    "γ": ("gamma", "ガンマ"),
    "δ": ("delta", "デルタ"),
    "ε": ("epsilon", "イプシロン"),
    "ζ": ("zeta", "ゼータ"),
    "η": ("eta", "イータ"),
    "θ": ("theta", "シータ"),
    "ι": ("iota", "イオタ"),
    "κ": ("kappa", "カッパ"),
    "λ": ("lambda", "ラムダ"),
    "μ": ("mu", "ミュー"),
    "ν": ("nu", "ニュー"),
    "ξ": ("xi", "クシー"),
    "ο": ("omicron", "オミクロン"),
    "π": ("pi", "パイ"),
    "ρ": ("rho", "ロー"),
    "σ": ("sigma", "シグマ"),
    "τ": ("tau", "タウ"),
    "υ": ("upsilon", "ウプシロン"),
    "φ": ("phi", "ファイ"),
    "χ": ("chi", "カイ"),
    "ψ": ("psi", "プサイ"),
    "ω": ("omega", "オメガ"),
}


SEPARATOR_VARIANTS = (
    " ",
    "\t",
    "\u3000",
    "-",
    "‐",
    "‑",
    "–",
    "—",
    "―",
    "−",
    "_",
    "/",
    "／",
    "・",
    "･",
    ",",
    "，",
    "、",
    "·",
)
