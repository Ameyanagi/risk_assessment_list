from __future__ import annotations

import re
import unicodedata

_SPACE_RE = re.compile(r"\s+")
_CAS_RE = re.compile(r"(?<!\d)\d{2,7}-\d{2}-\d(?!\d)")
_DASH_VARIANTS = {
    "\u2010": "-",
    "\u2011": "-",
    "\u2012": "-",
    "\u2013": "-",
    "\u2014": "-",
    "\u2015": "-",
    "\u2212": "-",
    "\u30fc": "-",
    "\uff0d": "-",
}


def normalize_text(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "")
    for variant, replacement in _DASH_VARIANTS.items():
        normalized = normalized.replace(variant, replacement)
    normalized = normalized.lower()
    normalized = normalized.replace("®", "")
    normalized = normalized.replace("・", "")
    normalized = normalized.replace(" ", "")
    normalized = _SPACE_RE.sub("", normalized)
    return normalized.strip()


def normalize_cas(value: str) -> str:
    normalized = unicodedata.normalize("NFKC", value or "")
    normalized = normalized.replace(" ", "")
    normalized = normalized.replace("　", "")
    normalized = normalized.replace("−", "-")
    return normalized.strip()


def extract_cas_rns(value: str) -> list[str]:
    if not value:
        return []
    cas_rns = []
    seen = set()
    for match in _CAS_RE.findall(unicodedata.normalize("NFKC", value)):
        cas = normalize_cas(match)
        if cas not in seen:
            seen.add(cas)
            cas_rns.append(cas)
    return cas_rns


def parse_float(value: str) -> float | None:
    if value is None:
        return None
    text = unicodedata.normalize("NFKC", str(value)).strip()
    if not text:
        return None
    match = re.search(r"\d+(?:\.\d+)?", text)
    if not match:
        return None
    return float(match.group(0))
