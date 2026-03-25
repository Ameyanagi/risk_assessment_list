from __future__ import annotations

import re
import unicodedata
from collections.abc import Iterable
from typing import Final

from .normalize import normalize_text
from .synonym_seeds import CURATED_ALIAS_MAP, GREEK_VARIANTS, SEPARATOR_VARIANTS

__all__ = [
    "build_synonym_index",
    "generate_synonyms",
    "normalize_synonym_text",
]

_PAREN_RE: Final = re.compile(r"[（(]\s*([^()（）]+?)\s*[）)]")
_ALIAS_MARKER_RE: Final = re.compile(
    r"(?:別名|別称|旧称|alias|aka|also known as)\s*[:：]?\s*",
    re.IGNORECASE,
)
_ASCII_GREEK_RE: Final = re.compile(
    r"\b(alpha|beta|gamma|delta|epsilon|zeta|eta|theta|iota|kappa|lambda|mu|nu|xi|omicron|pi|rho|sigma|tau|upsilon|phi|chi|psi|omega)\b",
    re.IGNORECASE,
)
_PUNCT_TRIM_RE: Final = re.compile(r"^[\s:：=＝,，;；/／\-]+|[\s:：=＝,，;；/／\-]+$")
_WHITESPACE_RE: Final = re.compile(r"\s+")


def normalize_synonym_text(value: str) -> str:
    text = unicodedata.normalize("NFKC", value or "")
    text = text.lower()
    text = text.replace("®", "")
    text = _replace_greek_symbols(text)
    text = _replace_ascii_greek_words(text)
    text = text.translate({ord(sep): " " for sep in SEPARATOR_VARIANTS})
    text = _WHITESPACE_RE.sub(" ", text).strip()
    text = text.replace(" ", "")
    text = text.replace("(", "").replace(")", "")
    text = text.replace("（", "").replace("）", "")
    return normalize_text(text)


def generate_synonyms(value: str) -> tuple[str, ...]:
    if not value:
        return tuple()

    normalized_value = unicodedata.normalize("NFKC", value).strip()
    candidates = [normalized_value]
    candidates.extend(_explicit_alias_variants(normalized_value))
    candidates.extend(_greek_name_variants(normalized_value))
    candidates.extend(_variant_curated_aliases(normalized_value))
    return tuple(_dedupe_preserve_order(candidates))


def build_synonym_index(names: Iterable[str]) -> dict[str, tuple[str, ...]]:
    index: dict[str, tuple[str, ...]] = {}
    for name in names:
        key = normalize_synonym_text(name)
        if not key or key in index:
            continue
        index[key] = generate_synonyms(name)
    return index


def _explicit_alias_variants(value: str) -> list[str]:
    normalized = value.replace("（", "(").replace("）", ")").strip()
    if not _contains_alias_marker(normalized):
        return []

    results: list[str] = []
    outer = _remove_alias_parentheticals(normalized)
    outer = _clean_outer_candidate(outer)
    if outer:
        results.append(outer)

    for inner in _PAREN_RE.findall(normalized):
        cleaned = _clean_alias_candidate(inner)
        if cleaned:
            results.append(cleaned)

    split_parts = [
        part.strip() for part in _ALIAS_MARKER_RE.split(normalized) if part.strip()
    ]
    for part in split_parts:
        cleaned = _clean_alias_candidate(part)
        if cleaned:
            results.append(cleaned)

    return results


def _greek_name_variants(value: str) -> list[str]:
    results = [value]
    current = unicodedata.normalize("NFKC", value)
    for symbol, variants in GREEK_VARIANTS.items():
        ascii_name, kana_name = variants
        replacements = (symbol, ascii_name, kana_name)
        for original in replacements:
            if original not in current:
                continue
            for replacement in replacements:
                if replacement == original:
                    continue
                results.append(current.replace(original, replacement))

    if _ASCII_GREEK_RE.search(current):
        for symbol, variants in GREEK_VARIANTS.items():
            ascii_name, kana_name = variants
            if ascii_name not in current.lower():
                continue
            results.append(_replace_ascii_greek_words(current))
            results.append(
                _ASCII_GREEK_RE.sub(
                    lambda match: kana_name
                    if match.group(1).lower() == ascii_name
                    else match.group(0),
                    current,
                )
            )
    return results


def _variant_curated_aliases(value: str) -> list[str]:
    results = [value]
    canonical_key = normalize_synonym_text(value)
    for seed_key, aliases in CURATED_ALIAS_MAP.items():
        seed_keys = {normalize_synonym_text(seed_key)}
        seed_keys.update(normalize_synonym_text(alias) for alias in aliases)
        if canonical_key not in seed_keys:
            continue
        results.append(seed_key)
        results.extend(aliases)
    return results


def _remove_alias_parentheticals(value: str) -> str:
    result = value
    while True:
        updated = re.sub(
            r"[（(][^()（）]*?(?:別名|別称|旧称|alias|aka|also known as)[^()（）]*?[）)]",
            "",
            result,
            flags=re.IGNORECASE,
        )
        if updated == result:
            return updated.strip()
        result = updated


def _clean_outer_candidate(value: str) -> str | None:
    text = unicodedata.normalize("NFKC", value or "").strip()
    text = _PUNCT_TRIM_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    if not text:
        return None
    if _contains_alias_marker(text):
        return None
    return text


def _clean_alias_candidate(value: str) -> str | None:
    text = unicodedata.normalize("NFKC", value or "").strip()
    text = _ALIAS_MARKER_RE.sub("", text)
    text = text.replace("（", "(").replace("）", ")")
    text = _strip_outer_parens(text)
    if "(" in text and not text.startswith("("):
        text = text.split("(", 1)[0].strip()
    text = _PUNCT_TRIM_RE.sub("", text)
    text = _WHITESPACE_RE.sub(" ", text).strip()
    if not text:
        return None
    if _contains_alias_marker(text):
        return None
    return text


def _strip_outer_parens(value: str) -> str:
    result = value.strip()
    while len(result) >= 2 and (
        (result[0] == "(" and result[-1] == ")")
        or (result[0] == "（" and result[-1] == "）")
    ):
        result = result[1:-1].strip()
    return result


def _contains_alias_marker(value: str) -> bool:
    lowered = unicodedata.normalize("NFKC", value or "").lower()
    return any(
        marker in lowered
        for marker in ("別名", "別称", "旧称", "alias", "aka", "also known as")
    )


def _replace_greek_symbols(value: str) -> str:
    result = value
    for symbol, variants in GREEK_VARIANTS.items():
        result = result.replace(symbol, variants[0])
    return result


def _replace_ascii_greek_words(value: str) -> str:
    def repl(match: re.Match[str]) -> str:
        word = match.group(1).lower()
        for symbol, variants in GREEK_VARIANTS.items():
            if word == variants[0]:
                return symbol
        return word

    return _ASCII_GREEK_RE.sub(repl, value)


def _dedupe_preserve_order(values: Iterable[str]) -> list[str]:
    seen: set[str] = set()
    result: list[str] = []
    for value in values:
        normalized = normalize_synonym_text(value)
        if not normalized or normalized in seen:
            continue
        seen.add(normalized)
        result.append(value)
    return result
