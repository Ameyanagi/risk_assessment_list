from __future__ import annotations

import argparse
import hashlib
import json
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError
from urllib.request import Request, urlopen

from .config import MANIFEST_PATH
from .config import ManifestEntry
from .config import REFERENCE_DIR
from .config import SOURCES
from .config import USER_AGENT


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    refresh_keys = set(args.refresh_key)
    REFERENCE_DIR.mkdir(parents=True, exist_ok=True)
    previous_manifest = load_previous_manifest()
    manifest = []
    refresh_failures: list[str] = []

    for source in SOURCES:
        previous_entry = previous_manifest.get(source["key"])
        refresh = args.refresh or source["key"] in refresh_keys
        try:
            entry = fetch_source(source, previous_entry, refresh=refresh)
        except Exception:
            if refresh and is_valid_cached_file(
                REFERENCE_DIR / source["filename"], previous_entry
            ):
                assert previous_entry is not None
                entry = build_cached_manifest_entry(
                    source=source,
                    previous_entry=previous_entry,
                    cache_status="refresh_failed",
                )
                refresh_failures.append(source["key"])
            else:
                raise
        manifest.append(entry)
        print(f"{source['key']}: {entry['cache_status']}")

    MANIFEST_PATH.write_text(
        json.dumps({"sources": manifest}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    if refresh_failures:
        raise SystemExit(
            f"Refresh failed for cached sources: {', '.join(sorted(refresh_failures))}"
        )


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Download source files into reference/ with an offline-first cache."
    )
    parser.add_argument(
        "--refresh",
        action="store_true",
        help="Revalidate all cached files with conditional HTTP requests.",
    )
    parser.add_argument(
        "--refresh-key",
        action="append",
        choices=[source["key"] for source in SOURCES],
        default=[],
        help="Revalidate only the specified source key. Can be repeated.",
    )
    return parser.parse_args(argv)


def load_previous_manifest() -> dict[str, ManifestEntry]:
    if not MANIFEST_PATH.exists():
        return {}
    payload = json.loads(MANIFEST_PATH.read_text(encoding="utf-8"))
    return {entry["key"]: entry for entry in payload.get("sources", [])}


def fetch_source(
    source: dict[str, str],
    previous_entry: ManifestEntry | None,
    *,
    refresh: bool,
) -> ManifestEntry:
    destination = REFERENCE_DIR / source["filename"]
    cached_valid = is_valid_cached_file(destination, previous_entry)

    if cached_valid and not refresh:
        assert previous_entry is not None
        return build_cached_manifest_entry(
            source=source,
            previous_entry=previous_entry,
            cache_status="cache_hit",
        )

    headers = {"User-Agent": USER_AGENT}
    if refresh and cached_valid:
        if previous_entry and previous_entry.get("etag"):
            headers["If-None-Match"] = previous_entry["etag"]
        if previous_entry and previous_entry.get("last_modified"):
            headers["If-Modified-Since"] = previous_entry["last_modified"]

    request = Request(source["url"], headers=headers)
    try:
        with urlopen(request) as response:
            data = response.read()
            destination.write_bytes(data)
            return build_downloaded_manifest_entry(
                source=source,
                data=data,
                content_type=response.headers.get("Content-Type", ""),
                etag=response.headers.get("ETag", ""),
                last_modified=response.headers.get("Last-Modified", ""),
                cache_status="refreshed" if refresh else "downloaded",
            )
    except HTTPError as exc:
        if exc.code != 304 or not cached_valid:
            raise
        assert previous_entry is not None
        return build_cached_manifest_entry(
            source=source,
            previous_entry=previous_entry,
            cache_status="not_modified",
        )


def is_valid_cached_file(
    destination: Path,
    previous_entry: ManifestEntry | None,
) -> bool:
    if not previous_entry or not destination.exists():
        return False
    expected_sha = previous_entry.get("sha256", "")
    if not expected_sha:
        return False
    return file_sha256(destination) == expected_sha


def build_cached_manifest_entry(
    *,
    source: dict[str, str],
    previous_entry: ManifestEntry,
    cache_status: str,
) -> ManifestEntry:
    timestamp = utc_now()
    fetched_at = (
        previous_entry.get("fetched_at") or previous_entry.get("cached_at") or timestamp
    )
    cached_at = previous_entry.get("cached_at") or fetched_at
    return {
        "key": source["key"],
        "url": source["url"],
        "filename": source["filename"],
        "local_path": str(Path("reference") / source["filename"]),
        "fetched_at": fetched_at,
        "cached_at": cached_at,
        "last_checked_at": timestamp,
        "last_modified": previous_entry.get("last_modified", ""),
        "etag": previous_entry.get("etag", ""),
        "content_type": previous_entry.get("content_type", ""),
        "sha256": str(previous_entry["sha256"]),
        "cache_status": cache_status,
        "not_modified": cache_status == "not_modified",
    }


def build_downloaded_manifest_entry(
    *,
    source: dict[str, str],
    data: bytes,
    content_type: str,
    etag: str,
    last_modified: str,
    cache_status: str,
) -> ManifestEntry:
    timestamp = utc_now()
    return {
        "key": source["key"],
        "url": source["url"],
        "filename": source["filename"],
        "local_path": str(Path("reference") / source["filename"]),
        "fetched_at": timestamp,
        "cached_at": timestamp,
        "last_checked_at": timestamp,
        "last_modified": last_modified,
        "etag": etag,
        "content_type": content_type,
        "sha256": hashlib.sha256(data).hexdigest(),
        "cache_status": cache_status,
        "not_modified": False,
    }


def file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(65536), b""):
            digest.update(chunk)
    return digest.hexdigest()


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()
