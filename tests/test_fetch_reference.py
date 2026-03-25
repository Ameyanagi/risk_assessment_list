from __future__ import annotations

import hashlib
import importlib
import json
from email.message import Message
from urllib.error import HTTPError, URLError

ManifestEntry = dict[str, str | bool]


class FakeResponse:
    def __init__(self, data: bytes, headers: dict[str, str]) -> None:
        self._data = data
        self.headers = headers

    def read(self) -> bytes:
        return self._data

    def __enter__(self) -> FakeResponse:
        return self

    def __exit__(self, exc_type, exc, tb) -> bool:
        return False


def load_fetch_reference_module():
    return importlib.import_module("risk_assessment_list._build.fetch")


def make_previous_entry(source: dict[str, str], data: bytes) -> ManifestEntry:
    sha256 = hashlib.sha256(data).hexdigest()
    timestamp = "2026-03-25T05:06:50+00:00"
    return {
        "key": source["key"],
        "url": source["url"],
        "filename": source["filename"],
        "local_path": f"reference/{source['filename']}",
        "fetched_at": timestamp,
        "cached_at": timestamp,
        "last_checked_at": timestamp,
        "last_modified": "Wed, 04 Mar 2026 01:57:06 GMT",
        "etag": '"example-etag"',
        "content_type": "application/octet-stream",
        "sha256": sha256,
        "cache_status": "downloaded",
        "not_modified": False,
    }


def test_fetch_source_uses_valid_cache_without_network(tmp_path, monkeypatch) -> None:
    module = load_fetch_reference_module()
    source = {
        "key": "example",
        "url": "https://example.com/example.xlsx",
        "filename": "example.xlsx",
    }
    cached_data = b"cached-bytes"
    destination = tmp_path / source["filename"]
    destination.write_bytes(cached_data)
    previous_entry = make_previous_entry(source, cached_data)

    monkeypatch.setattr(module, "REFERENCE_DIR", tmp_path)

    def fail_urlopen(request):
        raise AssertionError("network should not be called for a cache hit")

    monkeypatch.setattr(module, "urlopen", fail_urlopen)

    entry = module.fetch_source(source, previous_entry, refresh=False)

    assert entry["cache_status"] == "cache_hit"
    assert entry["sha256"] == previous_entry["sha256"]
    assert entry["local_path"] == f"reference/{source['filename']}"
    assert destination.read_bytes() == cached_data


def test_fetch_source_redownloads_when_cached_hash_is_invalid(
    tmp_path, monkeypatch
) -> None:
    module = load_fetch_reference_module()
    source = {
        "key": "example",
        "url": "https://example.com/example.xlsx",
        "filename": "example.xlsx",
    }
    destination = tmp_path / source["filename"]
    destination.write_bytes(b"corrupted")
    previous_entry = make_previous_entry(source, b"expected-old-data")
    seen_headers: dict[str, str] = {}

    monkeypatch.setattr(module, "REFERENCE_DIR", tmp_path)

    def fake_urlopen(request):
        seen_headers.update(
            {key.lower(): value for key, value in request.header_items()}
        )
        return FakeResponse(
            b"fresh-data",
            {
                "Content-Type": "application/octet-stream",
                "ETag": '"fresh-etag"',
                "Last-Modified": "Thu, 05 Mar 2026 01:57:06 GMT",
            },
        )

    monkeypatch.setattr(module, "urlopen", fake_urlopen)

    entry = module.fetch_source(source, previous_entry, refresh=False)

    assert entry["cache_status"] == "downloaded"
    assert destination.read_bytes() == b"fresh-data"
    assert entry["sha256"] == hashlib.sha256(b"fresh-data").hexdigest()
    assert "if-none-match" not in seen_headers
    assert "if-modified-since" not in seen_headers


def test_fetch_source_refresh_revalidates_with_http_validators(
    tmp_path, monkeypatch
) -> None:
    module = load_fetch_reference_module()
    source = {
        "key": "example",
        "url": "https://example.com/example.xlsx",
        "filename": "example.xlsx",
    }
    cached_data = b"cached-data"
    destination = tmp_path / source["filename"]
    destination.write_bytes(cached_data)
    previous_entry = make_previous_entry(source, cached_data)
    seen_headers: dict[str, str] = {}

    monkeypatch.setattr(module, "REFERENCE_DIR", tmp_path)

    def fake_urlopen(request):
        seen_headers.update(
            {key.lower(): value for key, value in request.header_items()}
        )
        raise HTTPError(source["url"], 304, "Not Modified", hdrs=Message(), fp=None)

    monkeypatch.setattr(module, "urlopen", fake_urlopen)

    entry = module.fetch_source(source, previous_entry, refresh=True)

    assert entry["cache_status"] == "not_modified"
    assert seen_headers["if-none-match"] == previous_entry["etag"]
    assert seen_headers["if-modified-since"] == previous_entry["last_modified"]
    assert destination.read_bytes() == cached_data


def test_main_keeps_cached_files_when_refresh_fails(tmp_path, monkeypatch) -> None:
    module = load_fetch_reference_module()
    source = {
        "key": "example",
        "url": "https://example.com/example.xlsx",
        "filename": "example.xlsx",
    }
    cached_data = b"cached-data"
    destination = tmp_path / source["filename"]
    destination.write_bytes(cached_data)
    previous_entry = make_previous_entry(source, cached_data)
    manifest_path = tmp_path / "manifest.json"
    manifest_path.write_text(
        json.dumps({"sources": [previous_entry]}, ensure_ascii=False, indent=2) + "\n",
        encoding="utf-8",
    )

    monkeypatch.setattr(module, "REFERENCE_DIR", tmp_path)
    monkeypatch.setattr(module, "MANIFEST_PATH", manifest_path)
    monkeypatch.setattr(module, "SOURCES", [source])

    def failing_urlopen(request):
        raise URLError("network down")

    monkeypatch.setattr(module, "urlopen", failing_urlopen)

    try:
        module.main(["--refresh"])
    except SystemExit as exc:
        assert str(exc) == "Refresh failed for cached sources: example"
    else:
        raise AssertionError("expected refresh failure to exit non-zero")

    written_manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
    assert written_manifest["sources"][0]["cache_status"] == "refresh_failed"
    assert destination.read_bytes() == cached_data
