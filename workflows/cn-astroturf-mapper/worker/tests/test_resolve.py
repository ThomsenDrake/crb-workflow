"""Unit tests for the pure handle-resolution helpers in cn_astroturf_mapper."""
import asyncio
import os

from worker.workflows.cn_astroturf_mapper import AstroturfMapperInput, _extract_status_ids, _resolve_handle_grok_cli


def test_extract_status_ids_parses_dedupes_and_filters_by_handle():
    text = (
        "https://x.com/HasanTheHun/status/123 some text\n"
        "https://x.com/someoneelse/status/999\n"
        "https://x.com/hasanthehun/status/123\n"
        "https://twitter.com/hasanthehun/statuses/456\n"
    )
    assert _extract_status_ids(text, "hasanthehun") == ["123", "456"]


def test_extract_status_ids_without_handle_keeps_all_authors():
    text = "https://x.com/a/status/1 https://x.com/b/status/2"
    assert _extract_status_ids(text) == ["1", "2"]


def test_extract_status_ids_empty_when_no_urls():
    assert _extract_status_ids("NONE") == []
    assert _extract_status_ids("") == []


def test_grok_cli_missing_binary_degrades_to_warning():
    os.environ["GROK_BIN"] = "/nonexistent/grok-xyz"
    try:
        entry = asyncio.run(
            _resolve_handle_grok_cli("hasanthehun", "2026-08-18", "2026-11-17", AstroturfMapperInput(target_handles=["hasanthehun"]))
        )
    finally:
        del os.environ["GROK_BIN"]
    assert entry["source"] == "grok_cli"
    assert entry["tweet_ids"] == []
    assert "not found" in entry["warning"]
