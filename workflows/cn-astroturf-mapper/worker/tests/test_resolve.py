"""Unit tests for the pure handle-resolution helpers in cn_astroturf_mapper."""
import asyncio
import os

from worker.workflows.cn_astroturf_mapper import (
    AstroturfMapperInput,
    _extract_status_ids,
    _parse_device_login_output,
    _resolve_handle_grok_cli,
)


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


def test_parse_device_login_output_url_with_embedded_code():
    text = 'To sign in, visit https://accounts.x.ai/oauth2/device?user_code=ABCD-1234 and enter code ABCD-1234\npolling...'
    out = _parse_device_login_output(text)
    assert out["verification_url"] == "https://accounts.x.ai/oauth2/device?user_code=ABCD-1234"
    assert out["user_code"] == "ABCD-1234"


def test_parse_device_login_output_bare_code():
    out = _parse_device_login_output("Open https://x.ai/device and enter code WXYZ-9876 to continue")
    assert out["verification_url"] == "https://x.ai/device"
    assert out["user_code"] == "WXYZ-9876"


def test_parse_device_login_output_empty():
    assert _parse_device_login_output("") == {}
    assert _parse_device_login_output("login failed") == {}


def test_handle_source_defaults_to_auto():
    assert AstroturfMapperInput(target_handles=["x"]).handle_source == "auto"
