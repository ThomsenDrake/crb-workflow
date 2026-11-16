"""Unit tests for the pure handle-resolution helpers in cn_astroturf_mapper."""
from worker.workflows.cn_astroturf_mapper import _extract_status_ids


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
