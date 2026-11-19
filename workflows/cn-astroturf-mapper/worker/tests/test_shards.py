"""Unit tests for _shard_urls probe classification and retry behavior (mocked httpx client)."""
import asyncio
import re
from collections import defaultdict
from datetime import datetime, timedelta, timezone

import httpx
import pytest

import worker.workflows.cn_astroturf_mapper as mod
from worker.workflows.cn_astroturf_mapper import _shard_urls

TODAY = datetime.now(timezone.utc).date().strftime("%Y/%m/%d")
YESTERDAY = (datetime.now(timezone.utc).date() - timedelta(days=1)).strftime("%Y/%m/%d")
BASE = "https://cn.test/birdwatch-public-data"


class FakeResponse:
    def __init__(self, status_code):
        self.status_code = status_code


class Router:
    """Maps URL regex -> ordered outcomes (int status or Exception); last outcome repeats."""

    def __init__(self, default=404):
        self.routes = []
        self.default = default

    def add(self, pattern, outcomes):
        self.routes.append((re.compile(pattern), list(outcomes)))
        return self

    def __call__(self, method, url, headers):
        for pattern, outcomes in self.routes:
            if pattern.search(url):
                if len(outcomes) > 1:
                    return outcomes.pop(0)
                return outcomes[0]
        return self.default


class FakeClient:
    def __init__(self, handler):
        self.handler = handler
        self.calls = []  # (method, url, headers)

    async def head(self, url, follow_redirects=False, **kwargs):
        return self._handle("HEAD", url, None)

    async def get(self, url, headers=None, follow_redirects=False, **kwargs):
        return self._handle("GET", url, headers)

    def _handle(self, method, url, headers):
        self.calls.append((method, url, headers))
        out = self.handler(method, url, headers)
        if isinstance(out, Exception):
            raise out
        return FakeResponse(out)


@pytest.fixture(autouse=True)
def fast_caps(monkeypatch):
    """Small caps + fake CN base + no real sleeping; returns the list of backoff attempts."""
    monkeypatch.setattr(mod, "CN_DATA_BASE", BASE)
    monkeypatch.setattr(mod, "CN_MAX_SHARDS", 10)
    monkeypatch.setattr(mod, "CN_LOOKBACK_DAYS", 3)
    delays = []

    async def fake_backoff(attempt):
        delays.append(attempt)

    monkeypatch.setattr(mod, "_backoff_sleep", fake_backoff)
    return delays


def test_429_then_200_retries_then_succeeds(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [429, 429, 200])
    router.add(r"-00001\.zip$", [404])
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert urls == [f"{BASE}/{TODAY}/notes/notes-00000.zip"]
    assert diag["snapshot_date"] == TODAY
    assert diag["shard_count"] == 1
    assert diag["shard_urls"] == urls
    assert diag["complete"] is True
    assert len(fast_caps) == 2  # backoff ran before each of the two retries


def test_5xx_and_network_errors_are_retried(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [httpx.ConnectError("boom"), 503, 200])
    router.add(r"-00001\.zip$", [404])
    client = FakeClient(router)
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"]))
    assert len(urls) == 1
    assert len(fast_caps) == 2


def test_persistent_429_returns_empty_only_after_all_candidates_exhausted(fast_caps):
    router = Router(default=429)  # everything 429s forever
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["ratings"], diag))
    assert urls == []
    assert diag["shard_count"] == 0
    assert diag["shard_urls"] == []
    assert diag["snapshot_date"] is None
    assert diag["complete"] is False
    assert mod.CN_PROBE_MAX_ATTEMPTS >= 5
    per_url = defaultdict(int)
    for _method, url, _headers in client.calls:
        per_url[url] += 1
    # 2 layouts x (3 dated dirs + 1 legacy dir) shard-00000 probes + 2 legacy single files
    assert len(per_url) == 2 * (3 + 1) + 2
    assert all(n == mod.CN_PROBE_MAX_ATTEMPTS for n in per_url.values())


def test_404_ends_enumeration_without_probing_further(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [200])
    router.add(r"-00001\.zip$", [404])
    router.add(r"-00002\.zip$", [200])  # must never be requested
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert len(urls) == 1
    assert diag["complete"] is True
    assert not any(u.endswith("-00002.zip") for _m, u, _h in client.calls)


def test_405_head_falls_back_to_ranged_get(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [405, 206])  # HEAD 405, then ranged GET 206
    router.add(r"-00001\.zip$", [404])
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert len(urls) == 1
    gets = [(u, h) for m, u, h in client.calls if m == "GET"]
    assert len(gets) == 1
    assert gets[0][1] == {"Range": "bytes=0-0"}
    assert diag["complete"] is True


def test_405_with_ranged_get_200_also_counts_as_ok(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [405, 200])
    router.add(r"-00001\.zip$", [404])
    client = FakeClient(router)
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"]))
    assert len(urls) == 1


def test_contiguous_shards_00000_to_0000N_then_404_returns_exactly_N_plus_1_urls(fast_caps):
    router = Router()
    for i in range(5):  # shards 00000..00004 exist (N=4)
        router.add(rf"-{i:05d}\.zip$", [200])
    router.add(r"-00005\.zip$", [404])
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert len(urls) == 5  # N+1
    assert [u.rsplit("-", 1)[1] for u in urls] == [f"{i:05d}.zip" for i in range(5)]
    assert diag["shard_count"] == 5
    assert diag["shard_urls"] == urls
    assert diag["complete"] is True


def test_midlist_transient_error_keeps_partial_flagged_incomplete(fast_caps):
    router = Router()
    router.add(r"-00000\.zip$", [200])
    router.add(r"-00001\.zip$", [200])
    router.add(r"-00002\.zip$", [429])  # persistent 429 mid-list
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert len(urls) == 2
    assert diag["shard_count"] == 2
    assert diag["complete"] is False
    # the failing shard was retried the full attempt budget before giving up
    n = sum(1 for _m, u, _h in client.calls if u.endswith("-00002.zip"))
    assert n == mod.CN_PROBE_MAX_ATTEMPTS


def test_walks_back_to_older_date_when_todays_snapshot_404s(fast_caps):
    router = Router()
    router.add(re.escape(TODAY) + r".*-00000\.zip$", [404])
    router.add(re.escape(YESTERDAY) + r".*-00000\.zip$", [200])
    router.add(re.escape(YESTERDAY) + r".*-00001\.zip$", [404])
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert len(urls) == 1
    assert urls[0].startswith(f"{BASE}/{YESTERDAY}/")
    assert diag["snapshot_date"] == YESTERDAY


def test_legacy_unsharded_single_file_fallback(fast_caps):
    router = Router()  # all shard probes 404 by default
    router.add(r"/notes\.zip$", [200])
    client = FakeClient(router)
    diag = {}
    urls = asyncio.run(_shard_urls(client, mod.CN_TABLES["notes"], diag))
    assert urls == [f"{BASE}/notes/notes.zip"]
    assert diag["snapshot_date"] == "legacy"
    assert diag["shard_count"] == 1
    assert diag["complete"] is True


class FakeStream:
    def __init__(self, chunks=(), exc=None, status=200):
        self._chunks = list(chunks)
        self._exc = exc
        self.status_code = status

    async def __aenter__(self):
        return self

    async def __aexit__(self, *args):
        return False

    def raise_for_status(self):
        if self.status_code >= 400:
            raise httpx.HTTPStatusError("http error", request=None, response=self)

    async def aiter_bytes(self, chunk_size=1 << 20):
        for c in self._chunks:
            yield c
        if self._exc is not None:
            raise self._exc


def test_download_shard_retries_dropped_connection(fast_caps, tmp_path):
    streams = [
        FakeStream(chunks=[b"abc"], exc=httpx.RemoteProtocolError("peer closed connection")),
        FakeStream(chunks=[b"abc", b"def"]),
    ]
    calls = []

    class StreamClient:
        def stream(self, method, url, **kwargs):
            calls.append(url)
            return streams.pop(0)

    out = tmp_path / "shard.zip"
    asyncio.run(mod._download_shard(StreamClient(), "https://cn.test/x.zip", str(out)))
    assert out.read_bytes() == b"abcdef"  # restarted download, no partial data
    assert len(calls) == 2
    assert len(fast_caps) == 1


def test_download_shard_gives_up_after_all_attempts(fast_caps, tmp_path):
    class StreamClient:
        def stream(self, method, url, **kwargs):
            return FakeStream(exc=httpx.ConnectError("down"))

    with pytest.raises(httpx.HTTPError):
        asyncio.run(mod._download_shard(StreamClient(), "https://cn.test/x.zip", str(tmp_path / "s.zip")))
    assert len(fast_caps) == mod.CN_PROBE_MAX_ATTEMPTS - 1
