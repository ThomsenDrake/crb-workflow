import asyncio

import worker.workflows.line_shopper as m
from worker.workflows.line_shopper import (
    LLM_CALL_TIMEOUT_MS,
    LLM_MAX_ATTEMPTS,
    _is_retryable_llm_error,
    _llm_call_with_retry,
    get_chat_client,
)
import inspect


class FakeSDKError(Exception):
    def __init__(self, status_code=None, retry_after=None):
        super().__init__("boom")
        self.status_code = status_code
        if retry_after is not None:
            class Raw:
                headers = {"retry-after": str(retry_after)}
            self.raw_response = Raw()


def test_retryable_statuses():
    assert _is_retryable_llm_error(FakeSDKError(520))  # Cloudflare 520 seen in the wild
    assert _is_retryable_llm_error(FakeSDKError(500))
    assert _is_retryable_llm_error(FakeSDKError(429))
    assert not _is_retryable_llm_error(FakeSDKError(400))
    assert not _is_retryable_llm_error(FakeSDKError(None))
    assert not _is_retryable_llm_error(ValueError("x"))


def test_llm_call_retries_then_succeeds(monkeypatch):
    monkeypatch.setattr(m, "LLM_RETRY_BASE_DELAY_S", 0.001)
    monkeypatch.setattr(m, "LLM_RETRY_MAX_DELAY_S", 0.001)
    calls = {"n": 0}

    async def call():
        calls["n"] += 1
        if calls["n"] < 3:
            raise FakeSDKError(520)
        return "ok"

    assert asyncio.run(_llm_call_with_retry(call)) == "ok"
    assert calls["n"] == 3


def test_llm_call_gives_up_after_max_attempts(monkeypatch):
    monkeypatch.setattr(m, "LLM_RETRY_BASE_DELAY_S", 0.001)
    monkeypatch.setattr(m, "LLM_RETRY_MAX_DELAY_S", 0.001)
    calls = {"n": 0}

    async def call():
        calls["n"] += 1
        raise FakeSDKError(520)

    try:
        asyncio.run(_llm_call_with_retry(call))
        assert False, "expected raise"
    except FakeSDKError:
        pass
    assert calls["n"] == LLM_MAX_ATTEMPTS


def test_llm_call_does_not_retry_4xx(monkeypatch):
    monkeypatch.setattr(m, "LLM_RETRY_BASE_DELAY_S", 0.001)
    calls = {"n": 0}

    async def call():
        calls["n"] += 1
        raise FakeSDKError(400)

    try:
        asyncio.run(_llm_call_with_retry(call))
        assert False, "expected raise"
    except FakeSDKError:
        pass
    assert calls["n"] == 1


def test_get_chat_client_wires_timeout_and_no_sdk_import_at_module_level():
    assert LLM_CALL_TIMEOUT_MS == 9 * 60 * 1000
    assert "timeout_ms=LLM_CALL_TIMEOUT_MS" in inspect.getsource(get_chat_client)
    # module-level imports of mistralai.client drag httpx2/urllib into the workflow
    # sandbox and fail validation, so keep them out of the module top level
    mod_src = inspect.getsource(m)
    top = mod_src.split("DISCLAIMER")[0]
    assert "mistralai.client" not in top
