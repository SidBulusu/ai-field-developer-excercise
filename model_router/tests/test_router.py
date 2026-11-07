"""Check routing decisions with controlled transports, clocks and worker barriers.

Observe provider calls and eventual ledger state across timeout/cancellation races;
a returned HTTP error alone cannot prove that no work or commit occurred.
"""

import asyncio
import hashlib
import json
import sqlite3
import threading
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor

import httpx
import pytest
from fastapi.testclient import TestClient

from model_router.app import Message, count_prompt_tokens, create_app


BODY = {"messages": [{"role": "user", "content": "x"}], "max_completion_tokens": 100}
API_KEY = "test-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
OTHER_KEY = "test-key-bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"


@pytest.fixture(autouse=True)
def configured_keys(monkeypatch):
    monkeypatch.setenv("ROUTER_API_KEYS", API_KEY + "," + OTHER_KEY)


HEADERS = {"X-API-Key": "test-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"}


def assert_error(response, status, code):
    assert response.status_code == status
    payload = response.json()
    assert set(payload) == {"error"}
    assert set(payload["error"]) == {"code", "message", "request_id"}
    assert payload["error"]["code"] == code
    assert payload["error"]["request_id"].startswith("req-")
    assert isinstance(payload["error"]["message"], str)
    assert "private-canary" not in response.text
    assert API_KEY not in response.text


def test_api_key_required_instead_of_caller_chosen_tenant(tmp_path):
    with router(tmp_path) as (http, calls, _):
        response = http.post("/v1/chat/completions", json=BODY, headers={"X-Tenant-ID": "admin"})
        assert_error(response, 401, "INVALID_API_KEY")
        assert calls == []


def test_revised_request_contract_and_single_choice(tmp_path):
    with router(tmp_path) as (http, calls, _):
        response = http.post("/v1/chat/completions", json={**BODY, "n": 1, "stream": False}, headers=HEADERS)
        assert response.status_code == 200
        assert len(calls) == 1
        for extra in ({"n": 2}, {"n": True}, {"max_tokens": 10}):
            assert_error(http.post("/v1/chat/completions", json={**BODY, **extra}, headers=HEADERS),
                         400, "INVALID_REQUEST")
        assert len(calls) == 1


def reply(request, text="Mock text"):
    prompt = sum(len(message["content"].encode()) for message in json.loads(request.content)["messages"])
    return {"choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                         "finish_reason": "stop"}],
            "usage": {"prompt_tokens": prompt, "completion_tokens": len(text.encode()),
                      "total_tokens": prompt + len(text.encode())}}


@contextmanager
def router(tmp_path, handler=None):
    calls = []
    async def forward(request):
        calls.append(request)
        if handler:
            return await handler(request)
        return httpx.Response(200, json=reply(request))
    client = httpx.AsyncClient(transport=httpx.MockTransport(forward))
    path = tmp_path / "usage.db"
    try:
        with TestClient(create_app(db_path=path, client=client)) as http:
            yield http, calls, path
    finally:
        asyncio.run(client.aclose())


def charges(path):
    with sqlite3.connect(path) as connection:
        return connection.execute("SELECT charged_tokens, state FROM reservations ORDER BY rowid").fetchall()


def test_primary_success_reconciles_and_uses_fixed_model(tmp_path):
    with router(tmp_path) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200
        assert response.json()["route"] == "primary"
        assert response.json()["model"] == "mock-primary"
        assert response.json()["usage"]["total_tokens"] == 10
        assert charges(path) == [(10, "settled")]
        assert json.loads(calls[0].content)["model"] == "mock-primary"


@pytest.mark.parametrize("failure", ["429", "timeout"])
def test_expected_primary_failures_fallback_once(tmp_path, failure):
    async def handler(request):
        if "8021" in str(request.url):
            if failure == "timeout":
                raise httpx.ReadTimeout("private-canary", request=request)
            return httpx.Response(429, text="private-canary")
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, handler) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200
        assert response.json()["route"] == "backup"
        assert len(calls) == 2
        assert charges(path) == ([(10, "settled")] if failure == "429" else [(101, "uncertain")])


def test_fallback_reuses_logical_reservation(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_TOKEN_LIMIT", "150")
    async def provider(request):
        if "8021" in str(request.url):
            raise httpx.ReadTimeout("private-canary", request=request)
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, provider) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200
        assert len(calls) == 2
        assert charges(path) == [(101, "uncertain")]


@pytest.mark.parametrize("bad", [{"max_completion_tokens": True}, {"max_completion_tokens": "100"},
    {"max_completion_tokens": 0}, {"max_completion_tokens": -1}, {"stream": True}, {"model": "arbitrary"}, {"tenant": "admin"},
    {"messages": [{"role": "user", "content": []}]}, {"messages": []}])
def test_invalid_requests_never_reach_provider(tmp_path, bad):
    with router(tmp_path) as (http, calls, _):
        response = http.post("/v1/chat/completions", json={**BODY, **bad}, headers=HEADERS)
        assert response.status_code == 400
        assert_error(response, 400, "INVALID_REQUEST")
        assert calls == []


@pytest.mark.parametrize("tenant", [None, "", "a b", "é", "a" * 129, "unknown-" + "x" * 32])
def test_invalid_tenant(tmp_path, tenant):
    with router(tmp_path) as (http, calls, _):
        headers = {} if tenant is None else {"X-API-Key": tenant.encode("utf-8")}
        response = http.post("/v1/chat/completions", json=BODY, headers=headers)
        assert_error(response, 401, "INVALID_API_KEY")
        assert calls == []


@pytest.mark.parametrize("kind", ["500", "connect", "bad_json", "bad_usage", "overspent", "surrogate"])
def test_ambiguous_provider_failures_keep_reservation_and_do_not_fallback(tmp_path, kind):
    async def handler(request):
        if kind == "500":
            return httpx.Response(500, text="private-canary")
        if kind == "connect":
            raise httpx.ConnectError("private-canary", request=request)
        if kind == "bad_json":
            return httpx.Response(200, content=b"private-canary", headers={"content-type": "application/json"})
        payload = reply(request)
        if kind == "bad_usage":
            payload["usage"]["total_tokens"] = 0
        elif kind == "overspent":
            payload = reply(request, "x" * 101)
        elif kind == "surrogate":
            payload["choices"][0]["message"]["content"] = "\ud800"
            return httpx.Response(200, content=json.dumps(payload).encode(), headers={"content-type": "application/json"})
        return httpx.Response(200, json=payload)
    with router(tmp_path, handler) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 502
        assert_error(response, 502, "UPSTREAM_UNAVAILABLE" if kind in {"500", "connect"} else "UPSTREAM_INVALID_RESPONSE")
        assert len(calls) == 1
        assert charges(path) == [(101, "pending")]


def test_both_providers_429_release_logical_reservation(tmp_path):
    async def handler(request):
        return httpx.Response(429, text="private-canary")
    with router(tmp_path, handler) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 502
        assert_error(response, 502, "UPSTREAM_UNAVAILABLE")
        assert len(calls) == 2
        assert charges(path) == [(0, "settled")]


def test_no_transaction_while_provider_waits_and_pending_requests_count(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_TOKEN_LIMIT", "101")
    async def check():
        entered, release = asyncio.Event(), asyncio.Event()
        async def provider(request):
            entered.set()
            await release.wait()
            return httpx.Response(200, json=reply(request))
        path = tmp_path / "usage.db"
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
            app = create_app(db_path=path, client=client)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as http:
                    first = asyncio.create_task(http.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    try:
                        await asyncio.wait_for(entered.wait(), 2)
                        with sqlite3.connect(path, timeout=0.1) as connection:
                            connection.execute("BEGIN IMMEDIATE")
                            connection.rollback()
                        denied = await http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
                        assert denied.status_code == 429
                    finally:
                        release.set()
                        assert (await first).status_code == 200
    asyncio.run(check())


def test_database_lock_fails_closed_without_provider_call(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_DB_TIMEOUT_SECONDS", "0.01")
    with router(tmp_path) as (http, calls, path):
        with sqlite3.connect(path) as connection:
            connection.execute("BEGIN IMMEDIATE")
            response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
            connection.rollback()
        assert response.status_code == 503
        assert_error(response, 503, "GATEWAY_UNAVAILABLE")
        assert calls == []


@pytest.mark.parametrize("kind", ["redirect", "compression", "content_type", "oversized"])
def test_unsupported_responses_are_not_relayed(tmp_path, monkeypatch, kind):
    monkeypatch.setenv("ROUTER_MAX_RESPONSE_BYTES", "256")
    async def provider(request):
        if kind == "redirect":
            return httpx.Response(307, headers={"Location": "http://private.invalid/secret"})
        if kind == "compression":
            return httpx.Response(200, headers={"content-type": "application/json", "content-encoding": "gzip"})
        if kind == "content_type":
            return httpx.Response(200, text="private-canary")
        return httpx.Response(200, content=b"x" * 257, headers={"content-type": "application/json"})
    with router(tmp_path, provider) as (http, calls, _):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 502
        assert_error(response, 502, "UPSTREAM_UNAVAILABLE")
        assert len(calls) == 1


@pytest.mark.parametrize("body", [b'{"max_completion_tokens":1,"max_completion_tokens":2}', b"[" * 2000,
    b'{"messages":NaN}', b"\xff", b'{"messages":[{"role":"user","content":"\\ud800"}],"max_completion_tokens":1}'])
def test_invalid_json_never_reaches_provider(tmp_path, body):
    with router(tmp_path) as (http, calls, _):
        assert http.post("/v1/chat/completions", content=body, headers=HEADERS).status_code == 400
        assert calls == []


def test_request_limits_and_duplicate_tenant_headers(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_MAX_REQUEST_BYTES", "32")
    with router(tmp_path) as (http, calls, _):
        assert http.post("/v1/chat/completions", json=BODY, headers=HEADERS).status_code == 413
        assert http.post("/v1/chat/completions", json=BODY,
            headers=[("X-API-Key", API_KEY), ("X-API-Key", OTHER_KEY)]).status_code == 401
        assert calls == []


def test_both_timeouts_are_sanitized_and_remain_charged(tmp_path):
    async def fail(request):
        raise httpx.ReadTimeout("private-canary", request=request)
    with router(tmp_path, fail) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 504
        assert_error(response, 504, "UPSTREAM_UNAVAILABLE")
        assert len(calls) == 2
        assert charges(path) == [(101, "uncertain")]


def test_inflight_cancellation_keeps_reservation(tmp_path):
    async def check():
        entered = asyncio.Event()
        async def provider(request):
            entered.set()
            await asyncio.Event().wait()
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as client:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=client)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as http:
                    request = asyncio.create_task(http.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    await asyncio.wait_for(entered.wait(), 2)
                    request.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await request
                assert charges(path) == [(101, "pending")]
    asyncio.run(check())


def test_caller_headers_and_provider_cookies_are_not_shared(tmp_path):
    async def provider(request):
        assert not {"authorization", "cookie", "x-api-key"} & set(request.headers)
        return httpx.Response(200, json=reply(request), headers={"Set-Cookie": "session=private"})
    with router(tmp_path, provider) as (http, calls, _):
        for _ in range(2):
            response = http.post("/v1/chat/completions", json=BODY,
                headers={**HEADERS, "Authorization": "Bearer private", "Cookie": "session=caller"})
            assert response.status_code == 200
            assert "set-cookie" not in response.headers
        assert len(calls) == 2


def test_mock_token_count_includes_documented_zero_framing_overhead():
    assert count_prompt_tokens([Message(role="user", content="é😀"), Message(role="system", content="x")]) == 7


def test_missing_usage_retains_charge_and_tenants_are_isolated(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_TOKEN_LIMIT", "101")
    async def provider(request):
        result = reply(request)
        result.pop("usage")
        return httpx.Response(200, json=result)
    with router(tmp_path, provider) as (http, calls, path):
        assert http.post("/v1/chat/completions", json=BODY, headers=HEADERS).status_code == 200
        assert_error(http.post("/v1/chat/completions", json=BODY, headers=HEADERS), 429, "RATE_LIMIT_EXCEEDED")
        assert http.post("/v1/chat/completions", json=BODY, headers={"X-API-Key": OTHER_KEY}).status_code == 200
        assert len(calls) == 2
        assert charges(path) == [(101, "uncertain"), (101, "uncertain")]
        with sqlite3.connect(path) as db:
            digests = {row[0] for row in db.execute("SELECT tenant_key_digest FROM reservations")}
        assert digests == {hashlib.sha256(key.encode()).hexdigest() for key in (API_KEY, OTHER_KEY)}
        assert API_KEY.encode() not in path.read_bytes()


def test_reported_nonvisible_tokens_are_charged(tmp_path):
    async def provider(request):
        result = reply(request)
        result["usage"]["completion_tokens"] += 3
        result["usage"]["total_tokens"] += 3
        return httpx.Response(200, json=result)
    with router(tmp_path, provider) as (http, _, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200
        assert response.json()["usage"]["total_tokens"] == 13
        assert charges(path) == [(13, "settled")]


@pytest.mark.parametrize("bad_usage", [True, {"prompt_tokens": True, "completion_tokens": 9, "total_tokens": 10},
    {"prompt_tokens": 1, "completion_tokens": -1, "total_tokens": 0},
    {"prompt_tokens": 1, "completion_tokens": 101, "total_tokens": 102}])
def test_invalid_usage_keeps_full_reservation(tmp_path, bad_usage):
    async def provider(request):
        result = reply(request)
        result["usage"] = bad_usage
        return httpx.Response(200, json=result)
    with router(tmp_path, provider) as (http, calls, path):
        assert_error(http.post("/v1/chat/completions", json=BODY, headers=HEADERS), 502, "UPSTREAM_INVALID_RESPONSE")
        assert len(calls) == 1
        assert charges(path) == [(101, "pending")]


@pytest.mark.parametrize("delay,route", [(0.025, "primary"), (0.075, "backup")])
def test_primary_deadline_selects_exactly_one_result(tmp_path, monkeypatch, delay, route):
    monkeypatch.setenv("ROUTER_PRIMARY_TIMEOUT_SECONDS", "0.05")
    cancelled = []
    async def provider(request):
        if "8021" in str(request.url):
            try:
                await asyncio.sleep(delay)
            except asyncio.CancelledError:
                cancelled.append(True)
                raise
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, provider) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200 and response.json()["route"] == route
        assert len(calls) == (1 if route == "primary" else 2)
        assert bool(cancelled) == (route == "backup")
        assert charges(path) == ([(10, "settled")] if route == "primary" else [(101, "uncertain")])


def test_trickling_body_is_bounded_by_whole_attempt_deadline(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_PRIMARY_TIMEOUT_SECONDS", "0.05")
    class Trickle(httpx.AsyncByteStream):
        closed = False
        reads = 0
        async def __aiter__(self):
            while True:
                self.reads += 1
                yield b" "
                await asyncio.sleep(0.005)
        async def aclose(self):
            self.closed = True
    stream = Trickle()
    async def provider(request):
        if "8021" in str(request.url):
            return httpx.Response(200, headers={"content-type": "application/json"}, stream=stream)
        assert stream.closed
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, provider) as (http, calls, path):
        started = time.monotonic()
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200 and response.json()["route"] == "backup"
        assert 0.04 <= time.monotonic() - started < 0.5
        assert stream.closed and stream.reads >= 2
        assert len(calls) == 2 and charges(path) == [(101, "uncertain")]


def test_two_gateway_instances_atomically_admit_one_30k_request(tmp_path):
    async def check():
        release = asyncio.Event()
        calls = []
        async def provider(request):
            calls.append(request)
            await release.wait()
            return httpx.Response(200, json=reply(request))
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            path = tmp_path / "usage.db"
            first_app = create_app(db_path=path, client=upstream)
            second_app = create_app(db_path=path, client=upstream)
            async with first_app.router.lifespan_context(first_app), second_app.router.lifespan_context(second_app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(first_app), base_url="http://first") as first:
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(second_app), base_url="http://second") as second:
                        body = {**BODY, "max_completion_tokens": 29999}
                        tasks = [asyncio.create_task(client.post("/v1/chat/completions", json=body, headers=HEADERS))
                                 for client in (first, second)]
                        try:
                            done, _ = await asyncio.wait(tasks, timeout=2, return_when=asyncio.FIRST_COMPLETED)
                            assert len(done) == 1
                            assert_error(next(iter(done)).result(), 429, "RATE_LIMIT_EXCEEDED")
                            assert len(calls) == 1
                            assert charges(path) == [(30000, "pending")]
                        finally:
                            release.set()
                            results = await asyncio.gather(*tasks)
                        assert sorted(result.status_code for result in results) == [200, 429]
    asyncio.run(check())


def test_timeout_then_backup_rejection_does_not_release_uncertain_charge(tmp_path):
    async def provider(request):
        if "8021" in str(request.url):
            raise httpx.ReadTimeout("private-canary")
        return httpx.Response(429)
    with router(tmp_path, provider) as (http, calls, path):
        assert_error(http.post("/v1/chat/completions", json=BODY, headers=HEADERS), 502, "UPSTREAM_UNAVAILABLE")
        assert len(calls) == 2 and charges(path) == [(101, "uncertain")]


def test_busy_database_does_not_block_other_async_requests(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_DB_TIMEOUT_SECONDS", "0.1")
    async def check():
        async with httpx.AsyncClient() as upstream:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    with sqlite3.connect(path) as db:
                        db.execute("BEGIN IMMEDIATE")
                        task = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                        await asyncio.sleep(0.01)
                        assert not task.done(), "SQLite operation blocked the event loop"
                        assert_error(await client.post("/v1/chat/completions", json=BODY), 401, "INVALID_API_KEY")
                        assert_error(await task, 503, "GATEWAY_UNAVAILABLE")
                        db.rollback()
    asyncio.run(check())


@pytest.mark.parametrize("keys", ["", "short", "a" * 32 + "," + "a" * 32])
def test_invalid_key_configuration_fails_closed(tmp_path, monkeypatch, keys):
    monkeypatch.setenv("ROUTER_API_KEYS", keys)
    with pytest.raises(ValueError, match="ROUTER_API_KEYS"):
        create_app(db_path=tmp_path / "usage.db")


def test_exact_50k_reservation_allowed_and_50001_rejected_before_dispatch(tmp_path):
    async def provider(request):
        result = reply(request)
        result.pop("usage")
        return httpx.Response(200, json=result)
    with router(tmp_path, provider) as (http, calls, path):
        assert_error(http.post("/v1/chat/completions", json={**BODY, "max_completion_tokens": 50000}, headers=HEADERS),
                     429, "RATE_LIMIT_EXCEEDED")
        assert calls == [] and charges(path) == []
        assert http.post("/v1/chat/completions", json={**BODY, "max_completion_tokens": 49999}, headers=HEADERS).status_code == 200
        assert charges(path) == [(50000, "uncertain")]


def test_cleanup_is_bounded_when_transport_close_stalls(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_CLEANUP_TIMEOUT_SECONDS", "0.02")
    class StalledClose(httpx.AsyncByteStream):
        stopped = False
        async def __aiter__(self):
            yield b""
        async def aclose(self):
            try:
                await asyncio.Event().wait()
            finally:
                self.stopped = True
    stream = StalledClose()
    async def provider(request):
        if "8021" in str(request.url):
            return httpx.Response(429, stream=stream)
        assert stream.stopped
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, provider) as (http, calls, _):
        started = time.monotonic()
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200 and len(calls) == 2
        assert stream.stopped and time.monotonic() - started < 0.5


def test_settlement_database_failure_returns_safe_error(tmp_path, monkeypatch):
    from model_router.limiter import TokenLimiter
    def fail(*args):
        raise sqlite3.OperationalError("private-canary raw SQL")
    monkeypatch.setattr(TokenLimiter, "settle", fail)
    with router(tmp_path) as (http, calls, path):
        assert_error(http.post("/v1/chat/completions", json=BODY, headers=HEADERS), 503, "GATEWAY_UNAVAILABLE")
        assert len(calls) == 1 and charges(path) == [(101, "pending")]


def test_request_upload_timeout_is_sanitized(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_REQUEST_TIMEOUT_SECONDS", "0.02")
    async def check():
        async def body():
            yield b"{"
            await asyncio.sleep(0.1)
            yield b"}"
        async with httpx.AsyncClient() as upstream:
            app = create_app(db_path=tmp_path / "usage.db", client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    assert_error(await client.post("/v1/chat/completions", content=body(), headers=HEADERS), 408, "INVALID_REQUEST")
    asyncio.run(check())


def test_provider_deadlines_must_fit_in_reservation_window(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_WINDOW_MS", "1000")
    with pytest.raises(ValueError, match="fit safely"):
        create_app(db_path=tmp_path / "usage.db")


def test_primary_rejection_still_falls_back_if_transport_close_fails(tmp_path):
    class BrokenClose(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b""
        async def aclose(self):
            raise OSError("private-canary close failure")
    async def provider(request):
        if "8021" in str(request.url):
            return httpx.Response(429, stream=BrokenClose())
        return httpx.Response(200, json=reply(request))
    with router(tmp_path, provider) as (http, calls, _):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200 and len(calls) == 2


def test_cancellation_closes_an_open_provider_response(tmp_path):
    async def check():
        entered = asyncio.Event()
        class OpenBody(httpx.AsyncByteStream):
            closed = False
            async def __aiter__(self):
                entered.set()
                await asyncio.Event().wait()
                yield b""
            async def aclose(self):
                self.closed = True
        stream = OpenBody()
        async def provider(_):
            return httpx.Response(200, stream=stream, headers={"content-type": "application/json"})
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    task = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    await asyncio.wait_for(entered.wait(), 2)
                    task.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await task
                    assert stream.closed and charges(path) == [(101, "pending")]
            assert not upstream.is_closed  # The injected client's owner controls its pool.
    asyncio.run(check())


@pytest.mark.parametrize("failure", ["500", "bad_json", "connect", "timeout", "429"])
def test_backup_failures_do_not_retry_or_leak_details(tmp_path, failure):
    async def provider(request):
        if "8021" in str(request.url) or failure == "429":
            return httpx.Response(429, text="private-canary")
        if failure == "connect":
            raise httpx.ConnectError("private-canary")
        if failure == "timeout":
            raise httpx.ReadTimeout("private-canary")
        if failure == "bad_json":
            return httpx.Response(200, content=b"private-canary", headers={"content-type": "application/json"})
        return httpx.Response(500, text="private-canary")
    with router(tmp_path, provider) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert_error(response, 504 if failure == "timeout" else 502,
                     "UPSTREAM_INVALID_RESPONSE" if failure == "bad_json" else "UPSTREAM_UNAVAILABLE")
        assert len(calls) == 2
        expected = (0, "settled") if failure == "429" else (101, "uncertain" if failure == "timeout" else "pending")
        assert charges(path) == [expected]


def test_validation_crossing_deadline_cannot_return_late_primary_success(tmp_path, monkeypatch):
    from model_router.app import ProviderReply
    monkeypatch.setenv("ROUTER_PRIMARY_TIMEOUT_SECONDS", "0.02")
    original = ProviderReply.model_validate
    validations = []
    def validate(cls, value):
        validations.append(True)
        if len(validations) == 1:
            # Model synchronous validation crossing the deadline without an
            # await where asyncio could deliver its scheduled cancellation.
            time.sleep(0.04)
        return original(value)
    monkeypatch.setattr(ProviderReply, "model_validate", classmethod(validate))
    with router(tmp_path) as (http, calls, path):
        response = http.post("/v1/chat/completions", json=BODY, headers=HEADERS)
        assert response.status_code == 200 and response.json()["route"] == "backup"
        assert len(calls) == 2 and charges(path) == [(101, "uncertain")]


@pytest.mark.parametrize("operation", ["reserve", "settle"])
@pytest.mark.parametrize("termination", ["cancel", "timeout"])
def test_database_worker_can_finish_after_request_ends(tmp_path, monkeypatch, operation, termination):
    from model_router.limiter import TokenLimiter
    monkeypatch.setenv("ROUTER_DB_OPERATION_TIMEOUT_SECONDS", "0.05")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = getattr(TokenLimiter, operation)

    def blocked(self, *args):
        # Admission has committed before cancellation; settlement has trusted
        # usage but has not committed yet. Neither running thread can be killed.
        if operation == "reserve":
            result = original(self, *args)
        entered.set()
        try:
            assert release.wait(3), "Worker was not released"
            return original(self, *args) if operation == "settle" else result
        finally:
            finished.set()

    monkeypatch.setattr(TokenLimiter, operation, blocked)

    async def check():
        calls = []
        async def provider(request):
            calls.append(request)
            return httpx.Response(200, json=reply(request))
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    task = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    try:
                        assert await asyncio.to_thread(entered.wait, 2)
                        if termination == "cancel":
                            task.cancel()
                            with pytest.raises(asyncio.CancelledError):
                                await task
                        else:
                            # A separate wait bound makes the old unbounded
                            # implementation fail without hanging the test.
                            assert_error(await asyncio.wait_for(task, 0.3), 503, "GATEWAY_UNAVAILABLE")
                        assert charges(path) == [(101, "pending")]
                    finally:
                        release.set()
                        assert await asyncio.to_thread(finished.wait, 2)
                        await asyncio.gather(task, return_exceptions=True)
                    assert len(calls) == (0 if operation == "reserve" else 1)
                    assert charges(path) == ([(101, "pending")] if operation == "reserve" else [(10, "settled")])
    asyncio.run(check())


def test_database_queue_is_finite_and_running_cancel_keeps_its_slot(tmp_path, monkeypatch):
    from model_router.limiter import TokenLimiter
    monkeypatch.setenv("ROUTER_DB_MAX_PENDING", "1")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = TokenLimiter.reserve
    count = []
    def blocked(self, *args):
        count.append(True)
        result = original(self, *args)
        entered.set()
        try:
            assert release.wait(3)
            return result
        finally:
            finished.set()
    monkeypatch.setattr(TokenLimiter, "reserve", blocked)

    async def check():
        calls = []
        async def provider(request):
            calls.append(request)
            return httpx.Response(200, json=reply(request))
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            app = create_app(db_path=tmp_path / "usage.db", client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    first = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    try:
                        assert await asyncio.to_thread(entered.wait, 2)
                        assert_error(await asyncio.wait_for(client.post("/v1/chat/completions", json=BODY, headers=HEADERS), 0.3),
                                     503, "GATEWAY_UNAVAILABLE")
                        first.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await first
                        assert_error(await asyncio.wait_for(client.post("/v1/chat/completions", json=BODY, headers=HEADERS), 0.3),
                                     503, "GATEWAY_UNAVAILABLE")
                        assert len(count) == 1 and calls == []
                    finally:
                        release.set()
                        assert await asyncio.to_thread(finished.wait, 2)
                        await asyncio.gather(first, return_exceptions=True)
                    # Let the worker-completion callback return its slot.
                    await asyncio.sleep(0.01)
                    assert (await client.post("/v1/chat/completions", json=BODY, headers=HEADERS)).status_code == 200
                    assert len(calls) == 1
    asyncio.run(check())


def test_expired_queued_database_admission_never_runs(tmp_path, monkeypatch):
    from model_router.limiter import TokenLimiter
    monkeypatch.setenv("ROUTER_DB_MAX_PENDING", "2")
    monkeypatch.setenv("ROUTER_DB_OPERATION_TIMEOUT_SECONDS", "0.05")
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = TokenLimiter.reserve
    executed = []
    def blocked(self, *args):
        executed.append(args[-1])
        result = original(self, *args)
        entered.set()
        try:
            assert release.wait(3)
            return result
        finally:
            finished.set()
    monkeypatch.setattr(TokenLimiter, "reserve", blocked)

    async def check():
        calls = []
        async def provider(request):
            calls.append(request)
            return httpx.Response(200, json=reply(request))
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    first = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    try:
                        assert await asyncio.to_thread(entered.wait, 2)
                        queued = await asyncio.wait_for(client.post("/v1/chat/completions", json=BODY, headers=HEADERS), 0.3)
                        assert_error(queued, 503, "GATEWAY_UNAVAILABLE")
                        assert_error(await first, 503, "GATEWAY_UNAVAILABLE")
                    finally:
                        release.set()
                        assert await asyncio.to_thread(finished.wait, 2)
                        await asyncio.gather(first, return_exceptions=True)
                    await asyncio.sleep(0.01)
                    assert len(executed) == 1 and calls == []
                    assert queued.json()["error"]["request_id"] not in executed
                    assert charges(path) == [(101, "pending")]
    asyncio.run(check())


def test_router_database_is_not_starved_by_default_executor(tmp_path):
    async def check():
        release, entered = threading.Event(), threading.Event()
        def occupy():
            entered.set()
            assert release.wait(3)
        async def provider(request):
            return httpx.Response(200, json=reply(request))
        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            app = create_app(db_path=tmp_path / "usage.db", client=upstream)
            async with app.router.lifespan_context(app):
                loop = asyncio.get_running_loop()
                loop.set_default_executor(ThreadPoolExecutor(max_workers=1))
                occupied = loop.run_in_executor(None, occupy)
                try:
                    while not entered.is_set():
                        await asyncio.sleep(0)
                    async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                        response = await asyncio.wait_for(client.post("/v1/chat/completions", json=BODY, headers=HEADERS), 0.3)
                        assert response.status_code == 200
                finally:
                    release.set()
                    await occupied
    asyncio.run(check())


def test_concurrent_deadlines_and_cancellation_have_one_outcome_each(tmp_path, monkeypatch):
    monkeypatch.setenv("ROUTER_PRIMARY_TIMEOUT_SECONDS", "0.1")
    monkeypatch.setenv("ROUTER_DB_MAX_PENDING", "32")
    async def check():
        labels = [f"{kind}-{i}" for kind, count in (("fast", 8), ("slow", 8), ("cancel", 4)) for i in range(count)]
        entered = {label: asyncio.Event() for label in labels if label.startswith("cancel")}
        calls = {label: [] for label in labels}
        closed = []

        class ReplyBody(httpx.AsyncByteStream):
            def __init__(self, request, label, route):
                self.wire = json.dumps(reply(request)).encode()
                self.label, self.route = label, route
            async def __aiter__(self):
                if self.route == "primary":
                    if self.label in entered:
                        entered[self.label].set()
                    await asyncio.sleep(0.005 if self.label.startswith("fast") else 0.2)
                yield self.wire
            async def aclose(self):
                closed.append((self.label, self.route))

        async def provider(request):
            label = json.loads(request.content)["messages"][0]["content"]
            route = "primary" if "8021" in str(request.url) else "backup"
            calls[label].append(route)
            return httpx.Response(200, headers={"content-type": "application/json"}, stream=ReplyBody(request, label, route))

        async with httpx.AsyncClient(transport=httpx.MockTransport(provider)) as upstream:
            path = tmp_path / "usage.db"
            app = create_app(db_path=path, client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    tasks = {label: asyncio.create_task(client.post("/v1/chat/completions", headers=HEADERS,
                             json={**BODY, "messages": [{"role": "user", "content": label}]})) for label in labels}
                    try:
                        await asyncio.wait_for(asyncio.gather(*(event.wait() for event in entered.values())), 2)
                        for label in entered:
                            tasks[label].cancel()
                        results = await asyncio.wait_for(asyncio.gather(*tasks.values(), return_exceptions=True), 3)
                        for label, result in zip(labels, results):
                            expected = ["primary"] if not label.startswith("slow") else ["primary", "backup"]
                            assert calls[label] == expected
                            if label in entered:
                                assert isinstance(result, asyncio.CancelledError)
                            else:
                                assert result.status_code == 200
                                assert result.json()["route"] == ("primary" if label.startswith("fast") else "backup")
                            assert all(closed.count((label, route)) == 1 for route in expected)
                        with sqlite3.connect(path) as db:
                            counts = dict(db.execute("SELECT state, count(*) FROM reservations GROUP BY state"))
                            assert db.execute("SELECT count(DISTINCT request_id) FROM reservations").fetchone()[0] == 20
                        assert counts == {"settled": 8, "uncertain": 8, "pending": 4}
                    finally:
                        for task in tasks.values():
                            if not task.done():
                                task.cancel()
                        await asyncio.gather(*tasks.values(), return_exceptions=True)
    asyncio.run(check())


def test_abandoned_queue_entries_keep_capacity_until_drained(tmp_path, monkeypatch):
    from model_router import app as router_module
    from model_router.limiter import TokenLimiter
    monkeypatch.setenv("ROUTER_DB_MAX_PENDING", "2")
    monkeypatch.setenv("ROUTER_DB_OPERATION_TIMEOUT_SECONDS", "0.05")
    submitted = []
    class CountingExecutor(ThreadPoolExecutor):
        def submit(self, *args, **kwargs):
            submitted.append(True)
            return super().submit(*args, **kwargs)
    monkeypatch.setattr(router_module, "ThreadPoolExecutor", CountingExecutor)
    entered, release, finished = threading.Event(), threading.Event(), threading.Event()
    original = TokenLimiter.reserve
    def blocked(self, *args):
        value = original(self, *args)
        entered.set()
        try:
            assert release.wait(3)
            return value
        finally:
            finished.set()
    monkeypatch.setattr(TokenLimiter, "reserve", blocked)

    async def check():
        async with httpx.AsyncClient() as upstream:
            app = create_app(db_path=tmp_path / "usage.db", client=upstream)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app), base_url="http://router") as client:
                    first = asyncio.create_task(client.post("/v1/chat/completions", json=BODY, headers=HEADERS))
                    try:
                        assert await asyncio.to_thread(entered.wait, 2)
                        assert_error(await client.post("/v1/chat/completions", json=BODY, headers=HEADERS),
                                     503, "GATEWAY_UNAVAILABLE")
                        assert_error(await first, 503, "GATEWAY_UNAVAILABLE")
                        await asyncio.sleep(0.01)  # Allow cancelled-future callbacks to run.
                        # One initialization plus two admissions were submitted.
                        # Cancelling an executor Future leaves its WorkItem in
                        # the physical queue; do not admit replacement tombstones.
                        assert len(submitted) == 3
                        for _ in range(10):
                            assert_error(await client.post("/v1/chat/completions", json=BODY, headers=HEADERS),
                                         503, "GATEWAY_UNAVAILABLE")
                            assert len(submitted) == 3
                    finally:
                        release.set()
                        assert await asyncio.to_thread(finished.wait, 2)
                        await asyncio.gather(first, return_exceptions=True)
    asyncio.run(check())
