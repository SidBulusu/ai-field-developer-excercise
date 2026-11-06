"""Exercise policy through HTTP middleware, with a spy at the downstream boundary."""

import asyncio
import gzip
import json
import time
from contextlib import contextmanager
from decimal import Decimal

import httpx
import jwt
import pytest
from fastapi.testclient import TestClient

from security_gateway.app import create_app
from security_gateway.mock_downstream import TOOLS, app as mock_app


SECRET = "test-only-signing-key-with-at-least-48-bytes-for-algorithm-tests"


def token(role="viewer", **claims):
    return jwt.encode({"role": role, "exp": time.time() + 300, **claims}, SECRET, algorithm="HS256")


def auth(role="viewer"):
    return {"Authorization": f"Bearer {token(role)}"}


def rpc(method="tools/list", identifier=42, **fields):
    return {"jsonrpc": "2.0", "id": identifier, "method": method, **fields}


@contextmanager
def gateway(handler=None):
    calls = []
    downstream = httpx.ASGITransport(app=mock_app)

    async def forward(request):
        # A returned denial alone cannot prove prevention of side effects. Count
        # at the transport boundary, before either the mock app or handler runs.
        calls.append(request)
        if handler is not None:
            return handler(request)
        return await downstream.handle_async_request(request)

    client = httpx.AsyncClient(transport=httpx.MockTransport(forward))
    try:
        with TestClient(create_app(secret=SECRET, client=client)) as http:
            yield http, calls
    finally:
        asyncio.run(client.aclose())


@pytest.mark.parametrize("role", ["admin", "viewer"])
def test_list_is_unfiltered(role):
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc(), headers=auth(role))
        assert response.status_code == 200
        assert response.json() == {"jsonrpc": "2.0", "id": 42, "result": {"tools": TOOLS}}
        assert len(calls) == 1


@pytest.mark.parametrize("role,name", [
    ("viewer", "get_status"), ("admin", "get_status"), ("admin", "admin_reset_key"),
])
def test_allowed_tools(role, name):
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc("tools/call", params={"name": name}), headers=auth(role))
        assert response.status_code == 200
        assert response.json()["result"]["isError"] is False
        assert "Mock" in response.json()["result"]["content"][0]["text"]
        assert len(calls) == 1


@pytest.mark.parametrize("name", ["admin_reset_key", "admin_anything", "admin_"])
@pytest.mark.parametrize("identifier", [0, "request-id", 42, None, 1.5])
def test_denials_never_reach_downstream(name, identifier):
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc("tools/call", identifier, params={"name": name}), headers=auth())
        assert response.status_code == 200
        assert response.json() == {
            "jsonrpc": "2.0", "id": identifier,
            "error": {"code": -32001, "message": "Unauthorized Tool Call"},
        }
        assert calls == []


@pytest.mark.parametrize("identifier", [0, "request-id", None, 1.5])
def test_ids_preserved_on_allowed_calls(identifier):
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc(identifier=identifier), headers=auth())
        assert response.json()["id"] == identifier
        assert len(calls) == 1


@pytest.mark.parametrize("params", [None, [], {}, {"name": None}, {"name": 1}, {"name": True},
    {"name": []}, {"name": "get_status", "arguments": []}])
def test_bad_tool_params(params):
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc("tools/call", 0, params=params), headers=auth())
        assert response.json()["id"] == 0
        assert response.json()["error"]["code"] == -32602
        assert calls == []


@pytest.mark.parametrize("body,code", [
    (b"{broken", -32700), (b"\xff", -32700),
    (b'{"jsonrpc":"2.0","method":"tools/list","id":NaN}', -32700),
    (b'{"jsonrpc":"2.0","method":"tools/list","id":1e999}', -32700),
    (b'{"jsonrpc":"2.0","method":"tools/list","method":"tools/call"}', -32700),
    (b"[]", -32600),
    (b'[{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"admin_reset_key"}}]', -32600),
    (b"null", -32600), (b"1", -32600), (b"{}", -32600),
    (b'{"jsonrpc":"1.0","id":1,"method":"tools/list"}', -32600),
    (b'{"jsonrpc":"2.0","id":true,"method":"tools/list"}', -32600),
    (b'{"jsonrpc":"2.0","id":[],"method":"tools/list"}', -32600),
    (b'{"jsonrpc":"2.0","method":3}', -32600),
    (b'{"jsonrpc":"2.0","method":"tools/list","extra":1}', -32600),
    (b'{"jsonrpc":"2.0","method":"other","params":null}', -32600),
])
def test_malformed_requests(body, code):
    with gateway() as (http, calls):
        response = http.post("/mcp", content=body, headers=auth())
        assert response.json()["id"] is None
        assert response.json()["error"]["code"] == code
        assert calls == []


@pytest.mark.parametrize("credential", [
    None, "", "Basic abc", "Bearer", "Bearer junk", "Bearer a b",
    f"Bearer {token(exp=time.time() - 60)}",
    f"Bearer {token(role='owner')}", f"Bearer {token(role=['admin'])}",
    f"Bearer {jwt.encode({'role': 'admin'}, SECRET, algorithm='HS256')}",
    f"Bearer {jwt.encode({'exp': time.time() + 300}, SECRET, algorithm='HS256')}",
    f"Bearer {token(exp='9999999999')}", f"Bearer {token(exp=float('inf'))}",
    f"Bearer {token(exp=True)}",
    f"Bearer {jwt.encode({'role': 'admin', 'exp': time.time() + 300}, 'different-test-secret-at-least-32-bytes', algorithm='HS256')}",
    f"Bearer {jwt.encode({'role': 'admin', 'exp': time.time() + 300}, SECRET, algorithm='HS384')}",
    f"Bearer {jwt.encode({'role': 'admin', 'exp': time.time() + 300}, '', algorithm='none')}",
    f"Bearer {token(nbf=time.time() + 300)}",
    f"Bearer {token(aud='unconfigured-audience')}",
], ids=lambda _: "credential")
def test_authentication_precedes_body_parsing_and_forwarding(credential):
    with gateway() as (http, calls):
        headers = {} if credential is None else {"Authorization": credential}
        response = http.post("/mcp", content=b"{broken", headers=headers)
        assert response.status_code == 401
        assert response.headers["www-authenticate"] == "Bearer"
        assert response.json() == {"detail": "Invalid or missing Bearer token"}
        assert calls == []


def test_duplicate_credentials_rejected_and_scheme_case_insensitive():
    with gateway() as (http, calls):
        response = http.post("/mcp", json=rpc(), headers=[
            ("Authorization", f"Bearer {token()}"), ("Authorization", f"Bearer {token('admin')}"),
        ])
        assert response.status_code == 401
        assert calls == []
        assert http.post("/mcp", json=rpc(), headers={"Authorization": f"bEaReR {token()}"}).status_code == 200
        assert len(calls) == 1


def test_exact_body_headers_status_and_error_relay():
    response_body = b'{ "jsonrpc": "2.0", "id": "abc", "error": {"code": -32602, "message": "mock failure"} }'

    def downstream(request):
        return httpx.Response(409, content=response_body, headers={
            "Content-Type": "application/json; charset=utf-8", "Mcp-Session-Id": "mock-session",
            "Retry-After": "3", "Connection": "close", "X-Private": "omit",
        })

    body = b'{ "jsonrpc":"2.0", "id":"abc", "method":"tools/call", "params":{"name":"get_status","arguments":{"nested":[1,{"value":"unchanged"}]}} }'
    with gateway(downstream) as (http, calls):
        response = http.post("/mcp", content=body, headers={
            **auth(), "Content-Type": "application/json", "Mcp-Protocol-Version": "2025-11-25",
            "Mcp-Session-Id": "request-session", "Connection": "close", "X-Private": "omit",
        })
        assert len(calls) == 1
        forwarded = calls[0]
        assert forwarded.content == body
        assert forwarded.headers["content-type"] == "application/json"
        assert forwarded.headers["mcp-protocol-version"] == "2025-11-25"
        assert forwarded.headers["mcp-session-id"] == "request-session"
        assert "authorization" not in forwarded.headers
        assert "x-private" not in forwarded.headers
        assert forwarded.headers.get("connection") != "close"
        assert response.status_code == 409
        assert response.content == response_body
        assert response.headers["content-type"] == "application/json; charset=utf-8"
        assert response.headers["mcp-session-id"] == "mock-session"
        assert response.headers["retry-after"] == "3"
        assert "connection" not in response.headers
        assert "x-private" not in response.headers


def test_compressed_response_and_connection_nominated_headers():
    body = b'{"mock":true}'

    def downstream(request):
        assert "mcp-session-id" not in request.headers
        return httpx.Response(200, content=gzip.compress(body), headers={
            "content-type": "application/json", "content-encoding": "gzip",
            "connection": "mcp-session-id", "mcp-session-id": "omit",
        })

    with gateway(downstream) as (http, calls):
        response = http.post("/mcp", json=rpc(), headers={
            **auth(), "connection": "mcp-session-id", "mcp-session-id": "omit",
        })
        assert response.content == body
        assert "content-encoding" not in response.headers
        assert "mcp-session-id" not in response.headers
        assert int(response.headers["content-length"]) == len(body)


@pytest.mark.parametrize("error,status,message", [
    (httpx.ConnectError, 502, "Downstream unavailable"),
    (httpx.ReadTimeout, 504, "Downstream timeout"),
    (httpx.ConnectTimeout, 504, "Downstream timeout"),
    (httpx.RemoteProtocolError, 502, "Downstream unavailable"),
    (httpx.DecodingError, 502, "Downstream unavailable"),
])
def test_downstream_failures_are_sanitized(error, status, message):
    def downstream(request):
        raise error("secret-canary https://private.internal", request=request)

    with gateway(downstream) as (http, calls):
        response = http.post("/mcp", json=rpc(identifier=0), headers=auth())
        assert response.status_code == status
        assert response.json() == {"jsonrpc": "2.0", "id": 0, "error": {"code": -32000, "message": message}}
        assert "secret-canary" not in response.text
        assert len(calls) == 1


def test_unknown_methods_and_tools_relay_downstream_errors():
    with gateway() as (http, calls):
        for payload, code in [(rpc("unknown", params=[1]), -32601),
                              (rpc("tools/call", params={"name": "unknown"}), -32602)]:
            response = http.post("/mcp", json=payload, headers=auth())
            assert response.json()["error"]["code"] == code
        assert len(calls) == 2


def test_redirect_is_relayed_without_following_or_forwarding_credentials():
    def downstream(request):
        return httpx.Response(307, content=b"mock redirect", headers={"Location": "http://elsewhere.invalid/mcp"})

    with gateway(downstream) as (http, calls):
        response = http.post("/mcp", json=rpc(), headers=auth(), follow_redirects=False)
        assert response.status_code == 307
        assert response.content == b"mock redirect"
        assert response.headers["location"] == "http://elsewhere.invalid/mcp"
        assert len(calls) == 1


@pytest.mark.parametrize("name,expected_calls", [("get_status", 1), ("admin_reset_key", 0)])
def test_notifications_have_no_json_rpc_response(name, expected_calls):
    with gateway() as (http, calls):
        response = http.post("/mcp", json={
            "jsonrpc": "2.0", "method": "tools/call", "params": {"name": name},
        }, headers=auth())
        assert response.status_code == 204
        assert response.content == b""
        assert len(calls) == expected_calls


def test_owned_client_closes_and_environment_configuration(monkeypatch):
    original_client = httpx.AsyncClient
    clients = []

    def downstream(request):
        assert str(request.url) == "http://mock.local/mcp"
        assert request.extensions["timeout"] == {
            "connect": 2, "read": 2, "write": 2, "pool": 2,
        }
        return httpx.Response(200, json={"mock": True})

    def make_client(**kwargs):
        assert kwargs == {"trust_env": False, "follow_redirects": False}
        instance = original_client(transport=httpx.MockTransport(downstream))
        clients.append(instance)
        return instance

    monkeypatch.setenv("GATEWAY_JWT_SECRET", SECRET)
    monkeypatch.setenv("GATEWAY_DOWNSTREAM_URL", "http://mock.local/mcp")
    monkeypatch.setenv("GATEWAY_TIMEOUT_SECONDS", "2")
    monkeypatch.setattr(httpx, "AsyncClient", make_client)
    with TestClient(create_app()) as http:
        for _ in range(2):
            assert http.post("/mcp", json=rpc(), headers=auth()).json() == {"mock": True}
        assert len(clients) == 1
        assert not clients[0].is_closed
    assert clients[0].is_closed


def test_missing_secret_fails_closed(monkeypatch):
    monkeypatch.delenv("GATEWAY_JWT_SECRET", raising=False)
    with pytest.raises(ValueError, match="GATEWAY_JWT_SECRET"):
        create_app()


@pytest.mark.parametrize("timeout", [0, -1, float("inf"), float("nan")])
def test_invalid_timeout_fails_at_startup(timeout):
    with pytest.raises(ValueError, match="GATEWAY_TIMEOUT_SECONDS"):
        create_app(secret=SECRET, timeout=timeout)


def test_mock_independent_id_zero_and_deterministic_tools():
    with TestClient(mock_app) as http:
        assert http.post("/mcp", json=rpc(identifier=0)).json() == {
            "jsonrpc": "2.0", "id": 0, "result": {"tools": TOOLS},
        }
        for name in ("get_status", "admin_reset_key"):
            payload = rpc("tools/call", 0, params={"name": name})
            first = http.post("/mcp", json=payload)
            assert first.json()["id"] == 0
            assert first.json()["result"]["isError"] is False
            assert http.post("/mcp", json=payload).content == first.content
        assert http.post("/mcp", content=b"{broken").json()["error"]["code"] == -32700


@pytest.mark.parametrize("body", [
    b'{"jsonrpc":"2.0","id":"\\ud800","method":"tools/call","params":{"name":"admin_reset_key"}}',
    b'{"jsonrpc":"2.0","id":0,"method":"tools/call","params":{"name":"get_status","arguments":{"text":"\\udfff"}}}',
    b'{"jsonrpc":"2.0","id":0,"method":"tools/call","params":{"name":"get_status","arguments":{"\\ud800":1}}}',
])
def test_unpaired_unicode_surrogates_are_rejected_without_crashing(body):
    with gateway() as (http, calls):
        response = http.post("/mcp", content=body, headers=auth())
        assert response.status_code == 200
        assert response.json() == {
            "jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"},
        }
        assert calls == []


def test_downstream_cookies_are_not_shared_between_callers():
    def downstream(request):
        return httpx.Response(200, json={"mock": True}, headers={"Set-Cookie": "session=first-caller; Path=/"})

    with gateway(downstream) as (http, calls):
        for role in ("viewer", "admin", "viewer"):
            response = http.post("/mcp", json=rpc(), headers={**auth(role), "Cookie": "client=private"})
            assert response.status_code == 200
            assert "set-cookie" not in response.headers
        assert len(calls) == 3
        assert all("cookie" not in request.headers for request in calls)


def test_valid_unicode_and_escaped_admin_prefix():
    with gateway() as (http, calls):
        # Escape spelling changes the wire bytes, not the decoded identifier
        # on which authorization must operate. A valid surrogate pair is Unicode.
        response = http.post("/mcp", content=(
            b'{"jsonrpc":"2.0","id":"\\ud83d\\ude00",'
            b'"method":"tools/call","params":{"name":"\\u0061dmin_reset_key"}}'
        ), headers=auth())
        assert response.json()["id"] == "😀"
        assert response.json()["error"]["code"] == -32001
        assert calls == []
        response = http.post("/mcp", json=rpc(identifier="café 😀"), headers=auth())
        assert response.json()["id"] == "café 😀"
        assert len(calls) == 1


@pytest.mark.parametrize("identifier", ["1.0000000000000001", "9007199254740993.5", "-0.000000000000000001"])
@pytest.mark.parametrize("method,params,expected_calls", [
    ("tools/list", {}, 1), ("tools/call", {"name": "admin_reset_key"}, 0),
])
def test_numeric_ids_do_not_lose_decimal_precision(identifier, method, params, expected_calls):
    with gateway() as (http, calls):
        body = ('{"jsonrpc":"2.0","id":' + identifier
                + ',"method":' + json.dumps(method) + ',"params":' + json.dumps(params) + '}')
        response = http.post("/mcp", content=body, headers=auth())
        # Inspect the numeric value with Decimal: a float-based assertion could
        # round both expected and actual values and conceal ID corruption.
        assert json.loads(response.content, parse_float=Decimal)["id"] == Decimal(identifier)
        if expected_calls:
            assert response.json()["result"]["tools"] == TOOLS
        else:
            assert response.json()["error"]["code"] == -32001
        assert len(calls) == expected_calls


@pytest.mark.parametrize("body", [
    b'{"jsonrpc":"2.0","id":1e-99999999999999999999,"method":"tools/list"}',
    b'{"jsonrpc":"2.0","id":0,"method":"tools/call","params":{"name":"get_status","arguments":{"value":1e-99999999999999999999}}}',
])
def test_unrepresentable_decimal_is_a_controlled_parse_error(body):
    with gateway() as (http, calls):
        response = http.post("/mcp", content=body, headers=auth())
        assert response.status_code == 200
        assert response.json() == {
            "jsonrpc": "2.0", "id": None, "error": {"code": -32700, "message": "Parse error"},
        }
        assert calls == []

        # A rejected message must not poison the reusable application's next call.
        recovered = http.post("/mcp", json=rpc(), headers=auth())
        assert recovered.json()["result"]["tools"] == TOOLS
        assert len(calls) == 1
