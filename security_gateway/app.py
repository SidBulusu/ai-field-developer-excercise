"""Authenticated HTTP/JSON-RPC proxy, independent of the stdio MCP server.

Authenticate before parsing or forwarding, then enforce the tool policy before
I/O. Preserve allowed request bytes for transparency while isolating credentials
and cookies; downstream response bodies remain a trusted-service boundary.
"""

import json
import math
import os
from contextlib import asynccontextmanager
from decimal import Decimal, InvalidOperation
from typing import Any

import httpx
import jwt
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response


def rpc_response(identifier: Any, fields: dict[str, Any], status: int = 200) -> Response:
    # The stdlib encoder cannot emit Decimal as a JSON number. Only the validated
    # numeric ID needs this treatment; encode strings and response fields normally
    # to prevent injection and keep fractional IDs out of lossy float conversion.
    encoded_id = str(identifier) if isinstance(identifier, Decimal) else json.dumps(identifier)
    body = '{"jsonrpc":"2.0","id":' + encoded_id + ',' + json.dumps(fields, allow_nan=False)[1:]
    return Response(body, status_code=status, media_type="application/json")


def rpc_error(identifier: Any, code: int, message: str, status: int = 200) -> Response:
    return rpc_response(
        identifier, {"error": {"code": code, "message": message}}, status,
    )


def finite_number(value: str) -> Decimal:
    # Keep the accepted numeric range compatible with float-based downstream
    # parsers; literals such as 1e999 would become infinity in those parsers.
    number = float(value)
    if not math.isfinite(number):
        raise ValueError("Non-finite JSON number")
    # Preserve the original numeric value for errors generated locally. Allowed
    # requests still use their original bytes regardless of parsed number types.
    return Decimal(value)


def unique_object(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    # Ambiguous duplicate names can make a policy inspect a different tool to downstream.
    result = {}
    for key, value in pairs:
        if key in result:
            raise ValueError("Duplicate JSON member")
        result[key] = value
    return result


def reject_constant(value: str) -> None:
    raise ValueError("Nonstandard JSON constant")


def validate_unicode(value: Any) -> None:
    # json.loads accepts escaped lone surrogates, but UTF-8 response writers do
    # not. Check keys and nested values before a bad ID can crash error rendering.
    pending = [value]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            item.encode("utf-8")
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)


def parse_rpc(body: bytes) -> dict[str, Any] | Response:
    """Validate the envelope without rewriting the bytes sent to downstream."""
    try:
        payload = json.loads(
            body, parse_float=finite_number, parse_constant=reject_constant,
            object_pairs_hook=unique_object,
        )
        validate_unicode(payload)
    except (ValueError, UnicodeError, RecursionError, InvalidOperation):
        # An extreme negative exponent can underflow float to zero yet exceed
        # Decimal's exponent range. It is still an input parsing failure, not an
        # application crash; the partially parsed envelope has no trusted ID.
        return rpc_error(None, -32700, "Parse error")
    if (
        not isinstance(payload, dict)
        or set(payload) - {"jsonrpc", "method", "params", "id"}
        or payload.get("jsonrpc") != "2.0"
        or not isinstance(payload.get("method"), str)
        # bool subclasses int in Python, but is not a JSON-RPC numeric ID.
        or ("id" in payload and type(payload["id"]) not in (str, int, Decimal, type(None)))
    ):
        return rpc_error(None, -32600, "Invalid request")
    # tools/call has a stricter params contract and its own Invalid params error.
    if payload["method"] == "tools/call":
        params = payload.get("params")
        if (
            not isinstance(params, dict)
            or not isinstance(params.get("name"), str)
            or ("arguments" in params and not isinstance(params["arguments"], dict))
        ):
            return request_error(payload, -32602, "Invalid params")
    elif "params" in payload and not isinstance(payload["params"], (dict, list)):
        return rpc_error(None, -32600, "Invalid request")
    return payload


def request_error(payload: dict[str, Any], code: int, message: str, status: int = 200) -> Response:
    # Absence, rather than truthiness, distinguishes notifications: 0 and null
    # are IDs. JSON-RPC forbids replies to notifications, even on denied calls.
    if "id" not in payload:
        return Response(status_code=204)
    return rpc_error(payload["id"], code, message, status)


def tool_denied(payload: dict[str, Any], role: str) -> bool:
    # Match the downstream name literally; trimming or changing case here would
    # make authorization operate on a different identifier from execution.
    return (
        payload["method"] == "tools/call"
        and payload["params"]["name"].startswith("admin_")
        and role != "admin"
    )


def relay_headers(headers: httpx.Headers, allowed: set[str]) -> dict[str, str]:
    # Connection can nominate additional hop-by-hop headers, even allowlisted ones.
    connection_headers = {name.strip().lower() for name in headers.get("connection", "").split(",")}
    return {name: value for name, value in headers.items()
            if name in allowed and name not in connection_headers}


def create_app(
    *, secret: str | None = None, downstream_url: str | None = None,
    timeout: float | None = None, client: httpx.AsyncClient | None = None,
) -> FastAPI:
    """Create a gateway. Injected clients belong to the caller; others use lifespan."""
    secret = secret if secret is not None else os.environ.get("GATEWAY_JWT_SECRET", "")
    if len(secret.encode()) < 32:
        raise ValueError("GATEWAY_JWT_SECRET must contain at least 32 bytes.")
    downstream_url = downstream_url or os.environ.get(
        "GATEWAY_DOWNSTREAM_URL", "http://127.0.0.1:8001/mcp"
    )
    url = httpx.URL(downstream_url)
    if url.scheme not in {"http", "https"} or not url.host or url.userinfo or url.fragment:
        raise ValueError("GATEWAY_DOWNSTREAM_URL must be an HTTP URL without credentials or fragment.")
    timeout = timeout if timeout is not None else float(os.environ.get("GATEWAY_TIMEOUT_SECONDS", "5"))
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("GATEWAY_TIMEOUT_SECONDS must be finite and positive.")

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        if client is not None:
            app.state.downstream = client
            yield
        else:
            # Reuse connections without letting ambient HTTP_PROXY settings
            # redirect this configured service-to-service connection.
            async with httpx.AsyncClient(trust_env=False, follow_redirects=False) as owned:
                app.state.downstream = owned
                yield

    app = FastAPI(lifespan=lifespan)

    @app.middleware("http")
    async def authenticate(request: Request, call_next):
        # Multiple credentials are ambiguous; do not let middleware and handlers
        # choose different headers. Authentication never needs the JSON body.
        authorization = request.headers.getlist("authorization")
        parts = authorization[0].split() if len(authorization) == 1 else []
        try:
            if len(parts) != 2 or parts[0].lower() != "bearer":
                raise ValueError("Missing Bearer credential")
            # The verifier, not the untrusted JWT header, chooses the algorithm.
            # Required claims prevent a signed token with no expiry or role
            # from silently acquiring a default authorization policy.
            claims = jwt.decode(
                parts[1], secret, algorithms=["HS256"], options={"require": ["exp", "role"]},
            )
            role = claims["role"]
            if not isinstance(role, str) or role not in {"admin", "viewer"}:
                raise ValueError("Invalid role")
            if type(claims["exp"]) not in (int, float) or not math.isfinite(claims["exp"]):
                raise ValueError("Invalid expiration")
        except (jwt.InvalidTokenError, ValueError, TypeError, OverflowError):
            return JSONResponse(
                {"detail": "Invalid or missing Bearer token"}, status_code=401,
                headers={"WWW-Authenticate": "Bearer"},
            )
        # Identity belongs to this request, never to the shared HTTP client.
        request.state.role = role
        return await call_next(request)

    @app.post("/mcp")
    async def proxy(request: Request):
        body = await request.body()
        payload = parse_rpc(body)
        if isinstance(payload, Response):
            return payload
        if tool_denied(payload, request.state.role):
            return request_error(payload, -32001, "Unauthorized Tool Call")

        # Only the inspected bytes may reach downstream: rebuilding params could
        # change nested inputs, while forwarding arbitrary headers leaks identity.
        headers = relay_headers(httpx.Headers(request.headers), {
            "content-type", "accept", "mcp-protocol-version", "mcp-session-id",
        })
        try:
            downstream = request.app.state.downstream
            forwarded = downstream.build_request(
                "POST", downstream_url, content=body, headers=headers, timeout=timeout,
            )
            # HTTPX merges its cookie jar during build_request. A connection pool
            # shared by all callers must not replay one response's session cookie
            # on another caller's request. Removing it here avoids racing on a
            # shared jar with clear(), while keeping connection reuse intact.
            forwarded.headers.pop("cookie", None)
            upstream = await downstream.send(forwarded, follow_redirects=False)
        except httpx.TimeoutException:
            return request_error(payload, -32000, "Downstream timeout", 504)
        except httpx.RequestError:
            return request_error(payload, -32000, "Downstream unavailable", 502)

        if "id" not in payload:
            return Response(status_code=204)
        # HTTPX decodes compression; omit original content-encoding and content-length.
        headers = relay_headers(upstream.headers, {
            "content-type", "mcp-protocol-version", "mcp-session-id", "retry-after", "location",
        })
        return Response(upstream.content, status_code=upstream.status_code, headers=headers)

    return app