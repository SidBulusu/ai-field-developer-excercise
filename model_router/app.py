"""Non-streaming routing with persistent logical-request token budgets.

Reserve once before provider I/O, then attempt primary and backup sequentially;
uncertain work stays charged. A bounded private SQLite worker keeps transactions
off the event loop without letting abandoned jobs create an unbounded backlog.
"""

import asyncio
import hashlib
import json
import math
import os
import re
import sqlite3
import uuid
from concurrent.futures import ThreadPoolExecutor
from contextlib import asynccontextmanager
from pathlib import Path
from threading import Event
from typing import Literal

import anyio
import httpx
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from pydantic import BaseModel, ConfigDict, Field
from starlette.exceptions import HTTPException as StarletteHTTPException

from model_router.limiter import RateLimited, TokenLimiter, wall_time_ms


class StrictModel(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)


class Message(StrictModel):
    role: Literal["system", "user", "assistant"]
    content: str = Field(min_length=1, max_length=32768)


class GenerationRequest(StrictModel):
    messages: list[Message] = Field(min_length=1, max_length=64)
    max_completion_tokens: int = Field(ge=1, le=50000)
    n: int = Field(default=1, ge=1, le=1)
    stream: bool = False


class Usage(StrictModel):
    prompt_tokens: int = Field(ge=0)
    completion_tokens: int = Field(ge=0)
    total_tokens: int = Field(ge=0)


class OutputMessage(StrictModel):
    role: Literal["assistant"]
    content: str


class Choice(StrictModel):
    index: int = Field(ge=0, le=0)
    message: OutputMessage
    finish_reason: Literal["stop", "length"]


class ProviderReply(StrictModel):
    choices: list[Choice] = Field(min_length=1, max_length=1)
    usage: Usage | None = None


ERROR_MESSAGES = {
    "INVALID_REQUEST": "Invalid generation request.",
    "INVALID_API_KEY": "A valid API key is required.",
    "RATE_LIMIT_EXCEEDED": "Tenant token limit exceeded.",
    "UPSTREAM_UNAVAILABLE": "Unable to complete the request.",
    "UPSTREAM_INVALID_RESPONSE": "Provider returned an unsupported response.",
    "GATEWAY_UNAVAILABLE": "Gateway temporarily unavailable.",
}


class GatewayError(HTTPException):
    def __init__(self, status: int, code: str, headers=None):
        super().__init__(status, detail=code, headers=headers)


def mock_token_count(text: str) -> int:
    """One UTF-8 byte is one mock token; never a real-model billing estimate."""
    return len(text.encode("utf-8"))


def count_prompt_tokens(messages: list[Message]) -> int:
    # This mock has zero role/framing overhead. Keep this shared with providers
    # so the admission upper bound and reported prompt usage cannot drift.
    return sum(mock_token_count(message.content) for message in messages)


def configured_key_digests() -> set[str]:
    keys = os.getenv("ROUTER_API_KEYS", "").split(",")
    if (not 1 <= len(keys) <= 64 or len(set(keys)) != len(keys)
        or any(re.fullmatch(r"[A-Za-z0-9_-]{32,128}", key) is None for key in keys)):
        raise ValueError("ROUTER_API_KEYS requires distinct locally generated API keys")
    # SHA-256 is suitable here because keys must be randomly generated high-
    # entropy secrets, not passwords. A stable digest preserves restart identity.
    return {hashlib.sha256(key.encode("ascii")).hexdigest() for key in keys}


class ProviderRejected(Exception):
    pass


class ProviderTimeout(Exception):
    pass


class ProviderFailure(Exception):
    pass


class InvalidProviderReply(Exception):
    pass


def strict_json(body: bytes):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON member")
            result[key] = value
        return result

    def reject(_):
        raise ValueError("Invalid JSON constant")

    return json.loads(body.decode("utf-8"), object_pairs_hook=unique, parse_constant=reject)


def positive_setting(name: str, default: str, *, integer=False):
    try:
        value = int(os.getenv(name, default)) if integer else float(os.getenv(name, default))
        if not math.isfinite(value) or value <= 0:
            raise ValueError
        return value
    except (ValueError, OverflowError):
        raise ValueError(f"{name} must be positive and finite") from None


def configured_url(name: str, default: str) -> httpx.URL:
    try:
        url = httpx.URL(os.getenv(name, default))
        if url.scheme not in ("http", "https") or not url.host or url.userinfo or url.fragment or url.query:
            raise ValueError
        return url
    except (ValueError, httpx.InvalidURL):
        raise ValueError(f"{name} must be an HTTP URL without credentials, query or fragment") from None


async def fetch(client, url, payload, timeout, response_limit, prompt_tokens, cleanup_timeout=1) -> ProviderReply:
    response = None
    try:
        async with asyncio.timeout(timeout) as deadline:
            outgoing = httpx.Request("POST", url, json=payload, headers={
                "Accept": "application/json", "Accept-Encoding": "identity",
            }, extensions={"timeout": httpx.Timeout(timeout).as_dict()})
            response = await client.send(outgoing, stream=True, auth=None, follow_redirects=False)
            if asyncio.get_running_loop().time() >= deadline.when():
                raise ProviderTimeout
            if response.status_code == 429:
                raise ProviderRejected
            if (response.status_code != 200
                or response.headers.get("content-type", "").split(";", 1)[0].strip().lower() != "application/json"
                or response.headers.get("content-encoding", "identity").lower() != "identity"):
                raise ProviderFailure
            body = bytearray()
            # Encoding is verified as identity before using the byte iterator.
            async for chunk in response.aiter_bytes():
                if len(body) + len(chunk) > response_limit:
                    raise ProviderFailure
                body.extend(chunk)
            try:
                result = ProviderReply.model_validate(strict_json(bytes(body)))
                usage = result.usage
                visible_tokens = mock_token_count(result.choices[0].message.content)
                output_cap = payload["max_completion_tokens"]
                if visible_tokens > output_cap or (usage is not None and (
                    usage.prompt_tokens != prompt_tokens
                    or usage.completion_tokens < visible_tokens
                    or usage.completion_tokens > output_cap
                    or usage.total_tokens != usage.prompt_tokens + usage.completion_tokens
                )):
                    raise InvalidProviderReply
            except (ValueError, UnicodeError, RecursionError, InvalidProviderReply):
                if asyncio.get_running_loop().time() >= deadline.when():
                    raise ProviderTimeout from None
                raise InvalidProviderReply from None
            # Synchronous parsing/validation can cross the deadline without
            # letting asyncio deliver cancellation. Check the clock explicitly
            # before accepting success, so a late result cannot win that race.
            if asyncio.get_running_loop().time() >= deadline.when():
                raise ProviderTimeout
            return result
    except (TimeoutError, httpx.TimeoutException):
        raise ProviderTimeout from None
    except (httpx.HTTPError, ValueError, UnicodeError, RecursionError):
        raise ProviderFailure from None
    finally:
        if response is not None:
            # Cleanup must survive caller cancellation, but a broken transport
            # must not indefinitely postpone fallback or request shutdown.
            with anyio.move_on_after(cleanup_timeout, shield=True):
                try:
                    await response.aclose()
                except (httpx.HTTPError, OSError):
                    # Best-effort closure must not replace a known 429/timeout
                    # decision with an unrelated transport-cleanup exception.
                    pass


def create_app(*, db_path: str | Path | None = None, client: httpx.AsyncClient | None = None,
               clock=wall_time_ms) -> FastAPI:
    key_digests = configured_key_digests()
    limit = positive_setting("ROUTER_TOKEN_LIMIT", "50000", integer=True)
    window_ms = positive_setting("ROUTER_WINDOW_MS", "60000", integer=True)
    busy_timeout = positive_setting("ROUTER_DB_TIMEOUT_SECONDS", "1")
    db_timeout = positive_setting("ROUTER_DB_OPERATION_TIMEOUT_SECONDS", "2")
    db_capacity = positive_setting("ROUTER_DB_MAX_PENDING", "16", integer=True)
    limiter = TokenLimiter(db_path if db_path is not None else os.getenv("ROUTER_DB_PATH", "model_router/usage.db"),
                           limit=limit, window_ms=window_ms, busy_timeout=busy_timeout, clock=clock)
    primary_url = configured_url("ROUTER_PRIMARY_URL", "http://127.0.0.1:8021/v1/chat/completions")
    backup_url = configured_url("ROUTER_BACKUP_URL", "http://127.0.0.1:8022/v1/chat/completions")
    primary_model = os.getenv("ROUTER_PRIMARY_MODEL", "mock-primary")
    backup_model = os.getenv("ROUTER_BACKUP_MODEL", "mock-backup")
    if any(not model or len(model) > 128 or not model.isascii() or not model.isprintable()
           for model in (primary_model, backup_model)):
        raise ValueError("Invalid configured model name")
    primary_timeout = positive_setting("ROUTER_PRIMARY_TIMEOUT_SECONDS", "3")
    backup_timeout = positive_setting("ROUTER_BACKUP_TIMEOUT_SECONDS", "5")
    cleanup_timeout = positive_setting("ROUTER_CLEANUP_TIMEOUT_SECONDS", "1")
    if primary_timeout + backup_timeout + 2 * cleanup_timeout + 3 * db_timeout >= window_ms / 1000:
        raise ValueError("Provider and database deadlines must fit safely inside the token window")
    upload_timeout = positive_setting("ROUTER_REQUEST_TIMEOUT_SECONDS", "5")
    request_limit = positive_setting("ROUTER_MAX_REQUEST_BYTES", "65536", integer=True)
    response_limit = positive_setting("ROUTER_MAX_RESPONSE_BYTES", "65536", integer=True)
    db_executor = None
    db_pending = 0

    async def database(operation, *args):
        nonlocal db_pending
        # SQLite permits one writer. A private single worker avoids competing
        # writes within this app and starvation by unrelated default-pool work.
        # Count queued AND running jobs; no await separates checking/incrementing.
        if db_executor is None or db_pending >= db_capacity:
            raise GatewayError(503, "GATEWAY_UNAVAILABLE")
        abandoned = Event()

        def run():
            if not abandoned.is_set():
                return operation(*args)

        work = db_executor.submit(run)
        db_pending += 1
        result = asyncio.wrap_future(work)

        def finished(done):
            nonlocal db_pending
            db_pending -= 1
            # A caller can leave before a running worker finishes. Consume its
            # exception even then, without logging SQL, keys or request data.
            if not done.cancelled():
                done.exception()

        result.add_done_callback(finished)
        try:
            async with asyncio.timeout(db_timeout) as deadline:
                value = await asyncio.shield(result)
                if asyncio.get_running_loop().time() >= deadline.when():
                    raise TimeoutError
                return value
        except asyncio.CancelledError:
            # Future.cancel leaves a WorkItem in the executor queue. Releasing
            # its slot then permits unbounded cancelled entries behind a stuck
            # worker. Instead skip abandoned work when drained, retaining the
            # slot until actual completion. Running transactions can still commit.
            abandoned.set()
            raise
        except TimeoutError:
            abandoned.set()
            raise GatewayError(503, "GATEWAY_UNAVAILABLE") from None
        except (sqlite3.Error, OSError, ValueError):
            raise GatewayError(503, "GATEWAY_UNAVAILABLE") from None

    @asynccontextmanager
    async def lifespan(app):
        nonlocal db_executor
        db_executor = ThreadPoolExecutor(max_workers=1, thread_name_prefix="router-db")
        try:
            try:
                await database(limiter.initialize)
            except GatewayError:
                raise ValueError("Unable to initialize router database") from None
            app.state.client = client or httpx.AsyncClient(trust_env=False, follow_redirects=False)
            try:
                yield
            finally:
                if client is None:
                    with anyio.move_on_after(cleanup_timeout, shield=True):
                        await app.state.client.aclose()
        finally:
            # Do not block the event loop waiting for a stuck worker. Running
            # transactions finish with their own connection; queued jobs stop.
            db_executor.shutdown(wait=False, cancel_futures=True)
            db_executor = None

    app = FastAPI(lifespan=lifespan)

    @app.exception_handler(StarletteHTTPException)
    async def http_error(request: Request, error):
        code = error.detail if isinstance(error, GatewayError) else "INVALID_REQUEST"
        request_id = getattr(request.state, "request_id", "req-" + uuid.uuid4().hex)
        return JSONResponse({"error": {"code": code, "message": ERROR_MESSAGES[code],
                                       "request_id": request_id}},
                            status_code=error.status_code, headers=error.headers)

    @app.exception_handler(Exception)
    async def unexpected_error(request: Request, error):
        # Unexpected faults use the same envelope without reflecting exception
        # details. Deliberate cancellation remains a BaseException and propagates.
        return await http_error(request, GatewayError(500, "GATEWAY_UNAVAILABLE"))

    async def reserve(tenant, tokens, request_id):
        try:
            return await database(limiter.reserve, tenant, tokens, request_id)
        except RateLimited as error:
            raise GatewayError(429, "RATE_LIMIT_EXCEEDED", headers={
                "Retry-After": str(error.retry_after),
            }) from None

    async def settle(identifier, actual):
        await database(limiter.settle, identifier, actual)

    async def mark_uncertain(identifier):
        await database(limiter.mark_uncertain, identifier)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        request.state.request_id = "req-" + uuid.uuid4().hex
        keys = request.headers.getlist("x-api-key")
        if len(keys) != 1 or re.fullmatch(r"[A-Za-z0-9_-]{32,128}", keys[0]) is None:
            raise GatewayError(401, "INVALID_API_KEY")
        tenant = hashlib.sha256(keys[0].encode("ascii")).hexdigest()
        if tenant not in key_digests:
            raise GatewayError(401, "INVALID_API_KEY")
        body = bytearray()
        try:
            async with asyncio.timeout(upload_timeout):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > request_limit:
                        raise GatewayError(413, "INVALID_REQUEST")
                    body.extend(chunk)
            generation = GenerationRequest.model_validate(strict_json(bytes(body)))
            if generation.stream:
                raise ValueError
            prompt_tokens = count_prompt_tokens(generation.messages)
        except TimeoutError:
            raise GatewayError(408, "INVALID_REQUEST") from None
        except (ValueError, UnicodeError, RecursionError):
            raise GatewayError(400, "INVALID_REQUEST") from None
        reserved_tokens = prompt_tokens + generation.max_completion_tokens
        if reserved_tokens > limit:
            raise GatewayError(429, "RATE_LIMIT_EXCEEDED")

        # One logical admission covers both sequential attempts. A 429 means
        # no work under the mock contract; a timeout makes the charge uncertain.
        identifier = await reserve(tenant, reserved_tokens, request.state.request_id)
        uncertain = False
        for route, url, model, timeout in [
            ("primary", primary_url, primary_model, primary_timeout),
            ("backup", backup_url, backup_model, backup_timeout),
        ]:
            try:
                result = await fetch(app.state.client, url, {
                    **generation.model_dump(), "model": model,
                }, timeout, response_limit, prompt_tokens, cleanup_timeout)
                usage = result.usage
            except ProviderRejected:
                if route == "primary":
                    continue
                if not uncertain:
                    await settle(identifier, 0)
                raise GatewayError(502, "UPSTREAM_UNAVAILABLE") from None
            except ProviderTimeout:
                uncertain = True
                await mark_uncertain(identifier)
                if route == "primary":
                    continue
                raise GatewayError(504, "UPSTREAM_UNAVAILABLE") from None
            except (InvalidProviderReply, UnicodeError):
                raise GatewayError(502, "UPSTREAM_INVALID_RESPONSE") from None
            except ProviderFailure:
                raise GatewayError(502, "UPSTREAM_UNAVAILABLE") from None
            if usage is not None and not uncertain:
                await settle(identifier, usage.total_tokens)
            elif usage is None:
                await mark_uncertain(identifier)
            return JSONResponse({"id": "router-" + uuid.uuid4().hex,
                                 "object": "chat.completion", "model": model, "route": route,
                                 **result.model_dump()})

    return app