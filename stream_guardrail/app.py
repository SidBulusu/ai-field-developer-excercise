"""Local text-only SSE gateway. Never relay provider frames or headers verbatim."""

import asyncio
import json
import math
import os
from collections.abc import Iterator
from contextlib import asynccontextmanager
from typing import Literal

import anyio
import httpx
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse
from pydantic import BaseModel, ConfigDict, Field, ValidationError

from stream_guardrail.redactor import RedactionError, StreamingRedactor


class StreamError(ValueError):
    """Untrusted stream cannot be safely continued."""


class SSEParser:
    """Frame bounded UTF-8 SSE events independently of HTTP byte boundaries."""

    def __init__(self, max_event_bytes: int):
        self.limit = max_event_bytes
        self.peak_event_bytes = 0
        self._size = 0
        self._line = bytearray()
        self._data: list[str] = []
        self._event_type = ""
        self._first_line = True
        self._cr = False
        self._event_ended_at_cr = False

    def _newline(self) -> Iterator[str]:
        try:
            line = self._line.decode("utf-8-sig" if self._first_line else "utf-8")
        except UnicodeError:
            raise StreamError("Invalid SSE encoding") from None
        self._first_line = False
        self._line.clear()
        if not line:
            data, event_type = self._data, self._event_type
            self._data, self._event_type = [], ""
            # CR already ends the line. Keep its byte count just long enough to
            # charge an optional split LF without delaying event delivery.
            self._event_ended_at_cr = self._cr
            if not self._cr:
                self._size = 0
            if event_type not in ("", "message"):
                raise StreamError("Unsupported SSE event")
            if data:
                yield "\n".join(data)
        elif not line.startswith(":"):
            name, separator, value = line.partition(":")
            if value.startswith(" "):
                value = value[1:]
            if name == "data":
                self._data.append(value if separator else "")
            elif name == "event":
                self._event_type = value
            # SSE id/retry and unknown fields do not contain supported deltas;
            # ignore them and never reflect them to clients.

    def _count_byte(self) -> None:
        # Comments and unfinished lines consume framing memory too; counting
        # only data fields would let an upstream bypass the event limit.
        if self._size == self.limit:
            raise StreamError("SSE event limit exceeded")
        self._size += 1
        self.peak_event_bytes = max(self.peak_event_bytes, self._size)

    def feed(self, chunk: bytes) -> Iterator[str]:
        for byte in chunk:
            if self._cr:
                self._cr = False
                if byte == 10:
                    self._count_byte()
                    if self._event_ended_at_cr:
                        self._size = 0
                    self._event_ended_at_cr = False
                    continue
                if self._event_ended_at_cr:
                    self._size = 0
                self._event_ended_at_cr = False
            self._count_byte()
            if byte == 13:
                self._cr = True
                # Waiting for another byte here stalls valid CR-only streams
                # whenever the provider pauses immediately after an event.
                yield from self._newline()
            elif byte == 10:
                yield from self._newline()
            else:
                self._line.append(byte)

    def finish(self) -> Iterator[str]:
        if self._event_ended_at_cr:
            self._size = 0
        self._cr = self._event_ended_at_cr = False
        if self._size:
            raise StreamError("Truncated SSE event")
        # EOF validates framing; it cannot invent the blank line that dispatches
        # an unfinished event. Completed CR events have already been delivered.
        return iter(())


def strict_json(data: bytes | str):
    def unique(pairs):
        result = {}
        for key, value in pairs:
            if key in result:
                raise ValueError("Duplicate JSON member")
            result[key] = value
        return result

    def reject_constant(_):
        raise ValueError("Nonfinite JSON number")

    # Ambiguous duplicate fields or nonstandard numbers must not be interpreted
    # differently by validation and downstream clients. Escaped lone surrogates
    # also need rejection before the UTF-8 writer can fail mid-response.
    result = json.loads(data, object_pairs_hook=unique, parse_constant=reject_constant)
    pending = [result]
    while pending:
        item = pending.pop()
        if isinstance(item, str):
            item.encode("utf-8")
        elif isinstance(item, float) and not math.isfinite(item):
            raise ValueError("Nonfinite JSON number")
        elif isinstance(item, dict):
            pending.extend(item.keys())
            pending.extend(item.values())
        elif isinstance(item, list):
            pending.extend(item)
    return result


class Message(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    role: Literal["system", "user", "assistant"]
    content: str = Field(max_length=32768)


class GenerationRequest(BaseModel):
    model_config = ConfigDict(extra="forbid", strict=True)
    model: str = Field(min_length=1, max_length=128)
    messages: list[Message] = Field(min_length=1, max_length=64)
    stream: bool
    n: int = Field(default=1, ge=1, le=1)


def parse_delta(data: str) -> tuple[str, str | None]:
    """Allowlist the supported provider subset; return only inspectable text."""
    # Accepting a familiar content field is insufficient: parallel reasoning,
    # logprob or tool-argument fields could carry generated text around it.
    payload = strict_json(data)
    if not isinstance(payload, dict) or set(payload) - {
        "id", "object", "created", "model", "system_fingerprint", "choices", "usage",
    }:
        raise StreamError("Unsupported provider event")
    for key in ("id", "model", "system_fingerprint"):
        if key in payload and payload[key] is not None and not isinstance(payload[key], str):
            raise StreamError("Unsupported provider metadata")
    if "object" in payload and payload["object"] != "chat.completion.chunk":
        raise StreamError("Unsupported provider object")
    if "created" in payload and (type(payload["created"]) is not int or payload["created"] < 0):
        raise StreamError("Unsupported provider timestamp")
    usage = payload.get("usage")
    if usage is not None:
        if not isinstance(usage, dict) or set(usage) != {"prompt_tokens", "completion_tokens", "total_tokens"}:
            raise StreamError("Unsupported provider usage")
        if any(type(value) is not int or value < 0 for value in usage.values()):
            raise StreamError("Unsupported provider usage")
    choices = payload.get("choices")
    if choices == [] and usage is not None:
        # Providers can send accounting separately from generated text. Accept
        # this narrow shape without reflecting provider metadata to the client.
        return "", None
    if not isinstance(choices, list) or len(choices) != 1:
        raise StreamError("Unsupported provider choices")
    choice = choices[0]
    if not isinstance(choice, dict) or set(choice) - {"index", "delta", "finish_reason", "logprobs"}:
        raise StreamError("Unsupported provider choice")
    if type(choice.get("index")) is not int or choice["index"] != 0 or choice.get("logprobs") is not None:
        raise StreamError("Unsupported provider choice")
    delta = choice.get("delta")
    if not isinstance(delta, dict) or set(delta) - {"content", "role"}:
        raise StreamError("Unsupported provider delta")
    if "role" in delta and delta["role"] != "assistant":
        raise StreamError("Unsupported provider role")
    content = delta.get("content")
    if content is not None and not isinstance(content, str):
        raise StreamError("Unsupported provider content")
    finish = choice.get("finish_reason")
    if finish is not None and finish not in ("stop", "length", "content_filter"):
        raise StreamError("Unsupported provider finish")
    return content or "", finish


def encode_delta(text: str = "", finish: str | None = None) -> bytes:
    delta = {"content": text} if text else {}
    # All metadata is generated locally; provider strings are never echoed.
    return ("data: " + json.dumps({"object": "chat.completion.chunk", "choices": [
        {"index": 0, "delta": delta, "finish_reason": finish},
    ]}, ensure_ascii=False) + "\n\n").encode()


class ProviderStreamResponse(StreamingResponse):
    """Own the upstream even if sending headers or consuming the body fails."""

    def __init__(self, content, upstream: httpx.Response):
        super().__init__(content, media_type="text/event-stream", headers={
            "Cache-Control": "no-cache", "X-Accel-Buffering": "no",
        })
        self.upstream = upstream

    async def __call__(self, scope, receive, send):
        try:
            await super().__call__(scope, receive, send)
        finally:
            # The response, not just the generator, owns the upstream: sending
            # headers can fail before the generator's finally block ever runs.
            # Shield cleanup from Starlette's disconnect cancellation scope.
            with anyio.CancelScope(shield=True):
                try:
                    await self.body_iterator.aclose()
                finally:
                    await self.upstream.aclose()


def positive_setting(name: str, default: str, integer: bool = False):
    try:
        value = int(os.getenv(name, default)) if integer else float(os.getenv(name, default))
        if not math.isfinite(value) or value <= 0:
            raise ValueError
        return value
    except (ValueError, OverflowError):
        raise ValueError(f"{name} must be positive and finite") from None


def create_app(*, client: httpx.AsyncClient | None = None) -> FastAPI:
    upstream_url = os.getenv("STREAM_UPSTREAM_URL", "http://127.0.0.1:8011/v1/chat/completions")
    url = httpx.URL(upstream_url)
    if url.scheme not in ("http", "https") or not url.host or url.userinfo or url.fragment:
        raise ValueError("STREAM_UPSTREAM_URL must be an HTTP URL without credentials or fragment")
    read_timeout = positive_setting("STREAM_READ_TIMEOUT_SECONDS", "5")
    total_timeout = positive_setting("STREAM_TOTAL_TIMEOUT_SECONDS", "30")
    request_limit = positive_setting("STREAM_MAX_REQUEST_BYTES", "65536", True)
    event_limit = positive_setting("STREAM_MAX_EVENT_BYTES", "65536", True)
    pending_limit = positive_setting("STREAM_MAX_PENDING_CHARS", "256", True)

    @asynccontextmanager
    async def lifespan(app):
        app.state.client = client or httpx.AsyncClient(trust_env=False, follow_redirects=False)
        try:
            yield
        finally:
            if client is None:
                # Injected clients may serve other applications; only the owner
                # should close their connection pool.
                with anyio.CancelScope(shield=True):
                    await app.state.client.aclose()

    app = FastAPI(lifespan=lifespan)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        body = bytearray()
        try:
            async with asyncio.timeout(read_timeout):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > request_limit:
                        return JSONResponse({"detail": "Request too large"}, status_code=413)
                    body.extend(chunk)
            generation = GenerationRequest.model_validate(strict_json(body.decode("utf-8")))
            if not generation.stream:
                raise ValueError
        except TimeoutError:
            return JSONResponse({"detail": "Request timeout"}, status_code=408)
        except (ValueError, UnicodeError, RecursionError, ValidationError):
            return JSONResponse({"detail": "Unsupported generation request"}, status_code=400)

        deadline = asyncio.get_running_loop().time() + total_timeout
        # A fresh Request avoids shared cookies, client default auth and caller
        # headers. The configured endpoint is the only routing authority.
        outgoing = httpx.Request("POST", url, json=generation.model_dump(), headers={
            "Accept": "text/event-stream", "Accept-Encoding": "identity",
        }, extensions={"timeout": httpx.Timeout(read_timeout).as_dict()})
        try:
            async with asyncio.timeout_at(deadline):
                upstream = await app.state.client.send(outgoing, stream=True, auth=None, follow_redirects=False)
        except (TimeoutError, httpx.TimeoutException):
            return JSONResponse({"detail": "Provider timeout"}, status_code=504)
        except httpx.HTTPError:
            return JSONResponse({"detail": "Provider unavailable"}, status_code=502)

        content_type = upstream.headers.get("content-type", "").split(";", 1)[0].strip().lower()
        # Decompression can allocate far more than the event cap before framing
        # sees a byte. Identity encoding keeps that cap meaningful.
        if upstream.status_code != 200 or content_type != "text/event-stream" or upstream.headers.get(
            "content-encoding", "identity"
        ).lower() != "identity":
            with anyio.CancelScope(shield=True):
                await upstream.aclose()
            return JSONResponse({"detail": "Provider unavailable"}, status_code=502)

        async def safe_stream():
            parser = SSEParser(event_limit)
            redactor = StreamingRedactor(pending_limit)
            finish_reason = None

            try:
                async with asyncio.timeout_at(deadline):
                    async for chunk in upstream.aiter_raw():
                        # A socket read or downstream send can complete without
                        # suspending. Bound CPU batches explicitly so comments,
                        # dense events and large deltas yield regularly to
                        # deadlines and other streams. These sizes are not new
                        # framing/candidate limits; state persists across them.
                        for offset in range(0, len(chunk), 8192):
                            await asyncio.sleep(0)
                            for data in parser.feed(chunk[offset:offset + 8192]):
                                if data == "[DONE]":
                                    if finish_reason is None:
                                        raise StreamError("Missing provider finish")
                                    safe = redactor.finish()
                                    if safe:
                                        yield encode_delta(safe)
                                    yield encode_delta(finish=finish_reason)
                                    yield b"data: [DONE]\n\n"
                                    return
                                text, finish = parse_delta(data)
                                if finish_reason is not None and (text or finish is not None):
                                    raise StreamError("Text after provider finish")
                                # 1,024-character batches missed the eight-stream
                                # heartbeat budget; 256 met it at default limits.
                                # See test_performance.py before increasing this.
                                for start in range(0, len(text), 256):
                                    safe = redactor.feed(text[start:start + 256])
                                    if safe:
                                        yield encode_delta(safe)
                                    await asyncio.sleep(0)
                                if finish is not None:
                                    # Finish alone cannot release ambiguity:
                                    # premature EOF must still discard it.
                                    finish_reason = finish
                    parser.finish()
                    raise StreamError("Truncated provider stream")
            except (StreamError, RedactionError, ValueError, UnicodeError, RecursionError,
                    TimeoutError, httpx.HTTPError):
                redactor.abort()
                # Headers may already say 200; only an in-band fixed error (or
                # connection termination) is possible now. Never echo raw data.
                yield b'event: error\ndata: {"error":{"code":"stream_failed","message":"Generation stream failed"}}\n\n'
            finally:
                redactor.abort()

        return ProviderStreamResponse(safe_stream(), upstream)

    return app
