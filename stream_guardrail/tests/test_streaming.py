"""Framing and gateway behavior with deterministic asynchronous byte streams."""

import asyncio
import json
from contextlib import contextmanager

import httpx
import pytest
from fastapi.testclient import TestClient
from starlette.requests import ClientDisconnect

from stream_guardrail.app import ProviderStreamResponse, SSEParser, StreamError, create_app, parse_delta
from stream_guardrail.redactor import StreamingRedactor
from stream_guardrail.mock_provider import create_app as create_mock


REQUEST = {"model": "mock-pii", "stream": True, "messages": [{"role": "user", "content": "Synthetic demo"}]}


def event(text=None, finish=None, **delta):
    if text is not None:
        delta["content"] = text
    return ("data: " + json.dumps({"choices": [{"index": 0, "delta": delta,
             "finish_reason": finish}]}, ensure_ascii=False) + "\n\n").encode()


class ByteStream(httpx.AsyncByteStream):
    def __init__(self, chunks, error=None):
        self.chunks = chunks
        self.error = error
        self.closed = False
        self.reads = 0

    async def __aiter__(self):
        for chunk in self.chunks:
            self.reads += 1
            yield chunk
        if self.error:
            raise self.error("private-canary")

    async def aclose(self):
        self.closed = True


@contextmanager
def gateway(chunks, status=200, headers=None, error=None):
    stream = ByteStream(chunks, error)
    calls = []

    async def handler(request):
        calls.append(request)
        return httpx.Response(status, headers=headers or {"content-type": "text/event-stream"}, stream=stream)

    client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    try:
        with TestClient(create_app(client=client)) as http:
            yield http, stream, calls
    finally:
        asyncio.run(client.aclose())


def parse_output(response):
    return [json.loads(line[6:]) for line in response.text.splitlines()
            if line.startswith("data: ") and line != "data: [DONE]"]


def output_text(response):
    return "".join(item["choices"][0]["delta"].get("content", "")
                   for item in parse_output(response) if "choices" in item)


@pytest.mark.parametrize("newline", [b"\n", b"\r\n", b"\r"])
def test_sse_every_byte_split(newline):
    wire = b"\xef\xbb\xbf: comment" + newline + b'data: {"text":' + newline
    wire += 'data: "café 😀"}'.encode() + newline + newline
    for split in range(len(wire) + 1):
        parser = SSEParser(1024)
        output = list(parser.feed(wire[:split])) + list(parser.feed(wire[split:]))
        output += list(parser.finish())
        assert output == ['{"text":\n"café 😀"}']
        assert parser.peak_event_bytes <= len(wire)


def test_cr_event_dispatches_before_more_input_or_eof():
    parser = SSEParser(1024)
    assert list(parser.feed(b"data: safe\r\r")) == ["safe"]
    # The LF of a split CRLF belongs to the already consumed newline, not an
    # extra blank line or the beginning of the next event's size accounting.
    assert list(parser.feed(b"\ndata: next\r\n\r")) == ["next"]
    assert list(parser.feed(b"\n")) == []
    assert list(parser.finish()) == []


@pytest.mark.parametrize("ending", [b"\n\n", b"\r\r", b"\r\n\r\n"])
def test_event_cap_counts_exact_wire_length_including_optional_lf(ending):
    wire = b"data: safe" + ending
    parser = SSEParser(len(wire))
    assert list(parser.feed(wire)) == ["safe"]
    assert list(parser.finish()) == []
    assert parser.peak_event_bytes == len(wire)
    parser = SSEParser(len(wire) - 1)
    with pytest.raises(StreamError):
        list(parser.feed(wire))
    assert parser.peak_event_bytes <= len(wire) - 1


@pytest.mark.parametrize("value", ["4111 1111 1111 1111@example.com", "a@b.co@c.com"])
def test_gateway_masks_overlapping_values_across_text_and_network_boundaries(value):
    wire = b"".join(event(char) for char in value) + event(finish="stop") + b"data: [DONE]\n\n"
    with gateway([bytes([byte]) for byte in wire]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == "[REDACTED]"
        assert response.text.endswith("data: [DONE]\n\n")
        assert stream.closed


def test_mock_provider_rejects_oversized_uploads(monkeypatch):
    monkeypatch.setenv("STREAM_MAX_REQUEST_BYTES", "16")
    with TestClient(create_mock()) as http:
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert response.status_code == 413
        assert response.json() == {"detail": "Mock request too large"}


def test_mock_provider_times_out_slow_uploads(monkeypatch):
    monkeypatch.setenv("STREAM_READ_TIMEOUT_SECONDS", "0.02")
    async def check():
        async def upload():
            yield b"{"
            await asyncio.sleep(0.2)
            yield b"}"
        async with httpx.AsyncClient(transport=httpx.ASGITransport(create_mock())) as http:
            response = await http.post("http://mock/v1/chat/completions", content=upload())
            assert response.status_code == 408
            assert response.json() == {"detail": "Mock request timeout"}
    asyncio.run(check())


@pytest.mark.parametrize("wire", [b":" + b"x" * 64, b"data:" + b"x" * 64, b":x\n" * 30])
def test_event_cap_includes_comments_and_unterminated_lines(wire):
    parser = SSEParser(32)
    with pytest.raises(StreamError):
        list(parser.feed(wire))
    assert parser.peak_event_bytes <= 32


def test_truncated_and_invalid_utf8_frames():
    for wire in [b"data: unfinished", b"data: \xff\n\n"]:
        parser = SSEParser(1024)
        with pytest.raises(StreamError):
            list(parser.feed(wire))
            list(parser.finish())


def test_gateway_redaction_utf8_and_finish_order():
    wire = event("Contact jane") + event("@example.com; café 😀! ")
    wire += event("123-45-") + event("6789; 4111 1111 1111 1111")
    wire += event(finish="stop") + b"data: [DONE]\n\n"
    with gateway([bytes([byte]) for byte in wire]) as (http, stream, calls):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert response.status_code == 200
        assert output_text(response) == "Contact [REDACTED]; café 😀! [REDACTED]; [REDACTED]"
        assert response.text.endswith('data: [DONE]\n\n')
        assert parse_output(response)[-1]["choices"][0]["finish_reason"] == "stop"
        assert stream.closed
        assert len(calls) == 1
        assert json.loads(calls[0].content)["messages"] == REQUEST["messages"]


def test_successful_length_finish_only_redacts_complete_supported_patterns():
    # A generation length limit is different from a broken transport. This
    # explicitly records the pattern-only policy for an incomplete address.
    with gateway([event("Contact jane@exam"), event(finish="length"), b"data: [DONE]\n\n"]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == "Contact jane@exam"
        assert response.text.endswith("data: [DONE]\n\n")
        assert stream.closed


@pytest.mark.parametrize("bad", [
    {"stream": False}, {"stream": 1}, {"n": True}, {"n": 2}, {"tools": []},
    {"model": ""}, {"messages": [{"role": "user", "content": []}]},
])
def test_invalid_requests_never_contact_provider(bad):
    with gateway([]) as (http, _, calls):
        response = http.post("/v1/chat/completions", json={**REQUEST, **bad})
        assert response.status_code == 400
        assert response.json() == {"detail": "Unsupported generation request"}
        assert calls == []


@pytest.mark.parametrize("tail", [
    b"", b"data: broken\n\n", b"data: [DONE]\n\n",
    event(tool_calls=[{"arguments": "private-canary"}]),
    event(finish="tool_calls"), event("private-canary", reasoning_content="private-canary"),
    b'data: {"choices":[],"error":"private-canary"}\n\n',
    b'data: {"choices":[],"choices":[]}\n\n',
    b'data: {"choices":[],"created":NaN}\n\n',
    b'data: {"choices":[{"index":0,"delta":{"content":"\\ud800"}}]}\n\n',
])
def test_midstream_failure_discards_pending_and_never_sends_done(tail):
    with gateway([event("Safe! jane@exam"), tail]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == "Safe! "
        assert "event: error\n" in response.text
        assert "private-canary" not in response.text
        assert "jane" not in response.text
        assert "[DONE]" not in response.text
        assert stream.closed


@pytest.mark.parametrize("status,headers", [
    (429, None), (500, None), (307, {"location": "http://private.invalid"}),
    (200, {"content-type": "application/json"}),
    (200, {"content-type": "text/event-stream", "content-encoding": "gzip"}),
])
def test_reject_bad_headers_before_streaming_without_reading_body(status, headers):
    with gateway([b"private-canary"], status=status, headers=headers) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert response.status_code == 502
        assert response.json() == {"detail": "Provider unavailable"}
        assert stream.reads == 0
        assert stream.closed


def test_midstream_transport_error_is_sanitized():
    with gateway([event("Safe! jane@exam")], error=httpx.ReadError) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == "Safe! "
        assert "private-canary" not in response.text
        assert "[DONE]" not in response.text
        assert stream.closed


def test_request_event_and_pending_limits(monkeypatch):
    monkeypatch.setenv("STREAM_MAX_REQUEST_BYTES", "16")
    with gateway([]) as (http, _, calls):
        assert http.post("/v1/chat/completions", json=REQUEST).status_code == 413
        assert calls == []
    monkeypatch.setenv("STREAM_MAX_REQUEST_BYTES", "65536")
    monkeypatch.setenv("STREAM_MAX_EVENT_BYTES", "32")
    with gateway([b":" + b"x" * 40]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert "event: error" in response.text
        assert stream.closed


@pytest.mark.parametrize("wire", [
    b'{"stream":true,"stream":false}', b'{"messages":NaN}',
    b'{"messages":1e999}', b'{"model":"\\ud800"}', b"[" * 2000,
    b"\xff", '{"model":"mock-pii"}'.encode("utf-16"),
])
def test_bad_json_is_sanitized_and_never_forwarded(wire):
    with gateway([]) as (http, _, calls):
        response = http.post("/v1/chat/completions", content=wire)
        assert response.status_code == 400
        assert response.json() == {"detail": "Unsupported generation request"}
        assert calls == []


def test_role_usage_and_provider_metadata_are_not_reflected():
    role = event(role="assistant")
    usage = b'data: {"id":"private-canary","choices":[],"usage":{"prompt_tokens":1,"completion_tokens":1,"total_tokens":2}}\n\n'
    with gateway([role, event("Hi! "), event(finish="stop"), usage, b"data: [DONE]\n\n"]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == "Hi! "
        assert "private-canary" not in response.text
        assert "[DONE]" in response.text
        assert stream.closed


@pytest.mark.parametrize("error,status", [(httpx.ConnectError, 502), (httpx.ConnectTimeout, 504)])
def test_preheader_transport_errors(error, status):
    async def check():
        def fail(request):
            raise error("private-canary", request=request)
        async with httpx.AsyncClient(transport=httpx.MockTransport(fail)) as client:
            app = create_app(client=client)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app)) as caller:
                    response = await caller.post("http://gateway/v1/chat/completions", json=REQUEST)
                    assert response.status_code == status
                    assert "private-canary" not in response.text
    asyncio.run(check())


def test_no_request_cookies_auth_or_upstream_set_cookie_are_relayed():
    async def check():
        calls = []
        def handler(request):
            calls.append(request)
            return httpx.Response(200, headers={
                "content-type": "text/event-stream", "set-cookie": "private=other-user",
            }, stream=ByteStream([event("Hi! "), event(finish="stop"), b"data: [DONE]\n\n"]))
        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as client:
            app = create_app(client=client)
            async with app.router.lifespan_context(app):
                async with httpx.AsyncClient(transport=httpx.ASGITransport(app)) as caller:
                    for _ in range(2):
                        response = await caller.post("http://gateway/v1/chat/completions", json=REQUEST,
                            headers={"Cookie": "caller=private", "Authorization": "Bearer private", "X-Private": "secret"})
                        assert "set-cookie" not in response.headers
            assert not client.is_closed  # The application does not own injected clients.
        assert len(calls) == 2
        for request in calls:
            assert not {"cookie", "authorization", "x-private"} & set(request.headers)
            assert request.headers["accept-encoding"] == "identity"
    asyncio.run(check())


@pytest.mark.parametrize("asgi_version", ["2.3", "2.4"])
@pytest.mark.parametrize("termination", ["cancel", "deadline"])
def test_slow_asgi_consumer_applies_backpressure_and_cancellation_closes_upstream(monkeypatch, termination, asgi_version):
    if termination == "deadline":
        monkeypatch.setenv("STREAM_TOTAL_TIMEOUT_SECONDS", "0.05")
    async def check():
        source = ByteStream([event("Safe! ")] * 100)
        async with httpx.AsyncClient(transport=httpx.MockTransport(
            lambda request: httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=source),
        )) as client:
            app = create_app(client=client)
            waiting = asyncio.Event()
            never = asyncio.Event()
            received = False

            async def receive():
                nonlocal received
                if not received:
                    received = True
                    return {"type": "http.request", "body": json.dumps(REQUEST).encode(), "more_body": False}
                await never.wait()

            async def send(message):
                if message["type"] == "http.response.body" and message.get("body"):
                    waiting.set()
                    await never.wait()  # Simulate a blocked socket write.

            scope = {"type": "http", "asgi": {"version": "3.0", "spec_version": asgi_version},
                     "method": "POST", "path": "/v1/chat/completions", "query_string": b"",
                     "headers": [], "scheme": "http", "server": ("gateway", 80), "client": ("caller", 1)}
            async with app.router.lifespan_context(app):
                task = asyncio.create_task(app(scope, receive, send))
                try:
                    await asyncio.wait_for(waiting.wait(), 2)
                    assert source.reads == 1
                    assert not source.closed
                    if termination == "deadline":
                        try:
                            await asyncio.wait_for(asyncio.shield(task), 0.5)
                        except asyncio.CancelledError:
                            # Newer ASGI send paths surface the write's deadline
                            # as cancellation rather than a writable SSE error.
                            assert task.cancelled()
                        assert source.closed
                finally:
                    if not task.done():
                        task.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await task
                assert source.closed
    asyncio.run(check())


def test_header_send_failure_closes_upstream_without_starting_generator():
    async def check():
        source = ByteStream([])
        upstream = httpx.Response(200, stream=source)
        entered = False
        async def body():
            nonlocal entered
            entered = True
            yield b"never sent"
        async def receive():
            return {"type": "http.disconnect"}
        async def send(message):
            raise OSError("Simulated disconnected writer")
        response = ProviderStreamResponse(body(), upstream)
        with pytest.raises(ClientDisconnect):
            await response({"type": "http", "asgi": {"spec_version": "2.4"}}, receive, send)
        assert not entered
        assert upstream.is_closed and source.closed
    asyncio.run(check())


def test_owned_client_closes_on_shutdown(monkeypatch):
    original = httpx.AsyncClient
    created = []
    def make_client(**kwargs):
        assert kwargs == {"trust_env": False, "follow_redirects": False}
        client = original(transport=httpx.MockTransport(lambda request: httpx.Response(
            200, headers={"content-type": "text/event-stream"}, stream=ByteStream([
                event("Hi! "), event(finish="stop"), b"data: [DONE]\n\n",
            ]),
        )))
        created.append(client)
        return client
    monkeypatch.setattr(httpx, "AsyncClient", make_client)
    with TestClient(create_app()) as http:
        for _ in range(2):
            assert http.post("/v1/chat/completions", json=REQUEST).status_code == 200
        assert len(created) == 1 and not created[0].is_closed
    assert created[0].is_closed


@pytest.mark.parametrize("name,value", [
    ("STREAM_READ_TIMEOUT_SECONDS", "nan"), ("STREAM_TOTAL_TIMEOUT_SECONDS", "0"),
    ("STREAM_MAX_EVENT_BYTES", "-1"), ("STREAM_MAX_PENDING_CHARS", "1.5"),
    ("STREAM_UPSTREAM_URL", "file:///tmp/private"), ("STREAM_UPSTREAM_URL", "http://user:secret@host/path"),
])
def test_invalid_configuration_fails_startup(monkeypatch, name, value):
    monkeypatch.setenv(name, value)
    with pytest.raises(ValueError):
        create_app()


def test_pending_limit_fails_closed_in_gateway(monkeypatch):
    monkeypatch.setenv("STREAM_MAX_EVENT_BYTES", "65536")
    monkeypatch.setenv("STREAM_MAX_PENDING_CHARS", "16")
    with gateway([event("x" * 17)]) as (http, stream, _):
        response = http.post("/v1/chat/completions", json=REQUEST)
        assert output_text(response) == ""
        assert "event: error" in response.text
        assert stream.closed


def test_long_stream_keeps_event_and_pending_windows_bounded():
    parser = SSEParser(256)
    redactor = StreamingRedactor(32)
    wire = event("Safe! ")
    for _ in range(10000):
        for data in parser.feed(wire):
            text, finish = parse_delta(data)
            assert finish is None
            assert redactor.feed(text) == "Safe! "
        assert redactor.pending_chars == 0
    assert list(parser.finish()) == []
    assert redactor.finish() == ""
    assert parser.peak_event_bytes == len(wire)
    assert redactor.peak_pending_chars == 5
    print(f"\n10000 events: peak event={parser.peak_event_bytes} bytes; "
          f"peak pending={redactor.peak_pending_chars} characters")
