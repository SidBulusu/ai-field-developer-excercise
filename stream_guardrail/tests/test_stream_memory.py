"""Isolated gateway allocation measurements and real socket backpressure.

Profiling endpoints and load generation exist only in this test module. They are
never mounted by the production app factory or normal mock provider.
"""

import asyncio
import gc
import json
import resource
import socket
import sys
import time
import tracemalloc

import httpx
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from stream_guardrail.app import create_app, encode_delta
from stream_guardrail.tests.test_http_streaming import payload, serve, serve_cli


def profiled_gateway():
    app = create_app()

    @app.post("/test/profile/start")
    async def start():
        tracemalloc.stop()
        gc.collect()
        tracemalloc.start()
        return {"started": True}

    @app.get("/test/profile")
    async def report():
        gc.collect()
        current, peak = tracemalloc.get_traced_memory()
        tracemalloc.stop()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        return {"current": current, "peak": peak,
                "rss_high_water_bytes": rss if sys.platform == "darwin" else rss * 1024}

    return app


def load_provider():
    app = FastAPI()
    app.state.generated = 0
    app.state.active = 0
    app.state.completed = 0
    frame = encode_delta("Safe! jane@example.com! ")

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        generation = await request.json()
        count = {"short": 1024, "long": 16384, "pressure": 1000000}[generation["model"]]

        async def body():
            app.state.active += 1
            try:
                for _ in range(count):
                    app.state.generated += 1
                    yield frame
                    # Uvicorn sends can complete synchronously, including after
                    # a socket closes. Let its disconnect listener run too.
                    await asyncio.sleep(0)
                yield encode_delta(finish="stop")
                yield b"data: [DONE]\n\n"
                app.state.completed += 1
            finally:
                app.state.active -= 1

        return StreamingResponse(body(), media_type="text/event-stream")

    return app


def test_long_http_stream_allocation_peak_does_not_scale_with_response_length():
    import os

    provider = load_provider()
    with serve(provider) as provider_url:
        env = dict(os.environ, STREAM_UPSTREAM_URL=provider_url + "/v1/chat/completions",
                   STREAM_TOTAL_TIMEOUT_SECONDS="120", STREAM_READ_TIMEOUT_SECONDS="15")
        with serve_cli("stream_guardrail.tests.test_stream_memory:profiled_gateway", env) as url:
            async def check():
                async with httpx.AsyncClient(trust_env=False, timeout=120) as client:
                    # Warm lazy HTTP imports/caches outside the traced samples.
                    async with client.stream("POST", url + "/v1/chat/completions", json=payload("short")) as warmup:
                        async for _ in warmup.aiter_raw():
                            pass
                    reports = []
                    for model, expected_count in [("short", 1024), ("long", 16384)]:
                        assert (await client.post(url + "/test/profile/start")).status_code == 200
                        count = 0
                        wire_bytes = 0
                        done = False
                        started = time.monotonic()
                        async with client.stream("POST", url + "/v1/chat/completions", json=payload(model)) as response:
                            assert response.status_code == 200
                            async for line in response.aiter_lines():
                                wire_bytes += len(line.encode()) + 1
                                if line == "data: [DONE]":
                                    done = True
                                elif line.startswith("data: "):
                                    delta = json.loads(line[6:])["choices"][0]["delta"]
                                    if delta:
                                        assert delta["content"] == "Safe! [REDACTED]! "
                                        count += 1
                        assert done and count == expected_count
                        report = (await client.get(url + "/test/profile")).json()
                        reports.append(report)
                        print(f"\n{model}: {count} events, {wire_bytes} output bytes, "
                              f"{time.monotonic() - started:.2f}s, gateway {report}")
                    # 16x more output must not retain a response-sized buffer.
                    # Tolerance covers HTTP/allocator batch variation, not a
                    # universal per-request memory ceiling.
                    assert reports[1]["peak"] < reports[0]["peak"] + 512 * 1024
                    assert reports[1]["current"] < reports[0]["current"] + 256 * 1024
            asyncio.run(check())


def test_stalled_and_throttled_socket_reader_backpressures_provider(monkeypatch):
    provider = load_provider()
    with serve(provider, send_buffer=8192) as provider_url:
        monkeypatch.setenv("STREAM_UPSTREAM_URL", provider_url + "/v1/chat/completions")
        monkeypatch.setenv("STREAM_TOTAL_TIMEOUT_SECONDS", "15")
        with serve(create_app(), send_buffer=8192) as url:
            with socket.socket() as reader:
                reader.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                reader.settimeout(2)
                reader.connect(("127.0.0.1", int(url.rsplit(":", 1)[1])))
                body = json.dumps(payload("pressure")).encode()
                reader.sendall(b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\n"
                               b"Content-Type: application/json\r\nContent-Length: "
                               + str(len(body)).encode() + b"\r\n\r\n" + body)
                # Leave the client unread until bounded transport buffers fill.
                deadline = time.monotonic() + 5
                unchanged = 0
                previous = -1
                while unchanged < 5:
                    assert time.monotonic() < deadline, "Provider never backpressured"
                    time.sleep(0.05)
                    current = provider.state.generated
                    unchanged = unchanged + 1 if current == previous and current > 0 else 0
                    previous = current
                stalled_at = provider.state.generated
                assert stalled_at < 1000000 and provider.state.completed == 0
                received = bytearray()
                for _ in range(50):
                    received.extend(reader.recv(1024))
                    time.sleep(0.01)
                assert b"[REDACTED]" in received
                assert b"jane@example.com" not in received
                assert provider.state.completed == 0
                print(f"\nSocket pressure: stalled after {stalled_at} provider events; "
                      f"read {len(received)} bytes over 50 throttled reads")
            deadline = time.monotonic() + 2
            while provider.state.active:
                assert time.monotonic() < deadline, "Disconnect did not close loaded provider"
                time.sleep(0.01)
