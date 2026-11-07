"""Repeatable local performance evidence, not deployment capacity guarantees.

Timing budgets catch event-loop monopolization with default limits. They are
deliberately loose for shared development machines, not provider latency SLAs.
"""

import asyncio
import gc
import json
import os
import resource
import socket
import statistics
import sys
import time
import tracemalloc
from contextlib import asynccontextmanager

import httpx
import pytest
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse

from stream_guardrail.app import create_app, encode_delta
from stream_guardrail.redactor import StreamingRedactor, redact_complete
from stream_guardrail.tests.test_http_streaming import payload, serve, serve_cli
from stream_guardrail.tests.test_streaming import ByteStream, output_text


WORKLOADS = {
    "prose": "Safe text! ",
    "local_part": "a" * 250 + "! ",
    "domain": "a@" + "b." * 120 + "co! ",
    "overlap": "a@b.co@c.com@d.net! ",
    "numeric": "1 " * 18 + "1! ",
}


@pytest.mark.parametrize("name", WORKLOADS)
def test_matching_throughput_at_default_pending_limit(name):
    unit = WORKLOADS[name]
    reports = []
    for target in (4096, 16384, 60000):
        text = unit * (target // len(unit))
        expected = redact_complete(text)
        samples = []
        for _ in range(3):
            redactor = StreamingRedactor()
            started = time.perf_counter()
            result = redactor.feed(text) + redactor.finish()
            samples.append(time.perf_counter() - started)
            assert result == expected
            assert redactor.peak_pending_chars <= 256
        median = statistics.median(samples)
        reports.append({"chars": len(text), "median_ms": round(median * 1000, 2),
                        "chars_per_second": round(len(text) / median)})
    # Report rather than assert scaling ratios: scheduler noise at the smallest
    # sample would make a numerical complexity claim misleading.
    print(f"\nMatching {name}: {json.dumps(reports)}")


@pytest.mark.parametrize("mode", ["success", "deadline", "cancel"])
def test_large_valid_delta_yields_to_heartbeat_and_deadline(monkeypatch, mode):
    if mode == "deadline":
        monkeypatch.setenv("STREAM_TOTAL_TIMEOUT_SECONDS", "0.05")

    async def check():
        text = WORKLOADS["local_part"] * 238
        stream = ByteStream([encode_delta(text), encode_delta(finish="stop"), b"data: [DONE]\n\n"])
        gaps = []
        running = True

        async def heartbeat():
            previous = time.perf_counter()
            while running:
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                gaps.append(now - previous)
                previous = now

        async def handler(_):
            return httpx.Response(200, headers={"content-type": "text/event-stream"}, stream=stream)

        async with httpx.AsyncClient(transport=httpx.MockTransport(handler)) as upstream:
            app = create_app(client=upstream)
            async with app.router.lifespan_context(app), httpx.AsyncClient(
                transport=httpx.ASGITransport(app), base_url="http://gateway"
            ) as client:
                monitor = asyncio.create_task(heartbeat())
                await asyncio.sleep(0)
                started = time.perf_counter()
                try:
                    if mode == "cancel":
                        request = asyncio.create_task(client.post("/v1/chat/completions", json=payload()))
                        await asyncio.sleep(0.05)
                        request.cancel()
                        with pytest.raises(asyncio.CancelledError):
                            await request
                    else:
                        response = await client.post("/v1/chat/completions", json=payload())
                    elapsed = time.perf_counter() - started
                    await asyncio.sleep(0.01)
                finally:
                    running = False
                    await monitor
        assert stream.closed
        assert max(gaps) < 0.1, f"Event loop stalled {max(gaps):.3f}s"
        if mode == "cancel":
            assert elapsed < 0.2
            assert stream.reads == 1  # Finish/DONE were not consumed after cancellation.
        elif mode == "deadline":
            assert "[DONE]" not in response.text
            assert "event: error" in response.text
            assert elapsed < 0.2
        else:
            assert response.text.endswith("data: [DONE]\n\n")
            assert output_text(response) == text
        print(f"\nLarge delta {mode}: total={elapsed:.3f}s, "
              f"max heartbeat gap={max(gaps) * 1000:.2f}ms")

    asyncio.run(check())


def measured_gateway():
    """Test-only metrics in the gateway process exclude client/provider memory."""
    app = create_app()
    active = 0
    max_gap = 0.0
    original_lifespan = app.router.lifespan_context

    @asynccontextmanager
    async def lifespan(app):
        async def heartbeat():
            nonlocal max_gap
            previous = time.perf_counter()
            while True:
                await asyncio.sleep(0.005)
                now = time.perf_counter()
                max_gap = max(max_gap, now - previous)
                previous = now

        async with original_lifespan(app):
            task = asyncio.create_task(heartbeat())
            try:
                yield
            finally:
                task.cancel()
                await asyncio.gather(task, return_exceptions=True)
                tracemalloc.stop()

    app.router.lifespan_context = lifespan

    @app.post("/test/metrics/start")
    async def start():
        nonlocal max_gap
        tracemalloc.stop()
        gc.collect()
        tracemalloc.start()
        max_gap = 0.0
        return {"started": True}

    @app.get("/test/metrics")
    async def metrics(finish: bool = False):
        nonlocal max_gap
        # Collection belongs outside the timed stream workloads. Retained
        # memory after cleanup is more useful than transient garbage here.
        gc.collect()
        current, peak = tracemalloc.get_traced_memory()
        rss = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        result = {"active": active, "current": current, "peak": peak,
                  "rss_high_water_bytes": rss if sys.platform == "darwin" else rss * 1024,
                  "max_heartbeat_gap_ms": round(max_gap * 1000, 2)}
        max_gap = 0.0
        if finish:
            tracemalloc.stop()
        return result

    async def instrumented(scope, receive, send):
        nonlocal active
        streaming = scope["type"] == "http" and scope["path"] == "/v1/chat/completions"
        if streaming:
            active += 1
        try:
            await app(scope, receive, send)
        finally:
            # Count through response cleanup, not merely until headers return.
            if streaming:
                active -= 1

    return instrumented


def concurrent_provider():
    app = FastAPI()
    app.state.active = 0
    app.state.pressure_generated = 0
    frame = encode_delta("Safe! jane@example.com! ")
    pressure_frame = encode_delta("Safe! jane@example.com! " * 64)
    cpu_frame = encode_delta(WORKLOADS["local_part"] * 238)

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        model = (await request.json())["model"]
        pressure = model == "pressure"
        cpu = model == "cpu"

        async def body():
            app.state.active += 1
            try:
                for _ in range(1000000 if pressure else 1 if cpu else 80):
                    if pressure:
                        app.state.pressure_generated += 1
                    yield pressure_frame if pressure else cpu_frame if cpu else frame
                    await asyncio.sleep(0.001 if pressure else 0.005)
                yield encode_delta(finish="stop")
                yield b"data: [DONE]\n\n"
            finally:
                app.state.active -= 1

        return StreamingResponse(body(), media_type="text/event-stream")

    return app


def test_concurrent_http_streams_with_stalled_reader():
    provider = concurrent_provider()
    with serve(provider, send_buffer=8192) as provider_url:
        env = dict(os.environ, STREAM_UPSTREAM_URL=provider_url + "/v1/chat/completions",
                   STREAM_TOTAL_TIMEOUT_SECONDS="60", STREAM_READ_TIMEOUT_SECONDS="15",
                   STREAM_MAX_PENDING_CHARS="256", STREAM_MAX_EVENT_BYTES="65536")
        with serve_cli("stream_guardrail.tests.test_performance:measured_gateway", env) as url:
            async def check():
                async with httpx.AsyncClient(trust_env=False, timeout=20) as client:
                    async def consume(model="mock-pii"):
                        started = time.perf_counter()
                        first = None
                        count = 0
                        chars = 0
                        done = False
                        expected = WORKLOADS["local_part"] * 238 if model == "cpu" else None
                        async with client.stream("POST", url + "/v1/chat/completions", json=payload(model)) as response:
                            assert response.status_code == 200
                            async for line in response.aiter_lines():
                                if line == "data: [DONE]":
                                    done = True
                                elif line.startswith("data: "):
                                    delta = json.loads(line[6:])["choices"][0]["delta"]
                                    if delta:
                                        content = delta["content"]
                                        if expected is None:
                                            assert content == "Safe! [REDACTED]! "
                                        else:
                                            assert content == expected[chars:chars + len(content)]
                                        chars += len(content)
                                        first = first if first is not None else time.perf_counter() - started
                                        count += 1
                        assert done and first is not None
                        assert count == 80 if expected is None else chars == len(expected)
                        return {"first_ms": round(first * 1000, 2),
                                "total_ms": round((time.perf_counter() - started) * 1000, 2)}

                    await consume()  # Exclude lazy imports and first connection setup.
                    assert (await client.post(url + "/test/metrics/start")).status_code == 200
                    baseline = (await client.get(url + "/test/metrics")).json()
                    alone = await consume()
                    print(f"\nConcurrent baseline: {alone}")
                    with socket.socket() as stalled:
                        stalled.setsockopt(socket.SOL_SOCKET, socket.SO_RCVBUF, 4096)
                        stalled.setblocking(False)
                        loop = asyncio.get_running_loop()
                        await loop.sock_connect(stalled, ("127.0.0.1", int(url.rsplit(":", 1)[1])))
                        body = json.dumps(payload("pressure")).encode()
                        await loop.sock_sendall(stalled,
                            b"POST /v1/chat/completions HTTP/1.1\r\nHost: localhost\r\n"
                            b"Content-Type: application/json\r\nContent-Length: "
                            + str(len(body)).encode() + b"\r\n\r\n" + body)
                        deadline = time.perf_counter() + 8
                        previous = -1
                        unchanged = 0
                        while unchanged < 5:
                            assert time.perf_counter() < deadline, "Stalled reader did not backpressure"
                            await asyncio.sleep(0.05)
                            current = provider.state.pressure_generated
                            unchanged = unchanged + 1 if current == previous and current > 0 else 0
                            previous = current
                        assert provider.state.active == 1
                        print(f"\nStalled reader backpressured at {previous} provider events")
                        for concurrency in (1, 4, 8):
                            # Reset between workloads; reported heartbeat gaps
                            # still include scheduling/instrumentation overhead.
                            await client.get(url + "/test/metrics")
                            results = await asyncio.gather(*(consume() for _ in range(concurrency)))
                            # Receiving DONE can precede server-side aclose.
                            # Observe cleanup rather than assuming these two
                            # independently scheduled tasks finish together.
                            cleanup_deadline = time.perf_counter() + 2
                            reports = []
                            while True:
                                report = (await client.get(url + "/test/metrics")).json()
                                reports.append(report)
                                if report["active"] == 1 and provider.state.active == 1:
                                    break
                                assert time.perf_counter() < cleanup_deadline, "Completed streams stayed active"
                                await asyncio.sleep(0.01)
                            report["max_heartbeat_gap_ms"] = max(r["max_heartbeat_gap_ms"] for r in reports)
                            assert report["max_heartbeat_gap_ms"] < 100
                            assert max(item["first_ms"] for item in results) < 1000
                            assert max(item["total_ms"] for item in results) < 3000
                            print(f"\nConcurrent {concurrency} + stalled: {json.dumps(results)}; gateway {report}")
                    started = time.perf_counter()
                    deadline = started + 2
                    while True:
                        report = (await client.get(url + "/test/metrics")).json()
                        if report["active"] == 0 and provider.state.active == 0:
                            break
                        assert time.perf_counter() < deadline, "Disconnect retained active stream"
                        await asyncio.sleep(0.01)
                    cleanup = time.perf_counter() - started
                    report = (await client.get(url + "/test/metrics?finish=true")).json()
                    # These regression tolerances allow connection caches and
                    # transport buffers; they are not per-request memory limits.
                    assert report["current"] < baseline["current"] + 1024 * 1024
                    assert report["peak"] < baseline["current"] + 8 * 1024 * 1024
                    print(f"\nConcurrent cleanup: {cleanup * 1000:.2f}ms; baseline {baseline}; final {report}")
                    # Allocation tracing multiplies regex cost. Measure CPU
                    # fairness separately, with profiling stopped, so these
                    # timings describe the normal application execution path.
                    for concurrency in (1, 4, 8):
                        await client.get(url + "/test/metrics")
                        results = await asyncio.gather(*(consume("cpu") for _ in range(concurrency)))
                        report = (await client.get(url + "/test/metrics")).json()
                        print(f"\nConcurrent CPU {concurrency}: {json.dumps(results)}; gateway {report}")
                        assert report["max_heartbeat_gap_ms"] < 100
                        assert max(item["first_ms"] for item in results) < 1000
                        assert max(item["total_ms"] for item in results) < 6000

            asyncio.run(check())
