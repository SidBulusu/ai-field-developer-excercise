"""Actual loopback HTTP: startup, early safe text, errors and disconnects."""

import asyncio
import json
import os
import signal
import socket
import subprocess
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path

import httpx
import pytest
import uvicorn
from fastapi import FastAPI
from fastapi.responses import StreamingResponse

from stream_guardrail.app import create_app, encode_delta
from stream_guardrail.mock_provider import create_app as create_mock


@contextmanager
def serve(app, send_buffer=None):
    with socket.socket() as listener:
        if send_buffer is not None:
            listener.setsockopt(socket.SOL_SOCKET, socket.SO_SNDBUF, send_buffer)
        listener.bind(("127.0.0.1", 0))
        address = f"http://127.0.0.1:{listener.getsockname()[1]}"
        server = uvicorn.Server(uvicorn.Config(
            app, log_level="error", access_log=False, loop="asyncio",
            ws="none", timeout_graceful_shutdown=1,
        ))
        thread = threading.Thread(target=server.run, kwargs={"sockets": [listener]}, daemon=True)
        thread.start()
        try:
            deadline = time.monotonic() + 5
            while not server.started:
                assert thread.is_alive() and time.monotonic() < deadline, "Server failed startup"
                time.sleep(0.01)
            yield address
        finally:
            server.should_exit = True
            thread.join(timeout=5)
            assert not thread.is_alive(), "Server did not shut down"


def payload(model="mock-pii"):
    return {"model": model, "stream": True, "messages": [{"role": "user", "content": "Synthetic demo"}]}


def test_real_http_redaction_and_time_to_first_safe_text(monkeypatch):
    provider = create_mock()
    with serve(provider) as provider_url:
        monkeypatch.setenv("STREAM_UPSTREAM_URL", provider_url + "/v1/chat/completions")
        with serve(create_app()) as url:
            async def check():
                async with httpx.AsyncClient(trust_env=False) as client:
                    started = time.monotonic()
                    first_safe = None
                    text = ""
                    done = False
                    async with client.stream("POST", url + "/v1/chat/completions", json=payload()) as response:
                        assert response.status_code == 200
                        async for line in response.aiter_lines():
                            if line == "data: [DONE]":
                                done = True
                            elif line.startswith("data: "):
                                content = json.loads(line[6:])["choices"][0]["delta"].get("content", "")
                                if content and first_safe is None:
                                    first_safe = time.monotonic() - started
                                    assert provider.state.completed_streams == 0
                                text += content
                    elapsed = time.monotonic() - started
                    assert done and text == (
                        "Mock output! Contact [REDACTED]; SSN: [REDACTED]; "
                        "card: [REDACTED]. café 😀!"
                    )
                    assert first_safe is not None and first_safe < elapsed
                    print(f"\nLoopback TTFT (safe text): {first_safe:.4f}s; total: {elapsed:.4f}s")
                    assert provider.state.active_streams == 0
                    for model, status in [("mock-reject", 502), ("mock-truncate", 200), ("mock-malformed", 200)]:
                        failed = await client.post(url + "/v1/chat/completions", json=payload(model))
                        assert failed.status_code == status
                        if status == 200:
                            assert "event: error" in failed.text
                            assert "[DONE]" not in failed.text
                            assert "jane" not in failed.text
            asyncio.run(check())


def test_disconnect_closes_provider_while_it_is_paused(monkeypatch):
    provider = create_mock()
    with serve(provider) as provider_url:
        monkeypatch.setenv("STREAM_UPSTREAM_URL", provider_url + "/v1/chat/completions")
        with serve(create_app()) as url:
            async def check():
                async with httpx.AsyncClient(trust_env=False) as client:
                    async with client.stream("POST", url + "/v1/chat/completions", json=payload("mock-slow")) as response:
                        async for line in response.aiter_lines():
                            if '"content": "Mock output! "' in line:
                                break
                        assert provider.state.active_streams == 1
                    deadline = time.monotonic() + 2
                    while provider.state.active_streams:
                        assert time.monotonic() < deadline, "Provider stayed open after disconnect"
                        await asyncio.sleep(0.01)
                    assert provider.state.cancelled_streams == 1
                    assert provider.state.completed_streams == 0
            asyncio.run(check())


def test_cr_only_event_reaches_client_while_provider_waits(monkeypatch):
    provider = FastAPI()
    release = threading.Event()

    @provider.post("/v1/chat/completions")
    async def completions():
        async def body():
            yield encode_delta("Safe! ").replace(b"\n", b"\r")
            while not release.is_set():
                await asyncio.sleep(0.01)
            yield encode_delta(finish="stop").replace(b"\n", b"\r")
            yield b"data: [DONE]\r\r"
        return StreamingResponse(body(), media_type="text/event-stream")

    with serve(provider) as provider_url:
        monkeypatch.setenv("STREAM_UPSTREAM_URL", provider_url + "/v1/chat/completions")
        with serve(create_app()) as url:
            async def check():
                async with httpx.AsyncClient(trust_env=False, timeout=2) as client:
                    done = False
                    try:
                        async with client.stream("POST", url + "/v1/chat/completions", json=payload()) as response:
                            async for line in response.aiter_lines():
                                if line == "data: [DONE]":
                                    done = True
                                elif line.startswith("data: "):
                                    delta = json.loads(line[6:])["choices"][0]["delta"]
                                    if delta:
                                        assert delta["content"] == "Safe! "
                                        assert not release.is_set()
                                        release.set()
                    finally:
                        release.set()
                    assert done
            asyncio.run(check())


@pytest.mark.parametrize("setting", ["STREAM_TOTAL_TIMEOUT_SECONDS", "STREAM_READ_TIMEOUT_SECONDS"])
def test_deadline_terminates_paused_provider(monkeypatch, setting):
    provider = create_mock()
    with serve(provider) as provider_url:
        monkeypatch.setenv("STREAM_UPSTREAM_URL", provider_url + "/v1/chat/completions")
        monkeypatch.setenv(setting, "0.15")
        with serve(create_app()) as url:
            with httpx.Client(trust_env=False) as client:
                response = client.post(url + "/v1/chat/completions", json=payload("mock-slow"))
                assert response.status_code == 200
                assert "event: error" in response.text
                assert "[DONE]" not in response.text
                deadline = time.monotonic() + 2
                while provider.state.active_streams:
                    assert time.monotonic() < deadline
                    time.sleep(0.01)


@contextmanager
def serve_cli(module, env):
    with socket.socket() as reservation:
        reservation.bind(("127.0.0.1", 0))
        port = reservation.getsockname()[1]
    command = [sys.executable, "-m", "uvicorn", module, "--factory", "--host",
               "127.0.0.1", "--port", str(port), "--no-access-log", "--log-level", "error"]
    process = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[2], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    url = f"http://127.0.0.1:{port}"
    try:
        with httpx.Client(trust_env=False, timeout=0.2) as client:
            deadline = time.monotonic() + 5
            while True:
                assert process.poll() is None, "CLI server exited early"
                try:
                    client.options(url + "/v1/chat/completions")
                    break
                except httpx.TransportError:
                    assert time.monotonic() < deadline, "CLI startup timed out"
                    time.sleep(0.01)
        yield url
    finally:
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError("CLI server failed shutdown") from None
        # Uvicorn re-raises the captured shutdown signal after graceful cleanup.
        assert process.returncode in (0, -signal.SIGTERM), stderr.decode()


def test_documented_uvicorn_factories_start_in_separate_processes():
    env = dict(os.environ, MOCK_STREAM_DELAY_SECONDS="0.001")
    with serve_cli("stream_guardrail.mock_provider:create_app", env) as provider_url:
        env["STREAM_UPSTREAM_URL"] = provider_url + "/v1/chat/completions"
        with serve_cli("stream_guardrail.app:create_app", env) as url:
            with httpx.Client(trust_env=False) as client:
                response = client.post(url + "/v1/chat/completions", json=payload())
            assert response.status_code == 200
            assert "[REDACTED]" in response.text
            assert "jane@example.com" not in response.text
            assert response.text.endswith("data: [DONE]\n\n")
