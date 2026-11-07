"""Exercise routing over real loopback sockets using the existing interpreter.

Process isolation exposes transport deadlines and persisted crash/restart state
that an in-process mock cannot establish; databases and keys are disposable.
"""

import asyncio
import os
import signal
import socket
import sqlite3
import subprocess
import sys
import time
from contextlib import contextmanager
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import httpx
import pytest
from fastapi import FastAPI
from fastapi.responses import StreamingResponse


@contextmanager
def service(module, env, *, allow_crash=False):
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        port = listener.getsockname()[1]
    command = [sys.executable, "-m", "uvicorn", module, "--factory", "--host", "127.0.0.1",
               "--port", str(port), "--no-access-log", "--log-level", "error"]
    process = subprocess.Popen(command, cwd=Path(__file__).resolve().parents[2], env=env,
                               stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    url = f"http://127.0.0.1:{port}"
    try:
        deadline = time.monotonic() + 5
        with httpx.Client(trust_env=False, timeout=0.2) as client:
            while True:
                assert process.poll() is None, "Service failed startup"
                try:
                    client.options(url + "/v1/chat/completions")
                    break
                except httpx.TransportError:
                    assert time.monotonic() < deadline, "Service failed readiness"
                    time.sleep(0.01)
        yield (url, process) if allow_crash else url
    finally:
        process.terminate()
        try:
            _, stderr = process.communicate(timeout=5)
        except subprocess.TimeoutExpired:
            process.kill()
            process.communicate()
            raise AssertionError("Service failed shutdown") from None
        allowed = (0, -signal.SIGTERM, -signal.SIGKILL) if allow_crash else (0, -signal.SIGTERM)
        assert process.returncode in allowed, stderr.decode()


@pytest.mark.parametrize("mode", ["ok", "429", "timeout", "trickle"])
def test_real_http_primary_and_backup_and_restart(tmp_path, mode):
    path = tmp_path / "usage.db"
    key = "test-http-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    env = dict(os.environ, ROUTER_DB_PATH=str(path), ROUTER_MOCK_PRIMARY_MODE=mode,
               ROUTER_API_KEYS=key, ROUTER_TOKEN_LIMIT="102", ROUTER_WINDOW_MS="60000",
               ROUTER_PRIMARY_TIMEOUT_SECONDS="3", ROUTER_MOCK_DELAY_SECONDS="3.2",
               ROUTER_MOCK_BACKUP_MODE="ok")
    with service("model_router.mock_provider:create_primary_app", env) as primary:
        with service("model_router.mock_provider:create_backup_app", env) as backup:
            env.update(ROUTER_PRIMARY_URL=primary + "/v1/chat/completions",
                       ROUTER_BACKUP_URL=backup + "/v1/chat/completions")
            with service("model_router.app:create_app", env) as router:
                started = time.monotonic()
                with httpx.Client(trust_env=False, timeout=10) as client:
                    response = client.post(router + "/v1/chat/completions", headers={"X-API-Key": key},
                                           json={"messages": [{"role": "user", "content": "é"}], "max_completion_tokens": 100})
                elapsed = time.monotonic() - started
                assert response.status_code == 200
                expected = "primary" if mode == "ok" else "backup"
                assert response.json()["route"] == expected
                assert response.json()["choices"][0]["message"]["content"] == f"Mock {expected} response."
                assert response.json()["usage"]["prompt_tokens"] == 2
                if mode in {"timeout", "trickle"}:
                    assert 2.8 <= elapsed < 7
                print(f"\nPrimary {mode}: routed to {expected} in {elapsed:.3f}s")
            # Reopening after the service process exits proves on-disk state.
            with sqlite3.connect(path) as connection:
                rows = connection.execute("SELECT charged_tokens, state FROM reservations ORDER BY rowid").fetchall()
                actual = response.json()["usage"]["total_tokens"]
                assert rows == ([(102, "uncertain")] if mode in {"timeout", "trickle"} else [(actual, "settled")])
            with service("model_router.app:create_app", env) as restarted:
                with httpx.Client(trust_env=False) as client:
                    denied = client.post(restarted + "/v1/chat/completions", headers={"X-API-Key": key},
                                         json={"messages": [{"role": "user", "content": "é"}], "max_completion_tokens": 100})
                assert denied.status_code == 429
                assert denied.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"


def hanging_provider():
    """Test-only provider exposes attempt count so a kill occurs during I/O."""
    app = FastAPI()
    app.state.calls = 0

    @app.get("/test/calls")
    async def calls():
        return {"calls": app.state.calls}

    @app.post("/v1/chat/completions")
    async def completions():
        app.state.calls += 1
        async def body():
            for _ in range(200):
                yield b" "
                await asyncio.sleep(0.01)
        return StreamingResponse(body(), media_type="application/json")

    return app


def test_hard_kill_during_provider_io_preserves_pending_charge(tmp_path):
    path = tmp_path / "usage.db"
    key = "test-crash-key-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
    body = {"messages": [{"role": "user", "content": "x"}], "max_completion_tokens": 100}
    env = dict(os.environ, ROUTER_API_KEYS=key, ROUTER_DB_PATH=str(path),
               ROUTER_TOKEN_LIMIT="101", ROUTER_WINDOW_MS="60000", ROUTER_PRIMARY_TIMEOUT_SECONDS="3")

    with service("model_router.tests.test_http:hanging_provider", env) as provider:
        env.update(ROUTER_PRIMARY_URL=provider + "/v1/chat/completions",
                   ROUTER_BACKUP_URL=provider + "/v1/chat/completions")
        with service("model_router.app:create_app", env, allow_crash=True) as (url, process):
            def request():
                try:
                    with httpx.Client(trust_env=False, timeout=5) as client:
                        return client.post(url + "/v1/chat/completions", json=body, headers={"X-API-Key": key})
                except httpx.HTTPError:
                    return None
            with ThreadPoolExecutor(max_workers=1) as pool:
                pending = pool.submit(request)
                with httpx.Client(trust_env=False) as client:
                    deadline = time.monotonic() + 2
                    while client.get(provider + "/test/calls").json()["calls"] != 1:
                        assert time.monotonic() < deadline
                        time.sleep(0.01)
                with sqlite3.connect(path) as db:
                    assert db.execute("SELECT charged_tokens, state FROM reservations").fetchall() == [(101, "pending")]
                # Kill only the disposable gateway child, after admission has
                # committed and the provider has begun. No finally can reconcile.
                process.kill()
                process.wait(timeout=2)
                assert process.returncode == -signal.SIGKILL
                assert pending.result(timeout=2) is None
        with service("model_router.app:create_app", env) as restarted:
            with httpx.Client(trust_env=False) as client:
                denied = client.post(restarted + "/v1/chat/completions", json=body, headers={"X-API-Key": key})
                assert denied.status_code == 429
                assert denied.json()["error"]["code"] == "RATE_LIMIT_EXCEEDED"
                assert client.get(provider + "/test/calls").json()["calls"] == 1
