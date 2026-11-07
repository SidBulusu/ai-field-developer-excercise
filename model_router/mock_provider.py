"""Deterministic byte-token providers, with no hosted model or real billing.

Share the gateway's mock counter to make expected charges reproducible. Scripted
usage and timing failures exercise accounting and routing without external APIs.
"""

import asyncio
import os

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, Response, StreamingResponse
from pydantic import Field

from model_router.app import GenerationRequest, count_prompt_tokens, mock_token_count, positive_setting, strict_json


class MockRequest(GenerationRequest):
    model: str = Field(min_length=1, max_length=128)


def create_provider(role: str) -> FastAPI:
    mode = os.getenv(f"ROUTER_MOCK_{role.upper()}_MODE", "ok")
    if mode not in {"ok", "429", "timeout", "late", "trickle", "malformed", "error",
                    "overspent", "missing_usage", "bad_usage", "hidden_usage"}:
        raise ValueError("Unsupported mock provider mode")
    model = os.getenv(f"ROUTER_{role.upper()}_MODEL", f"mock-{role}")
    delay = positive_setting("ROUTER_MOCK_DELAY_SECONDS", "3.2")
    request_limit = positive_setting("ROUTER_MAX_REQUEST_BYTES", "65536", integer=True)
    upload_timeout = positive_setting("ROUTER_REQUEST_TIMEOUT_SECONDS", "5")
    app = FastAPI()

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        body = bytearray()
        try:
            async with asyncio.timeout(upload_timeout):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > request_limit:
                        return JSONResponse({"detail": "Mock request too large"}, status_code=413)
                    body.extend(chunk)
            generation = MockRequest.model_validate(strict_json(bytes(body)))
            if generation.stream or generation.model != model:
                raise ValueError
            prompt_tokens = count_prompt_tokens(generation.messages)
        except TimeoutError:
            return JSONResponse({"detail": "Mock request timeout"}, status_code=408)
        except (ValueError, UnicodeError, RecursionError):
            return JSONResponse({"detail": "Invalid mock request"}, status_code=400)
        if mode == "429":
            return JSONResponse({"detail": "Simulated provider rejection"}, status_code=429)
        if mode == "error":
            return JSONResponse({"detail": "Simulated provider failure"}, status_code=500)
        if mode in {"timeout", "late"}:
            await asyncio.sleep(delay)
        if mode == "malformed":
            return Response(b"simulated-invalid-json", media_type="application/json")
        scripted_text = f"Mock {role} response."
        text = scripted_text[:generation.max_completion_tokens]
        if mode == "overspent":
            text = "x" * (generation.max_completion_tokens + 1)
        completion_tokens = mock_token_count(text) + (3 if mode == "hidden_usage" else 0)
        result = {"choices": [{"index": 0, "message": {"role": "assistant", "content": text},
                               "finish_reason": "length" if len(scripted_text) > generation.max_completion_tokens else "stop"}],
                  "usage": {"prompt_tokens": prompt_tokens, "completion_tokens": completion_tokens,
                            "total_tokens": prompt_tokens + completion_tokens}}
        if mode == "missing_usage":
            result.pop("usage")
        elif mode == "bad_usage":
            result["usage"]["total_tokens"] = -1
        elif mode == "trickle":
            wire = JSONResponse(result).body

            async def body():
                # Every byte arrives within a read timeout, while the complete
                # response exceeds the whole-attempt deadline.
                for byte in wire:
                    yield bytes([byte])
                    await asyncio.sleep(delay / 100)

            return StreamingResponse(body(), media_type="application/json")
        return result

    return app


def create_primary_app() -> FastAPI:
    return create_provider("primary")


def create_backup_app() -> FastAPI:
    return create_provider("backup")