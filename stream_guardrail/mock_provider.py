"""Scripted synthetic SSE provider. No model, real customer data or credentials."""

import asyncio

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse, StreamingResponse

from stream_guardrail.app import (
    GenerationRequest, encode_delta, positive_setting, strict_json,
)


def create_app() -> FastAPI:
    delay = positive_setting("MOCK_STREAM_DELAY_SECONDS", "0.02")
    text_chars = positive_setting("MOCK_STREAM_TEXT_CHARS", "5", True)
    wire_bytes = positive_setting("MOCK_STREAM_WIRE_BYTES", "7", True)
    request_limit = positive_setting("STREAM_MAX_REQUEST_BYTES", "65536", True)
    upload_timeout = positive_setting("STREAM_READ_TIMEOUT_SECONDS", "5")
    app = FastAPI()
    app.state.active_streams = 0
    app.state.completed_streams = 0
    app.state.cancelled_streams = 0

    @app.post("/v1/chat/completions")
    async def completions(request: Request):
        body = bytearray()
        try:
            # Local mocks also accept untrusted HTTP; a slow uploader must not
            # outlive the bounds demonstrated by the gateway itself.
            async with asyncio.timeout(upload_timeout):
                async for chunk in request.stream():
                    if len(body) + len(chunk) > request_limit:
                        return JSONResponse({"detail": "Mock request too large"}, status_code=413)
                    body.extend(chunk)
            generation = GenerationRequest.model_validate(strict_json(body.decode("utf-8")))
            if not generation.stream or generation.model not in {
                "mock-pii", "mock-reject", "mock-truncate", "mock-malformed", "mock-slow",
            }:
                raise ValueError
        except TimeoutError:
            return JSONResponse({"detail": "Mock request timeout"}, status_code=408)
        except (ValueError, UnicodeError, RecursionError):
            return JSONResponse({"detail": "Unsupported mock request"}, status_code=400)
        if generation.model == "mock-reject":
            return JSONResponse({"detail": "Simulated provider rejection"}, status_code=429)

        async def script():
            app.state.active_streams += 1
            complete = False
            try:
                yield encode_delta("Mock output! ")
                await asyncio.sleep(delay)
                if generation.model == "mock-slow":
                    await asyncio.sleep(30)
                yield encode_delta("Contact jane")
                if generation.model == "mock-truncate":
                    return
                if generation.model == "mock-malformed":
                    yield b"data: simulated-invalid-json\n\n"
                    return
                text = "@example.com; SSN: 123-45-6789; card: 4111 1111 1111 1111. café 😀!"
                for offset in range(0, len(text), text_chars):
                    await asyncio.sleep(delay)
                    yield encode_delta(text[offset:offset + text_chars])
                yield encode_delta(finish="stop")
                yield b"data: [DONE]\n\n"
                complete = True
                app.state.completed_streams += 1
            finally:
                app.state.active_streams -= 1
                if not complete:
                    # This records all unfinished scripts, including simulated
                    # malformed/truncated output, not just client disconnects.
                    app.state.cancelled_streams += 1

        async def segmented():
            # The adapter can be cancelled while its source is suspended at a
            # yield. Explicit closure ensures the source's lifecycle completes.
            source = script()
            try:
                async for frame in source:
                    for offset in range(0, len(frame), wire_bytes):
                        yield frame[offset:offset + wire_bytes]
            finally:
                await source.aclose()

        return StreamingResponse(segmented(), media_type="text/event-stream")

    return app
