# Streaming PII guardrail

A local HTTP proxy parses SSE events, redacts supported email, SSN and card-like
patterns across text deltas, and emits newly encoded SSE. Its provider is a
scripted synthetic mock; prompts do not influence generated text.

## Automated validation

From the repository root, with Python 3.11+ and uv:

```bash
uv sync --locked
uv run --no-sync pytest stream_guardrail/tests -q -s
uv run --no-sync ruff check stream_guardrail
```

No credentials or manual service startup are needed. Tests reuse the root `.venv`
and start local processes where necessary. HTTP tests need loopback-socket
permission; memory measurements use Unix process/resource APIs. The latest
recorded verification passed **119 guardrail tests**, included in the root
366-test run, plus configured Ruff checks on macOS arm64/Python 3.13.3.

## Manual demonstration

After setup, run each command in a separate terminal at the repository root:

```bash
uv run --no-sync python -m uvicorn stream_guardrail.mock_provider:create_app --factory --host 127.0.0.1 --port 8011 --no-access-log
```

```bash
uv run --no-sync python -m uvicorn stream_guardrail.app:create_app --factory --host 127.0.0.1 --port 8010 --no-access-log
```

Wait for startup, then send a request from a third terminal:

```bash
curl -N http://127.0.0.1:8010/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-pii","stream":true,"messages":[{"role":"user","content":"Synthetic demo"}]}'
```

Read `choices[0].delta.content` to reconstruct the text:

```text
Mock output! Contact [REDACTED]; SSN: [REDACTED]; card: [REDACTED]. café 😀!
```

Successful output ends with a finish reason and `[DONE]`. Stop both services
with Ctrl+C. Factory startup, requests and shutdown are exercised with HTTPX in
real HTTP tests; the curl command is illustrative, not a separately executed check.

| Mock model | Expected result |
| --- | --- |
| `mock-pii` | Three replacements, preserved safe text, finish and DONE |
| `mock-malformed` / `mock-truncate` | Fixed stream error, unresolved text discarded, no DONE |
| `mock-reject` | HTTP 502 with a fixed message; mock error body withheld |
| `mock-slow` | Early safe text, then a 30-second pause; tests verify disconnect cleanup |

## Contract and completion policy

Requests accept only `model`, text `messages`, required `stream: true` and
optional `n: 1`. Roles are system/user/assistant. Defaults cap the request at
65,536 bytes, 64 messages, 32,768 characters per content string and 128 characters
per model name. Unknown fields, coercion and unsupported output modes are rejected.

Every emitted text character passes through stateful redaction. The supported
patterns are ASCII email addresses, `DDD-DD-DDDD` SSNs, and 13–19 ASCII-digit
cards with optional single spaces/hyphens. Matching uses substrings, without SSN
validity or Luhn checks. Intersecting matches are redacted as one connected span.
See the [implementation guide](STREAM_GUARDRAIL_IMPLEMENTATION.md#implemented-decisions)
for the exact grammar and false-positive policy.

Incomplete candidates stay pending across deltas. The default cap is 256 Unicode
characters; overflow fails closed, including for long legitimate words/URLs.
Numeric candidates exceeding 19 digits also fail closed. The three completion
cases are deliberately distinct:

| Case | Policy |
| --- | --- |
| Identifier spans multiple chunks | Retain its possible prefix; redact when the complete pattern is established |
| Broken transport, invalid event, deadline or cancellation | Discard unresolved text; never emit a success marker |
| Valid finish plus DONE with an incomplete identifier | Release nonmatching text: `jane@exam` is preserved even with a `length` finish |

This is pattern-based redaction. Incomplete identifiers and patterns outside the
documented grammar are not comprehensive PII protection. Already emitted safe
text cannot be retracted.

Upstream events allow one index-0 choice, text/role/empty deltas and `stop`,
`length` or `content_filter` finishes. Basic usage events with empty choices are
validated and discarded. Metadata is validated and discarded; tool calls,
reasoning, multimodal fields, extra choices and non-null logprobs are rejected.
This narrow OpenAI-style subset does not promise universal provider/SDK compatibility.

Before streaming, invalid requests return 400/408/413 and upstream failures
502/504 with fixed messages. After headers, failures emit this when writable:

```text
event: error
data: {"error":{"code":"stream_failed","message":"Generation stream failed"}}
```

A blocked/disconnected writer may close without an error event. Clients must
treat missing `[DONE]` as incomplete even when the HTTP status was 200.

## Architecture, measurements and limits

`app.py` owns bounded SSE framing, validation and the upstream response lifetime;
`redactor.py` owns pending text and match decisions; `mock_provider.py` independently
splits text and wire bytes to exercise both kinds of boundaries. No whole-response
buffer or background producer queue is used. Backpressure stops application
consumption, while HTTP/kernel buffers still exist.

Framing yields every 8,192 bytes and redaction every 256 characters so CPU-heavy
deltas do not monopolize the event loop. Output delta boundaries can differ from
provider boundaries. Separate-process tests compare 16-fold stream lengths,
stalled/throttled readers and 1/4/8 concurrent streams. Detailed workloads,
measurements, regression budgets and their limits live in [AUDIT.md](AUDIT.md).
Repeat the focused checks with:

```bash
uv run --no-sync pytest stream_guardrail/tests/test_performance.py -q -s
uv run --no-sync pytest stream_guardrail/tests/test_stream_memory.py -q -s
```

The remaining suites cover the redactor (`test_redactor.py`), framing/lifecycle
(`test_streaming.py`) and real HTTP streaming/disconnects (`test_http_streaming.py`).
A curl result cannot establish bounded memory, zero leakage under all splits or
resource cleanup; those properties need the assertions and measurements.

[.env.example](../.env.example) lists configuration; export it before startup.
Request/event limits count bytes; pending limits count Unicode characters. The
read timeout also bounds uploads; the total provider deadline starts after request
validation. Fixed trusted upstream URLs, rejected compression/redirects and no
client credential/cookie forwarding constrain transport behavior.

Baseline acceptance is complete for this local contract. Remaining hardening
includes a separate cleanup deadline, aggregate authenticated stream admission,
content-free observability and live-client integration. Shielded closure currently
has no separate time budget. Per-stream bounds and measured loads do not certify
production capacity. Python 3.11 and the final review commit's clean checkout
remain unverified; see the [root verification scope](../README.md#verification-and-limits).
