# Streaming PII Guardrail — Implementation Plan

> **Location:** `stream_guardrail/`
>
> **Goal:** Responsive, bounded-memory streaming redaction for the documented PII grammar.
>
> **Shared rules:** [AGENTS.md](../AGENTS.md).
>
> **Status:** Baseline acceptance verified: 119 guardrail tests included in the latest recorded 366-test repository run; configured Ruff checks passed. Detailed findings and performance measurements are retained in [AUDIT.md](AUDIT.md).

## Implemented decisions

- **Delivery order:** Phase C (standalone redactor) preceded A/B (mock and bounded
  HTTP/SSE parsing), then D (lifecycle integration) and E (real-socket acceptance
  and measurements). This established the redaction invariant before exposing a
  proxy. Existing dependencies and the root environment were reused.
- **Email grammar:** ASCII
  `[A-Za-z0-9._%+-]+@[A-Za-z0-9-]+(?:\.[A-Za-z0-9-]+)*\.[A-Za-z]{2,}`.
  This permissive syntax supports dots/plus signs, not quoted/Unicode mailboxes,
  obfuscation, deliverability or DNS validation.
- **Numbers:** SSNs are exactly `DDD-DD-DDDD`; cards are 13–19 ASCII digits with
  optional single ASCII spaces/hyphens, including mixed separators. No SSN group
  validity or Luhn check. Tabs, repeated spaces and Unicode digits are excluded.
  Substring matching deliberately permits false positives. All overlapping match
  intervals are merged, including chains, so one replacement cannot expose part
  of a match beginning inside another.
- **Pending state:** Retain the earliest suffix that might complete a supported
  match, including intersecting complete matches. This is candidate detection,
  rather than a fixed-size tail. Ordinary words can become email local parts.
  The default 256-character cap rejects before appending beyond it, even when
  the next character would resolve the candidate. Long words/URLs can fail closed;
  numeric candidates beyond 19 digits also abort. Failure clears pending state
  and closes the redactor. Event/request bounds separately cap transient input.
- **Completion policy:** Only a valid finish reason followed by DONE calls
  `finish()`: complete supported patterns are redacted and incomplete nonmatching
  text is released. Thus `jane@exam` survives a valid `length` finish plus DONE.
  Transport truncation, malformed events, cancellation and deadlines call `abort()`
  and discard pending text. No failure emits DONE. Safe text already sent cannot
  be retracted. This explicitly defines pattern-based, not comprehensive, privacy.
- **No output bypass:** Strict event/JSON validation rejects unknown output modes,
  duplicate keys, invalid Unicode and malformed shapes. Provider metadata is
  discarded and outgoing SSE is generated locally. One text choice is supported;
  reasoning, tool calls, multimodal content and arbitrary SDK compatibility are not.
- **Framing and fairness:** Byte limits include comments, delimiters and incomplete
  lines. UTF-8/BOM and LF/CRLF/CR splits are handled independently of text deltas;
  lone CR dispatches immediately. Cooperative 8,192-byte framing slices and
  256-character redaction batches preserve state while yielding the event loop.
- **Resource ownership:** Fixed configured upstream, bounded uploads/events,
  read/total deadlines, rejected compression/redirects and isolated headers avoid
  transport bypasses. The streaming response owns upstream cleanup, including
  failed header writes and unstarted generators. Consumption follows client
  backpressure without a producer queue. Shielded cleanup still lacks a separate
  deadline; see the audit's remaining hardening priorities.

## Verification record

The latest functional run in the existing macOS arm64/Python 3.13.3 environment:

```bash
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# 366 passed in 55.04 s, including 119 guardrail tests.
uv run --no-sync ruff check .
# All checks passed.
```

Acceptance includes every split/one-character redaction, overlapping matches,
strict framing and event validation, successful/incomplete/truncated completion,
real HTTP early output/disconnects, stalled readers, a 16-fold stream-length
allocation comparison and concurrent performance/deadline/cancellation checks.
The audit preserves corrected overlap disclosure, delayed CR dispatch and mock
upload deadline defects, plus the CPU-starvation reproduction and measurements.

Earlier locked source-snapshot/environment checks were additional reproducibility
checks; their temporary environments were removed. They do not establish a clean
checkout of the final review commit. Python 3.11, other platforms, live providers
and production capacity remain unverified. See [README.md](README.md) for current
run commands and [AUDIT.md](AUDIT.md) for fixture-specific evidence and open limits.

## 1. Requirements and completion evidence

Build an **LLM Gateway proxy endpoint** that sends text-generation requests to an upstream LLM-style provider and returns an **incrementally streamed** response to the client. While receiving upstream events, inspect generated text and replace recognized emails, Social Security numbers (SSNs), and credit-card-number patterns with the exact text `[REDACTED]`.

| Requirement / evaluation area | Evidence required |
| --- | --- |
| Route generation requests to a provider | Real HTTP request to a configurable downstream mock with a streamed response |
| Intercept response chunks in real time | Consume the upstream response incrementally, not with `.read()`/`.aread()` over the whole stream |
| Parse deltas and redact PII | Decode SSE events, extract text from the documented delta field, and redact supported patterns |
| Handle partial streams | Correct redaction when sensitive values span any tested text-delta, SSE, or network-byte boundary |
| Responsive streaming / low TTFT | Return safe text before the upstream completes when safe text is available; report measured TTFT and total latency |
| Memory efficiency | Bound **both** unresolved text and incoming SSE event framing; do not collect the whole response |
| Robust asynchronous behavior | Handle slow readers, disconnects, malformed events, provider errors and cleanup predictably |

**Scope choice, not a hidden requirement:** Implement the **text-only, single-choice, OpenAI-style Chat Completions SSE subset** first. Use a local mock provider that speaks this interface. Do not claim universal compatibility with all LLM APIs. Live-provider integration is optional, not necessary for validating the gateway logic.

## 2. Design boundaries and working assumptions

1. Use existing FastAPI, HTTPX, pytest and standard-library utilities (`asyncio`, `json`, `re`, incremental UTF-8 decoding as needed). Inspect the root `pyproject.toml` before adding anything; do **not** install new packages by default.
2. Expose `POST /v1/chat/completions` or another clearly documented completion route. Accept a JSON generation request, require/normalize `stream: true` according to the documented contract, and relay it to an **environment-configured** upstream URL. Never take an arbitrary provider URL from an untrusted request.
3. Upstream mock returns `text/event-stream`: UTF-8 SSE frames with JSON Chat Completion chunks containing `choices[0].delta.content`, plus `[DONE]`. Network chunks may cut across UTF-8 code points, SSE lines, JSON frames, or PII spans.
4. Initially support **one generated text choice (`n=1`)**. Treat unsupported output modes (e.g., `tool_calls`, function-call argument streaming, multimodal content, multiple choices, logprob token content) as **unsupported**, not as unchecked pass-throughs. Reject unsupported client configurations up front where possible; if unexpected upstream content appears mid-stream, terminate safely rather than relaying it unexamined.
5. Redact common, documented email, SSN and credit-card formats. State exactly which patterns and separators are supported, how boundaries are recognized, and how false positives are handled. Regex-only detection is not comprehensive PII protection.
6. Keep a **bounded pending-text state** for incomplete possible matches, and a separate **maximum SSE event size**. Suggested starting *configurable* limits for testing: 256 pending characters and 64 KiB per SSE event (not security guarantees or mandated values). Reconsider limits after measuring realistic stream behavior and test fixtures.
7. If a possible sensitive sequence cannot be resolved within the supported bound, **fail closed** (do not emit that unresolved text). Define a sanitized error/termination policy; never silently release suspicious text just to empty the buffer.
8. Do not buffer the whole upstream response, retry completed text, log raw PII, or advertise comprehensive production DLP protection.

## 3. Minimal file layout

```text
stream_guardrail/
├── app.py                    # HTTP endpoint, upstream stream lifecycle, SSE orchestration
├── redactor.py               # Stateful streaming text redactor
├── mock_provider.py          # Local scripted, HTTP/SSE-compatible provider
├── STREAM_GUARDRAIL_IMPLEMENTATION.md
└── tests/                    # Behavior, streaming, and performance checks
```

**File discipline:** Start with these implementation files, using the test files actually needed. Put a small SSE parsing helper in `app.py` initially. Move it into `sse.py` only if complexity harms readability or tests. Add a `README.md` when verified startup and test commands are available; do not introduce a shared framework or separate environment.

## 4. Architecture and data flow

```text
Client POST /v1/chat/completions (stream=true)
                |
                v
      FastAPI proxy endpoint
      - validate supported request shape
      - select configured mock provider
                |
                v
       HTTPX streamed HTTP request
                |
                v
    Upstream UTF-8 SSE byte stream
                |
                v
    Incremental bounded SSE framing
      - partial bytes/UTF-8 safe
      - one complete event at a time
      - maximum SSE event size
                |
                v
      Parse JSON text delta
                |
                v
    Stateful pending-text redactor
      - compiled patterns / safe-prefix logic
      - emit only confirmed-safe content
      - `[REDACTED]` replacements
      - bounded pending candidate state
                |
                v
      Re-encode safe SSE deltas
                |
                v
         StreamingResponse
                |
                v
               Client
```

**Important separation:** A network byte chunk is **not** necessarily an SSE event, and an SSE event is **not** necessarily a complete email or card number. Never search or rewrite raw `data:` JSON bytes with a PII regex.

## 5. Implementation phases

### Phase A — Deterministic provider and basic HTTP proxy

- [x] Inspect existing Task 2 dependencies and tests; reuse FastAPI, Uvicorn, HTTPX and pytest if available. Add packages only for a demonstrated need.
- [x] Implement a local mock endpoint with a `stream: true` request and scripted SSE deltas. Include delayed chunks, configurable segmentation, final `[DONE]`, a controllable provider error, and a disconnect-friendly stream.
- [x] Implement the gateway endpoint and a configurable fixed upstream base URL. Avoid an open proxy to arbitrary request-controlled URLs.
- [x] Send a genuine asynchronous HTTP request from gateway to mock. Consume upstream chunks incrementally and establish a working downstream SSE stream before adding redaction.
- [x] Verify that the HTTPX upstream response remains open **while the downstream async generator consumes it**; close it in all success, failure, and cancellation paths. Do not exit an `async with client.stream(...)` block before its iterator is consumed.

### Phase B — Bounded incremental SSE parsing

- [x] Parse the streamed bytes as UTF-8 incrementally, correctly handling split multibyte characters, LF/CRLF line boundaries, comments, blank-line event delimiters and `data:` fields. Multiple `data:` lines in one event are joined according to SSE rules.
- [x] Bound the in-progress SSE event size; reject or safely terminate on oversized or malformed events. Do not accumulate unlimited event data while waiting for a blank line.
- [x] Parse complete `data:` JSON events, extract only supported `choices[0].delta.content` strings, and handle role/finish/usage-only events and `[DONE]` deliberately.
- [x] Avoid forwarding unrecognized generated-text fields uninspected. Apply a documented policy to unsupported upstream event shapes and unexpected provider content.
- [x] Reconstruct valid, UTF-8 SSE output. Maintain output order: buffered safe text must be flushed/redacted **before** finish markers and `[DONE]`.

### Phase C — Stateful, bounded PII redaction

- [x] Define explicit fixtures for common emails (including dots/plus signs and mixed case), SSNs (`123-45-6789` and any additional documented formats), and credit cards (13–19 digits with supported spaces/hyphens). Use **synthetic test values** only.
- [x] Decide and document the credit-card policy (syntactic candidates vs. Luhn-validated values); tests must match the policy. Do not quietly pass obvious credit-card-like strings because a particular fixture fails a checksum.
- [x] Implement a small redactor with `feed(text) -> safe_text` and `finish() -> safe_text` (or equivalent). It must preserve the order of nonsensitive text and use **exactly** `[REDACTED]` for complete supported matches.
- [x] Match complete patterns with compiled regex or a targeted state machine. Separately identify any **unresolved suffix** that might become a supported match after future deltas. Keep only what is necessary; do not use a naive per-chunk regex or assume that retaining the last fixed number of characters automatically prevents leakage.
- [x] When more input resolves a candidate, emit the safely redacted version as promptly as possible. Do not wait for upstream completion unless the input remains genuinely ambiguous.
- [x] Keep pending text within the configured bound; if ambiguity or candidate length exceeds it, suppress the unresolved content and terminate safely. Reject pathological input rather than allowing unbounded memory use or catastrophic regex behavior.
- [x] Compare simple standard-library techniques before adding a third-party regex/PII package. Introduce a dependency only if tests reveal a specific problem the existing tools cannot solve cleanly.

**Illustrative sequence:**

```text
Input delta 1: "Contact jane"
Input delta 2: "@example"
Input delta 3: ".com for details."
Safe combined output: "Contact [REDACTED] for details."
```

A method that emits `jane` during delta 1 fails this test, even if later deltas are individually redacted.

### Phase D — Correct asynchronous streaming and errors

- [x] Connect the SSE parser and redactor directly inside an asynchronous producer. Yield only safe encoded SSE output; do not build a list of all upstream events.
- [x] Keep backpressure naturally coupled: do not start an unbounded background consumer when the downstream client is slow. Use an async generator without an unbounded queue.
- [x] Handle upstream non-2xx errors **before** beginning the client stream when possible; return a sanitized HTTP error, not raw upstream content.
- [x] If a provider fails or emits malformed content **after** response streaming begins, emit a documented safe error event or close the stream cleanly; do not attempt to replace the already-sent HTTP status or leak partial raw payloads.
- [x] Ensure `[DONE]` is emitted only after pending text has been safely finalized and no error requires early termination.
- [x] On client disconnect/cancellation, promptly stop upstream consumption and close connections. Check cancellation behavior in an actual async integration test.
- [x] Keep logs free of PII-bearing raw chunks, provider secrets, and full prompt bodies.

### Phase E — Acceptance tests and measurements

- [x] Redact a complete supported email, SSN, and credit-card number in a single delta.
- [x] For **each** supported sensitive fixture, test every possible single split point and character-by-character text deltas. Verify the concatenation of downstream content never contains the sensitive value or an unredacted portion of it.
- [x] Independently split SSE framing, JSON events and UTF-8 byte sequences at varied offsets. Verify parse/re-encode correctness and complete redaction.
- [x] Test multiple PII values across consecutive deltas, adjacent punctuation, ordinary safe prose, empty deltas and finishes. Verify unchanged safe text and the required replacement marker.
- [x] Test unsupported upstream fields/output modes and unexpectedly malformed JSON. Assert that unchecked generated text never reaches the client.
- [x] Test overlong unresolved candidates and oversized SSE events. Assert bounded memory and fail-closed behavior, with no sensitive leakage.
- [x] Test streaming responsiveness with a mock that pauses partway through: assert the client receives **safe content before the provider finishes**, not one combined response at the end.
- [x] Measure **TTFT to first safe text** (exclude mere response headers/keepalive comments), total stream duration, and peak unresolved-text/event buffer sizes under repeatable scripted delays.
- [x] Test a long synthetic stream, confirming buffer growth stays bounded with response length. Ensure tests themselves do not confuse whole-response collection by a test client with gateway-side buffering.
- [x] Test provider rejection, mid-stream error, slow client and disconnection. Assert resource cleanup and safe termination.
- [x] Record reproducible setup, run, test and measurement commands **after they are verified** in `stream_guardrail/README.md`.

## 6. Acceptance matrix

| Case | Expected behavior |
| --- | --- |
| Single-delta email / SSN / card | Supported sensitive value replaced with `[REDACTED]` |
| Sensitive value split at arbitrary tested boundaries | No sensitive value or partial leaked prefix reaches client |
| Multiple values and punctuation | Ordered safe text, consistent replacements |
| Nonsensitive text | Preserved exactly after any necessary bounded delay |
| Split HTTP bytes, SSE lines, JSON, UTF-8 | Correctly parsed and re-emitted valid SSE |
| Upstream role / finish / usage event | Expected supported metadata handled without bypassing guardrail |
| Unsupported extra generated-text fields | Not forwarded unchecked; documented safe failure |
| SSE event exceeds cap | Safe termination, no unbounded event buffer |
| Pending PII candidate exceeds cap | Fail closed; no release of unresolved sensitive text |
| Provider slow but emits safe text early | Client receives safe output before completion; measured TTFT |
| Long generated response | No whole-response buffering; bounded peak buffers |
| Upstream error / disconnect | Sanitized failure, resources released, no raw PII disclosed |

## 7. Performance approach (part of the task, not optional polish)

- Prefer one async producer/consumer flow and reusable HTTP client resources; do not copy the complete upstream body.
- Run compiled regex only against a **bounded active candidate/pending window**, rather than rescanning all prior generated text for every delta.
- Avoid catastrophic-backtracking patterns and repeated concatenation of unbounded strings. The implementation should have predictable memory use relative to configured buffer/event limits.
- Choose the smallest safe holdback consistent with supported PII formats. A larger holdback can reduce leaks but also increases TTFT; explain and test that tradeoff.
- Use controlled mock delays and measure actual latency, rather than asserting zero overhead or quoting unverified numbers.

## 8. Scope boundaries

**Required now:** A real HTTP streaming gateway, correct incremental parsing, supported PII redaction across arbitrary tested splits, bounded memory, responsiveness, and error/connection cleanup.

**Defer:** Hosted models, user authentication, prompt scanning, comprehensive DLP/NER models, database storage, dashboards, model routing/fallback (Task 4), generic provider abstraction frameworks, and cloud deployment. Do not add more files or packages solely for hypothetical future capabilities.

**Important limitation:** With finite memory, the gateway cannot promise perfect redaction of every possible arbitrary-length PII string. Explicitly define the formats/maximum candidate length and fail closed when the detector cannot guarantee a safe release. Do not characterize tests on supported fixtures as universal PII guarantees.

## 9. Definition of done

- [x] Gateway forwards text-generation requests to a real local HTTP mock and streams valid SSE responses.
- [x] Supported emails, SSNs, and credit-card patterns are replaced by `[REDACTED]`, including across all tested delta/split boundaries.
- [x] No uninspected upstream content bypasses the redactor.
- [x] Pending text and SSE event memory remain bounded; long outputs do not accumulate in application memory.
- [x] Safe text is emitted before upstream completion whenever safe text is available; TTFT and total latency are measured and reported.
- [x] Disconnects, invalid events, oversized inputs and provider failures are handled safely; upstream resources are closed.
- [x] All required acceptance tests run successfully, and startup/test instructions have been verified.

## References

- [FastAPI streaming](https://fastapi.tiangolo.com/advanced/custom-response/)
- [HTTPX async streaming](https://www.python-httpx.org/async/)
- [SSE event-stream format (MDN)](https://developer.mozilla.org/en-US/docs/Web/API/Server-sent_events/Using_server-sent_events)
- [OpenAI Chat Completions streaming events](https://developers.openai.com/api/reference/resources/chat/subresources/completions/streaming-events)
