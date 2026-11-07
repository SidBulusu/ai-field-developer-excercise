# Streaming PII Guardrail — Implementation Plan

> **Location:** `stream_guardrail/`  
> **Goal:** Build a responsive, bounded-memory LLM streaming proxy that redacts supported sensitive text before any of it reaches a client.  
> **Shared rules:** Follow `../AGENTS.md`: inspect existing code first, explain the smallest viable approach, prefer readable code, reuse dependencies, and create files only when justified.  
> **Status:** Latest performance follow-up verified: 315 passing repository tests, including 119 guardrail tests, and configured Ruff checks. AUDIT.md records reproduced defects, fixes, matching/concurrency measurements and remaining suggestions. Earlier sections are historical runs, not proof that every edge case had been covered.

## Review decisions and phased delivery

The first increment implements Phase C before A/B. A standalone redactor lets us
test the safety invariant before creating an HTTP endpoint. Delivery continued with A/B together
(scripted provider, bounded SSE parser and validated HTTP stream), D (connect the
redactor and verify errors/cancellation), then E (real-socket acceptance and
measurements). These increments are now verified for the documented subset. Do not expose an unchecked pass-through endpoint as the finished
guardrail. Existing Task 1/2 implementations and dependencies remain unchanged.

### Resolved for the first increment

- **Email grammar:** ASCII `[A-Za-z0-9._%+-]+` local part, `@`, one or more
  dot-separated `[A-Za-z0-9-]+` domain labels, and a final alphabetic label of
  at least two letters. Matching is case sensitive only in the sense that both
  cases are explicitly accepted. Dots/plus signs are supported. This permissive
  syntactic grammar does not validate DNS, deliverability or RFC mailbox rules.
  Quoted local parts, Unicode mailboxes and obfuscated addresses are outside it.
- **Numbers:** SSNs use exactly `DDD-DD-DDDD`; cards use 13–19 ASCII digits,
  optionally separated by a single ASCII space or hyphen (mixed separators are
  accepted). No Luhn requirement. Tabs, repeated spaces and Unicode digits are
  outside this grammar. SSN group validity is not checked. Patterns are matched
  as substrings without requiring word boundaries, deliberately allowing false
  positives. Emails/cards take precedence over SSN-shaped substrings.
- **Pending state:** Retain the earliest suffix that could extend into an email
  or numeric match, plus any overlapping complete match. Ordinary unfinished
  words must wait because they can become email local parts. This is candidate
  detection, not a fixed tail of N characters. Detection uses standard-library
  regexes over the bounded window; no extra dependencies were justified.
- **Limits and false positives:** `max_pending_chars` defaults to 256 and must be
  a positive integer. Reject before appending beyond it; a candidate filling the
  cap fails on the next character even if that character would resolve it.
  Long ordinary words/URLs can therefore fail closed. Numeric candidates exceeding
  19 digits also fail closed, including long non-card identifiers. Failure clears
  pending text and permanently closes the redactor. These are conservative local
  policies, not universal PII coverage. Future HTTP configuration must pass the
  validated limit to each request's redactor.
- **Lifecycle:** `finish()` is terminal and treats end of generated text as a
  boundary, redacting complete matches and releasing incomplete nonsensitive
  candidates. Call it only after verified successful provider completion.
  `abort()` discards pending text on errors, truncated EOF and cancellation.
  A `feed()` failure returns no output from that call. Error messages contain no
  input text. Previously released safe output cannot be retracted.
- **Memory scope:** Only unresolved characters persist between calls. `feed`
  returns output for its supplied delta, so transient output scales with that
  delta. Transport must separately cap event/delta size. The redactor's unit
  tests establish bounded pending state, not total HTTP-process memory.

### Review concerns tracked across increments

1. **Request contract:** Set request-byte and message-length limits before JSON
   parsing. Define accepted roles/fields, require strict `stream=true`, reject
   `n != 1`, tools, multimodal content and unknown options before contacting the
   provider. Do not echo validation inputs into error responses.
2. **Output bypasses:** Allowlist upstream event shapes, including role, finish
   and usage events. Reject unknown generated-text fields. Generate safe output
   metadata locally rather than reflecting arbitrary provider IDs, errors or
   headers. Strict JSON parsing must handle duplicate keys, nonfinite numbers,
   lone surrogates and excessive nesting safely.
3. **Framing:** Count raw bytes, including comments and unterminated lines, toward
   the framing cap; otherwise a data-only cap has a bypass. Cover LF, CRLF and lone
   CR, split UTF-8 and the stream BOM deliberately. SSE permits multi-line data.
   Undelimited trailing data at EOF is not a complete event. The authoritative
   [SSE parsing rules](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
   are broader than just splitting on `\n\n`.
4. **Successful completion:** Define finish ordering and require `[DONE]`;
   premature EOF must abort rather than call `finish()`. Error events must never
   be followed by a success marker. Earlier HTTP 200 headers cannot be replaced
   after streaming starts.
5. **Resource bounds:** Configure connect/read timeouts and a total stream
   deadline; keepalives can defeat read timeouts. Reject redirects and unexpected
   content types. Decide compression policy so decompression cannot allocate an
   unbounded decoded chunk before the event cap is checked. Disable unnecessary
   environment proxies and prevent client-cookie/header forwarding.
6. **Cleanup and evidence:** Keep the upstream open through generator consumption
   and close it even during cancellation (see [HTTPX streaming lifecycle](https://www.python-httpx.org/async/#streaming-responses)).
   Test with actual loopback sockets: in-process clients may buffer streams and
   conceal TTFT, backpressure or cancellation failures. No HTTP, SSE, latency,
   total-memory or cancellation claims are established by the redactor tests.
7. **Shared documentation:** AGENTS.md uses generic `IMPLEMENTATION.md` paths that
   differ from the existing task-specific filenames. Preserve existing filenames
   and use their actual paths. Clean-clone verification and linting remain open;
   this increment neither adds a linter nor claims that gate has passed.

### Verification for this increment

Executed successfully in the existing environment:

```bash
UV_CACHE_DIR=/private/tmp/ai-field-uv-cache uv run --no-sync pytest stream_guardrail/tests -q
# 32 passed
UV_CACHE_DIR=/private/tmp/ai-field-uv-cache uv run --no-sync pytest -q
# 176 passed, including Tasks 1 and 2
```

The cache override keeps uv's cache in a writable temporary location. `--no-sync`
uses the existing environment; dependency installation/clean-clone checks were
not repeated. Before implementation the new tests failed collection because the
redactor module did not yet exist. HTTP startup commands and performance
measurements remain unverified and are not supplied as working instructions.

## Second increment — HTTP/SSE integration and lifecycle verification

Implemented `app.py` and `mock_provider.py` without installing packages. SSE
framing stays in `app.py`; it handles split UTF-8, BOM, LF/CRLF/CR, comments and
multi-line data. The cap includes all event bytes, including comment lines and
line endings. Events and JSON are validated before text enters the redactor.
Unknown generated-text fields fail closed. Request and event limits default to
64 KiB; the pending window defaults to 256 characters. These configuration values
and the exact narrow request/output contract are documented in `README.md`.

Concerns 1–6 above are addressed for the documented subset: fixed configured URL,
bounded request upload, strict validation, no reflected metadata, no compression
or redirects, read and total deadlines, verified finish + `[DONE]`, sanitized
errors and cancellation-shielded upstream cleanup. AnyIO is already installed
and required by the HTTP stack; no dependency declarations or lockfile changed.
The reusable owned HTTPX client closes on application shutdown; injected clients
remain caller-owned. Cleanup is also placed at the response boundary so failures
before generator iteration cannot strand an open upstream response.

Verification executed successfully:

```bash
uv run --no-sync pytest -q -s
# 233 passed in 7.19 s, including 89 guardrail tests
```

Loopback sockets were initially denied by the sandbox (`PermissionError`); the
suite then passed with unrestricted local-socket access. Both provider and gateway
also started through `python -m uvicorn ... --factory` in separate processes.
The CLI test initially assumed exit code zero on SIGTERM; inspection of installed
Uvicorn confirmed it re-raises captured signals after cleanup, so the test now
accepts the corresponding SIGTERM exit as well. HTTPX exercised the documented
request; the equivalent README curl command was not separately executed.

Measured on the final full-suite run: first **safe text** at 0.0081 seconds;
total scripted response duration 0.3324 seconds. The assertion checks that the
provider has not completed when safe text arrives. A separate 10,000-event
parser/redactor run reported peak event size 90 bytes and peak unresolved text
5 characters. These measurements apply to their fixtures, not worst-case RSS or
universal latency. A blocked ASGI writer read only one upstream chunk until
cancellation; actual loopback disconnect and read/total-timeout tests confirmed
that paused upstream providers close promptly.

**Open at the end of this increment (resolved in the third increment below):**
long HTTP-stream process-memory profiling, sustained socket-level slow-reader
pressure, and fresh-environment acceptance.
There is no gateway background producer/queue, but the operating system and HTTP
stack also buffer data. Do not infer a bound on total resident memory from the
redactor/parser counters alone. Linting was not installed or run. Concern 7's
shared stale filenames remain documented rather than changed across unrelated
tasks. Do not check the full completion gate until outstanding evidence exists.

## Third increment — long streams, socket pressure and fresh environment

Added `tests/test_stream_memory.py` because process-isolated allocation profiling
and sustained load fixtures have a separate responsibility from protocol tests.
No production endpoints, runtime code, dependencies or lockfile changes were
needed. The profiling factory and load provider exist only in the test module.
The existing loopback server helper now optionally configures its send buffer.

**Long HTTP stream:** Run a gateway in a separate Uvicorn process, warm lazy
imports, and use standard-library `tracemalloc` to compare 1,024 and 16,384 events.
The client validates and discards each event rather than collecting the response.
The fixture repeats a synthetic email and verifies each replacement. The test
allows at most 512 KiB additional traced peak and 256 KiB additional retained
traced allocations when output grows 16-fold. It also reports the gateway
process's RSS high-water mark using `resource.getrusage`; RSS is measured, not
asserted as an absolute per-request ceiling.

Final full-suite measurements on macOS / Python 3.13.3:

| Events | Output bytes | Duration | Traced peak bytes | Retained traced bytes | RSS high-water bytes |
| --- | --- | --- | --- | --- | --- |
| 1,024 | 140,410 | 0.54 s | 345,125 | 45,401 | 69,304,320 |
| 16,384 | 2,244,730 | 8.12 s | 604,044 | 43,828 | 69,615,616 |

Traced peak grew by 258,919 bytes; retained allocations did not grow. Profiling
adds overhead, so these durations are not comparable to the unprofiled short
mock's TTFT. Tracemalloc does not cover every native allocation, RSS is a process
high-water mark, and these are single-request fixtures. None establishes a
universal memory ceiling under arbitrary event sizes or concurrent traffic.

**Socket pressure:** A raw client with a small receive buffer initially stops
reading, then performs 50 throttled reads of at most 1,024 bytes. Small server
send buffers force transport backpressure. Upstream production stalled after
7,536 events in the final run, far before its million-event limit; 51,200 bytes
were read without an exposed email, and disconnect closed the active provider.
The stall count depends on OS buffering and is reported rather than hardcoded.
The earlier ASGI blocked-write test independently proves that the application
does not launch an unbounded upstream consumer.

The initial load fixture starved disconnect processing because synchronous socket
writes did not yield control; it now explicitly yields to the event loop. The
first allocation sample also included lazy imports, so an unprofiled warm-up was
added before comparing lengths. Both issues were in the new test harness; the
passing measurements above use the corrected fixture.

**Fresh environment:** Copied tracked and nonignored current source files to
`/private/tmp/guardrail-clean-ketsqfwu`, excluding `.git`, `.venv`, `.env` and
caches, then created a new environment using the unchanged lockfile. This is a
clean source snapshot, not a literal Git clone: Task 3 files remain untracked in
the working repository. Executed from the snapshot:

```bash
uv sync --locked
uv run --no-sync pytest stream_guardrail/tests -q -s --tb=short --log-level=ERROR
# 91 passed in 17.28 s
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# 235 passed in 20.51 s
```

The executable was invoked as `/Users/sid/.local/bin/uv` because it was not on
PATH in the temporary directory. Local sockets required sandbox escalation.
Factory CLI startup and shutdown passed in both fresh-environment suites.
Unprofiled mock TTFT was 0.0088 s and total duration 0.3350 s in the final run.

All Task 3 required acceptance checks are now complete for the documented local,
text-only subset. The project-wide Task 4, linting and literal clean-clone gate
remain separate work. Ruff was not installed or run; Python 3.11 was not separately
tested. The existing PII grammar exclusions and conservative false positives
remain deliberate limitations, not claims of comprehensive production DLP.

## Follow-up audit and environment cleanup

At the user's request, temporary verification environments/source copies at
`/private/tmp/guardrail-clean-ketsqfwu` and
`/private/tmp/router-acceptance-99ttew2a`, plus the small temporary uv cache,
were removed. The working repository's `.venv` was preserved. These environments
were additional dependency/source reproducibility checks, not prerequisites for
running the task and not a replacement for the working environment. No further
virtual environment or source clone was created during this audit.

The audit identified unnecessary complete-pattern scans on every character of an
entirely ambiguous buffer. The redactor now skips those scans until a prefix can
actually be released. On the same 25,200-character fixture with 250-character
words, elapsed time changed from 0.9820 s to 0.1721 s. This is a local observation,
not a universal performance bound. Long local-part and bounded-prose regression
fixtures preserve the release/redaction behavior.

Added verification of blocked-write deadlines and cancellation for ASGI 2.3/2.4,
header-send failure before the body generator starts, and reuse/shutdown of the
owned HTTPX client. Deadline checks passed without changing lifecycle code. On an
unwritable/disconnected connection the safe outcome can be termination without an
error event; clients must not interpret an HTTP 200 header alone as success.

The successful-EOS policy was also made explicit: an incomplete `jane@exam` is
released after a valid length finish and DONE because it does not match a complete
supported pattern. On broken transport it is discarded instead. A test records
that distinction; broader partial-identifier redaction is not claimed.

Executed in the normal project environment:

```bash
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# 294 passed in 25.38 s, including 98 guardrail tests
uv run --no-sync ruff check .
# All checks passed
```

The run includes all real HTTP, memory, backpressure and new lifecycle checks.
Mock TTFT was 0.0091 s and total duration 0.3347 s. The 1,024/16,384-event memory
fixtures reported traced peaks of 345,639/610,687 bytes; these remain single-stream
fixture measurements. No model-router implementation was changed in this audit.

For the current concern-by-concern assessment and reviewer commands, use the
README's reviewer walkthrough. Historical claims about missing linting or
not-yet-executed acceptance below describe the earlier increment, not open work.

## Detailed audit — overlapping matches and CR responsiveness

The later audit reproduced a partial PII disclosure missed by the earlier suite:
ordinary non-overlapping regex replacement could consume a card and expose the
rest of an email beginning inside it. Matching now discovers all overlapping
starts and merges intersecting spans, both for release decisions and replacement.
Transitive-overlap tests cover every text split, individual characters and network
bytes. Connected recognized values produce one replacement marker.

CR-delimited SSE events also waited for more input or EOF. They now dispatch at
CR while an optional following LF remains part of byte-limit accounting. Exact
wire caps are tested for LF, CR and CRLF. A real HTTP handshake proves safe text
arrives while a CR-only provider is paused. EOF no longer needs a second event
finalization branch. The mock now enforces configured upload size/time bounds.

Seven targeted cases failed before correction; the focused suites then passed
102 tests and the full working-environment run passed 306 tests in 26.29 s,
including 110 guardrail tests. Configured Ruff checks pass. Decision-focused code
comments explain overlap safety, CR accounting, strict event validation,
compression policy, completion and response/client ownership.

See [AUDIT.md](AUDIT.md) for evidence, limitations and ordered suggestions. Privacy
policy for incomplete identifiers on valid forced finishes, aggregate concurrency,
CPU fairness, bounded cleanup and operational observability were still
unimplemented recommendations at that point. No dependency or environment was added, no fresh
clone was created, and no model-router implementation was changed in this audit.

## Performance follow-up — matching, scheduling and concurrency

- [x] Measure 4/16/60 KB prose, near-limit local parts, domain labels, overlapping
  matches and numeric candidates; verify output and bounded pending state.
- [x] Reproduce and fix a 429 ms event-loop stall on a valid large delta. Use
  cooperative 8,192-byte framing slices and 256-character text batches, retaining
  redaction state and direct backpressure. A 1,024-character batch failed the
  eight-stream <100 ms heartbeat target; the final batch measured 49 ms.
- [x] Verify a large delta yields to heartbeat, 50 ms provider deadline and
  explicit cancellation; cancelled processing closes the upstream.
- [x] Measure 1/4/8 real HTTP clients with a stalled reader, tracing gateway
  allocations in a separate process using the existing environment; verify
  redaction, completion and cleanup through the response lifetime.
- [x] Separately measure 1/4/8 CPU-heavy streams without allocation tracing.
- [x] Run the complete repository suite: 315 tests passed in 44.72 s, including
  119 guardrail tests; configured Ruff checks pass.

See [AUDIT.md](AUDIT.md#performance-validation-follow-up) for reproducible workloads,
local regression budgets, measured costs and remaining limits. No environment,
dependency or model-router change was needed. Higher concurrency, saturation,
arbitrary tuning limits and other Python/platform versions remain unverified.

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

**Scope choice, not a hidden requirement:** Implement the **text-only, single-choice, OpenAI-compatible Chat Completions SSE subset** first. Use a local mock provider that speaks this interface. Do not claim universal compatibility with all LLM APIs. Live-provider integration is optional, not necessary for validating the gateway logic.

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
