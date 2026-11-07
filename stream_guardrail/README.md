# Streaming PII guardrail

A local HTTP gateway parses streamed SSE, redacts supported email, SSN and
card-like patterns across deltas, and emits newly encoded SSE. The provider is
a scripted synthetic mock; no hosted model or credentials are involved.

From the repository root, using the existing synced environment, start these
in separate terminals:

```bash
uv run --no-sync python -m uvicorn stream_guardrail.mock_provider:create_app --factory --host 127.0.0.1 --port 8011 --no-access-log
uv run --no-sync python -m uvicorn stream_guardrail.app:create_app --factory --host 127.0.0.1 --port 8010 --no-access-log
```

Factory startup, a real HTTP request and shutdown are tested in separate
Uvicorn processes with dynamically allocated loopback ports.

```bash
curl -N http://127.0.0.1:8010/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"model":"mock-pii","stream":true,"messages":[{"role":"user","content":"Synthetic demo"}]}'
```

The concatenated content is:

```text
Mock output! Contact [REDACTED]; SSN: [REDACTED]; card: [REDACTED]. café 😀!
```

The request above is exercised with HTTPX in integration tests; the curl example
was not separately executed. Other mock model names: `mock-reject` (429),
`mock-truncate` (premature EOF), `mock-malformed` (invalid JSON), and `mock-slow`
(30-second pause after early safe text). Prompts are validated but do not influence
this scripted output.

Run tests and repeat measurements:

```bash
uv run --no-sync pytest stream_guardrail/tests -q -s
uv run --no-sync pytest -q
```

Latest follow-up verification in the existing project environment: **119 guardrail
tests included in 315 passing repository tests**, plus configured Ruff
correctness checks. The latest mock run measured 9 ms to first safe text and
335 ms total. Historical measurements below remain fixture-specific evidence,
not latency or memory guarantees.

Tests require permission to bind loopback sockets. Restricted sandboxes may
need an unrestricted run. `--no-sync` uses the existing environment; it does not
perform dependency synchronization. Earlier fresh-source installation was
verified separately with `uv sync --locked`; the latest audit used the normal
working environment. See the plan for the historical verification details.
No dependencies were added in this task.
AnyIO shielding reuses the dependency already required by the installed
FastAPI/Starlette and HTTPX stack.

Configuration defaults are in [../.env.example](../.env.example); export values
in your shell, since `.env` files are not loaded automatically. The upstream URL
is fixed by trusted configuration. Limits count request/event bytes and pending
Unicode characters. Read timeout also bounds incoming request upload time; total
provider timeout starts after validated request admission. Compressed upstream
responses and redirects are rejected. No client cookies/auth headers are relayed.
Mock delay, text segmentation and wire segmentation settings must be positive.
Mock uploads use the configured request-byte limit and upload deadline too.
Intersecting supported PII values are masked as one connected span, so a card
match cannot expose the domain of an email beginning inside it.

The supported request has only `model`, `messages`, `stream: true` and optional
`n: 1`. Message roles: system/user/assistant; content must be text. At most 64
messages, 32,768 characters per content string, 128 characters per model name and
65,536 request bytes by default. Unknown fields and coercion are rejected.

Upstream events must contain exactly one index-0 choice, a text/role/empty delta,
and an optional `stop`, `length` or `content_filter` finish. Basic token usage
events with empty choices are accepted and discarded. Tool calls, reasoning
fields, multimodal content, multiple choices and non-null logprobs are rejected.
Provider metadata is validated and discarded; no uninspected fields are relayed.
This intentionally narrow SSE subset does not promise compatibility with every
Chat Completions provider or client.

Before streaming, invalid requests return 400/413/408 and upstream failures return
502/504 with fixed messages. After streaming begins, failure emits the following
when the downstream remains writable:

```text
event: error
data: {"error":{"code":"stream_failed","message":"Generation stream failed"}}
```

If a writer is blocked or disconnected, the stream can terminate without an
error event. Clients must treat missing `[DONE]` as incomplete, including when
HTTP headers already said 200. There is no `[DONE]` after failure. Success requires a finish reason followed by
`[DONE]`; buffered redacted text precedes the outgoing finish and `[DONE]`.
Premature EOF discards unresolved text. Cancellation closes the upstream.
Already emitted safe text cannot be retracted.

See [the implementation plan](STREAM_GUARDRAIL_IMPLEMENTATION.md) for the exact
PII grammar and false positives. Ordinary unfinished words can become email
local parts, so long words may hit the fail-closed limit. The detector is not
comprehensive PII protection. Task 3 baseline acceptance is complete. The latest
[audit report](AUDIT.md) records corrected overlap/CR-framing defects and
prioritized hardening suggestions; passing the earlier suite did not rule out
those defects.

Long-stream acceptance runs the gateway in a separate process and validates each
output event without collecting the full response. With 16× output (140,410 to
2,244,730 bytes), traced allocation peaks were 345,125 and 604,044 bytes; retained
traced allocations stayed near 44–45 KB. RSS high-water increased from 69,304,320
to 69,615,616 bytes in that run. Warm-up excludes lazy-import costs. These are
fixture measurements; tracing excludes some native allocations and RSS is a
process high-water mark, not a per-request allocation counter.

A real socket-pressure test stops reads until upstream production stalls, then
performs 50 throttled reads and disconnects. It verifies redaction, backpressure
and provider cleanup. Repeat just these measurements with:

```bash
uv run --no-sync pytest stream_guardrail/tests/test_stream_memory.py -q -s
```

The fresh-environment check used a source snapshot with a new virtual environment,
not a literal Git clone, because the implementation files are still untracked.
Verification ran on macOS with Python 3.13.3; Python 3.11 was not separately tested.
Linting and the later Task 4 implementation are tracked in the root README;
the measurements above record this task's acceptance run.

## Reviewer walkthrough and current gap assessment

This task is a bounded local streaming filter, not comprehensive production DLP.
Every generated text delta follows the same path: HTTP bytes → SSE framing →
strict event validation → stateful redaction → newly encoded SSE. Provider
metadata is discarded. There is no unchecked pass-through path or background
queue collecting the complete response.

Review `redactor.py` for safety decisions; `app.py` for framing, request/event
validation and response ownership; `mock_provider.py` for the synthetic script.
Keeping the small parser in app.py follows the implementation plan. A generic
provider framework or shared service layer would not improve the current scope.

| Concern | Current handling and evidence | Limit to disclose |
| --- | --- | --- |
| Partial PII across deltas | Overlapping match intervals are merged before release/replacement; candidate suffix retention; every split and character-by-character fixtures in test_redactor.py | Only the documented grammar is covered |
| Unbounded/expensive candidates | Pending and event caps; overlong candidates abort; skip complete-pattern scans when nothing can be released | Long legitimate words may fail closed; no universal CPU bound under arbitrary configured limits |
| Byte/SSE/JSON boundaries | UTF-8, BOM, LF/CRLF/CR, multi-line events and malformed input tests | This is the documented single-choice Chat Completions subset |
| Generated text in unexpected fields | Strict event allowlist; unsupported modes fail rather than relay | Reasoning, tools, multimodal output and general SDK compatibility are unsupported |
| Completion and truncation | Finish plus DONE required; transport errors discard pending text | Verified successful EOS releases incomplete nonmatching candidates, even on a length finish |
| Header/send failure, disconnect, deadline | Response owns upstream cleanup; tests include an unstarted generator, blocked writes and ASGI 2.3/2.4 paths | An unwritable client cannot be guaranteed an SSE error event |
| Streaming and backpressure | Real HTTP early text, socket backpressure, 1/4/8 clients plus a stalled reader, and CPU-heavy concurrency tests | Kernel/HTTP buffers still exist; tested concurrency is not a capacity guarantee |
| Memory efficiency | Separate gateway process; 16-fold stream-length comparison and concurrent traced allocation samples | Tracing/RSS samples do not prove a universal concurrency/RSS limit |
| Production exposure | Fixed trusted upstream URL; bounded input, no caller credentials relayed | Authentication, concurrent-stream admission, deployment controls and live-provider compatibility are outside this baseline |
| Reproducibility | Lockfile and prior fresh-environment/source-clone runs; current project environment rerun | Tested on macOS arm64/Python 3.13.3; no Python 3.11 or saturation certification |

An important policy example: `jane@example.com` is redacted. If a provider sends
`jane@exam`, a valid length finish, then DONE, that incomplete address is emitted
because it is not a complete supported pattern. The same pending text is discarded
on premature network EOF. `test_successful_length_finish_only_redacts_complete_supported_patterns`
records this distinction. A stricter partial-identifier policy would be a separate
privacy/false-positive decision; do not claim the current filter covers it.

From the repository root, validate the reviewed working tree using the existing
environment:

```bash
uv sync --locked
uv run pytest stream_guardrail/tests -q -s
uv run ruff check stream_guardrail
```

The first command synchronizes the repository's `.venv`; it does not create one
virtual environment per task. The tests spawn temporary processes using that
same interpreter. A separate process for measuring gateway allocations prevents
the test client/provider from contaminating the gateway measurement; it is not
a separate dependency environment.

Then inspect the suites by responsibility:

```bash
uv run pytest stream_guardrail/tests/test_redactor.py -q
uv run pytest stream_guardrail/tests/test_streaming.py -q
uv run pytest stream_guardrail/tests/test_http_streaming.py -q -s
uv run pytest stream_guardrail/tests/test_stream_memory.py -q -s
```

The first two cover candidate boundaries, failure policy, framing, validation
and lifecycle edge cases. The HTTP suite proves text arrives before the provider
finishes and exercises actual disconnects/timeouts. The memory suite checks an
isolated gateway and a stalled/throttled socket; output validation stays
incremental rather than collecting the response in a test client.

For manual inspection, start the two services using the commands above and send
the curl request. Read only `choices[0].delta.content` when reconstructing output;
finish/DONE must come after the final redacted text. Change only the model name:

| Request | Expected observation |
| --- | --- |
| mock-pii | Three REDACTED replacements, preserved safe text, finish then DONE |
| mock-malformed / mock-truncate | Safe prefix, fixed error event, no unresolved email prefix and no DONE |
| mock-reject | HTTP 502 with fixed provider-unavailable JSON, not the mock's body |
| stream false or an unknown request field | HTTP 400 before contacting the provider |
| mock-slow then abort the client | Automated disconnect test proves the provider closes; a curl response alone cannot prove resource cleanup |

Curl is a convenient demonstration. The automated side-effect, allocation and
cleanup assertions are the evidence for properties that cannot be proved by
looking at the response alone.

## Performance validation

Repeat the matching benchmarks, event-loop/deadline/cancellation regressions and
concurrent HTTP measurements in the existing environment:

```bash
uv run --no-sync pytest stream_guardrail/tests/test_performance.py -q -s
```

A ~60 KB adversarial delta originally blocked the event loop for 429 ms. The
gateway now explicitly yields between 8,192-byte framing slices and 256-character
redaction batches. The redactor retains the same state across those batches;
only safe output is emitted, and no background queue was added. Large input
deltas may produce multiple outgoing SSE deltas, so clients must reconstruct
content rather than assume provider/output event boundaries match.

At default limits, the final run measured a 10 ms single-stream heartbeat gap;
50 ms deadline/cancellation requests stopped at about 55/56 ms. With eight
CPU-heavy streams, the maximum gap was 49 ms and completion took 3.29–3.44 s.
With eight paced streams plus a stalled reader and allocation tracing enabled,
first safe text arrived in 67–84 ms. After the stalled reader disconnected,
gateway/provider active counts returned to zero in 47 ms. Profiled peak was
approximately 1.05 MB and retained growth after cleanup approximately 144 KB.

These results demonstrate responsiveness at the tested load, not increased CPU
capacity or unlimited concurrency. Matching was approximately 146k–897k chars/s
depending on the fixture. See [AUDIT.md](AUDIT.md#performance-validation-follow-up)
for exact workloads, budgets, before/after evidence and measurement limits.

The stream_guardrail directory remains untracked in the working repository.
Before another reviewer clones a review branch, include it and all required
metadata/docs in that branch; earlier temporary snapshot verification does not
make untracked files part of the working repository's HEAD.

## Why temporary verification environments existed

Development and most tests used the repository's `.venv`. Temporary environments
were additional reproducibility checks: a new environment installed from uv.lock
can reveal undeclared dependencies or reliance on local environment state. A
fresh source checkout also exposes dependence on files omitted from the review.
They do not make redaction logically safer, and they are not needed on every run.
One final clean install or a CI job is usually enough.

At the user's request, the temporary guardrail snapshot, router acceptance
snapshot/clone, and temporary uv cache were removed. The working `.venv`, source,
lockfile and application data were preserved. Historical temporary paths in the
implementation plan are verification records, not directories needed to run the
task. No new environment was created for this follow-up audit.
