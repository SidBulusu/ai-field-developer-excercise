# Stream guardrail audit

Scope: the stream_guardrail implementation, its tests and documented contract.
The model router was not changed. This review used the existing project `.venv`;
no dependency, additional environment or source clone was created.

The earlier passing suite missed a partial-disclosure case. The findings below
are based on reproduced behavior and regressions, not an assumption that passing
tests establish complete PII protection.

## Findings corrected

### High: overlapping matches exposed a portion of recognized PII

Before this audit, `4111 1111 1111 1111@example.com` became
`[REDACTED]@example.com`. Ordinary non-overlapping regex substitution consumed
the card and skipped the email starting inside it. A chain such as
`a@b.co@c.com@d.net` could also release text before the final delimiter. These
examples matter because the documented grammar recognizes substrings, even when
the complete enclosing token is not a valid mailbox.

Correction: [redactor.py](redactor.py) now discovers overlapping match starts and
merges intersecting intervals. Both release-boundary calculation and replacement
use that same union. A flush cannot cut into a recognized overlapping value.
Connected overlaps produce one `[REDACTED]`; nonintersecting values remain separate.

Evidence: the new overlap regressions failed before the fix and pass afterward.
They cover every single text split, character-at-a-time input, transitive overlap,
and gateway input split at every network byte. Existing ordinary text, multiple
values, UTF-8 and marker-order tests also pass.

### Medium: valid CR-delimited events stalled until another byte or EOF

`SSEParser.feed(b"data: safe\r\r")` previously returned no event. It dispatched
only when another byte arrived or `finish()` ran. A provider pausing after that
valid delimiter therefore prevented otherwise safe text reaching the client.
The [SSE parsing rules](https://html.spec.whatwg.org/multipage/server-sent-events.html#parsing-an-event-stream)
recognize CR as a line ending; waiting for a possible LF is unnecessary.

Correction: [app.py](app.py) dispatches CR immediately and treats a following LF
as the optional remainder of that newline. Its byte count is retained until that
decision, so split CRLF cannot bypass the raw event cap. An optional LF is charged
when it arrives; a cap violation at that point can terminate after an already
redacted event was delivered. The byte is not buffered beyond the cap.

Evidence: immediate-dispatch regression, exact-fit/one-byte-over event caps for
LF/CR/CRLF, and real loopback HTTP where the provider waits until the client
receives the first safe CR-delimited event. The old behavior deadlocks that
handshake until a client timeout. EOF now validates framing without a duplicate
event-finalization branch in the gateway.

### Medium: mock-provider uploads had no deadline

The mock could retain a connection while a caller slowly uploaded an unfinished
body. Its size limit was also hardcoded rather than using configuration.

Correction: [mock_provider.py](mock_provider.py) uses the same configured request
limit and upload deadline as the gateway, with fixed 413/408 messages. Tests
exercise an oversized request and an asynchronous stalled upload independently
of the gateway.

## Decision comments added

Comments now explain the invariants or failure modes at the relevant code:

- Why consuming regex matches is unsafe for intersecting PII, and why matching
  and emission must share the merged spans.
- Why an ambiguous candidate cannot temporarily exceed the cap, even if the next
  character might resolve it.
- Why CR dispatch must be immediate while optional LF bytes still count.
- Why comments and unfinished SSE lines count toward the framing limit.
- Why duplicate JSON fields, nonfinite values and lone surrogates are rejected.
- Why an allowlist must cover the whole provider event, not just content.
- Why the response owns cleanup even before its generator starts, and injected
  HTTP clients remain caller-owned.
- Why compressed upstream responses are rejected before decoding.
- Why finish alone cannot release pending text, and errors after headers require
  fixed in-band messages or connection termination.
- Why the mock's segmentation adapter explicitly closes its source iterator.

These comments describe constraints and consequences. They do not annotate each
assignment, loop or HTTP call with an English restatement.

## Recommended robustness work, in priority order

These are suggestions, not newly implemented guarantees. P1 denotes a decision or
control to address before broader exposure; P2 denotes operational hardening.

| Priority | Concern | Suggested change | Required validation |
| --- | --- | --- | --- |
| P1 privacy policy | A valid length/content-filter finish plus DONE still releases incomplete nonmatching identifiers such as jane@exam | Decide whether forced generation termination should discard unresolved text; keep normal-word preservation and privacy tradeoffs explicit | Complete and incomplete addresses/SSNs/cards under stop, length, content_filter, transport EOF and cancellation |
| P1 deployment | Per-stream caps do not bound aggregate concurrent requests, queued connection-pool waiters or CPU | Add authenticated admission where exposed, a finite active-stream limit and bounded/short queue; hold admission until response cleanup | Overload denied before upstream I/O, cancelled requests release permits, slow readers cannot starve admitted clients, worker-wide limits are understood |
| P1 integration | The documented SSE subset is not a drop-in implementation of every provider/client schema | Before a live integration, add sanitized provider fixtures and actual client compatibility tests; generate required response metadata locally | Role/usage/finish variations, explicit rejection of unsupported modes, required SDK response fields and absence of metadata bypass |
| P2 CPU fairness, partially addressed below | Default-limit benchmarks reproduced stalls; cooperative framing/text batches now yield to other tasks | Retain the regression budgets; remeasure before increasing configured limits or supported concurrency | Default-limit heartbeat, deadlines, cancellation and 1/4/8 concurrent heavy streams now tested; arbitrary tuning and saturation remain unverified |
| P2 cleanup lifetime | Shielded upstream/client closure has no separate time budget | Bound best-effort cleanup so a stuck transport cannot hold shutdown forever | A deliberately stalled aclose completes/aborts within policy without swallowing caller cancellation or writing raw text |
| P2 observability | Fixed errors are safe but provide little operational diagnosis; configured URL queries may contain secrets in HTTPX INFO logs | Add content-free failure/latency/buffer counters and correlation IDs; prohibit credential-bearing queries or sanitize URL logging | Canary secrets never appear in application/dependency logs, counters distinguish framing/redaction/timeout failures, metrics do not contain text |
| P2 reproducibility | Python 3.11 and the eventual review branch have not been tested by CI | Include currently untracked task files in the review commit and run locked CI on the supported Python/platform matrix | Fresh checkout of that actual commit, independent Task 3 tests, full suite and configured lint |

Do not add a generic provider framework, large PII dependency or file split solely
to make the implementation appear more sophisticated. The current three-file
runtime layout has identifiable responsibilities. A future extraction should
remove demonstrated complexity or duplication and preserve these invariants:

1. Every emitted generated-text character passes through the redactor.
2. Every character in a recognized intersecting match is redacted.
3. Possible future match prefixes stay pending until resolved or trusted EOS.
4. Framing and pending text stay bounded independently of stream length.
5. Backpressure stops further application consumption; no unbounded queue exists.
6. Errors, cancellation and premature EOF never flush unresolved text or emit DONE.
7. The response owns its upstream lifetime, including failed header writes.

## Verification performed

Before correction, seven targeted regression cases failed: overlapping PII at
unit/gateway levels and CR-delivery cases. After correction the focused redactor
and parser/gateway suites passed 102 tests. Then, in the existing environment:

```bash
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# 306 passed in 26.29 s, including 110 stream_guardrail tests
uv run --no-sync ruff check .
# All checks passed
```

The full run includes the new real-HTTP CR handshake, long-stream allocation
comparison, stalled/throttled socket reader, header-send failure, ASGI 2.3/2.4
cancellation/deadlines, and both provider cleanup and owned-client shutdown.
The network tests required local-socket permission outside the sandbox.

In that run the scripted mock produced first safe text at 0.0081 s and completed
in 0.3365 s. For 1,024/16,384 events, traced gateway peaks were 345,457/606,046
bytes; retained traced memory was 45,021/44,218 bytes. RSS high-water was
69,681,152/69,992,448 bytes. These are single-stream fixtures with profiling
overhead; they do not establish a universal latency, RSS or concurrency bound.

No fresh-install, live-provider, hosted deployment, cross-version Python or
multi-client saturation certification was performed in that audit. Earlier
source-snapshot verification remains historical evidence for that earlier code.

## Performance validation follow-up

The evaluation criteria emphasize asynchronous chunking/state, partial-stream
matching performance, and bounded-memory low-latency proxying. The follow-up
therefore measured matching first, then concurrent HTTP streams. It used the
existing `.venv`, macOS arm64, Python 3.13.3, synthetic providers and default
256-character pending / 65,536-byte event limits. No dependencies, environments
or clones were added. A separate gateway process uses the same interpreter and
keeps provider/client allocations out of gateway measurements.

### Reproduced scheduling defect and correction

A valid ~60 KB delta containing repeated 250-character words blocked the event
loop for **429 ms**. Its synchronous matching also prevented a 50 ms asyncio
deadline from being serviced promptly. Both new heartbeat/deadline regressions
failed before correction. Bounded candidate memory had not bounded the duration
of one synchronous call over a complete delta.

`app.py` now schedules framing in at most 8,192-byte input slices and redaction
in at most 256-character text batches, with explicit `await asyncio.sleep(0)`
checkpoints. Awaiting a socket read or yielding an SSE frame alone is insufficient:
those operations can complete without suspending. Pending state survives batch
boundaries, and only confirmed-safe output is emitted. There is still one
producer, no background queue or thread pool, and no whole-response collection.
Safe portions of a large event can now reach clients before its entire text delta
has been redacted. An eventual failure cannot retract those already safe portions.

The first 1,024-character batch experiment reduced the single-stream gap to
~23 ms but still produced **173 ms** gaps with eight CPU-heavy streams. Reducing
to 256 characters brought the eight-stream gap to **49 ms** in the final run.
This is a measured scheduling choice, not a universal CPU bound or a faster regex.

### Matching measurements

`test_performance.py` measures three repetitions at approximately 4/16/60 KB,
using the median, and verifies exact output and the pending-state bound. Fixtures
cover ordinary prose, near-limit local parts, many domain labels, transitive
overlaps and 19-digit space-separated numbers. Reference substitution and fixture
construction are outside the measured interval.

| Workload | ~4 KB median | ~16 KB median | ~60 KB median | ~60 KB throughput |
| --- | ---: | ---: | ---: | ---: |
| Ordinary prose | 4.62 ms | 18.30 ms | 66.90 ms | 897k chars/s |
| 250-character local parts | 27.58 ms | 112.63 ms | 411.16 ms | 146k chars/s |
| Repeated domain labels | 26.63 ms | 108.80 ms | 401.91 ms | 149k chars/s |
| Overlapping email chains | 20.64 ms | 82.05 ms | 299.93 ms | 200k chars/s |
| Separated numeric candidates | 17.84 ms | 71.37 ms | 261.12 ms | 230k chars/s |

Throughput stayed approximately stable as total input length increased for these
fixed candidate sizes. This supports bounded-window matching; it does not prove
linear complexity for arbitrary strings or larger configured candidate limits.
The stand-alone redactor remains synchronous; cooperative scheduling belongs in
the async gateway. On the ~60 KB adversarial delta, final gateway measurements
were 429 ms total with a 10.11 ms maximum heartbeat gap. A 50 ms deadline stopped
at 55 ms, and explicit cancellation at 50 ms completed at 56 ms and closed the
provider stream without consuming its finish/DONE frames.

### Concurrent HTTP measurements

First warm the gateway. Then profile 80-event streams with 5 ms provider pauses,
using 1/4/8 clients while a separate raw-socket client stops reading. The test
waits for upstream production to stall before measuring the other clients. Its
475-event stall point is OS/transport dependent, not a hardcoded expectation.
Every normal stream is checked incrementally for exact redaction and DONE.

| Active reading clients | First safe text range | Total duration range | Maximum heartbeat gap |
| --- | ---: | ---: | ---: |
| 1, without stalled client | 8.80 ms | 460 ms | — |
| 1 + stalled client | 39.22 ms | 505 ms | 31.61 ms |
| 4 + stalled client | 52.21–63.28 ms | 542–554 ms | 27.64 ms |
| 8 + stalled client | 67.11–83.79 ms | 669–687 ms | 19.64 ms |

These runs include allocation tracing and instrumentation overhead. After closing
the stalled socket, gateway and provider active counts reached zero within
47 ms. Across the profiled workload, traced peak was 1,052,833 bytes; retained
allocations rose from 19,754 to 163,331 bytes after cleanup. Connection caches and
transport allocations remain; memory is not expected to return exactly to the
initial sample. RSS high-water rose from 72,843,264 to 74,694,656 bytes. RSS is a
process lifetime high-water value, and tracing excludes some native allocations.

Next, stop allocation tracing and run 1/4/8 concurrent streams, each with one
~60 KB delta of near-limit local parts. There is no stalled reader in this CPU
phase. At eight clients, first safe text ranged 26–195 ms, total duration was
3.29–3.44 s, and the maximum heartbeat gap was 48.94 ms. This demonstrates
cooperation under the tested load; throughput still shares a single event-loop
thread. It does not establish unlimited concurrency or replace admission limits.

### Regression budgets and reproduction

Budgets were chosen as local regression targets, not supplied by the rubric:
maximum heartbeat gap <100 ms; single-stream deadline/cancellation completion
<200 ms; concurrent first safe text <1 s; normal streams <3 s; CPU-heavy streams
<6 s. After cleanup, retained traced growth must be <1 MiB and peak growth
<8 MiB relative to the warmed baseline. These loose tolerances catch stalls or
response-sized retention; they are not production service objectives. Hardware,
Python version, profiling and other machine load affect results.

```bash
uv run --no-sync pytest stream_guardrail/tests/test_performance.py -q -s
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# Final run: 315 passed in 44.72 s, including 119 guardrail tests.
uv run --no-sync ruff check .
# All checks passed.
```

Loopback binding requires socket permission. The profiling/heartbeat endpoints
exist only in the test factory and are never mounted on the production gateway.
Existing long-stream and socket-pressure tests also pass after the scheduling
change. Still unverified: higher concurrency, mixed heavy/stalled saturation,
multiworker deployment, different configured caps, Python 3.11 and live providers.
