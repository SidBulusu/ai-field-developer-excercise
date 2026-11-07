# Model Router — Implementation Plan

> **Location:** `model_router/`  
> **Goal:** Build a reliable LLM completion router with per-tenant token limits, automatic primary-provider fallback, sanitized errors, and persistent on-disk SQLite state.  
> **Shared rules:** Follow `../AGENTS.md`: review existing code first, plan small changes, favor readable functions, reuse existing libraries, and add dependencies only when the active task needs them.  
> **Status:** Audit gaps resolved and verified in the existing environment: 103 router tests included in 366 passing repository tests; configured Ruff checks pass. Section 9 records bounded database scheduling, cancellation policy and crash/concurrency evidence.

## 1. Required behavior and evaluation evidence

Implement a model-routing HTTP endpoint that accepts completion requests, identifies the tenant by API key, limits the tenant to **50,000 tokens in a rolling 60-second window**, attempts the primary model, and invokes a backup model if the primary returns **HTTP 429** or fails to complete within **3,000 ms**. Persist rate-limit state in **a real SQLite file on disk** and return consistent sanitized gateway errors.

| Requirement / evaluation criterion | Evidence to provide |
| --- | --- |
| Token-aware sliding-window limiter | Prompt/output token accounting, atomic reservations, exact rolling 60-second expiry, tenant isolation |
| On-disk SQLite | A real file-backed database, schema/indexes, process-restart persistence, concurrent-connection tests |
| Primary returns 429 → backup | Primary called once, backup called once, valid backup completion returned |
| Primary exceeds 3,000 ms → backup | Enforced **whole-attempt** deadline, cancelled/closed primary request, successful backup response |
| Async concurrency and timeout races | Concurrent requests do not exceed budget; timeout vs. success and cancellation do not duplicate responses |
| Rate-limit state eviction | Exact boundary tests and safe treatment of abandoned in-flight reservations |
| Standardized error sanitization | Stable error schema; no raw exception, upstream body, stack trace, credential, or internal URL leaks |

**Supported subset:** Implement **text-only, single-choice, non-streaming JSON chat completions** using the same local mock-provider shape throughout. Task 3 owns streaming; do not add an SSE streaming router here without a requirement. Live LLM providers are optional and must not be required to run tests.

## 2. Explicit behavior and design decisions

These are project choices made to remove ambiguity; they are **not extra requirements** from the task statement.

### 2.1 Request and tenant contract

- Expose `POST /v1/chat/completions` (or one documented equivalent) and read the tenant key from `X-API-Key` or another single documented header. Do **not** accept an arbitrary client-supplied tenant identifier without tying it to the API key.
- Require a valid key, a supported text-only `messages` array, **one completion** (`n=1`), `stream: false`, and an explicit positive integer `max_completion_tokens`; bound its allowed maximum. Reject unsupported request shapes **before** reserving tokens or calling an upstream provider.
- For the local implementation, use configured high-entropy development keys. Persist a stable **keyed digest or hash** for tenant identification, never the raw API key. Do not introduce a user database or OAuth service.
- Use fixed upstream URLs from configuration, not URLs supplied in client request bodies. Keep provider credentials, if any, out of code and logs.

### 2.2 What counts as a token?

- **Budget unit:** count **prompt tokens + completion tokens**, per logical client completion request. Use `50_000` tokens and `60_000` milliseconds as configurable defaults.
- **Before dispatch:** calculate the prompt-token count with a **documented tokenizer compatible with the provider under test**; add the required `max_completion_tokens` to form a conservative reservation. The input tokenizer must include any supported message-format overhead. Do not count characters, words, or client-declared token totals and label them *exact provider tokens*.
- **Local mock contract:** specify a simple deterministic mock tokenizer and use the **same token-count contract** in both gateway tests and mock-provider `usage` responses. This makes local accounting independently testable without a hosted API. Clearly document that its counts are **mock-token counts**, not universal model-token counts. A real provider would need its corresponding tokenizer/counting API and message-overhead handling; add a tokenizer dependency only if required by the active implementation.
- **At admission:** require `reservation = counted_prompt_tokens + max_completion_tokens`; reject requests exceeding the tenant's remaining budget. Neither the client nor a mock response may silently raise the output cap after admission.
- **On known success:** if the provider returns valid `usage.prompt_tokens`, `usage.completion_tokens`, and `usage.total_tokens` consistent with the documented contract and output cap, **atomically replace the reservation** with the observed logical usage for that request. Provider-reported usage can include non-visible completion tokens; do not infer completion usage from response text length.
- **Missing or unreliable usage:** retain the full reservation through the remainder of its 60-second window (conservative), rather than guessing that a request cost zero. Reject structurally invalid successful provider responses with a sanitized error. If reported usage exceeds the reserved upper bound, record/retain a conservative charge, return a controlled upstream-protocol error, and document the provider-contract violation; do not silently undercount.
- **Fallback accounting:** one logical client request creates **one** rate-limit reservation. On primary HTTP 429 (assume no generated completion), a successful backup can reconcile to the backup's validated usage. After a **primary timeout**, the primary may have consumed unknown tokens; retain the original reservation for the window even if the backup succeeds. This is a deliberately conservative tenant-quota policy, **not** a claim of exact provider billing across both attempts. For known no-work failures, release only when safe; for ambiguous failures, retain reservation.

### 2.3 Exact rolling-window and crash policy

- Define the active window at time `now_ms` as records where `recorded_at_ms > now_ms - 60_000` and `recorded_at_ms <= now_ms`. A record becomes eligible for eviction at exactly **60,000 ms** after admission. This is **not** a calendar-minute counter.
- Timestamp each request at its successful admission. Reconciliation updates its charged tokens but **does not move its original admission timestamp**, which would inadvertently extend the window.
- **Pending/in-flight reservations count against capacity.** After a process crash or cancellation before trusted usage is settled, an unfinished reservation remains conservatively counted until the original 60-second boundary. A settlement already running with validated usage may finish after cancellation; it records known usage without moving admission time. Queued abandoned DB operations are skipped when drained; running operations cannot be forcibly cancelled. No background cleanup worker is required; expired rows can be deleted during subsequent short write transactions. This policy assumes bounded provider-attempt durations significantly shorter than 60 seconds.
- Use an injectable **wall-clock millisecond source** for persisted time-window calculations and deterministic expiry tests. Use a **monotonic event-loop clock/deadline** for provider timeouts. These are separate concepts.

### 2.4 Routing and failure policy

- **Primary attempt:** give the complete primary HTTP operation a **3.000-second total deadline** (for example, `asyncio.timeout(3.0)` around the HTTPX request). Do not assume an HTTPX read timeout limits total duration; its read timeout measures waiting for data, not total elapsed time.
- **Fallback triggers:** only (a) primary **HTTP 429** or (b) primary **whole-attempt timeout** are mandatory. On primary success, do not call backup. On primary HTTP 400/401/403/500 or a malformed primary success response, return a sanitized error rather than automatically falling back; document optional handling of transport failures separately.
- **Secondary attempt:** use a separate bounded/configurable timeout (suggested local default: **5 seconds**). Never retry indefinitely or cascade to more providers. A second 429 or timeout ends in a sanitized gateway error.
- **At the deadline:** do not emit a late primary success after fallback has begun. Cancellation must close local HTTP resources; a remote model may still be running, so quota accounting remains conservative.
- **Single response:** return exactly one selected successful completion or one standardized gateway error. Because this task uses non-streaming completions, do not forward partial primary responses before fallback is decided.

### 2.5 Standardized client errors

Use one stable payload shape for **gateway-generated errors**, regardless of their source:

```json
{
  "error": {
    "code": "UPSTREAM_UNAVAILABLE",
    "message": "Unable to complete the request.",
    "request_id": "req-example"
  }
}
```

| Condition | Suggested HTTP status | Safe code |
| --- | --- | --- |
| Malformed or unsupported completion request | 400 / 422 | `INVALID_REQUEST` |
| Missing or unrecognized tenant API key | 401 | `INVALID_API_KEY` |
| Tenant would exceed its token window | 429 | `RATE_LIMIT_EXCEEDED` |
| Primary non-429 provider failure | 502 | `UPSTREAM_UNAVAILABLE` |
| Backup fails after required fallback | 502 / 504 | `UPSTREAM_UNAVAILABLE` |
| SQLite is temporarily busy/unavailable | 503 | `GATEWAY_UNAVAILABLE` |
| Upstream success has invalid/missing essential response structure | 502 | `UPSTREAM_INVALID_RESPONSE` |

Never return internal provider URLs, full upstream response bodies, raw exceptions, stack traces, SQL details, or secret header values. Log minimal request IDs and safe failure categories, not plaintext API keys or customer prompts.

## 3. Minimal implementation layout

```text
model_router/
├── app.py                             # HTTP route, validation, provider routing
├── limiter.py                         # SQLite ledger, atomic admission/reconciliation, clock policy
├── mock_provider.py                   # Scripted primary/backup responses and usage
├── MODEL_ROUTER_IMPLEMENTATION.md     # This plan
└── tests/                             # Acceptance tests created as needed
```

**File discipline:** Inspect existing implementations first; do not replace working code. Keep SQLite routines in `limiter.py` if this reduces complexity; extract `routing.py` or `database.py` **only when a real readability or testability need emerges**. Reuse root `pyproject.toml`, `uv.lock`, and `.venv/`; do not create a new environment or generic provider framework. Record verified run commands in a local `README.md` after they actually work.

**Dependencies:** Reuse FastAPI, HTTPX, Uvicorn, Pydantic, and pytest if already installed for Tasks 1–3. Start with Python's **standard-library `sqlite3`, `asyncio`, `hashlib`/`hmac`, and time utilities**. If `sqlite3` runs within async request handling, use `asyncio.to_thread` (or a comparably small approach) for short blocking database transactions; do not block the event loop or share a SQLite connection across the wrong threads. Add `aiosqlite`, a tokenizer library, or async-test plugins **only if current functionality/tests justify them**.

## 4. End-to-end request flow

```text
Client POST completion + tenant API key
    |
    v
Validate key, request shape, prompt token count, output cap
    |
    v
SQLite BEGIN IMMEDIATE (short transaction)
    - calculate cutoff from injected wall clock
    - evict expired ledger rows
    - sum active usage + pending reservations for this tenant
    - admit and insert ONE reservation, or reject
SQLite COMMIT / ROLLBACK
    |
    +---- over budget --> standardized HTTP 429; NO provider calls
    |
    v
Primary provider (entire attempt deadline: 3,000 ms)
    |
    +---- success --> validate usage, reconcile once, return
    |
    +---- HTTP 429 / total timeout --> secondary provider
    |                                      |
    |                                      +--> success: apply documented accounting; return
    |                                      +--> failure: sanitized gateway error
    |
    +---- other error --> sanitized gateway error (no automatic fallback)

All ledger finalization is short/atomic; no SQLite transaction
is held open while waiting for a model provider.
```

## 5. Implementation phases

### Phase A — Establish request contract and deterministic provider mocks

- [x] Read existing `model_router/` code and root dependencies; preserve working behavior and install nothing speculatively.
- [x] Define one supported non-streaming text-completion request/response contract and tenant-key header. Validate `max_completion_tokens`, message shapes, and unsupported streaming/multimodal features.
- [x] Implement a provider-compatible deterministic mock token counter, with explicit examples and test expectations. Avoid pretending that mock tokens are exact live-model tokens.
- [x] Make the mock provider return success, HTTP 429, HTTP 500, late 200 response, timeout, malformed response, and valid/invalid `usage`. Make primary and secondary distinguishable in test responses.
- [x] Verify primary and backup can run locally on separately configured loopback endpoints and that the gateway can forward a normal request.

### Phase B — Persistent SQLite ledger and atomic sliding-window admission

- [x] Configure a **file-backed** database path (e.g., `model_router/runtime/model_router.db`, ignored by Git); explicitly reject `:memory:` in normal run configuration.
- [x] Create a minimal `reservations` ledger with `request_id` (unique), tenant key digest, `recorded_at_ms`, `charged_tokens`, and state (`pending`, `settled`, `uncertain`) as needed. Add an index on `(tenant_key_digest, recorded_at_ms)`.
- [x] Use one **`BEGIN IMMEDIATE`** transaction to (i) delete rows with `recorded_at_ms <= now_ms - 60_000`, (ii) sum tenant active charged tokens, (iii) check admission, and (iv) insert the reservation. Commit before any HTTP await; rollback on all failures.
- [x] Set a finite SQLite busy timeout. On lock/IO errors, return a sanitized gateway failure without calling a provider. WAL mode may be used for readers/writers, but **WAL does not permit simultaneous SQLite writers**.
- [x] Open/close connections with controlled lifetimes; when using `sqlite3` in async FastAPI handlers, move short blocking DB operations off the event-loop thread. Ensure one connection is used only in its owning thread.
- [x] Reconcile successful usage in a **separate short transaction** on the original ledger row, not by inserting a second charge or modifying the admission timestamp.
- [x] Treat cancelled and crash-abandoned reservations conservatively, expiring them at the original window boundary; no in-memory-only authoritative counters.

### Phase C — Async primary/secondary fallback

- [x] Use a reusable lifecycle-managed HTTPX `AsyncClient`, fixed configured provider URLs, and provider-specific credentials held only in environment configuration if needed.
- [x] Apply an actual **3.000-second whole-primary-attempt deadline**. Distinguish it from HTTPX connect/read/write/pool timeouts, and test a response that trickles bytes without ever finishing.
- [x] Return primary success directly; never contact backup in this case.
- [x] Fall back once on primary HTTP 429; fall back once on primary whole-attempt timeout, closing/cancelling local resources as appropriate.
- [x] Bound the secondary attempt by its own timeout. Do not retry arbitrarily, attempt both providers in parallel, or return a partially completed primary response.
- [x] When primary succeeds just before or just after the deadline, ensure the request resolves into **one** decision and one client response; avoid using a late result after fallback starts.
- [x] Ensure fallback shares the existing logical reservation; apply the distinct `429` versus uncertain-timeout accounting rules.

### Phase D — Errors, cleanup and persistence behavior

- [x] Centralize the *shape* of gateway errors in a small helper; do not build a generic error framework.
- [x] Distinguish a **tenant's own HTTP 429** from the **provider's HTTP 429**. Tenant limit → reject without upstream; provider 429 → invoke backup.
- [x] Map invalid requests, missing keys, database failures, upstream failures, malformed provider responses, and backup failures to documented safe codes.
- [x] Validate provider response shape and usage (nonnegative integer counters, consistent `total_tokens`, completion count within cap) before accounting and returning it.
- [x] Keep the SQLite file and its WAL/SHM sidecars out of Git; ensure a restart with the same DB file preserves live-window state.
- [x] Ensure cancellation, errors, and deadline expiry cannot leak credentials, expose raw traces, or leave local connections open.

### Phase E — Acceptance, concurrency, and boundary tests

- [x] **Admission boundary:** exactly 50,000 active tokens is allowed; 50,001 is denied. Test a zero/negative output cap, unknown key, malformed token usage, and excessive request size.
- [x] **Actual token accounting:** verified mock token counts, reservation before provider call, reconciliation after known success, conservative reservation when usage is missing, and no second reservation on fallback.
- [x] **Atomic concurrency:** with 50,000 free tokens, issue two simultaneous requests each requiring 30,000 tokens against the **same on-disk DB** via separate connections; exactly one succeeds. Repeat with multiple gateway instances/connections if practical.
- [x] **Tenant isolation:** exhaust one tenant's budget and prove a different tenant can still proceed.
- [x] **Rolling-window boundaries:** fake clock at `t=0`, `t=59,999 ms`, `t=60,000 ms`, and `t=60,001 ms`; test exact expiry and cleanup. Verify reconciliation never resets the timestamp.
- [x] **Crash/restart:** create an active reservation, restart app/database connections, and prove it still counts until expiry; test safe handling of pending rows after interrupted requests.
- [x] **Provider outcomes:** primary success (backup zero calls); primary 429 (backup exactly once); primary >3,000 ms (backup once); primary 500 (no fallback); secondary failure/429/timeout (sanitized error).
- [x] **Timeout race:** success just before deadline and just after it, including a slow trickling response; no simultaneous successes, duplicate fallbacks, or leaked HTTP resources.
- [x] **Failure accounting:** 429 → backup success settles once; ambiguous primary timeout → full logical reservation retained; missing usage → no free completion; DB busy/error → zero provider calls.
- [x] **Error sanitization:** assert identical error structure across failure classes and verify secrets, exception messages, stack traces, internal provider URLs, and raw SQL errors never appear in client responses.
- [x] Verify the *actual* HTTP app against two local mock providers, not only isolated helper functions. Record verified startup and test commands in `model_router/README.md`.

## 6. Acceptance matrix

| Scenario | Expected result | Provider attempts |
| --- | --- | --- |
| Tenant has capacity, primary succeeds | Primary completion; settle documented usage | Primary 1, backup 0 |
| Token reservation would exceed 50,000 | HTTP 429 `RATE_LIMIT_EXCEEDED` before dispatch | Primary 0, backup 0 |
| Two concurrent 30k reservations, budget 50k | Exactly one admitted | Exactly one accepted upstream dispatch |
| Window record ages exactly 60 seconds | Capacity becomes available | As admitted |
| Different tenant API keys | Independent token windows | Independent |
| Primary returns HTTP 429 | One backup attempt; reuse reservation | Primary 1, backup 1 |
| Primary total duration exceeds 3000 ms | Cancel/close primary attempt; use backup | Primary 1, backup 1 |
| Primary returns HTTP 500 | Sanitized failure, no automatic fallback | Primary 1, backup 0 |
| Backup fails / times out | Standardized safe gateway error | Primary 1, backup 1 |
| Primary times out after possible work | Reserve conservatively; no double charge | Primary 1, backup 1 |
| Successful response missing trustworthy usage | Keep reservation, do not undercount | Depends on route |
| Router restarts with same SQLite file | Recent charges and pending reservations remain enforced | None for restart check |
| SQLite busy / unavailable | Sanitized gateway failure, no dispatch | 0 |

## 7. Scope boundaries

**Required now:** Actual persisted SQLite, rolling per-tenant token admissions, concurrency-safe reservations and reconciliation, 429/3,000 ms fallback, async cleanup, and sanitized error payloads.

**Deferred:** Streaming SSE integration (covered in Task 3), live hosted models, a billing system for every upstream retry token, OAuth/login/user database, Redis/distributed rate limiting, circuit breakers, jittered retries, background schedulers, Docker/cloud deployment, dashboards, and a generic provider framework. None is required to demonstrate the specified behavior.

**Explicit limitations:** Mock-token accounting is exact only for the documented mock tokenizer, not a claim of exact billing for arbitrary real models. An upstream request cancelled locally may have already consumed provider tokens. SQLite with WAL is appropriate for a local, file-backed gateway; it is **not** a distributed rate-limiter across multiple hosts.

## 8. Definition of done

- [x] The router accepts supported completion requests and makes correct decisions per tenant API key.
- [x] The on-disk SQLite ledger enforces a real rolling 60-second / 50,000-token budget across concurrent admissions and process restarts.
- [x] Reservations and actual usage are handled with the documented policy, including ambiguous provider failures.
- [x] Primary HTTP 429 and **whole-attempt** timeout after 3,000 ms each trigger exactly one secondary attempt.
- [x] No other primary failure silently triggers unrequested fallback; backup failures produce sanitized errors.
- [x] SQLite transactions never remain open across provider awaits; DB operations do not unnecessarily block the async event loop.
- [x] All acceptance tests are executed and pass; startup/test steps are verified and documented.

## 9. Verified implementation and review decisions

The revised implementation replaced caller-selected tenants, `max_tokens`,
per-attempt reservations and detail-only errors with this guide's API-key identity,
logical-request ledger and structured errors. It reused dependencies and the root
`.venv`; no new environment or provider framework was needed.

| Phase | Implemented behavior | Evidence |
| --- | --- | --- |
| A: request/identity | Strict schema, allowlisted keys, SHA-256 tenant digests, shared mock counter | Invalid/duplicate/unknown keys and unsupported shapes cause zero dispatch |
| B: ledger | Versioned disk SQLite; atomic sum/check/insert; admission-time expiry and persisted clock high-water | Exact 0/59999/60000/60001 ms boundaries, rollback/restart, threads/processes and two-app 30k admission race |
| C: fallback | Sequential primary then at most one backup; one shared reservation | Success/429, real delayed/trickling HTTP, deadline races and no fallback on other failures |
| D: errors/cleanup | Safe envelope and bounded shielded close | Secret/SQL canaries, usage violations, cancellation and stalled/failing close |
| E: acceptance | Existing-interpreter processes and persisted state | Gateway hard-kill/restart, 20 concurrent mixed outcomes and 2,000-operation ledger/reference comparison |

### Corrected defects and concurrency decisions

A failing transport close previously overrode primary 429 and suppressed fallback.
Cleanup now has a finite shielded best-effort budget and cannot replace the known
routing decision. A synchronous validation operation could also return success
after its asyncio deadline without yielding. Validation now stays inside the
attempt, with explicit monotonic checks before accepting results. A 20 ms budget /
40 ms validation regression selects backup once and retains uncertainty. Clock
checks reject late success; they cannot preempt synchronous CPU work.

SQLite busy timeout did not bound executor queue waits. One private worker per
app avoids default-executor starvation; `ROUTER_DB_MAX_PENDING=16` bounds running
plus undrained jobs, `ROUTER_DB_OPERATION_TIMEOUT_SECONDS=2` bounds queue/execution
wait, and `ROUTER_DB_TIMEOUT_SECONDS=1` separately bounds SQLite lock wait. Overflow
and operation deadlines return safe 503 `GATEWAY_UNAVAILABLE`. Initialization uses
the same worker; the reservation-window budget check includes all three potential
ledger waits.

Cancelling an executor Future alone leaves its WorkItem queued. Releasing capacity
then permits unbounded abandoned entries behind a stuck worker. Jobs instead use
an abandonment flag, skip SQLite when drained and retain their slot until actual
completion. Late exceptions are retrieved without logging raw SQL/request data.
Shutdown drains queued entries without waiting on the event loop for running
native calls. Barrier-controlled regressions cover saturation, repeated queue
timeouts, cancellation, skipped admissions and eventual persisted state.

### Accounting and cancellation policy

- Reserve prompt plus maximum completion once per logical request. Mock prompt
  tokens are UTF-8 content bytes, with zero role/framing overhead. Validated
  reported usage can include hidden tokens and determines settlement.
- Missing usage retains the full charge and returns null usage; malformed usage
  fails safely without refund. Both 429s settle to zero only under the no-work
  mock contract. Primary timeout preserves the original uncertain charge even
  after backup success. This limits logical quota, not combined provider billing.
- Abandoned queued admission makes no ledger change. Running admission can commit
  after cancellation/503; no provider is dispatched after its wait fails, and the
  pending charge expires at its original admission boundary.
- Cancellation before trusted settlement starts can retain the full charge.
  Already-running settlement with validated usage may still commit actual usage.
  It never moves admission time or reduces an uncertain primary-timeout charge.

### Verification and remaining limits

The latest functional run used the existing macOS arm64/Python 3.13.3 environment:

```bash
uv run --no-sync pytest -q -s --tb=short --log-level=ERROR
# 366 passed in 55.04 s, including 103 router tests.
uv run --no-sync ruff check .
# All checks passed.
```

Tests include eight primary successes, eight single-backup successes and four
cancellations concurrently, with expected ledger states and one close per opened
response. A real HTTP test kills its disposable gateway process during provider
I/O, then restarts with the same file/key: the pending charge still denies another
admission without a new provider call. Seeded reference tests compare full rows,
including timestamps/state, after each of 2,000 ledger operations. Real delayed/
trickling fallback measured approximately 3.03 seconds; fixture results are not
service-level guarantees. See [README.md](README.md#reviewer-validation) for suite
responsibilities and runnable commands.

Limits remain explicit: native SQLite/OS calls can outlive client waits and delay
interpreter exit; forward clock jumps expire history; DB limits are per app, not
aggregate HTTP/provider admission. Socket disconnect alone need not cancel a
non-streaming handler. Key rotation creates a new tenant; legacy identity/data
migration is deliberately manual. Production identity, multi-host coordination,
live tokenizers/providers, per-attempt billing and capacity are unverified.
Python 3.11 and a clean checkout of the final review commit remain project checks.

## References

- [SQLite transactions and `BEGIN IMMEDIATE`](https://www.sqlite.org/lang_transaction.html)
- [SQLite write-ahead logging](https://www.sqlite.org/wal.html)
- [Python `asyncio` timeouts and cancellation](https://docs.python.org/3/library/asyncio-task.html)
- [HTTPX timeout behavior](https://www.python-httpx.org/advanced/timeouts/)
- [OpenAI Chat Completions token usage fields — reference format](https://developers.openai.com/api/reference/resources/chat/subresources/completions/methods/create)
- [pytest](https://docs.pytest.org/)
