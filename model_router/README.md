# Model router

A local, text-only, non-streaming completion gateway. A configured API key
identifies one tenant; on-disk SQLite enforces a 50,000 mock-token rolling
60,000 ms budget. Primary HTTP 429 or timeout selects exactly one backup attempt.
Both supplied providers are deterministic synthetic mocks, not hosted models.

## Evaluator quick start: automated validation

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).
Run these commands from the repository root:

```bash
uv sync --locked
uv run --no-sync pytest model_router/tests -q -s
uv run --no-sync ruff check model_router
```

**No manual API-key setup, provider startup or external credentials are needed
for these tests.** Tests supply synthetic keys, create temporary on-disk SQLite
databases and start local provider/router processes where needed. Those processes
use the repository's single `.venv`; they are not additional environments.
Local-socket permission is required for HTTP tests in restricted sandboxes.
The latest router verification passed 103 tests.

The manual walkthrough below demonstrates the request flow. The automated suite
provides evidence for concurrency, expiry, restart persistence and timeout races
that a single curl response cannot establish.

## Manual walkthrough: three terminals

Open three terminals at the repository root, after the dependency setup above.
The providers generate synthetic text locally; no OpenAI/provider account, paid
API key or network service is required. The router's key is a local shared secret
that identifies whose token budget to check. It prevents callers from inventing
new tenant names on every request to bypass their limit.

**Terminal 1 — start the primary provider.** Leave this process running:

```bash
uv run --no-sync python -m uvicorn model_router.mock_provider:create_primary_app --factory --host 127.0.0.1 --port 8021 --no-access-log
```

**Terminal 2 — start the backup provider.** Leave this process running:

```bash
uv run --no-sync python -m uvicorn model_router.mock_provider:create_backup_app --factory --host 127.0.0.1 --port 8022 --no-access-log
```

**Terminal 3 — generate one local key and start the router.** The export command
assigns the key without printing it. Starting the router in the background keeps
this same terminal available for the client commands below:

```bash
export ROUTER_API_KEYS="$(uv run --no-sync python -c 'import secrets; print(secrets.token_urlsafe(32))')"
uv run --no-sync python -m uvicorn model_router.app:create_app --factory --host 127.0.0.1 --port 8020 --no-access-log &
ROUTER_PID=$!
```

Wait for the router's startup message before sending requests. If it fails to
start because an old router database exists, see **Legacy database compatibility**
below; that database is preserved rather than silently reset.

**Still in Terminal 3 — send an authenticated request:**

```bash
curl -i http://127.0.0.1:8020/v1/chat/completions \
  -H 'Content-Type: application/json' -H "X-API-Key: $ROUTER_API_KEYS" \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_completion_tokens":100,"n":1,"stream":false}'
```

Expect HTTP 200 with `route: primary`, `model: mock-primary`, assistant content
`Mock primary response.`, and usage. The router reserves the prompt plus maximum
output before dispatch, then settles to validated usage on known success.

**Still in Terminal 3 — verify that a missing key is rejected:**

```bash
curl -i http://127.0.0.1:8020/v1/chat/completions \
  -H 'Content-Type: application/json' \
  -d '{"messages":[{"role":"user","content":"Hello"}],"max_completion_tokens":100}'
```

Expect HTTP 401 with `error.code: INVALID_API_KEY` and no provider dispatch.

**Optional fallback demonstration:** stop Terminal 1 with Ctrl+C, then restart
its provider with one of these commands (choose one):

```bash
ROUTER_MOCK_PRIMARY_MODE=429 uv run --no-sync python -m uvicorn model_router.mock_provider:create_primary_app --factory --host 127.0.0.1 --port 8021 --no-access-log
ROUTER_MOCK_PRIMARY_MODE=timeout uv run --no-sync python -m uvicorn model_router.mock_provider:create_primary_app --factory --host 127.0.0.1 --port 8021 --no-access-log
```

Repeat the authenticated request in Terminal 3. Expect `route: backup`; the timeout
case takes about three seconds before fallback. Automated tests assert exact call
counts and ledger accounting for both cases.

**Stop the demo:** in Terminal 3 run:

```bash
kill "$ROUTER_PID"
```

Use Ctrl+C in Terminals 1 and 2 to stop the providers. Keep the key variable and
database when restarting the router to preserve the same tenant's recent usage.
Do not rerun the key-generation command for a restart/persistence check: a newly
generated key identifies another tenant. Shell variables are not automatically
shared with new terminals and disappear when the shell closes.

For multiple tenants, configure a comma-separated allowlist in `ROUTER_API_KEYS`
and send exactly one key per request. Keys must be distinct, 32–128 ASCII
alphanumeric/underscore/hyphen characters generated with a cryptographic random
generator. Length and syntax checks cannot establish entropy; do not configure
passwords or sample keys. Key aliases and quota-preserving rotation are outside
this local scope.

Configuration is read at startup. `.env.example` is a non-secret template;
`.env` is not automatically loaded. Keep real keys out of Git. Client model/URL
selection and incoming credentials are not forwarded. A successful response has
validated `usage`, or null usage when the otherwise-valid response omitted it.

HTTPX integration tests exercise the same factories. The interactive curl/job-control
sequence is guidance, not a separately executed acceptance run.

## Accounting and identity decisions

One UTF-8 byte of message content is one mock prompt token. Roles and JSON
framing have **zero overhead in this mock contract**: `Hello` costs 5, `é` costs 2,
and `é😀` costs 6. Gateway and providers share the same counter. This is not a
real model tokenizer. A real provider needs its own tokenizer/counting API and
message-overhead contract before integration.

Reserve `counted_prompt_tokens + max_completion_tokens` **once per logical
request**, before either provider is called. A single row covers both attempts:

| Outcome | Ledger treatment |
| --- | --- |
| Primary success with valid usage | Replace reservation with reported total; settle once |
| Primary 429, backup succeeds with valid usage | Settle the same row to backup usage |
| Primary timeout, backup succeeds or fails | Keep original full reservation; mark uncertain |
| Both providers return 429 | Settle original row to zero under the mock no-work contract |
| Otherwise-valid success without usage | Return completion with null usage; retain full reservation as uncertain |
| Malformed response, invalid usage or ambiguous provider failure | Retain original charge; no speculative refund |
| Cancellation/DB timeout before a trusted settlement starts | An admitted reservation stays fully charged; abandoned queued work is skipped |
| Cancellation/DB timeout during an already-running settlement with trusted usage | Worker may finish settling actual usage; admission timestamp stays unchanged |

Validated usage requires strict nonnegative integers, the correct prompt count,
consistent totals, and completion usage within the admitted cap and at least the
visible output count. Hidden completion tokens may increase reported usage;
validated reported totals determine the charge. Invalid usage or output-cap
violations return `UPSTREAM_INVALID_RESPONSE` and retain the reservation.

After a timeout, one retained reservation limits **logical client quota**, not
the combined physical work/billing of primary plus backup. A remote provider may
continue after local cancellation. Retrying the client request creates a new
reservation; no idempotency-key or per-attempt billing contract is implemented.

Only SHA-256 key digests reach SQLite, never raw API keys. Hashing assumes
high-entropy keys and makes identity stable across restarts. Each configured key
represents a separate tenant; this is a local allowlist, not an account service.

## Persistence, clocks and concurrency

The `reservations` ledger contains request ID, tenant key digest, integer
admission milliseconds, reserved/charged tokens and pending/settled/uncertain
state. Tenant/time and time-only indexes support admission and expiry. WAL and
`BEGIN IMMEDIATE` serialize sum/check/insert. WAL does not permit simultaneous
writers. Each short-lived connection is created, used and closed by one dedicated
database worker per app instance; transactions never span provider awaits.

Database scheduling has three separate controls:

| Setting | Default | Meaning |
| --- | ---: | --- |
| `ROUTER_DB_TIMEOUT_SECONDS` | 1 s | SQLite lock wait once an operation reaches SQLite |
| `ROUTER_DB_OPERATION_TIMEOUT_SECONDS` | 2 s | Request's wait for a DB operation, including queue and execution |
| `ROUTER_DB_MAX_PENDING` | 16 | Maximum running plus undrained queued operations per app instance |

The private worker avoids starvation by unrelated default-executor work. Overflow
is rejected immediately with 503 `GATEWAY_UNAVAILABLE`; an operation deadline
returns the same safe error. A timed-out/cancelled queued job is marked abandoned
and skips SQLite when drained. Its capacity slot stays occupied until drained:
cancelling an executor Future alone would leave its queue entry behind and allow
unbounded cancelled entries to accumulate. Running jobs keep their slot until
actual completion and may still commit. Nothing dispatches a provider after its
admission wait fails. These controls bound database backlog, not total HTTP/model
concurrency, and are local to each worker/app instance.

The active window is `(effective_now_ms - 60000, effective_now_ms]`. A charge
expires at exactly 60,000 ms after admission; reconciliation never moves that
timestamp. Pending and uncertain rows count until that same boundary, including
through crashes/restarts. Admission prunes expired rows. Persisted clock
high-water prevents backward wall-clock jumps from creating capacity; forward
jumps expire history. Use a reliable host clock. Provider deadlines use the
separate monotonic event-loop clock.

Workers sharing a database must use identical limits/window settings; startup
rejects disagreements. Provider, cleanup and DB-operation budgets must fit below
the window. A native SQLite call cannot be forcibly stopped by task cancellation:
it may outlive the client wait and commit an admission or trusted settlement.
Admission after a failed wait never dispatches a provider. Shutdown drains queued
work without waiting on the event loop for a running native call. OS stalls and
interpreter exit with a stuck thread are not hard real-time guarantees.

**Legacy database compatibility:** the former caller-selected tenant/per-attempt
schema is rejected with rows preserved; old labels cannot safely map to API-key
digests. No database is deleted/reset. For a deliberately separate local run,
stop old workers, allow their quota window to drain and explicitly choose a new
`ROUTER_DB_PATH`. Preserve the old file if its state matters. Live quotas need an
explicit identity/data migration.

Runtime database/WAL/SHM files remain ignored by Git. The new schema survives
restart and rejects incomplete/unsupported schema versions rather than silently
recreating missing state.

## Routing, errors and limits

Accepted body fields: `messages` (1–64 system/user/assistant text messages),
required strict integer `max_completion_tokens` (1–50,000), optional `n: 1` and
`stream: false`. Message content is 1–32,768 characters. Unknown fields,
coercion, duplicate JSON keys, invalid Unicode, streaming and multimodal shapes
are rejected before admission. `max_tokens` and caller-selected `X-Tenant-ID`
are no longer supported identity/request contracts.

Primary whole-attempt deadline defaults to 3 seconds, including reading the
complete response. Backup has its own 5-second deadline. HTTPX transport timeouts
also trigger fallback; other transport errors, primary non-429 statuses and
invalid primary replies do not. There are no parallel attempts or extra retries.
No partial primary response reaches the client. Incoming/provider body caps
both default to 64 KiB; redirects, compression and unexpected formats are rejected.

Cleanup is shielded from task cancellation but bounded to 1 second by default;
transport close errors cannot replace a known fallback decision. Closure is
best effort if a transport stalls or fails. Request-task cancellation closes an
open response without automatically refunding admitted work; an already-running
trusted settlement may finish. A network disconnect alone does not
necessarily cancel a non-streaming ASGI handler: work may continue until its
bounded provider deadline. No immediate-disconnect guarantee is claimed.

Gateway errors always have this shape, with a locally generated request ID:

```json
{"error":{"code":"UPSTREAM_UNAVAILABLE","message":"Unable to complete the request.","request_id":"req-..."}}
```

| HTTP status | Safe code / meaning |
| --- | --- |
| 400 / 408 / 413 | `INVALID_REQUEST`: invalid body, upload timeout or size limit |
| 401 | `INVALID_API_KEY`: missing, malformed, duplicate or unknown key |
| 429 | `RATE_LIMIT_EXCEEDED`: reservation cannot fit; no provider dispatch |
| 502 | `UPSTREAM_UNAVAILABLE` or `UPSTREAM_INVALID_RESPONSE` |
| 503 | `GATEWAY_UNAVAILABLE`: DB capacity/deadline exceeded or admission/finalization failed |
| 504 | `UPSTREAM_UNAVAILABLE`: backup timeout |

429 responses caused by existing usage include `Retry-After`; an individually
oversized reservation cannot ever fit and has no retry estimate. A prior
reservation may settle downward sooner. Raw provider bodies, exceptions, SQL,
internal URLs and credentials are never used in client error messages.

## Reviewer validation

Review `limiter.py` for atomic admission, clock and state invariants; `app.py` for
identity, bounded DB scheduling, sequential fallback, deadlines and safe errors;
`mock_provider.py` for the synthetic accounting contract.

| Suite | Evidence |
| --- | --- |
| `tests/test_limiter.py` | Exact millisecond expiry, tenant isolation, shared-file concurrency, restart/rollback and 2,000 seeded reference-ledger operations |
| `tests/test_router.py` | Validation and zero-dispatch denials, usage policies, timeout/cancellation races, cleanup, two-app admission, DB starvation/backlog limits and controlled in-flight commits |
| `tests/test_http.py` | Real three-second delayed/trickling fallback and gateway hard-kill/restart with the same DB/key |

The latest recorded run passed **103 router tests included in 366 repository
tests**, with configured Ruff checks. Local delayed/trickling primary fallback
measured approximately 3.03 seconds. See the [implementation evidence](MODEL_ROUTER_IMPLEMENTATION.md#9-verified-implementation-and-review-decisions)
for corrected defects, regression details and verification scope. These are
fixture results, not service-level guarantees.

Mock modes are `ok`, `429`, `timeout`, `late` (delayed 200), `trickle`, `error`
(500), `malformed`, `overspent`, `missing_usage`, `bad_usage` and `hidden_usage`
(three hidden tokens; allow enough output budget). Delay defaults to 3.2 seconds;
trickle emits one byte per delay/100. Set modes in each provider's terminal before
startup; they never contact a real model.

Remaining limits include live tokenizers/providers, multi-host coordination,
aggregate HTTP/provider admission, key aliases/rotation, legacy migration and
billing across attempts. Python 3.11 and a final clean-checkout run are unverified;
see the [root verification scope](../README.md#verification-and-limits).
