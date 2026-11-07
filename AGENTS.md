# Shared Engineering Rules and Guardrails

> **Scope:** Applies to the repository root and all four task folders. This file defines shared decisions and working rules. Task-specific requirements, implementation steps, and acceptance tests live in each folder's named implementation guide (linked below).
>
> **Status:** This is a plan, not a statement that any feature has been implemented or tested.

## Project structure

Keep the existing folder names. Do not reorganize the project unless a concrete requirement makes it necessary.

```text
project-root/
├── AGENTS.md                     # Shared engineering conventions (this file)
├── README.md                     # Project overview and verified quick start (create as work progresses)
├── pyproject.toml                # Python dependencies and tool configuration
├── uv.lock                       # Lockfile after dependencies are resolved
├── .env.example                  # Safe, documented configuration placeholders
├── .gitignore
├── mcp_server/
│   └── MCP_SERVER_IMPLEMENTATION.md  # Task 1: stdio MCP server
├── security_gateway/
│   └── SECURITY_GATEWAY_IMPLEMENTATION.md  # Task 2: HTTP MCP security proxy
├── stream_guardrail/
│   └── STREAM_GUARDRAIL_IMPLEMENTATION.md  # Task 3: streaming PII redaction
└── model_router/
    └── MODEL_ROUTER_IMPLEMENTATION.md  # Task 4: SQLite rate limiting and model fallback
```

Each task owns its implementation and tests. Shared dependencies may be configured centrally, but **every task must be runnable and testable independently**.

## Baseline scope

Implement the following required behavior before adding optional features:

| Folder | Required output |
| --- | --- |
| `mcp_server/` | MCP server with `get_customer_record` and `trigger_refund`; strict validation, stdio, JSON-RPC correctness |
| `security_gateway/` | HTTP/JSON-RPC proxy; Bearer-token role authorization; `admin_` tool-call denial |
| `stream_guardrail/` | Streaming LLM proxy that redacts emails, SSNs, and credit-card patterns across chunk boundaries |
| `model_router/` | Per-tenant 50,000-token rolling 60-second limit; primary 429/3,000 ms timeout fallback; sanitized errors; on-disk SQLite |

**Not part of the baseline:** frontend, hosted deployment, real payment processing, customer records from a live system, dashboards, complex orchestration, or optional integrations. Do not add these until the four required implementations and their tests are complete.

## Implementation workflow: reason before writing code

The primary goal is **correct, readable, maintainable implementations**, not maximum code output. Follow these rules for every change, including changes made by AI coding assistants:

1. **Inspect first.** Read the relevant task implementation guide, the existing code, and the related tests. Identify what already works before proposing changes.
2. **Form a small plan before substantial edits.** State the requirement, intended behavior, affected existing files/functions, and how correctness will be verified. Prefer a short, concrete plan over a large speculative design.
3. **Reuse before adding.** Prefer editing an existing function or using an official SDK/library capability over writing a duplicate implementation. Check for similar code before creating a helper.
4. **Create only necessary files.** Do not invent broad `utils/`, `services/`, `interfaces/`, or shared frameworks without a demonstrated need. A new file should have a clear responsibility that is difficult to express cleanly within an existing module.
5. **Prioritize readability.** Use descriptive names, straightforward control flow, focused functions, type hints where they clarify contracts, and comments for non-obvious reasoning or constraints. Avoid clever one-liners and excessive boilerplate.
6. **Implement in small, verifiable increments.** Build the simplest correct path, test it, then add the required edge cases. Do not generate large code blocks before reviewing the behavior and existing structure.
7. **Abstract only when warranted.** A small amount of repetition may be easier to understand than premature generic infrastructure. Extract common code when duplication is real and the resulting abstraction simplifies both callers.
8. **Avoid unrelated changes.** Do not refactor or rename working code while implementing a separate requirement unless the change is necessary and tested.
9. **Be precise about verification.** Report which commands ran and which did not. Update completion checklists only for verified behavior.
10. **Install dependencies just in time.** Add a runtime or development dependency only when working on a task that actually uses it. Inspect `pyproject.toml` and the standard library first; reuse already installed packages when appropriate. Do not preinstall the entire anticipated stack, add speculative packages, or introduce libraries merely to avoid writing a few clear lines of code. If a package is added, explain which current requirement needs it and update `uv.lock` with `uv add`.

For coding assistants: **explain the smallest viable change before implementing it; preserve existing working functionality; do not create new files or dependencies without a concrete reason. Do not install packages intended for later tasks.**

## Shared technical decisions

These are working assumptions, not additional requirements imposed by an external party. **The stack below describes tools we expect to use across the project; it is not a list to install up front.** Packages are introduced only when the corresponding task begins and has a concrete need.

- **Language:** Python 3.11+ throughout.
- **Package management:** `uv` with a root `pyproject.toml` and committed `uv.lock`.
- **MCP:** Official Python MCP SDK v2; verify installed API against its documentation and pin the tested version in the lockfile. Do not mix v1 and v2 examples.
- **HTTP services:** FastAPI and HTTPX (asynchronous requests and streaming).
- **Validation:** Pydantic v2; reject unexpected fields and unintended data coercion.
- **Authentication:** Signed development JWTs with an allowlisted `admin` or `viewer` role for the security gateway.
- **Mock services:** Synthetic customer data, a mock HTTP MCP downstream server, and deterministic local LLM provider mocks. The gateway and routing logic remain real. A live provider is not a prerequisite unless project requirements change.
- **Streaming:** OpenAI-style SSE (`data:` JSON deltas, completion marker) as the documented local interface.
- **Rate-limit state:** SQLite **on disk**, with atomic admission checks and persisted usage/reservations.
- **Run mode:** Local, reproducible execution without mandatory external credentials.

## Protocol and architecture boundaries

1. **Keep transports distinct.** `mcp_server` is a stdio MCP process; `security_gateway` is an HTTP/JSON-RPC proxy with its own mock HTTP downstream. Do not quietly substitute one for the other.
2. **Let the SDK handle MCP framing and negotiation.** Never hand-roll the Task 1 protocol when an official SDK capability exists. Validate the emitted behavior rather than assuming it.
3. **Use correct error layers.** JSON-RPC errors apply to malformed protocol requests; MCP tool-level failures can be tool results with `isError: true`. The task-specific notes explain how to make the requested error mapping explicit.
4. **Keep mocks truthful.** Label mock outputs and simulated failures clearly. Do not imply a local mock generated real model text, executed a refund, or contacted a hosted provider.
5. **No silent reinterpretation.** Document the assumptions about `CUST-XXXXX`, token accounting, PII patterns and maximum candidate length, auth failures, and provider response formats.

## Security rules

- No hardcoded production credentials, real customer records, raw tokens in logs, or committed `.env` files.
- Keep `.env.example` non-secret. Ignore `.env`, `.venv/`, `*.db`, `*.db-wal`, `*.db-shm`, caches, and other runtime files in `.gitignore`.
- Treat all HTTP payloads and MCP tool inputs as untrusted. Validate required fields and reject unexpected or malformed shapes.
- Enforce authorization **server-side before** any privileged downstream call. Never rely on an LLM's compliance with instructions as a security boundary.
- Return safe, stable error messages to clients. Do not expose tracebacks, upstream credentials, internal URLs or secret-bearing request headers.
- For Task 1, stdout belongs **exclusively** to stdio JSON-RPC. Application logs and diagnostics must go to stderr.
- For Task 3, never emit text that may still complete a supported PII pattern; prioritize preventing leakage over marginal latency improvements.
- For Task 4, keep the sliding-window admission operation atomic. Never hold a SQLite write transaction open while awaiting a provider response.

## Code and testing conventions

- Favor small modules, readable functions, clear names, and direct dependencies over elaborate abstractions.
- **Separate calculations from external I/O when useful.** Database access, HTTP requests, clock reads, and streaming are side effects. Keep the core decisions (such as role checks or token-budget calculations) independently testable where practical; pass a client, connection, or clock into a function when that is simpler than relying on global state. **Do not introduce classes or interfaces solely for this purpose.**
- Write a failing or meaningful test for each requirement and edge case before declaring a task complete.
- Test adverse conditions (bad input, broken upstream responses, timeouts, authorization denials, chunk splits, concurrent admission), not only happy paths.
- Use deterministic mock responses and a controllable clock where possible; don't make core tests depend on live networks or API keys.
- If behavior is not verified, mark it `TODO` or unchecked; **never claim a test passed until it ran successfully**.
- Update the corresponding task implementation guide as items are completed, and place user-facing run commands in that task's `README.md` once verified.

## Initial repository setup — minimum first

**Dependency policy: install only what the active task needs.** Begin with the Task 1 dependencies listed below. Do not install the Task 2, 3, or 4 stack during initial setup. When transitioning to a new task, inspect the existing dependencies, add only missing packages needed for its first working implementation, and avoid optional packages until justified. Add test and lint tools when they are actually used, not simply because they appear in a future plan.

We will use `uv` to manage a single Python environment and shared dependencies for the four folders. `uv` is a **project convenience**, not a functional requirement of the gateways; the same applications could be packaged with standard `venv` and `pip`. Do not reinitialize a project or recreate files that already exist.

### Step 1: Initialize metadata (once)

From the existing repository root, **only if there is no `pyproject.toml` yet**:

```bash
uv init --bare
```

This creates a minimal `pyproject.toml` for dependency declarations. The `--bare` option avoids generating sample application code, an extra `src/` directory, or a second README. Set its `requires-python` to `>=3.11` to match the project's supported baseline. `uv` will create `.venv/` and `uv.lock` when dependencies are installed or synced.

### Step 2: Install only Task 1 dependencies

```bash
uv add 'mcp[cli]>=2,<3' 'pydantic>=2,<3'
uv add --dev pytest
```

- `mcp[cli]`: official MCP server SDK, including development commands; the CLI extra is convenient rather than strictly necessary.
- `pydantic`: explicit strict schemas and validation.
- `pytest`: repeatable automated acceptance tests (development dependency, not deployed application behavior).

`uv add` updates `pyproject.toml`, creates/updates `uv.lock` (exact resolved versions), and synchronizes the local `.venv/` environment. Commit `pyproject.toml` and `uv.lock`, but **not** `.venv/`.

### Step 3: Add dependencies only when moving to the task that needs them

The following are **examples of commands to run later, not commands to run during initial setup**:

- **When starting Task 2:** Check what is already installed, then add only the packages used by the chosen HTTP proxy implementation. Likely candidates are `fastapi`, `uvicorn`, `httpx`, and `PyJWT`. For example, `uv add fastapi uvicorn httpx PyJWT` if all four are needed. Do not install these for Task 1.
- **When starting Task 3:** Reuse FastAPI and HTTPX from Task 2. Prefer the standard-library `asyncio`, `re`, and `json` for streaming and pattern matching; add a new package only if there is a specific unmet requirement.
- **When starting Task 4:** Start with the standard-library `sqlite3`, which already supports on-disk SQLite. Add `aiosqlite` only if the implementation genuinely requires it. Add `pytest-asyncio` only when asynchronous tests require it.
- **When actually introducing linting:** Add `ruff` as a development dependency (`uv add --dev ruff`). Do not add it solely to complete an initial dependency list.

At each transition: **(1)** inspect `pyproject.toml`, **(2)** justify each new package against the current task, **(3)** run the appropriate `uv add` command, and **(4)** commit the resulting `pyproject.toml` and `uv.lock` changes. Do not run a bulk install of future-task dependencies.

### Step 4: Create supporting files only when needed

- `.gitignore`: needed early to keep `.venv/`, `.env`, `*.db*`, caches, and secrets out of commits.
- `.env.example`: create when the first HTTP service needs configurable non-secret settings.
- `README.md`: add verified installation, run, and test instructions as they become real.
- `__init__.py`, `conftest.py`, and other project files: add only when needed for package imports, fixtures, or test discovery; do not scaffold them speculatively.

### Useful verification commands

```bash
uv run pytest -v
uv run pytest mcp_server/tests -v
# Run other task-specific suites once those folders contain tests.
# After installing Ruff:
uv run ruff check .
```

These are **intended commands**, not assertions that their tests or configuration already exist. Check the relevant task documentation for its specific run instructions.

`uv` installation guide: https://docs.astral.sh/uv/getting-started/installation/

## Local services and configuration

Suggested ports (defaults, not fixed requirements):

| Service | Address |
| --- | --- |
| MCP server | stdio; **no network port** |
| Security gateway / downstream mock | `127.0.0.1:8000` / `127.0.0.1:8001` |
| Stream guardrail / mock provider | `127.0.0.1:8010` / `127.0.0.1:8011` |
| Model router / primary mock / backup mock | `127.0.0.1:8020` / `127.0.0.1:8021` / `127.0.0.1:8022` |

Put URL endpoints, secrets, database path, timeouts and limits in environment configuration rather than repeating constants across files. Local development endpoints should bind to loopback by default.

## Implementation order and completion gate

1. Set up the minimal Task 1 environment and `.gitignore`; introduce additional dependencies and `.env.example` only when actually needed.
2. Complete [`mcp_server/MCP_SERVER_IMPLEMENTATION.md`](mcp_server/MCP_SERVER_IMPLEMENTATION.md).
3. Complete [`security_gateway/SECURITY_GATEWAY_IMPLEMENTATION.md`](security_gateway/SECURITY_GATEWAY_IMPLEMENTATION.md).
4. Complete [`stream_guardrail/STREAM_GUARDRAIL_IMPLEMENTATION.md`](stream_guardrail/STREAM_GUARDRAIL_IMPLEMENTATION.md).
5. Complete [`model_router/MODEL_ROUTER_IMPLEMENTATION.md`](model_router/MODEL_ROUTER_IMPLEMENTATION.md).
6. Run all tests and linting; verify documented commands from a clean clone.

A task is complete only when its required behavior works **and** the acceptance checks in its implementation document have been executed. Defer polish and bonus features until then.

## Reference documentation

- [Official Python MCP SDK v2](https://py.sdk.modelcontextprotocol.io/)
- [MCP specification](https://modelcontextprotocol.io/specification/)
- [JSON-RPC 2.0](https://www.jsonrpc.org/specification)
- [FastAPI](https://fastapi.tiangolo.com/)
- [HTTPX async](https://www.python-httpx.org/async/)
- [Pydantic](https://docs.pydantic.dev/latest/)
- [SQLite transactions](https://www.sqlite.org/lang_transaction.html)
