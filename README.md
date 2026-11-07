# AI field developer exercise

Four independent local Python tasks use synthetic data and deterministic mocks.
No hosted provider account, payment service or external credentials are required.

| Task | Interface and purpose | Run and review | Implementation guide |
| --- | --- | --- | --- |
| 1: MCP server | stdio customer lookup and simulated refunds | [README](mcp_server/README.md) | [Requirements and evidence](mcp_server/MCP_SERVER_IMPLEMENTATION.md) |
| 2: Security gateway | HTTP/JSON-RPC proxy with signed-JWT role checks | [README](security_gateway/README.md) | [Requirements and evidence](security_gateway/SECURITY_GATEWAY_IMPLEMENTATION.md) |
| 3: Stream guardrail | HTTP/SSE proxy with cross-chunk PII redaction | [README](stream_guardrail/README.md) | [Requirements and evidence](stream_guardrail/STREAM_GUARDRAIL_IMPLEMENTATION.md) |
| 4: Model router | HTTP completion router with SQLite token limits and fallback | [README](model_router/README.md) | [Requirements and evidence](model_router/MODEL_ROUTER_IMPLEMENTATION.md) |

## Setup and automated review

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).
From the repository root:

```bash
uv sync --locked
uv run --no-sync pytest -q
uv run --no-sync ruff check .
```

All suites run independently; for example:

```bash
uv run --no-sync pytest model_router/tests -q -s
```

Tests supply synthetic keys, temporary on-disk databases and local services.
Manual key generation and server startup are unnecessary for automated review.
HTTP tests require permission to bind loopback sockets. Some stream memory tests
use Unix process/resource APIs.

The project uses one root `.venv` and `uv.lock`. Test subprocesses reuse that
interpreter; they are separate processes, not additional Python environments.
`--no-sync` uses the installed environment after the initial locked sync.

## Manual operation and configuration

Use each module README for startup, sample requests and expected results.
[.env.example](.env.example) documents defaults and intentionally leaves secrets
blank. Export settings before startup; applications do not automatically load
`.env`. The gateway needs a locally generated JWT signing secret; the router
needs locally generated tenant keys. Neither is a hosted-provider credential.

The tasks are not wired into a single pipeline. Task 1 listens on stdio, with
stdout reserved for MCP protocol messages. Task 2 has its own HTTP mock downstream;
it is not a transport adapter for Task 1. Tasks 3 and 4 each have their own mock
providers. All documented HTTP listeners bind to loopback.

## Verification and limits

The latest recorded functional run passed **366 tests**: 45 MCP, 99 security,
119 guardrail and 103 router tests. Configured Ruff correctness checks passed.
These results cover the documented local acceptance criteria; the implementation
guides record decisions and evidence, and the [guardrail audit](stream_guardrail/AUDIT.md)
retains detailed findings and performance measurements.

Verification used macOS arm64/Python 3.13.3. Python 3.11, other platforms, live
providers and production capacity have not been certified. A locked install and
full test/lint run from a clean checkout of the actual review commit remains a
project completion check. Earlier temporary source/environment checks were
removed after use and do not establish that final checkout check.

See [AGENTS.md](AGENTS.md) for shared engineering rules and scope boundaries.
