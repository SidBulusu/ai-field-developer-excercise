# AI field developer exercise

This project contains four local Python tasks. Task 1 meets its original
requirements and automated acceptance checks. Task 2's HTTP security gateway is
implemented and tested. Task 3 has a tested HTTP streaming proxy, bounded SSE
parser, redactor and local mock; its baseline acceptance checks are complete.
Task 4 implements SQLite tenant token budgets and primary/backup model routing.
Latest working-environment verification passes 315 tests and configured Ruff
correctness checks, including streaming-guardrail audit and performance follow-ups. The earlier
287-test milestone also passed from a clean clone of its source snapshot.
See [AGENTS.md](AGENTS.md) for shared rules and
[the MCP server README](mcp_server/README.md) for inputs, error semantics, and SDK
compatibility details.

## Task 1 quick start

Requires Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/).
From this repository's root:

```bash
uv sync --locked
uv run python -m mcp_server.server
```

The stdio server uses synthetic records and simulated refunds. No credentials
or external services are required. stdout is reserved for MCP protocol traffic.

Run the Task 1 acceptance suite:

```bash
uv run pytest mcp_server/tests -v
```

The implementation checklist is in
[MCP_SERVER_IMPLEMENTATION.md](mcp_server/MCP_SERVER_IMPLEMENTATION.md).

## Task 2 security gateway

See the [gateway README](security_gateway/README.md) for verified local startup,
JWT generation, sample requests, configuration, and HTTP/JSON-RPC limitations.
Its downstream HTTP mock runs independently of Task 1's stdio server.

```bash
uv run pytest security_gateway/tests -q
```

## Task 3 streaming guardrail

See the [guardrail README](stream_guardrail/README.md) for local startup, the
supported streaming contract, verified acceptance tests and documented limitations.

## Task 4 model router

See the [router README](model_router/README.md) for the byte-token mock contract,
SQLite persistence, fallback behavior, local startup and acceptance tests.

```bash
uv run pytest model_router/tests -q
```

Run all task suites and the configured Ruff correctness checks:

```bash
uv run pytest -q
uv run ruff check .
```
