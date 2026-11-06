# AI field developer exercise

This project contains four local Python tasks. Task 1 meets its original
requirements and automated acceptance checks. Task 2's HTTP security gateway is
implemented and tested; Tasks 3–4 are not implemented.
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
