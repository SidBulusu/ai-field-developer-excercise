# AI field developer exercise

This project contains four local Python tasks. Task 1 meets its original
requirements and automated acceptance checks; Tasks 2–4 are not implemented.
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
