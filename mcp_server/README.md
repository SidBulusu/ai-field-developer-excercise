# Synthetic customer MCP server

Task 1 serves two mock customer tools over stdio using the official MCP Python
SDK v2. No external credentials, network listener, customer system, or payment
provider is needed. Tested with Python 3.13.3 and MCP SDK 2.3.0; exact dependency
versions are recorded in the root `uv.lock`.

## Install and run

From the repository root, with Python 3.11+ and [uv](https://docs.astral.sh/uv/getting-started/installation/) installed:

```bash
uv sync --locked
uv run python -m mcp_server.server
```

The server waits for newline-delimited MCP JSON-RPC input. stdout contains only
protocol frames; startup and other diagnostics go to stderr. For a local MCP
host, set its command to the absolute path to `.venv/bin/python`, its arguments
to `["-m", "mcp_server.server"]`, and its working directory to this repository.

## Tools

`get_customer_record` accepts:

```json
{"customer_id": "CUST-12345"}
```

The synthetic dataset contains `CUST-12345` (Example Customer One) and
`CUST-54321` (Example Customer Two). Returned records include `synthetic: true`.

`trigger_refund` accepts:

```json
{"customer_id": "CUST-12345", "amount": 12.5, "reason": "Duplicate charge"}
```

It returns a deterministic acknowledgment with `mock: true`, `status: "simulated"`,
a `MOCK-` identifier, and the validated inputs. It does not transfer money or
persist a refund. The identifier is derived from the validated request, not a
payment identifier or an idempotency guarantee for real processing.

Customer IDs must be exactly `CUST-` followed by five ASCII digits. Amounts must
be finite positive JSON numbers; integer numbers are accepted, while booleans
and numeric strings are rejected. The mock performs no currency arithmetic:
amounts are validated as floats and returned as decimal strings without cent
rounding or a currency assumption. Reasons are trimmed before their 10-character
minimum is checked. All fields are required and unexpected fields are rejected.

## Errors and SDK integration

| Condition | Wire behavior |
| --- | --- |
| Invalid JSON syntax | JSON-RPC error `-32700`, null request ID |
| Invalid JSON-RPC envelope | JSON-RPC error `-32600`, null request ID |
| Unknown protocol method | SDK JSON-RPC error `-32601` |
| Malformed `tools/call` parameters, such as a missing tool name | SDK JSON-RPC error `-32602` |
| Invalid tool arguments or unknown tool name | JSON-RPC error `-32602`, preserving the request ID |
| Well-formed request for a missing synthetic customer | MCP tool result with `isError: true` |
| Unexpected exception inside a customer tool | Generic tool failure with `isError: true`; internal details are omitted |

The SDK's default tool wrapper ignores extra input fields and pre-parses some
string values as JSON. `CustomerMCPServer` overrides tool discovery and dispatch
to apply the same strict Pydantic models to the original flat arguments and
published schemas. JSON-looking reasons remain strings. Validation messages
do not echo rejected input values.

The assignment explicitly requires standard JSON-RPC codes for invalid input
formats. Validation failures therefore raise the SDK's `MCPError` with `-32602`
rather than becoming `isError` tool results. This is a deliberate choice to match
the assignment: the [MCP tools specification](https://modelcontextprotocol.io/specification/2025-11-25/server/tools#error-handling)
documents tool input validation failures as tool-level errors, while unknown
tools use protocol errors. SDK clients receiving a protocol error raise an
exception; applications must catch it and decide whether to pass the message
to the model for correction. A tool result with `isError: true` instead arrives
as a normal result that the application can inspect. Neither error form retries
automatically. Customer-not-found outcomes and tool execution failures remain
tool-level errors.

SDK 2.3.0's stdio transport emits Pydantic validation errors for malformed input,
but its standard runner discards those errors. A small stream adapter maps them
to SDK `JSONRPCError` objects; the SDK still owns parsing, framing, initialization,
and dispatch. The adapter uses the SDK's `_lowlevel_server` launch path, so its
subprocess regression tests must pass before updating the SDK. This compatibility
point is covered by the lockfile and wire tests.

References: [SDK v2](https://py.sdk.modelcontextprotocol.io/),
[JSON-RPC 2.0](https://www.jsonrpc.org/specification).

## Verification

From the repository root:

```bash
uv run pytest mcp_server/tests -v
```

The suite covers strict schemas, SDK-client discovery and both tools over actual
stdio, malformed protocol input and recovery, tool failures, rejection before
execution, sanitized crashes, and stdout isolation. Tests are deterministic and
require no network access or provider credentials.

Acceptance result: **45 passing tests**. The implementation checklist and
verification scope are in [MCP_SERVER_IMPLEMENTATION.md](MCP_SERVER_IMPLEMENTATION.md).
