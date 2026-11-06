# Task 1 — Custom MCP Server

> **Folder:** `mcp_server/`  
> **Goal:** Run a standards-aware stdio MCP server exposing two strictly validated customer tools.  
> **Shared conventions:** See [`../AGENTS.md`](../AGENTS.md).  
> **Status:** Task 1 meets the original requirements and automated acceptance checks. Checked items track verified implementation; instructions remain reusable.

## Required behavior

Implement a Python MCP server using the **official MCP SDK**, with these tools:

| Tool | Inputs | Behavior |
| --- | --- | --- |
| `get_customer_record` | `customer_id: str` with shape `CUST-XXXXX` | Return a synthetic customer record or a controlled not-found outcome |
| `trigger_refund` | `customer_id: str`, `amount: positive float`, `reason: str` of at least 10 characters | Validate input and simulate a refund without payment side effects |

**Mandatory:** strict input validation through Pydantic, JSON-RPC/MCP error behavior, **stdio transport**, and stdout restricted to protocol messages. Send logs exclusively to stderr.

## Working decisions and assumptions

- Interpret `CUST-XXXXX` as `CUST-` followed by exactly **five digits** (`^CUST-[0-9]{5}$`), unless requirements are clarified. Explain this in the module README.
- Use a deterministic in-memory dataset (e.g., `CUST-12345`, `CUST-54321`). Customer IDs that are well formed but absent from the store are not considered malformed JSON-RPC.
- Treat refund amounts as finite positive JSON numbers; reject booleans, numeric strings, zero, negatives, `NaN` and infinity. Avoid floating-point currency calculations in simulated business logic; use decimal-safe handling if representing money.
- Trim the reason before checking the 10-character minimum and reject whitespace-only reasons.
- Reject extra input fields rather than silently ignoring them.
- `trigger_refund` records only a **mock** result; never send money or call a real payment provider.

## Suggested files

```text
mcp_server/
├── __init__.py
├── server.py                 # MCPServer, synthetic data, mock refunds, stdio
├── schemas.py                # Pydantic strict tool inputs
├── README.md                 # Verified startup, tool examples, decisions
├── MCP_SERVER_IMPLEMENTATION.md  # This document
└── tests/
    ├── test_validation.py
    └── test_stdio_protocol.py
```

The small synthetic store lives in `server.py`; no separate store module is needed.

## Step-by-step implementation

### Phase A — Understand and initialize

- [x] Read the [MCP Python SDK v2 quickstart](https://py.sdk.modelcontextprotocol.io/) and [JSON-RPC specification](https://www.jsonrpc.org/specification).
- [x] Confirm a tested v2 version is installed (use root `uv.lock`). Import `MCPServer` from `mcp.server`; don't mix older `FastMCP` v1 examples with v2 APIs.
- [x] Create the app, register exactly the two required tools, and implement a proper stdio entry point using the SDK's supported launch path.
- [x] Test discovery using the SDK client before adding complex business logic.

### Phase B — Define strict schemas

- [x] Create Pydantic input models for both tools with forbidden extra fields.
- [x] Validate the customer ID against the chosen five-digit pattern.
- [x] Validate amount is a finite positive number (not a bool or numeric string).
- [x] Validate reason after whitespace trim and enforce `length >= 10`.
- [x] Verify the schemas published by `tools/list` correctly describe required inputs and constraints; check whether SDK registration preserves the desired schema restrictions.

### Phase C — Implement tool behavior

- [x] Add a small deterministic synthetic customer store.
- [x] Implement lookup: valid/known ID → safe structured record; valid/unknown ID → clear not-found outcome.
- [x] Implement mock refund: validate customer existence, generate/return a stable mock response, and do not perform actual payment actions.
- [x] Avoid returning internal exception details in tool output.

### Phase D — Protocol errors and stdio isolation

- [x] Ensure syntax-level invalid JSON, invalid JSON-RPC envelopes, unknown methods and malformed parameter objects produce SDK-compliant protocol errors.
- [x] Know standard codes: `-32700` parse error; `-32600` invalid request; `-32601` method not found; `-32602` invalid params; `-32603` internal error.
- [x] Match the original assignment's **invalid input error mapping**: invalid tool arguments and unknown tool names return JSON-RPC `-32602`. Well-formed requests for missing customers and execution failures remain tool results with `isError: true`. Verify SDK wire output and explain the tradeoff with MCP's documented validation-error convention in the README.
- [x] Route all logging and debug output to **stderr**. Remove all stdout `print()` calls, startup banners, and accidental tracebacks.

### Phase E — Verify with real transport

- [x] Exercise discovery and both tools using an SDK client over stdio.
- [x] Run an end-to-end subprocess or SDK-client test over **actual stdio** rather than calling the functions directly only.
- [x] Record the exact working startup and verification commands in `mcp_server/README.md`.

## Required acceptance checks

- [x] `tools/list` reports both names and the expected required arguments.
- [x] `get_customer_record(customer_id="CUST-12345")` returns the expected synthetic record.
- [x] A valid `trigger_refund` returns a mock refund acknowledgment.
- [x] Reject malformed IDs: missing prefix, too short/long, wrong character type, wrong Python/JSON type.
- [x] Reject absent fields, unexpected fields, `amount <= 0`, non-finite amount, bool or numeric-string amount, and reasons shorter than 10 characters.
- [x] Handle unknown but well-formed customer IDs without crashing.
- [x] Probe malformed JSON/JSON-RPC and unknown methods; assert the correct error category and code.
- [x] Assert **every stdout line/frame** from the stdio process is valid protocol output and no diagnostics leak into stdout; diagnostics go to stderr.

## Definition of done

A reviewer can launch the MCP process, discover both tools and call them through a compliant MCP client, and reproduce strict validation, correct protocol behavior and stdout isolation with tests.

## Resources

- [Official Python MCP SDK v2](https://py.sdk.modelcontextprotocol.io/)
- [MCP tools specification](https://modelcontextprotocol.io/specification/)
- [Pydantic](https://docs.pydantic.dev/latest/)
- [JSON-RPC 2.0](https://www.jsonrpc.org/specification)

## Verification record

Verified on Python 3.13.3 with MCP SDK 2.3.0 and Pydantic 2.14.3.

- `uv sync --locked` succeeded.
- `uv run pytest mcp_server/tests -v`: 45 tests passed.
- `.venv/bin/python -m pytest mcp_server/tests -q`: 45 passing tests, including exact `-32602` wire assertions, SDK-client exceptions and recovery, and retained business-error results.
- SDK-client discovery and both tools were exercised over actual stdio.
- Raw-pipe tests checked protocol error codes, recovery, and stdout isolation.

Project-wide linting and clean-clone verification belong to the final project
completion gate in `AGENTS.md`; neither has verification recorded here. They do
not add requirements to the original Task 1 acceptance criteria.

See [README.md](README.md) for the verified launch path, input assumptions,
error mapping, and the SDK 2.3.0 stdio compatibility adapter.
