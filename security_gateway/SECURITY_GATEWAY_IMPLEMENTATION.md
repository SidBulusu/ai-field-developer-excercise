# Security Gateway — Implementation Plan

> **Location:** `security_gateway/`  
> **Task:** Implement a lightweight HTTP/JSON-RPC MCP security gateway.  
> **Shared rules:** Follow [`../AGENTS.md`](../AGENTS.md), especially the rules on readable code, planning before coding, avoiding duplicate functionality, and adding dependencies only when needed.  
> **Status:** Required implementation and acceptance checks verified locally. See the verification record below.

## 1. Goal and scope

Build a lightweight HTTP reverse proxy between an AI agent client and a **downstream mock MCP server**. The gateway must authenticate incoming requests, inspect the JSON-RPC method, deny unauthorized `admin_` tool calls **before contacting downstream**, and forward permitted requests and responses accurately.

This project is an **HTTP/JSON-RPC proxy with task-focused MCP tool methods**. It does not require implementing every feature of the full MCP Streamable HTTP transport, session lifecycle, or OAuth stack.

### Required behavior

1. Read `Authorization: Bearer <token>` and determine the caller's **verified** role (for example, `admin` or `viewer`).
2. Parse and inspect the incoming JSON-RPC payload.
3. For `method == "tools/list"`, forward the request transparently and relay the downstream result. **Do not filter administrative tools out of the tool list.**
4. For `method == "tools/call"`, inspect `params.name`:
   - When the tool name starts with `admin_` **and** the verified role is not `admin`, return a JSON-RPC error with code **`-32001`** and message **`Unauthorized Tool Call`**.
   - Return the denial **without making any downstream HTTP request**.
   - Otherwise, forward the request and relay the downstream result.
5. Preserve JSON-RPC request IDs and handle malformed requests, authentication failures, and downstream failures cleanly.

### Evaluation areas and evidence

| Area | What we must demonstrate |
| --- | --- |
| JSON-RPC wire-format parsing | Correct `jsonrpc`, `method`, `params`, and `id` handling; controlled errors for malformed input |
| Proxy middleware construction | A clear authentication middleware boundary and separated request-policy/forwarding logic |
| HTTP request/response forwarding | Allowed requests reach downstream unchanged in meaning; downstream status, body, and relevant headers return correctly |
| Fine-grained method-level authorization | Dynamic `admin_` prefix check on `tools/call`, independent of the particular tool name |
| Clean error handling | Correct required denial response, safe protocol errors, and no sensitive implementation details in responses |

## 2. Implementation choices (not extra requirements)

- **Python + FastAPI** for the gateway and the small downstream mock.
- **HTTPX** for asynchronous HTTP forwarding.
- **Signed JWT Bearer tokens** for a simple, verifiable `admin`/`viewer` role. JWT is an implementation choice; the task only requires role extraction from a Bearer token.
- **One HTTP POST endpoint** such as `/mcp` on each service.
- **Independent downstream HTTP mock** for this task. Do not assume `mcp_server/` from Task 1 can be called via HTTP; that task uses stdio.
- **Single-request JSON-RPC objects** as the initial supported request shape. Reject JSON-RPC batch arrays explicitly rather than forwarding them unchecked and creating an authorization bypass. Document this limitation.
- **No real identities, customers, payment systems, or external services.** Use local demo credentials and side-effect-free tools.

**Incremental dependencies:** Before beginning, inspect the root `pyproject.toml`. Add only missing packages required to implement this task: typically `fastapi`, `uvicorn`, `httpx`, and `PyJWT`. Reuse existing `pytest`; add async testing packages only if the selected tests require them. Commit updated `pyproject.toml` and `uv.lock`.

## 3. Minimal files

```text
security_gateway/
├── app.py                    # Gateway: authentication middleware, policy, forwarding
├── mock_downstream.py        # Minimal HTTP JSON-RPC tool server
├── SECURITY_GATEWAY_IMPLEMENTATION.md
└── tests/                    # Gateway and downstream behavior tests
```

Start with these files only. Introduce `auth.py`, `proxy.py`, or additional helpers **only if** keeping the code in `app.py` materially harms readability, testing, or reuse. Do not create a second dependency manifest or virtual environment. Add a local `README.md` only when startup commands have been verified.

## 4. Request processing design

```text
Client: POST /mcp + Bearer token + JSON-RPC body
                         |
                         v
            Authentication middleware
            - verify token and trusted role
            - attach role to request context
            - reject missing/invalid tokens
                         |
                         v
              JSON-RPC request handling
              - parse/validate envelope
              - inspect method and params
                         |
          +--------------+----------------+
          |                               |
    tools/call, admin_*                 tools/list,
    role != admin                     allowed tools/call,
          |                            other valid methods
          v                               |
    JSON-RPC -32001                       v
    no downstream call              HTTPX forwarding
                                          |
                                          v
                                 Downstream mock MCP
                                          |
                                          v
                               Relay downstream response
```

**Separation of responsibilities:**

- **Middleware:** validate Bearer authentication once per request and expose a trusted role using `request.state` or an equally simple mechanism. It must not interpret arbitrary JSON-RPC tool names.
- **Request handler/policy:** validate the JSON-RPC structure, inspect `method` and `params.name`, and decide whether to deny or forward. A small pure policy function is appropriate; a general-purpose authorization framework is not.
- **Forwarding operation:** send only allowed requests through HTTPX; relay downstream HTTP responses without unnecessary JSON reconstruction.

Middleware can be implemented using FastAPI/Starlette's middleware support. The three responsibilities need not live in three separate modules.

## 5. Implementation phases

### Phase A — Minimal downstream HTTP mock

- [x] Expose `POST /mcp` and accept a JSON-RPC request object.
- [x] Implement `tools/list` returning at least `get_status` and `admin_reset_key`.
- [x] Implement `tools/call` with deterministic, harmless results for both tools.
- [x] Preserve the request ID in responses when a response is expected (including ID `0`).
- [x] Expose downstream invocation counts **in tests**, using a spy/mock transport or simple counter; no monitoring system is required.
- [x] Confirm the mock can be started and called independently (verified during the local TCP smoke check).

### Phase B — Authentication middleware

- [x] Create the gateway FastAPI application and a `POST /mcp` route.
- [x] Read the incoming `Authorization` header; require the Bearer scheme and a non-empty token.
- [x] Verify JWT signature with an explicitly configured allowed algorithm; validate expiration and any issuer/audience claims you choose to use.
- [x] Accept only the expected roles; never trust unverified JWT payload claims.
- [x] Attach the verified role to request context for downstream handler logic.
- [x] Reject missing, invalid, or expired tokens consistently (for example HTTP `401` with `WWW-Authenticate: Bearer`); do not confuse authentication errors with the required JSON-RPC `-32001` authorization denial.
- [x] Keep the signing secret in local environment configuration; never hard-code or commit real credentials.

### Phase C — JSON-RPC parsing and method-level policy

- [x] Parse JSON safely; a parse error returns a controlled JSON-RPC `-32700` error.
- [x] Require a single JSON-RPC object with `jsonrpc: "2.0"` and a string `method`; respond to malformed envelopes with a controlled `-32600` error.
- [x] Preserve valid request IDs **exactly**, including `0` and string IDs. Use JSON-RPC `id: null` where the protocol requires an error ID for unparseable/unknown requests.
- [x] For `tools/call`, require `params` to be an object and `params.name` to be a string; otherwise return `-32602` (Invalid params) without contacting downstream.
- [x] For `tools/list`, do not filter or rewrite any tool definitions.
- [x] For `tools/call`, check the prefix dynamically: `name.startswith("admin_")`.
- [x] If the verified role is not `admin`, return the exact required error below **before constructing/sending an upstream request**:

```json
{
  "jsonrpc": "2.0",
  "id": 42,
  "error": {
    "code": -32001,
    "message": "Unauthorized Tool Call"
  }
}
```

- [x] Allow ordinary tools for viewers and admins; allow `admin_` calls only for admins.
- [x] Treat other valid JSON-RPC methods as pass-through, rather than inventing unrelated access policies. A limited mock may return `Method not found`.
- [x] Explicitly reject unsupported JSON-RPC batch requests rather than permitting unchecked calls to bypass the policy.

### Phase D — Transparent HTTP request and response forwarding

- [x] Use one reusable HTTPX `AsyncClient` (or a similarly controlled lifetime) for downstream calls; do not create unnecessary clients for every request.
- [x] For authorized requests, forward the original JSON-RPC body rather than unnecessarily rebuilding its contents. Ensure nested `params` are unchanged.
- [x] Preserve the relevant request metadata (for example content type and any MCP protocol/session headers the mock actually uses).
- [x] Do **not** forward the client's Bearer token to a downstream mock that does not require it; do not blindly forward hop-by-hop HTTP headers such as `Connection`.
- [x] Relay downstream HTTP **status code, response body, and appropriate content type**. Preserve JSON-RPC result/error payloads and IDs as returned; don't silently transform them.
- [x] Handle downstream connection errors and timeouts with a controlled gateway failure (e.g., HTTP `502`/`504` and a sanitized error payload). Do not include URLs with secrets, stack traces, or raw exception text in client responses.
- [x] Close HTTP clients cleanly on shutdown.

### Phase E — Verification and documentation

- [x] Test actual HTTP/ASGI request handling through the gateway, not only helper functions.
- [x] Verify middleware rejects invalid credentials before downstream work.
- [x] Verify denied `admin_` calls produce **zero downstream HTTP requests**.
- [x] Verify forwarded body, IDs, status codes, and relevant headers; simulate at least one downstream JSON-RPC error response.
- [x] Verify malformed JSON, malformed tool names, unexpected batches, and unavailable downstream are handled without crashes or policy bypass.
- [x] Document the supported HTTP/JSON-RPC subset and the decision to use JWTs.
- [x] Record **tested** commands for starting gateway/mock, generating test tokens, sending sample requests, and running tests in `security_gateway/README.md`.

## 6. Acceptance-test matrix

| Scenario | Expected outcome | Downstream calls |
| --- | --- | --- |
| Viewer + `tools/list` | Unmodified mock tool list, including `admin_reset_key` | 1 |
| Admin + `tools/list` | Same downstream tool list | 1 |
| Viewer + `tools/call` for `get_status` | Forwarded and succeeds | 1 |
| Admin + `tools/call` for `admin_reset_key` | Forwarded and succeeds | 1 |
| Viewer + `tools/call` for `admin_reset_key` | JSON-RPC `-32001`, `Unauthorized Tool Call`, matching ID | **0** |
| Viewer + `tools/call` for `admin_anything` | Same denial; proves dynamic `admin_` prefix enforcement | **0** |
| Valid JSON-RPC `id: 0` or string ID | Exact ID preserved on responses/denials | Depends on policy |
| Nested arguments on an allowed call | Original JSON-RPC body/arguments forwarded faithfully | 1 |
| Missing or non-string `params.name` | JSON-RPC `-32602` without crash | **0** |
| Invalid JSON or malformed JSON-RPC envelope | Controlled protocol error | **0** |
| JSON-RPC batch array | Explicitly rejected; no unchecked authorization bypass | **0** |
| Missing, tampered, or expired Bearer token | Controlled authentication failure | **0** |
| Downstream JSON-RPC error or non-200 status | Correctly relayed when forwarding succeeds | 1 |
| Downstream unavailable or times out | Sanitized gateway failure, no stack trace | Attempted |

**Testing guidance:** Use FastAPI test facilities and HTTPX's mock/ASGI transport where practical; keep test dependencies minimal. For the denial-path tests, assert against a spy or counter instead of relying only on the returned error message. Tests should verify the observable HTTP behavior end to end.

## 7. Explicitly out of scope

Do **not** add these while implementing the required behavior:

- OAuth/OIDC authorization server, login frontend, refresh-token handling, or a user/role database.
- Filtering `admin_` tools out of the `tools/list` response (contradicts transparent forwarding).
- Full MCP Streamable HTTP session negotiation, SSE support, or stdio-to-HTTP adapter without a specific need.
- A generic policy engine, multiple proxy abstractions, cloud hosting, Docker infrastructure, or dashboards.
- Extra source files or packages created solely for speculative future tasks.

## 8. Definition of done

- [x] A reviewer can run the gateway and downstream mock locally.
- [x] `tools/list` is forwarded transparently.
- [x] Permitted `tools/call` requests reach downstream and their responses return correctly.
- [x] Non-admin callers receive the **exact** `-32001: Unauthorized Tool Call` JSON-RPC response for any `admin_` tool.
- [x] Tests prove denied calls never contact downstream.
- [x] Middleware, request parsing/policy, and forwarding have clear and readable responsibilities.
- [x] Invalid credentials, malformed JSON-RPC, and downstream failures are handled without leaking sensitive details.
- [x] All acceptance tests have been executed successfully and startup instructions have been verified.

## Reference documentation

- [JSON-RPC 2.0 specification](https://www.jsonrpc.org/specification)
- [MCP specification](https://modelcontextprotocol.io/specification/)
- [FastAPI](https://fastapi.tiangolo.com/)
- [HTTPX async client](https://www.python-httpx.org/async/)
- [PyJWT](https://pyjwt.readthedocs.io/)

## Verification record

- The latest recorded functional run passed 99 gateway tests within 366 repository
  tests; configured Ruff checks passed on macOS arm64/Python 3.13.3.
- `uv sync --locked` and an earlier temporary source/environment check succeeded.
  That historical snapshot is not a clean checkout of the final review commit.
- Uvicorn startup, independent mock discovery, token generation and viewer/admin
  curl requests were checked on loopback. Both services were stopped afterward.
- `tests/test_gateway.py` uses middleware plus spy/ASGI transports to prove zero
  downstream calls for invalid credentials and denied requests. It covers strict
  JSON/envelopes, notifications, IDs, body/header/status relay, safe transport
  failures, timeouts and client shutdown.
- Regression fixes cover unpaired Unicode, shared downstream cookies, fractional
  ID precision and an uncaught Decimal conversion for extreme negative exponents.
  Invalid numeric input now returns controlled `-32700` with null ID; the next
  valid request still succeeds. Valid Unicode pairs and escaped tool names are tested.
- [README.md](README.md) documents the exact HTTP/JSON-RPC subset and JWT/header
  policies. Payload byte caps, sustained load, full MCP transport and production
  identity controls remain outside this implementation. HTTPX timeout is per
  operation, not a whole-request deadline. Downstream bodies are trusted and relayed.
- Python 3.11 and the actual review commit's clean checkout remain unverified;
  see the [root verification scope](../README.md#verification-and-limits).
