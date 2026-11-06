# HTTP security gateway

Task 2 runs independently of the stdio MCP server. The gateway verifies signed
development JWTs, then blocks viewer calls to any `admin_` tool before downstream
I/O. Tool discovery stays unfiltered. The downstream service is a harmless mock;
`admin_reset_key` only returns a simulated acknowledgment.

Run these commands from the repository root with Python 3.11+ and uv.

## Start the services

```bash
uv sync --locked
uv run uvicorn security_gateway.mock_downstream:app --host 127.0.0.1 --port 8001
```

In a second terminal, generate a local secret and start the gateway:

```bash
export GATEWAY_JWT_SECRET="$(uv run python -c 'import secrets; print(secrets.token_hex(32))')"
uv run uvicorn security_gateway.app:create_app --factory --host 127.0.0.1 --port 8000 &
GATEWAY_PID=$!
```

The gateway runs in the background so the client commands below can use the
same shell's signing secret. After verification, run `kill "$GATEWAY_PID"` and
press Ctrl+C in the mock terminal.
The mock intentionally has no authentication and should stay on loopback.

Configuration is documented in [../.env.example](../.env.example). The gateway
requires `GATEWAY_JWT_SECRET` (at least 32 bytes), defaults
`GATEWAY_DOWNSTREAM_URL` to `http://127.0.0.1:8001/mcp`, and defaults
`GATEWAY_TIMEOUT_SECONDS` to `5` (positive, finite HTTPX per-operation timeout).
Environment files are not automatically loaded; export values into the shell.

## Generate tokens and send requests

In the gateway terminal with the same exported signing secret:

```bash
export VIEWER_TOKEN="$(uv run python -c "import os,time,jwt; print(jwt.encode({'role':'viewer','exp':int(time.time())+3600},os.environ['GATEWAY_JWT_SECRET'],algorithm='HS256'))")"
export ADMIN_TOKEN="$(uv run python -c "import os,time,jwt; print(jwt.encode({'role':'admin','exp':int(time.time())+3600},os.environ['GATEWAY_JWT_SECRET'],algorithm='HS256'))")"

curl --silent --show-error http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":0,"method":"tools/list"}'

curl --silent --show-error http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":42,"method":"tools/call","params":{"name":"admin_reset_key"}}'
```

The list includes `get_status` and `admin_reset_key`. The second request returns
exactly:

```json
{"jsonrpc":"2.0","id":42,"error":{"code":-32001,"message":"Unauthorized Tool Call"}}
```

Use `$ADMIN_TOKEN` for the second request to receive the simulated success result.
Either role can call `get_status`. The mock tools accept empty or omitted arguments.

Check ordinary access, admin access, and missing authentication:

```bash
curl --silent --show-error http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $VIEWER_TOKEN" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":1,"method":"tools/call","params":{"name":"get_status"}}'

curl --silent --show-error http://127.0.0.1:8000/mcp \
  -H "Authorization: Bearer $ADMIN_TOKEN" -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":2,"method":"tools/call","params":{"name":"admin_reset_key"}}'

curl --silent --show-error -i http://127.0.0.1:8000/mcp \
  -H 'Content-Type: application/json' \
  --data '{"jsonrpc":"2.0","id":3,"method":"tools/list"}'
```

The first two return mock success results. The last returns HTTP 401 with
`WWW-Authenticate: Bearer`. A denial response alone cannot prove the absence of
downstream work; the automated spy assertions below verify that separately.

## Supported subset and failure behavior

- Only single JSON-RPC 2.0 objects at `POST /mcp`; batches are rejected. Full MCP
  Streamable HTTP sessions, SSE, OAuth, and a stdio adapter are outside this task.
- Envelopes allow `jsonrpc`, `method`, `params`, and `id`; unexpected envelope
  fields, invalid ID types, and malformed envelopes return `-32600` with null ID.
  IDs may be strings, finite numbers, or null; `0` is preserved. Fractional IDs
  use Decimal internally so locally generated errors do not round their values.
- Invalid JSON, duplicate JSON members, numbers outside the supported float/Decimal
  range, and unpaired Unicode surrogates return `-32700`. Valid Unicode surrogate
  pairs are accepted.
  `tools/call` requires object params, a string name, and object arguments if
  supplied; violations return `-32602`. Other params are forwarded unchanged;
  generic methods accept object/array params and pass through to downstream.
- Valid notifications omit `id`; they receive empty HTTP 204 responses, including
  denied calls. Denied notifications still make zero downstream requests. Explicit
  null IDs are treated as requests. This follows JSON-RPC notification semantics.
- JWTs use only HS256, with required numeric `exp` and allowlisted `role` claims.
  Signatures, expiry and optional `nbf` are checked. No issuer/audience is configured;
  audience-bearing tokens are rejected. Invalid authentication returns HTTP 401
  with `WWW-Authenticate: Bearer`, before body parsing.
- Allowed request bodies are forwarded as original bytes. Request headers are
  allowlisted to content type, accept, MCP protocol version and session ID. Client
  Bearer credentials and hop-by-hop metadata are not relayed.
- Downstream status and body are preserved, including JSON-RPC errors and non-200
  responses. Response headers allow content type, MCP protocol/session headers,
  retry-after and location. Redirects are relayed without following them. HTTPX
  decodes compressed bodies, so content length is recalculated and content encoding
  omitted. Headers named by `Connection` are excluded in both directions.
- Downstream transport failures return HTTP 502; timeouts return 504. Both use
  sanitized JSON-RPC `-32000` errors with the request ID. Downstream HTTP responses
  themselves are passed through as received; use a trusted downstream service.
- One HTTPX client is shared across requests and closed at application shutdown.
  Connection reuse does not share caller identity: incoming cookies are excluded,
  and cookies accumulated from downstream responses are stripped from each built
  request before it is sent.

Reference behavior: [JSON-RPC specification](https://www.jsonrpc.org/specification),
[PyJWT usage](https://pyjwt.readthedocs.io/en/stable/usage.html),
[HTTPX async clients](https://www.python-httpx.org/async/), and
[FastAPI lifespan](https://fastapi.tiangolo.com/advanced/events/).

## Verification

```bash
uv run pytest security_gateway/tests -q
uv run pytest -v
```

To inspect the most important security assertions by name:

```bash
uv run pytest security_gateway/tests -v -k 'denials_never or authentication_precedes or downstream_cookies or unicode or decimal_precision'
```

The gateway suite exercises HTTP middleware with spy/ASGI downstream transports,
including zero-call denial assertions, malformed input, invalid credentials,
exact body forwarding, status/header relay, timeouts and client cleanup.
Both Uvicorn startup commands, independent mock requests, token generation and
curl requests were also checked against local TCP services. No hosted credentials
or live providers are needed.

Completion is measured against the task's acceptance matrix. This is a local
exercise with development JWTs and a trusted mock downstream. Requests and
responses are buffered without explicit byte limits; sustained load and denial
of service resilience have not been tested. Full MCP transport support and
production identity/deployment controls remain outside the specified scope.
Linting is not configured. A fresh temporary source copy and virtual environment
were checked with the repository lockfile; an actual Git clean-clone check
has not been performed because these changes are still uncommitted.
