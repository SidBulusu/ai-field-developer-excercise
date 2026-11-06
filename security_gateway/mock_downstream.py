"""Side-effect-free HTTP tool mock; never resets a real key."""

from fastapi import FastAPI, Request
from fastapi.responses import Response

from security_gateway.app import parse_rpc, request_error, rpc_response


app = FastAPI()
# This service deliberately has no role policy: tests must prove the gateway
# blocks privileged calls before they reach an otherwise callable mock tool.
TOOLS = [
    {"name": "get_status", "description": "Return deterministic mock status.",
     "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}},
    {"name": "admin_reset_key", "description": "Simulate a key reset; no side effects.",
     "inputSchema": {"type": "object", "properties": {}, "additionalProperties": False}},
]


@app.post("/mcp")
async def handle(request: Request):
    payload = parse_rpc(await request.body())
    if isinstance(payload, Response):
        return payload
    if payload["method"] == "tools/list":
        result = {"tools": TOOLS}
    elif payload["method"] == "tools/call":
        params = payload["params"]
        if params["name"] not in {tool["name"] for tool in TOOLS}:
            return request_error(payload, -32602, "Unknown mock tool")
        if params.get("arguments", {}):
            return request_error(payload, -32602, "Mock tools accept no arguments")
        text = "Mock service ready" if params["name"] == "get_status" else "Mock key reset simulated"
        result = {"content": [{"type": "text", "text": text}], "isError": False}
    else:
        return request_error(payload, -32601, "Method not found")
    if "id" not in payload:
        return Response(status_code=204)
    return rpc_response(payload["id"], {"result": result})
