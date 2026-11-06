"""Local stdio MCP server. All customer data and refunds are synthetic."""

import json
import logging
import sys
from decimal import Decimal
from hashlib import sha256
from typing import Any

import anyio
from mcp.server import MCPServer
from mcp.server.mcpserver import Context
from mcp.server.mcpserver.exceptions import ToolError
from mcp.server.stdio import stdio_server
from mcp.shared.exceptions import MCPError
from mcp.shared.message import SessionMessage
from mcp.types import (
    INVALID_PARAMS,
    INVALID_REQUEST,
    PARSE_ERROR,
    CallToolResult,
    ErrorData,
    JSONRPCError,
    TextContent,
    Tool,
)
from pydantic import ValidationError

from mcp_server.schemas import CustomerLookup, RefundRequest


logger = logging.getLogger(__name__)

CUSTOMERS = {
    "CUST-12345": {"customer_id": "CUST-12345", "name": "Example Customer One", "synthetic": True},
    "CUST-54321": {"customer_id": "CUST-54321", "name": "Example Customer Two", "synthetic": True},
}


def get_customer_record(customer_id: str) -> dict[str, Any]:
    """Look up a synthetic customer record; no live customer system is contacted."""
    record = CUSTOMERS.get(customer_id)
    if record is None:
        raise ToolError("Customer not found in the synthetic dataset.")
    return dict(record)


def trigger_refund(customer_id: str, amount: float, reason: str) -> dict[str, Any]:
    """Simulate a refund acknowledgment. No payment or external side effect occurs."""
    get_customer_record(customer_id)
    # The mock has no currency or cent policy; avoid introducing implicit rounding.
    amount_text = format(Decimal(str(amount)).normalize(), "f")
    payload = {"customer_id": customer_id, "amount": amount_text, "reason": reason}
    # Stable IDs make mock responses reproducible, without implying payment idempotency.
    fingerprint = sha256(json.dumps(payload, sort_keys=True).encode()).hexdigest()[:16]
    return {"mock": True, "status": "simulated", "refund_id": f"MOCK-{fingerprint}", **payload}


TOOL_INPUTS = {
    "get_customer_record": CustomerLookup,
    "trigger_refund": RefundRequest,
}
TOOL_HANDLERS = {
    "get_customer_record": get_customer_record,
    "trigger_refund": trigger_refund,
}


class CustomerMCPServer(MCPServer):
    """Preserve strict inputs despite SDK 2.3.0's permissive tool wrapper.

    The default wrapper ignores extra fields and pre-parses JSON-looking strings.
    Share exact schemas between discovery and validation so clients see the same
    contract that dispatch enforces. A stdio adapter covers dropped parse errors;
    framing and negotiation remain with the SDK.
    """

    async def list_tools(self) -> list[Tool]:
        tools = await super().list_tools()
        for tool in tools:
            tool.input_schema = TOOL_INPUTS[tool.name].model_json_schema()
        return tools

    async def call_tool(
        self, name: str, arguments: dict[str, Any], context: Context | None = None
    ) -> CallToolResult:
        if name not in TOOL_INPUTS:
            raise MCPError(code=INVALID_PARAMS, message="Unknown tool.")
        try:
            request = TOOL_INPUTS[name].model_validate(arguments)
        except ValidationError:
            # The assignment explicitly requires numeric JSON-RPC validation errors.
            # MCPError bypasses the SDK's conversion of ToolError to isError results.
            # Keep the message generic so rejected input values cannot leak to clients.
            raise MCPError(
                code=INVALID_PARAMS,
                message="Invalid tool arguments. Check the published input schema.",
            ) from None
        try:
            result = TOOL_HANDLERS[name](**request.model_dump())
        except ToolError:
            raise
        except Exception as exc:
            # Exception messages can contain credentials; log only the exception type.
            logger.error("Tool execution failed (%s).", type(exc).__name__)
            raise ToolError("Tool execution failed.") from None
        return CallToolResult(
            content=[TextContent(type="text", text=json.dumps(result, allow_nan=False))],
            structured_content=result,
        )

    async def run_stdio_async(self) -> None:
        # SDK 2.3.0 emits transport ValidationErrors but its runner discards them.
        # Translate only those errors; the SDK still parses and frames every byte.
        async with stdio_server() as (incoming, outgoing):
            sender, receiver = anyio.create_memory_object_stream[SessionMessage | Exception](0)

            async def forward_messages() -> None:
                async with sender:
                    async for message in incoming:
                        if not isinstance(message, ValidationError):
                            await sender.send(message)
                            continue
                        errors = message.errors(include_input=False, include_url=False)
                        is_parse_error = any(error["type"] == "json_invalid" for error in errors)
                        code = PARSE_ERROR if is_parse_error else INVALID_REQUEST
                        label = "Parse error" if is_parse_error else "Invalid request"
                        logger.warning("Rejected malformed stdio message: %s.", label)
                        # A malformed envelope provides no trustworthy request ID.
                        await outgoing.send(SessionMessage(JSONRPCError(
                            jsonrpc="2.0", id=None, error=ErrorData(code=code, message=label)
                        )))

            async with anyio.create_task_group() as tasks:
                tasks.start_soon(forward_messages)
                # Reuse the SDK lifecycle rather than duplicating negotiation. This
                # private entry point needs wire-test verification on SDK upgrades.
                await self._lowlevel_server.run(
                    receiver, outgoing, self._lowlevel_server.create_initialization_options()
                )


mcp = CustomerMCPServer("Synthetic Customer Tools", version="0.1.0")
for handler in TOOL_HANDLERS.values():
    mcp.add_tool(handler)


def main() -> None:
    # Replace SDK-installed handlers so application diagnostics stay on stderr.
    logging.basicConfig(stream=sys.stderr, level=logging.INFO, force=True)
    logger.info("Starting synthetic customer MCP server over stdio.")
    mcp.run(transport="stdio")


if __name__ == "__main__":
    main()
