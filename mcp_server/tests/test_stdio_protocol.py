"""Acceptance checks through the SDK client and actual subprocess pipes."""

import asyncio
import json
import sys
from contextlib import asynccontextmanager
from pathlib import Path

import pytest
from mcp import Client
from mcp.client.stdio import StdioServerParameters
from mcp.shared.exceptions import MCPError
from mcp.types import jsonrpc_message_adapter

from mcp_server.schemas import CustomerLookup, RefundRequest
from mcp_server.server import TOOL_HANDLERS, mcp


ROOT = Path(__file__).resolve().parents[2]


def test_sdk_client_discovers_and_calls_both_tools_over_stdio():
    async def check():
        parameters = StdioServerParameters(
            command=sys.executable, args=["-m", "mcp_server.server"], cwd=ROOT
        )
        async with Client(parameters, read_timeout_seconds=5) as client:
            discovery = await client.list_tools()
            tools = {tool.name: tool for tool in discovery.tools}
            assert set(tools) == {"get_customer_record", "trigger_refund"}
            assert tools["get_customer_record"].input_schema == CustomerLookup.model_json_schema()
            assert tools["trigger_refund"].input_schema == RefundRequest.model_json_schema()

            customer = await client.call_tool("get_customer_record", {"customer_id": "CUST-12345"})
            assert not customer.is_error
            assert customer.structured_content == {
                "customer_id": "CUST-12345", "name": "Example Customer One", "synthetic": True
            }
            arguments = {"customer_id": "CUST-12345", "amount": 12.5, "reason": "  Duplicate charge  "}
            refund = await client.call_tool("trigger_refund", arguments)
            assert not refund.is_error
            assert refund.structured_content["mock"] is True
            assert refund.structured_content["status"] == "simulated"
            assert refund.structured_content["amount"] == "12.5"
            assert refund.structured_content["reason"] == "Duplicate charge"
            repeat = await client.call_tool("trigger_refund", arguments)
            assert repeat.structured_content == refund.structured_content

            # JSON-looking text must stay text, rather than being coerced by the SDK.
            arguments["reason"] = '{"reason":"duplicate"}'
            result = await client.call_tool("trigger_refund", arguments)
            assert not result.is_error
            assert result.structured_content["reason"] == arguments["reason"]

            for name, invalid_arguments in [
                ("trigger_refund", {**arguments, "amount": -1}),
                ("unknown_tool", {}),
            ]:
                with pytest.raises(MCPError) as rejected:
                    await client.call_tool(name, invalid_arguments)
                assert rejected.value.code == -32602
            recovered = await client.call_tool("get_customer_record", {"customer_id": "CUST-12345"})
            assert not recovered.is_error

    asyncio.run(asyncio.wait_for(check(), 15))


@asynccontextmanager
async def wire_client():
    process = await asyncio.create_subprocess_exec(
        sys.executable, "-m", "mcp_server.server", cwd=ROOT,
        stdin=asyncio.subprocess.PIPE, stdout=asyncio.subprocess.PIPE, stderr=asyncio.subprocess.PIPE,
    )

    async def exchange(message):
        raw = message if isinstance(message, str) else json.dumps(message, allow_nan=False)
        process.stdin.write((raw + "\n").encode())
        await process.stdin.drain()
        line = await asyncio.wait_for(process.stdout.readline(), 5)
        assert line, "MCP process closed stdout before responding"
        jsonrpc_message_adapter.validate_json(line, by_name=False)
        return json.loads(line)

    try:
        initialized = await exchange({
            "jsonrpc": "2.0", "id": 1, "method": "initialize", "params": {
                "protocolVersion": "2025-11-25", "capabilities": {},
                "clientInfo": {"name": "acceptance-test", "version": "1"},
            },
        })
        assert initialized["id"] == 1
        assert "result" in initialized
        process.stdin.write(b'{"jsonrpc":"2.0","method":"notifications/initialized"}\n')
        await process.stdin.drain()
        yield exchange
    finally:
        process.stdin.close()
        try:
            remaining, diagnostics = await asyncio.wait_for(process.communicate(), 5)
        except TimeoutError:
            process.kill()
            await process.communicate()
            raise
        assert process.returncode == 0
        assert remaining == b"", "Unexpected frames or diagnostics on stdout"
        assert b"Starting synthetic customer MCP server over stdio." in diagnostics
        assert b"secret-canary" not in diagnostics


def test_protocol_errors_and_recovery_with_stdout_isolation():
    async def check():
        async with wire_client() as exchange:
            for raw, code in [
                ("{broken", -32700),
                ('{"jsonrpc":"1.0","id":2,"method":"ping"}', -32600),
                ('{"jsonrpc":"2.0","id":2}', -32600),
                ("[]", -32600),
                ("null", -32600),
            ]:
                response = await exchange(raw)
                assert response == {"jsonrpc": "2.0", "id": None, "error": {
                    "code": code, "message": "Parse error" if code == -32700 else "Invalid request"
                }}

            for identifier, method, params, code in [
                (3, "nonexistent/method", {}, -32601),
                (4, "tools/call", {}, -32602),
                (5, "tools/call", {"name": 12}, -32602),
                (6, "tools/call", {"name": "trigger_refund", "arguments": []}, -32602),
            ]:
                response = await exchange({"jsonrpc": "2.0", "id": identifier,
                                           "method": method, "params": params})
                assert response["id"] == identifier
                assert response["error"]["code"] == code
                assert "result" not in response

            response = await exchange({"jsonrpc": "2.0", "id": 7, "method": "ping"})
            assert response == {"jsonrpc": "2.0", "id": 7, "result": {}}
            response = await exchange({"jsonrpc": "2.0", "id": 8, "method": "tools/list"})
            assert {tool["name"] for tool in response["result"]["tools"]} == set(TOOL_HANDLERS)

    asyncio.run(asyncio.wait_for(check(), 20))


def test_invalid_tool_inputs_and_unknown_customers_on_the_wire():
    async def check():
        valid = {"customer_id": "CUST-12345", "amount": 1, "reason": "Duplicate charge"}
        invalid_refunds = [
            {}, {**valid, "extra": "secret-canary"},
            *[{**valid, "amount": amount} for amount in [0, -1, True, "1", None, {}, []]],
            *[{**valid, "reason": reason} for reason in ["short", " " * 20, 123, None]],
            *[{key: value for key, value in valid.items() if key != missing}
              for missing in valid],
        ]
        invalid_lookups = [
            {}, {"customer_id": "CUST-12345", "extra": True},
            *[{"customer_id": identifier} for identifier in
              ["12345", "CUST-1234", "CUST-123456", "CUST-ABCDE", 12345, True, None]],
        ]
        async with wire_client() as exchange:
            identifier = 10
            for name, arguments in [
                *[("trigger_refund", args) for args in invalid_refunds],
                *[("get_customer_record", args) for args in invalid_lookups],
                ("unknown_tool", {}),
            ]:
                response = await exchange({"jsonrpc": "2.0", "id": identifier, "method": "tools/call",
                                           "params": {"name": name, "arguments": arguments}})
                assert response["id"] == identifier
                assert response["error"]["code"] == -32602
                assert "result" not in response
                assert "secret-canary" not in json.dumps(response)
                assert "Traceback" not in json.dumps(response)
                identifier += 1

            # Nonstandard JSON numeric literals must never succeed as refund amounts.
            for amount in ["NaN", "Infinity", "-Infinity", "1e999"]:
                raw = ('{"jsonrpc":"2.0","id":100,"method":"tools/call","params":'
                       '{"name":"trigger_refund","arguments":{"customer_id":"CUST-12345",'
                       '"amount":' + amount + ',"reason":"Duplicate charge"}}}')
                response = await exchange(raw)
                assert response["error"]["code"] == -32602
                assert "result" not in response

            # Well-formed requests that miss the dataset remain business failures.
            for name, arguments in [
                ("get_customer_record", {"customer_id": "CUST-99999"}),
                ("trigger_refund", {**valid, "customer_id": "CUST-99999"}),
            ]:
                response = await exchange({"jsonrpc": "2.0", "id": identifier, "method": "tools/call",
                                           "params": {"name": name, "arguments": arguments}})
                assert response["id"] == identifier
                assert "error" not in response
                assert response["result"]["isError"] is True
                assert response["result"]["content"][0]["text"] == "Customer not found in the synthetic dataset."
                identifier += 1

    asyncio.run(asyncio.wait_for(check(), 20))


def test_validation_precedes_execution_and_crashes_are_sanitized(monkeypatch):
    calls = []

    def broken_refund(**arguments):
        calls.append(arguments)
        raise RuntimeError("secret-canary: https://private.internal")

    monkeypatch.setitem(TOOL_HANDLERS, "trigger_refund", broken_refund)

    async def check():
        async with Client(mcp) as client:
            with pytest.raises(MCPError) as rejected:
                await client.call_tool("trigger_refund", {
                    "customer_id": "CUST-12345", "amount": True, "reason": "Duplicate charge"
                })
            assert rejected.value.code == -32602
            assert not calls
            failed = await client.call_tool("trigger_refund", {
                "customer_id": "CUST-12345", "amount": 1, "reason": "Duplicate charge"
            })
            assert len(calls) == 1
            assert failed.is_error
            assert failed.content[0].text == "Tool execution failed."
            assert "secret-canary" not in failed.model_dump_json()

    asyncio.run(asyncio.wait_for(check(), 5))
