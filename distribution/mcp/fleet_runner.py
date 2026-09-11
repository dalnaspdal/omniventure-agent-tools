#!/usr/bin/env python3
"""
OmniVenture Modular MCP Fleet Stdio Runner
(distribution/mcp/fleet_runner.py)

Runs an individual 5-tool MCP fleet over standard I/O (JSON-RPC 2.0) for:
- Smithery.ai CLI (npx -y @smithery/cli run ...)
- Claude Desktop (`claude_desktop_config.json`)
- Cursor (`~/.cursor/mcp.json`)
- Antigravity & autonomous agent loops

Usage:
  python3 -m distribution.mcp.fleet_runner --fleet syntax
  python3 -m distribution.mcp.fleet_runner --fleet distill
  python3 -m distribution.mcp.fleet_runner --fleet verify
  python3 -m distribution.mcp.fleet_runner --fleet finance
  python3 -m distribution.mcp.fleet_runner --fleet infra
  python3 -m distribution.mcp.fleet_runner --fleet all
"""

import sys
import json
import asyncio
import argparse
from typing import Dict, Any, List

from distribution.mcp.mcp_server import (
    FLEETS,
    FLEET_SYNTAX,
    FLEET_DISTILL,
    FLEET_VERIFY,
    FLEET_FINANCE,
    FLEET_INFRA,
    execute_tool_call
)

FLEET_MAP: Dict[str, List[Dict[str, Any]]] = {
    "syntax": FLEET_SYNTAX,
    "distill": FLEET_DISTILL,
    "verify": FLEET_VERIFY,
    "finance": FLEET_FINANCE,
    "infra": FLEET_INFRA,
}


def get_fleet_tools(fleet_name: str) -> List[Dict[str, Any]]:
    """Returns the tool list for a given fleet, or all tools if 'all'."""
    if fleet_name == "all":
        combined: List[Dict[str, Any]] = []
        for fl_tools in FLEET_MAP.values():
            combined.extend(fl_tools)
        return combined
    return FLEET_MAP.get(fleet_name.lower(), FLEET_SYNTAX)


def send_response(response_dict: dict):
    """Writes a JSON-RPC response to stdout followed by newline."""
    line = json.dumps(response_dict)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


async def handle_request(req: Dict[str, Any], fleet_name: str, tools: List[Dict[str, Any]]):
    """Processes a single JSON-RPC 2.0 request."""
    req_id = req.get("id")
    method = req.get("method")
    params = req.get("params", {})

    if method == "initialize":
        send_response({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {"tools": {}},
                "serverInfo": {
                    "name": f"omniventure-mcp-{fleet_name}",
                    "version": "1.0.0"
                }
            }
        })
    elif method == "notifications/initialized":
        # Notification acknowledgment, no response required
        pass
    elif method == "ping":
        send_response({"jsonrpc": "2.0", "id": req_id, "result": {}})
    elif method == "tools/list":
        send_response({
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"tools": tools}
        })
    elif method == "tools/call":
        tool_name = params.get("name")
        arguments = params.get("arguments", {})

        # Verify tool belongs to active fleet
        valid_tool_names = {t["name"] for t in tools}
        if tool_name not in valid_tool_names:
            send_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": f"Tool '{tool_name}' not available in fleet '{fleet_name}'"
                }
            })
            return

        try:
            result_text = await execute_tool_call(tool_name, arguments)
            send_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {
                            "type": "text",
                            "text": result_text
                        }
                    ],
                    "isError": False
                }
            })
        except Exception as e:
            send_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [
                        {"type": "text", "text": f"Execution error: {str(e)}"}
                    ],
                    "isError": True
                }
            })
    else:
        if req_id is not None:
            send_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": f"Method '{method}' not implemented"
                }
            })


async def run_stdio(fleet_name: str):
    """Asynchronous stdin reader and event loop."""
    tools = get_fleet_tools(fleet_name)
    loop = asyncio.get_event_loop()
    reader = asyncio.StreamReader()
    protocol = asyncio.StreamReaderProtocol(reader)
    await loop.connect_read_pipe(lambda: protocol, sys.stdin)

    while True:
        line_bytes = await reader.readline()
        if not line_bytes:
            break
        raw_line = line_bytes.decode("utf-8").strip()
        if not raw_line:
            continue
        try:
            req = json.loads(raw_line)
        except json.JSONDecodeError:
            continue
        await handle_request(req, fleet_name, tools)


def main():
    parser = argparse.ArgumentParser(description="OmniVenture Modular MCP Fleet Runner")
    parser.add_argument(
        "--fleet",
        type=str,
        default="syntax",
        choices=["syntax", "distill", "verify", "finance", "infra", "all"],
        help="Fleet to expose (default: syntax)"
    )
    args = parser.parse_args()
    asyncio.run(run_stdio(args.fleet))


if __name__ == "__main__":
    main()
