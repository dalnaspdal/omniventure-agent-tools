#!/usr/bin/env python3
"""
OmniVenture MCP Syntax Repair Fleet Runner
(distribution/mcp/fleets/syntax/run.py)
"""
import sys
import os
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parents[3]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from distribution.mcp.fleet_runner import run_stdio
import asyncio

if __name__ == "__main__":
    asyncio.run(run_stdio("syntax"))
