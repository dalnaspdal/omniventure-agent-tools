#!/usr/bin/env python3
"""
OmniVenture MCP Autonomous Bot Rails & Infrastructure Fleet Runner
(distribution/mcp/fleets/infra/run.py)
"""
import sys
import os
from pathlib import Path

root_dir = Path(__file__).resolve().parents[3]
if str(root_dir) not in sys.path:
    sys.path.insert(0, str(root_dir))

from distribution.mcp.fleet_runner import run_stdio
import asyncio

if __name__ == "__main__":
    asyncio.run(run_stdio("infra"))
