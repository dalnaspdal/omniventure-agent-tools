"""
OmniVenture Model Context Protocol (MCP) Server (distribution/mcp/mcp_server.py)

Exposes OmniVenture OS tools (Email Triage, JSON Repair, Context Distillation, etc.)
over standard MCP transports:
  - Server-Sent Events (SSE) at GET /mcp/sse + POST /mcp/messages
  - Streamable HTTP at POST /mcp
  - Full-Duplex WebSocket at /mcp/ws

Compatible with llama.cpp Web UI (:8080), Claude Desktop, Cursor, and Antigravity.
"""

import asyncio
import json
import uuid
import logging
from datetime import datetime
from typing import Dict, Any, Optional, List
from fastapi import APIRouter, Request, Response, WebSocket, WebSocketDisconnect
from fastapi.responses import StreamingResponse, JSONResponse

from executive.triage import triage_service
from m2m.forge import m2m_forge

logger = logging.getLogger("mcp_server")

mcp_router = APIRouter(prefix="/mcp", tags=["MCP"])

# Active SSE sessions: sessionId -> asyncio.Queue
active_sse_sessions: Dict[str, asyncio.Queue] = {}

# ─────────────────────────────────────────────────────────────────────────────
# 1. FLEET 1: Financial Oracles & Arbitrage (5 Tools)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_FINANCE: List[Dict[str, Any]] = [
    {
        "name": "get_market_quote",
        "description": "Real-time multi-asset market spot price oracle for crypto (BTC, ETH, SOL, XRP) and equities with 30s TTL edge cache.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Asset ticker (e.g. 'BTC', 'ETH', 'SOL', 'AAPL').", "default": "BTC"},
                "currency": {"type": "string", "description": "Target currency ('USD' or 'GBP').", "default": "USD"},
                "nocache": {"type": "boolean", "description": "Bypass edge cache for fresh query.", "default": False}
            },
            "required": ["symbol"]
        }
    },
    {
        "name": "scan_market_delta",
        "description": "Calculates statistical delta, spread, and discrepancy between two series or asset prices for arbitrage detection.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "series_a": {"description": "Primary value or price list."},
                "series_b": {"description": "Secondary comparison baseline."},
                "threshold_pct": {"type": "number", "description": "Divergence threshold (default: 5.0%).", "default": 5.0}
            },
            "required": ["series_a", "series_b"]
        }
    },
    {
        "name": "get_crypto_ticker",
        "description": "Multi-currency crypto price ticker and exchange conversion rates (BTC, ETH, SOL to GBP/USD/EUR).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "symbol": {"type": "string", "description": "Base crypto symbol (e.g. 'BTC').", "default": "BTC"}
            }
        }
    },
    {
        "name": "estimate_gas_priority",
        "description": "Real-time fee priority estimations for Bitcoin (sat/vB: fast, half_hour, hour) and Ethereum (gwei: rapid, fast, standard).",
        "inputSchema": {
            "type": "object",
            "properties": {
                "network": {"type": "string", "description": "Network: 'BTC' or 'ETH'.", "enum": ["BTC", "ETH"], "default": "BTC"}
            }
        }
    },
    {
        "name": "check_market_volatility",
        "description": "Calculates statistical volatility metrics (standard deviation, annualized volatility, price span) across price points.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "series": {"type": "array", "items": {"type": "number"}, "description": "Array of numeric price points."},
                "symbol": {"type": "string", "description": "Asset ticker to generate volatility from if series is omitted."}
            }
        }
    }
]

# ─────────────────────────────────────────────────────────────────────────────
# 2. FLEET 2: Syntax Repair & AST Healing (5 Tools)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_SYNTAX: List[Dict[str, Any]] = [
    {
        "name": "repair_json",
        "description": "Universal LLM JSON repair and syntax sanitizer. Fixes malformed JSON (code blocks, single quotes, trailing commas, Python literals) into RFC 8259 format in <1ms.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_json": {"type": "string", "description": "Malformed or broken JSON string to repair."}
            },
            "required": ["raw_json"]
        }
    },
    {
        "name": "unwrap_markdown",
        "description": "Strips markdown code fences (```json, ```python, etc.) and conversational preamble from LLM outputs to isolate clean payloads.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Raw LLM output text containing markdown code fences."}
            },
            "required": ["text"]
        }
    },
    {
        "name": "validate_json_schema",
        "description": "Deterministic AST schema validator checking presence and basic types of required keys with zero token cost.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "data": {"type": "object", "description": "Parsed JSON object to validate."},
                "required_keys": {"type": "array", "items": {"type": "string"}, "description": "List of required top-level keys."}
            },
            "required": ["data", "required_keys"]
        }
    },
    {
        "name": "repair_csv",
        "description": "Normalizes and repairs malformed CSV text (inconsistent delimiters, ragged rows, unescaped quotes) into RFC 4180 format.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_csv": {"type": "string", "description": "Raw unformatted or malformed CSV string."}
            },
            "required": ["raw_csv"]
        }
    },
    {
        "name": "diff_ast_keys",
        "description": "Compares key structures between source and target dictionaries, reporting missing, extra, and common keys.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "source": {"type": "object", "description": "Source reference dictionary."},
                "target": {"type": "object", "description": "Target dictionary to compare."}
            },
            "required": ["source", "target"]
        }
    }
]

# ─────────────────────────────────────────────────────────────────────────────
# 3. FLEET 3: Context Distillation & Web Scraping (5 Tools)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_DISTILL: List[Dict[str, Any]] = [
    {
        "name": "extract_markdown",
        "description": "Web-to-Markdown context compressor. Scrapes web URLs or parses raw HTML into clean, token-efficient Markdown, saving 80-95% context tokens.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Public URL to fetch and convert to Markdown."},
                "html": {"type": "string", "description": "Raw HTML string to parse directly."},
                "include_links": {"type": "boolean", "description": "Retain hyperlinks in markdown.", "default": False}
            }
        }
    },
    {
        "name": "distill_context",
        "description": "Compresses long articles or documents into dense structured bullet takeaways, key metrics, and top keywords.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_text": {"type": "string", "description": "Raw article or document text to distill."},
                "max_keywords": {"type": "integer", "description": "Maximum keywords to extract (default: 5).", "default": 5}
            },
            "required": ["raw_text"]
        }
    },
    {
        "name": "extract_tables",
        "description": "Parses markdown or HTML tables into structured JSON lists of records.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Text or HTML containing tabular data."}
            },
            "required": ["text"]
        }
    },
    {
        "name": "strip_boilerplate",
        "description": "Strips cookie notices, legal disclaimers, navigation bars, and social share boilerplate from documents.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Document text to sanitize."}
            },
            "required": ["text"]
        }
    },
    {
        "name": "score_readability",
        "description": "Computes text metrics, token density, and Flesch Reading Ease score for automated content grading.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Text to analyze."}
            },
            "required": ["text"]
        }
    }
]

# ─────────────────────────────────────────────────────────────────────────────
# 4. FLEET 4: Deterministic Verification & Privacy (5 Tools)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_VERIFY: List[Dict[str, Any]] = [
    {
        "name": "sanitize_pii",
        "description": "Zero-latency privacy and PII sanitizer. Redacts emails, phone numbers, payment cards, and SSNs before data leaves trust boundaries.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Raw text string to scan and redact PII from."}
            },
            "required": ["text"]
        }
    },
    {
        "name": "verify_email",
        "description": "B2B email and deliverability verifier. Validates RFC 5322 syntax, performs live DNS MX lookup, and detects disposable/burner domains.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "email": {"type": "string", "description": "Target email address to validate."},
                "check_mx": {"type": "boolean", "description": "Verify live DNS MX records (default: true).", "default": True}
            },
            "required": ["email"]
        }
    },
    {
        "name": "validate_hash",
        "description": "Cryptographic SHA-256 integrity validator. Computes and checks payload digests against expected hashes.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "payload": {"description": "Arbitrary JSON payload or dictionary to hash."},
                "expected_hash": {"type": "string", "description": "Optional expected 64-char hexadecimal SHA-256 digest."}
            },
            "required": ["payload"]
        }
    },
    {
        "name": "scan_prompt_injection",
        "description": "Fast regex heuristic scanner detecting prompt injection, jailbreak attempts, and system prompt override patterns.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "prompt": {"type": "string", "description": "Prompt text to inspect for injection patterns."}
            },
            "required": ["prompt"]
        }
    },
    {
        "name": "detect_entropy",
        "description": "Calculates Shannon entropy to detect exposed credentials, API keys, and cryptographic secrets in text.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {"type": "string", "description": "Text or token string to scan for high-entropy secrets."}
            },
            "required": ["text"]
        }
    }
]

# ─────────────────────────────────────────────────────────────────────────────
# 5. FLEET 5: Autonomous Bot Rails & Relays (5 Tools)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_INFRA: List[Dict[str, Any]] = [
    {
        "name": "mint_bolt11_invoice",
        "description": "Mints real BOLT-11 Lightning invoices via Alby Hub Lightning node (:8029) for autonomous machine payments.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "amount_sats": {"type": "integer", "description": "Amount in satoshis to invoice."},
                "memo": {"type": "string", "description": "Invoice memo description.", "default": "OmniVenture M2M Micro-Utility"}
            },
            "required": ["amount_sats"]
        }
    },
    {
        "name": "verify_payment_hash",
        "description": "Verifies settlement status of a Lightning invoice or payment hash via Alby Hub node.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "payment_hash": {"type": "string", "description": "32-byte hex payment hash."}
            },
            "required": ["payment_hash"]
        }
    },
    {
        "name": "relay_signed_webhook",
        "description": "Outbound webhook dispatcher with HMAC-SHA256 signature, latency metering, and delivery receipts.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {"type": "string", "description": "Target webhook destination URL."},
                "payload": {"type": "object", "description": "JSON payload to transmit."},
                "secret": {"type": "string", "description": "HMAC secret key for signature."}
            },
            "required": ["url", "payload"]
        }
    },
    {
        "name": "lookup_ip",
        "description": "Edge IP and network taxonomy lookup. Resolves IP routing taxonomy, private/loopback CIDRs, and reverse DNS PTR.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "ip": {"type": "string", "description": "IPv4 or IPv6 address string to inspect."}
            },
            "required": ["ip"]
        }
    },
    {
        "name": "ping_health_probe",
        "description": "Edge hardware telemetry, load averages, uptime, and sub-millisecond heartbeat verification.",
        "inputSchema": {
            "type": "object",
            "properties": {}
        }
    }
]

# ─────────────────────────────────────────────────────────────────────────────
# 6. FLEET: Executive Cockpit & Email Triage (Pillar I)
# ─────────────────────────────────────────────────────────────────────────────

FLEET_EXECUTIVE: List[Dict[str, Any]] = [
    {
        "name": "get_email_briefing",
        "description": "Primary email intelligence tool. Reads pre-triaged email dossiers from the local ledger and returns a concise factual digest.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {"type": "string", "description": "Account: 'amber' or 'dalnaspidal'.", "enum": ["amber", "dalnaspidal"], "default": "amber"},
                "limit": {"type": "integer", "description": "Number of recent emails to summarize (default: 5).", "default": 5},
                "action_only": {"type": "boolean", "description": "Return only emails requiring action.", "default": False},
                "all_recent": {"type": "boolean", "description": "Return all emails from today.", "default": False}
            }
        }
    },
    {
        "name": "get_email_detail",
        "description": "Drills down into the full dossier of a specific email by UID or keyword from the local ledger.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {"type": "string", "description": "Email UID (e.g. '1320') or keyword."},
                "account": {"type": "string", "description": "Account: 'amber' or 'dalnaspidal'.", "enum": ["amber", "dalnaspidal"], "default": "amber"}
            },
            "required": ["query"]
        }
    },
    {
        "name": "sweep_email",
        "description": "Single-item FIFO inbox sweeper. Fetches next unprocessed email, evaluates in pristine context, logs to ledger, and trashes spam.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {"type": "string", "description": "Account: 'amber' or 'dalnaspidal'.", "enum": ["amber", "dalnaspidal"], "default": "amber"},
                "auto_trash": {"type": "boolean", "description": "Move marketing/spam to Trash.", "default": True}
            }
        }
    },
    {
        "name": "trash_email",
        "description": "Safely moves one or multiple emails to [Gmail]/Trash by their message ID/UID in a single call.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "message_ids": {"type": "string", "description": "Comma-separated email IDs (e.g. '1326, 1325')."},
                "account": {"type": "string", "description": "Account: 'amber' or 'dalnaspidal'.", "enum": ["amber", "dalnaspidal"], "default": "amber"}
            },
            "required": ["message_ids"]
        }
    },
    {
        "name": "triage_inbox",
        "description": "Executive email & inbox triage tool. Fetches recent emails from live IMAP, sanitizes HTML/PII, and categorizes into Action, Commercial, and Routine tiers.",
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {"type": "string", "description": "Account: 'amber' or 'dalnaspidal'.", "enum": ["amber", "dalnaspidal"], "default": "amber"},
                "limit": {"type": "integer", "description": "Number of recent emails to fetch (default: 5).", "default": 5}
            }
        }
    }
]

# Fleet Catalog Map
FLEETS: Dict[str, List[Dict[str, Any]]] = {
    "finance": FLEET_FINANCE,
    "syntax": FLEET_SYNTAX,
    "distill": FLEET_DISTILL,
    "verify": FLEET_VERIFY,
    "infra": FLEET_INFRA,
    "executive": FLEET_EXECUTIVE
}

# Legacy default suite (backward-compatible)
MCP_TOOLS: List[Dict[str, Any]] = [
    FLEET_EXECUTIVE[4],  # triage_inbox
    FLEET_EXECUTIVE[3],  # trash_email
    FLEET_EXECUTIVE[2],  # sweep_email
    FLEET_EXECUTIVE[0],  # get_email_briefing
    FLEET_EXECUTIVE[1],  # get_email_detail
    FLEET_SYNTAX[0],     # repair_json
    FLEET_DISTILL[0],    # extract_markdown
    FLEET_VERIFY[0],     # sanitize_pii
    FLEET_DISTILL[1],    # distill_context
    FLEET_VERIFY[1],     # verify_email
    FLEET_INFRA[3]       # lookup_ip
]


async def execute_tool_call(name: str, arguments: Dict[str, Any]) -> str:
    """Executes an MCP tool and returns the text result."""
    if name == "triage_inbox":
        account = arguments.get("account", "amber")
        limit = int(arguments.get("limit", 5))
        # Use fast deterministic triage (<1s) so chatbot Qwen only generates once
        triage_res = await triage_service.run_email_triage(limit=limit, account=account, use_llm_summary=False)
        summary = triage_res.get("summary", "No emails found.")
        source = triage_res.get("source", "UNKNOWN")
        count = triage_res.get("count", 0)
        return (
            f"**OmniVenture Executive Email Triage**\n"
            f"• Source: {source}\n"
            f"• Processed: {count} emails (Fetched in {triage_res.get('latency_ms', 0)}ms)\n\n"
            f"{summary}"
        )

    elif name == "trash_email":
        account = arguments.get("account", "amber")
        ids = arguments.get("message_ids") or arguments.get("message_id")
        if not ids:
            return "Error: message_ids is required."
        trash_res = triage_service.trash_email(message_ids=ids, account=account)
        if trash_res.get("status") == "SUCCESS":
            trashed_items = trash_res.get("trashed", [])
            lines = [f"✅ Successfully moved {trash_res.get('trashed_count', 0)} email(s) to Trash:"]
            for item in trashed_items:
                lines.append(f"• [ID: {item.get('id')}] {item.get('sender', 'Unknown')} — {item.get('subject', 'No Subject')}")
            lines.append("• Destination: [Gmail]/Trash (recoverable for 30 days)")
            return "\n".join(lines)
        elif trash_res.get("status") == "SIMULATED_SUCCESS":
            return f"ℹ️ [Sandbox Simulation] Trashed {trash_res.get('count', 0)} email(s): {trash_res.get('message_ids')}"
        else:
            return f"❌ Failed to trash email(s): {trash_res.get('error', 'Unknown error')}"

    elif name == "sweep_email":
        account = arguments.get("account", "amber")
        auto_trash = bool(arguments.get("auto_trash", True))
        from executive.email_sweeper import email_sweeper
        res = email_sweeper.sweep_single_email(account=account, auto_trash=auto_trash)
        if not res:
            return f"Inbox for {account} is completely clean! No new unprocessed emails found."
        return (
            f"🧹 **Email Swept & Triaged (1-by-1 FIFO)**\n"
            f"• UID: {res['uid']} ({res['account']})\n"
            f"• From: {res['sender']}\n"
            f"• Subject: {res['subject']}\n"
            f"• Category: {res['category']} (Evaluated by {res['engine']})\n"
            f"• Summary: {res['summary']}\n"
            f"• Action: {res['action_taken']}\n"
            f"• Daily Ledger: `data/email_ledger/{datetime.now().strftime('%Y-%m-%d')}.md`"
        )

    elif name == "get_email_briefing":
        account = arguments.get("account", "amber")
        all_recent = bool(arguments.get("all_recent", False))
        action_only = bool(arguments.get("action_only", False))
        limit_val = arguments.get("limit")
        limit = None
        if limit_val is not None:
            try:
                limit = int(limit_val)
            except Exception:
                limit = 5
        from executive.email_sweeper import email_sweeper
        return email_sweeper.get_ledger_briefing(account=account, all_recent=all_recent, limit=limit, action_only=action_only)

    elif name == "get_email_detail":
        query = str(arguments.get("query", "")).strip()
        account = arguments.get("account", "amber")
        from executive.email_sweeper import email_sweeper
        return email_sweeper.get_email_detail(query=query, account=account)

    # --- Fleet 1: Finance ---
    elif name == "get_market_quote":
        res = m2m_forge._exec_market_feed(arguments)
        return json.dumps(res, indent=2)

    elif name == "scan_market_delta":
        res = m2m_forge._exec_delta_scan(arguments)
        return json.dumps(res, indent=2)

    elif name == "get_crypto_ticker":
        res = m2m_forge._exec_crypto_ticker(arguments)
        return json.dumps(res, indent=2)

    elif name == "estimate_gas_priority":
        res = m2m_forge._exec_gas_priority(arguments)
        return json.dumps(res, indent=2)

    elif name == "check_market_volatility":
        res = m2m_forge._exec_market_volatility(arguments)
        return json.dumps(res, indent=2)

    # --- Fleet 2: Syntax ---
    elif name == "repair_json":
        res = m2m_forge._exec_repair_json(arguments)
        return json.dumps(res, indent=2)

    elif name == "unwrap_markdown":
        res = m2m_forge._exec_unwrap_markdown(arguments)
        return json.dumps(res, indent=2)

    elif name == "validate_json_schema":
        res = m2m_forge._exec_validate_schema(arguments)
        return json.dumps(res, indent=2)

    elif name == "repair_csv":
        res = m2m_forge._exec_repair_csv(arguments)
        return json.dumps(res, indent=2)

    elif name == "diff_ast_keys":
        res = m2m_forge._exec_diff_ast_keys(arguments)
        return json.dumps(res, indent=2)

    # --- Fleet 3: Distill ---
    elif name == "extract_markdown":
        res = m2m_forge._exec_extract_markdown(arguments)
        return json.dumps(res, indent=2)

    elif name == "distill_context":
        res = m2m_forge._exec_distill(arguments)
        return json.dumps(res, indent=2)

    elif name == "extract_tables":
        res = m2m_forge._exec_extract_tables(arguments)
        return json.dumps(res, indent=2)

    elif name == "strip_boilerplate":
        res = m2m_forge._exec_strip_boilerplate(arguments)
        return json.dumps(res, indent=2)

    elif name == "score_readability":
        res = m2m_forge._exec_score_readability(arguments)
        return json.dumps(res, indent=2)

    # --- Fleet 4: Verify ---
    elif name == "sanitize_pii":
        res = m2m_forge._exec_sanitize_pii(arguments)
        return json.dumps(res, indent=2)

    elif name == "verify_email":
        res = m2m_forge._exec_verify_email(arguments)
        return json.dumps(res, indent=2)

    elif name == "validate_hash":
        res = m2m_forge._exec_validate(arguments)
        return json.dumps(res, indent=2)

    elif name == "scan_prompt_injection":
        res = m2m_forge._exec_scan_injection(arguments)
        return json.dumps(res, indent=2)

    elif name == "detect_entropy":
        res = m2m_forge._exec_detect_entropy(arguments)
        return json.dumps(res, indent=2)

    # --- Fleet 5: Infra ---
    elif name == "mint_bolt11_invoice":
        res = m2m_forge._exec_bot_invoice(arguments)
        return json.dumps(res, indent=2)

    elif name == "verify_payment_hash":
        res = m2m_forge._exec_verify_payment(arguments)
        return json.dumps(res, indent=2)

    elif name == "relay_signed_webhook":
        res = m2m_forge._exec_bot_webhook(arguments)
        return json.dumps(res, indent=2)

    elif name == "lookup_ip":
        res = m2m_forge._exec_lookup_ip(arguments)
        return json.dumps(res, indent=2)

    elif name == "ping_health_probe":
        res = m2m_forge._exec_health_probe(arguments)
        return json.dumps(res, indent=2)

    return f"Tool '{name}' executed successfully with arguments: {arguments}"


async def handle_jsonrpc_request(req: Dict[str, Any], fleet: Optional[str] = None) -> Optional[Dict[str, Any]]:
    """Handles an incoming JSON-RPC 2.0 MCP request scoped to an optional fleet."""
    req_id = req.get("id")
    method = req.get("method", "")
    params = req.get("params", {})

    # Select appropriate tool catalog
    if fleet and fleet.lower() in FLEETS:
        fleet_tools = FLEETS[fleet.lower()]
        server_name = f"OmniVenture-Fleet-{fleet.capitalize()}"
        instructions = f"OmniVenture OS 2.0 {fleet.capitalize()} Fleet. High-speed Simian edge utilities for autonomous agents."
    else:
        fleet_tools = MCP_TOOLS
        server_name = "OmniVenture-Agent-Tools"
        instructions = (
            "OmniVenture OS Executive Agent Suite. "
            "CRITICAL AGENT RULES: "
            "1. When the user asks for email updates or summaries, ALWAYS invoke 'get_email_briefing'. "
            "2. Speak conversationally as Amber's Chief of Staff. Deliver a 2-sentence executive summary. "
            "3. If asked to delete/trash emails, execute 'trash_email' directly with all target IDs in ONE call. "
            "4. Keep responses under 70 words."
        )

    if method == "initialize":
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "protocolVersion": "2024-11-05",
                "capabilities": {
                    "tools": {"listChanged": False},
                    "prompts": {"listChanged": False},
                    "resources": {"listChanged": False}
                },
                "serverInfo": {
                    "name": server_name,
                    "version": "2.0.0"
                },
                "instructions": instructions
            }
        }

    elif method == "notifications/initialized":
        return None

    elif method == "ping":
        return {"jsonrpc": "2.0", "id": req_id, "result": {}}

    elif method == "tools/list":
        # Support compact format for edge models with bounded context
        compact_mode = params.get("format") == "compact" or params.get("compact") is True
        if compact_mode:
            from core.mcp_distiller import COMPACT_SIGNATURES
            compact_tools = [{"name": k, "signature": v} for k, v in COMPACT_SIGNATURES.items() if any(t["name"] == k for t in fleet_tools)]
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": compact_tools, "format": "compact"}
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": fleet_tools
            }
        }

    elif method in ["tools/decoupled_call", "chat/decoupled"]:
        user_query = params.get("query", "")
        account = params.get("account", "amber")
        from core.mcp_distiller import mcp_distiller
        decoupled_res = await mcp_distiller.execute_two_pass(user_query=user_query, account=account)
        if decoupled_res:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": decoupled_res
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {"status": "NO_TOOL_MATCHED", "query": user_query}
        }

    elif method == "prompts/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"prompts": []}}

    elif method == "resources/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"resources": []}}

    elif method == "resources/templates/list":
        return {"jsonrpc": "2.0", "id": req_id, "result": {"resourceTemplates": []}}

    elif method == "tools/call":
        tool_name = params.get("name", "")
        tool_args = params.get("arguments", {})
        try:
            result_text = await execute_tool_call(tool_name, tool_args)
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": result_text}],
                    "isError": False
                }
            }
        except Exception as e:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {
                    "content": [{"type": "text", "text": f"Error executing tool '{tool_name}': {str(e)}"}],
                    "isError": True
                }
            }

    else:
        if req_id is not None:
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "error": {
                    "code": -32601,
                    "message": f"Method '{method}' not implemented"
                }
            }
        return None


# ─────────────────────────────────────────────────────────────────────────────
# Fleet Discovery & Catalog Endpoints
# ─────────────────────────────────────────────────────────────────────────────

@mcp_router.get("/fleets")
async def list_mcp_fleets():
    """Lists available 5x5 modular tool fleets and their capabilities."""
    summary = {}
    for name, tools in FLEETS.items():
        summary[name] = {
            "name": f"OmniVenture {name.capitalize()} Fleet",
            "tool_count": len(tools),
            "sse_endpoint": f"/mcp/{name}/sse",
            "http_endpoint": f"/mcp/{name}",
            "tools": [{"name": t["name"], "description": t["description"]} for t in tools]
        }
    return JSONResponse(status_code=200, content={
        "status": "ONLINE",
        "fleets_count": len(FLEETS),
        "total_tools": sum(len(t) for t in FLEETS.values()),
        "fleets": summary
    })


# ─────────────────────────────────────────────────────────────────────────────
# Legacy Default Transports (Backward Compatibility)
# ─────────────────────────────────────────────────────────────────────────────

@mcp_router.get("/sse")
async def mcp_legacy_sse_endpoint(request: Request):
    """Legacy default SSE endpoint (maps to global/executive suite)."""
    session_id = uuid.uuid4().hex
    queue = asyncio.Queue()
    active_sse_sessions[session_id] = queue

    base = str(request.base_url).rstrip("/")
    endpoint_url = f"{base}/mcp/messages?sessionId={session_id}"
    await queue.put(f"event: endpoint\r\ndata: {endpoint_url}\r\n\r\n")

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield payload
                except asyncio.TimeoutError:
                    yield ": keepalive\r\n\r\n"
        finally:
            active_sse_sessions.pop(session_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        }
    )


@mcp_router.post("/messages")
async def mcp_legacy_messages_endpoint(request: Request, sessionId: Optional[str] = None):
    """Legacy default messages endpoint."""
    session_id = sessionId or request.query_params.get("sessionId")
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body, fleet=None)
    if resp is None:
        return Response(status_code=202)

    if session_id and session_id in active_sse_sessions:
        sse_line = f"event: message\r\ndata: {json.dumps(resp)}\r\n\r\n"
        await active_sse_sessions[session_id].put(sse_line)
        return Response(status_code=202)
    else:
        return JSONResponse(status_code=200, content=resp)


@mcp_router.post("")
@mcp_router.post("/")
async def mcp_legacy_streamable_http_endpoint(request: Request):
    """Legacy default streamable HTTP endpoint."""
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body, fleet=None)
    if resp is None:
        return Response(status_code=202)
    return JSONResponse(status_code=200, content=resp)


@mcp_router.post("/decoupled")
async def mcp_decoupled_endpoint(request: Request):
    """Executes a user query via decoupled two-pass MCP distillation."""
    try:
        body = await request.json()
    except Exception:
        return JSONResponse(status_code=400, content={"error": "Invalid JSON body"})
    query = body.get("query", "")
    account = body.get("account", "amber")
    from core.mcp_distiller import mcp_distiller
    res = await mcp_distiller.execute_two_pass(user_query=query, account=account)
    if not res:
        return JSONResponse(status_code=200, content={"status": "FALLBACK_REQUIRED", "query": query})
    return JSONResponse(status_code=200, content=res)


@mcp_router.websocket("/ws")
async def mcp_websocket_endpoint(websocket: WebSocket):
    """Full-duplex WebSocket MCP transport."""
    await websocket.accept()
    try:
        while True:
            text = await websocket.receive_text()
            try:
                data = json.loads(text)
            except json.JSONDecodeError:
                continue

            resp = await handle_jsonrpc_request(data, fleet=None)
            if resp is not None:
                await websocket.send_text(json.dumps(resp))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"MCP WebSocket exception: {e}")


# ─────────────────────────────────────────────────────────────────────────────
# Modular Fleet Transports (SSE & HTTP)
# ─────────────────────────────────────────────────────────────────────────────

@mcp_router.get("/{fleet}/tools")
async def get_fleet_tools(fleet: str):
    """Returns the JSONSchema tools catalog for a specific fleet."""
    fleet_key = fleet.lower()
    if fleet_key not in FLEETS:
        return JSONResponse(status_code=404, content={"error": f"Unknown fleet '{fleet}'. Available: {list(FLEETS.keys())}"})
    return JSONResponse(status_code=200, content={"fleet": fleet_key, "tools": FLEETS[fleet_key]})


@mcp_router.get("/{fleet}/sse")
async def mcp_fleet_sse_endpoint(fleet: str, request: Request):
    """
    Dedicated SSE transport for a specific 5x5 fleet (e.g. /mcp/finance/sse).
    Limits tool schemas strictly to this fleet to prevent context rot.
    """
    fleet_key = fleet.lower()
    if fleet_key not in FLEETS:
        return JSONResponse(status_code=404, content={"error": f"Unknown fleet '{fleet}'. Available: {list(FLEETS.keys())}"})

    session_id = uuid.uuid4().hex
    queue = asyncio.Queue()
    active_sse_sessions[session_id] = queue

    base = str(request.base_url).rstrip("/")
    endpoint_url = f"{base}/mcp/{fleet_key}/messages?sessionId={session_id}"
    await queue.put(f"event: endpoint\r\ndata: {endpoint_url}\r\n\r\n")

    async def event_generator():
        try:
            while True:
                if await request.is_disconnected():
                    break
                try:
                    payload = await asyncio.wait_for(queue.get(), timeout=15.0)
                    yield payload
                except asyncio.TimeoutError:
                    yield ": keepalive\r\n\r\n"
        finally:
            active_sse_sessions.pop(session_id, None)

    return StreamingResponse(
        event_generator(),
        media_type="text/event-stream",
        headers={
            "Cache-Control": "no-cache",
            "Connection": "keep-alive",
            "X-Accel-Buffering": "no",
            "Access-Control-Allow-Origin": "*",
        }
    )


@mcp_router.post("/{fleet}/messages")
async def mcp_fleet_messages_endpoint(fleet: str, request: Request, sessionId: Optional[str] = None):
    """Receives JSON-RPC messages from fleet SSE clients."""
    fleet_key = fleet.lower()
    if fleet_key not in FLEETS:
        return JSONResponse(status_code=404, content={"error": f"Unknown fleet '{fleet}'"})

    session_id = sessionId or request.query_params.get("sessionId")
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body, fleet=fleet_key)
    if resp is None:
        return Response(status_code=202)

    if session_id and session_id in active_sse_sessions:
        sse_line = f"event: message\r\ndata: {json.dumps(resp)}\r\n\r\n"
        await active_sse_sessions[session_id].put(sse_line)
        return Response(status_code=202)
    else:
        return JSONResponse(status_code=200, content=resp)


@mcp_router.post("/{fleet}")
async def mcp_fleet_streamable_http_endpoint(fleet: str, request: Request):
    """Direct HTTP POST JSON-RPC endpoint scoped to a specific fleet."""
    fleet_key = fleet.lower()
    if fleet_key not in FLEETS:
        return JSONResponse(status_code=404, content={"error": f"Unknown fleet '{fleet}'"})

    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body, fleet=fleet_key)
    if resp is None:
        return Response(status_code=202)
    return JSONResponse(status_code=200, content=resp)
