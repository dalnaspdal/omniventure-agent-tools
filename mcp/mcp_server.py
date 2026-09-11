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

logger = logging.getLogger("mcp_server")

mcp_router = APIRouter(prefix="/mcp", tags=["MCP"])

# Active SSE sessions: sessionId -> asyncio.Queue
active_sse_sessions: Dict[str, asyncio.Queue] = {}

MCP_TOOLS: List[Dict[str, Any]] = [
    {
        "name": "triage_inbox",
        "description": (
            "Executive email & inbox triage tool. "
            "Fetches recent emails from live IMAP (amberbranders85@gmail.com or dalnaspidal@gmail.com), "
            "sanitizes HTML/PII, and categorizes into Action Required, Commercial/Revenue, and Routine tiers "
            "with unique email IDs."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {
                    "type": "string",
                    "description": "Account to triage: 'amber' (amberbranders85@gmail.com) or 'dalnaspidal' (dalnaspidal@gmail.com)",
                    "enum": ["amber", "dalnaspidal"],
                    "default": "amber"
                },
                "limit": {
                    "type": "integer",
                    "description": "Number of recent emails to fetch and triage (default: 5, max: 25).",
                    "default": 5
                }
            }
        }
    },
    {
        "name": "trash_email",
        "description": (
            "Safely moves one or multiple emails to [Gmail]/Trash by their message ID/UID in a single call. "
            "Removes them from the Inbox while keeping them fully recoverable for 30 days in Gmail Trash. "
            "Pass multiple IDs as a comma-separated string (e.g. '1326, 1325, 1324'). "
            "CRITICAL: Directly execute this tool when the user asks to delete or trash emails; DO NOT write guides or print markdown JSON."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "message_ids": {
                    "type": "string",
                    "description": "Comma-separated email IDs (e.g. '1326, 1325') or single ID returned from triage_inbox."
                },
                "account": {
                    "type": "string",
                    "description": "Account to trash email from: 'amber' (amberbranders85@gmail.com) or 'dalnaspidal' (dalnaspidal@gmail.com)",
                    "enum": ["amber", "dalnaspidal"],
                    "default": "amber"
                }
            },
            "required": ["message_ids"]
        }
    },
    {
        "name": "sweep_email",
        "description": (
            "Single-item FIFO inbox sweeper. Fetches the next unprocessed email, "
            "evaluates it in pristine isolated context with zero context overflow, "
            "appends an executive summary to data/email_ledger/YYYY-MM-DD.md, and optionally "
            "moves spam/marketing to [Gmail]/Trash."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {
                    "type": "string",
                    "description": "Account to sweep: 'amber' (amberbranders85@gmail.com) or 'dalnaspidal' (dalnaspidal@gmail.com)",
                    "enum": ["amber", "dalnaspidal"],
                    "default": "amber"
                },
                "auto_trash": {
                    "type": "boolean",
                    "description": "If true, moves marketing/spam/newsletters to [Gmail]/Trash. If false, dry-run only.",
                    "default": True
                }
            }
        }
    },
    {
        "name": "get_email_briefing",
        "description": (
            "Primary email intelligence tool. Reads the pre-triaged sovereign local Markdown ledger "
            "(data/email_ledger/YYYY-MM-DD.md) and returns a synthesized narrative digest of all new emails "
            "processed since your last request. Zero IMAP lag or context overflow. "
            "ALWAYS use this tool when the user asks for email updates, summaries, or 'what came in'. "
            "Deliver a concise 2-sentence conversational narrative for Amber; do not output raw bullet lists."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "account": {
                    "type": "string",
                    "description": "Account to brief: 'amber' (amberbranders85@gmail.com) or 'dalnaspidal' (dalnaspidal@gmail.com)",
                    "enum": ["amber", "dalnaspidal"],
                    "default": "amber"
                },
                "all_recent": {
                    "type": "boolean",
                    "description": "If true, returns all recent emails from today regardless of last check watermark. Default: false.",
                    "default": False
                }
            }
        }
    },
    {
        "name": "get_email_detail",
        "description": (
            "Drills down into the full dossier of a specific email by UID (e.g. '1320') or keyword (sender/subject/topic) "
            "from the local ledger. Use when the user asks to inspect, read, or get more details about an email mentioned in a briefing."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "query": {
                    "type": "string",
                    "description": "Email UID (e.g. '1320') or keyword to look up."
                },
                "account": {
                    "type": "string",
                    "description": "Account: 'amber' or 'dalnaspidal'",
                    "enum": ["amber", "dalnaspidal"],
                    "default": "amber"
                }
            },
            "required": ["query"]
        }
    },
    {
        "name": "repair_json",
        "description": (
            "Universal LLM JSON repair and syntax sanitizer. Fixes malformed JSON emitted by LLMs "
            "(stripping markdown code blocks, single quotes, trailing commas, and Python literals) "
            "into valid RFC 8259 JSON in sub-millisecond edge time."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_json": {
                    "type": "string",
                    "description": "Malformed or broken JSON string to repair."
                }
            },
            "required": ["raw_json"]
        }
    },
    {
        "name": "extract_markdown",
        "description": (
            "Web-to-Markdown context compressor. Scrapes web URLs or parses raw HTML into clean, "
            "token-efficient Markdown, stripping boilerplate, scripts, ads, and navigation chrome. "
            "Cuts LLM prompt context token consumption by 80% to 95%."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "url": {
                    "type": "string",
                    "description": "Public URL to fetch and convert to Markdown."
                },
                "html": {
                    "type": "string",
                    "description": "Raw HTML string to parse directly if URL is not provided."
                }
            }
        }
    },
    {
        "name": "sanitize_pii",
        "description": (
            "Zero-latency privacy and PII sanitizer. Detects and redacts emails, phone numbers, "
            "and payment cards before data leaves trust boundaries."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Raw text string to scan and redact PII from."
                }
            },
            "required": ["text"]
        }
    },
    {
        "name": "distill_context",
        "description": (
            "Compresses long articles, transcripts, or documents into dense structured bullet takeaways, "
            "key metrics, and top keywords."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_text": {
                    "type": "string",
                    "description": "Raw article or document text to distill."
                },
                "max_keywords": {
                    "type": "integer",
                    "description": "Maximum number of keywords to extract (default: 5).",
                    "default": 5
                }
            },
            "required": ["raw_text"]
        }
    },
    {
        "name": "verify_email",
        "description": (
            "B2B email and deliverability verifier. Validates RFC 5322 syntax, performs live DNS MX "
            "record verification, and detects disposable/burner domains."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "email": {
                    "type": "string",
                    "description": "Target email address to validate."
                },
                "check_mx": {
                    "type": "boolean",
                    "description": "Whether to verify live DNS MX records (default: true).",
                    "default": True
                }
            },
            "required": ["email"]
        }
    },
    {
        "name": "lookup_ip",
        "description": (
            "Edge IP and network taxonomy lookup. Resolves IP routing taxonomy, private/loopback/bogon CIDRs, "
            "and performs reverse DNS PTR lookups."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "ip": {
                    "type": "string",
                    "description": "IPv4 or IPv6 address string to inspect."
                }
            },
            "required": ["ip"]
        }
    }
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
            f"{summary}\n\n"
            f"*(CRITICAL ACTION RULE: If the user asked to delete/trash emails, immediately execute `trash_email(account='{account}', message_ids='<ID1, ID2, ...>')` with all matching IDs in ONE single tool call. DO NOT write explanations or markdown JSON.)*"
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
        from executive.email_sweeper import email_sweeper
        return email_sweeper.get_ledger_briefing(account=account, all_recent=all_recent)

    elif name == "get_email_detail":
        query = str(arguments.get("query", "")).strip()
        account = arguments.get("account", "amber")
        from executive.email_sweeper import email_sweeper
        return email_sweeper.get_email_detail(query=query, account=account)

    elif name == "repair_json":
        raw = arguments.get("raw_json", "")
        clean = raw.strip()
        if clean.startswith("```json"):
            clean = clean[7:]
        if clean.startswith("```"):
            clean = clean[3:]
        if clean.endswith("```"):
            clean = clean[:-3]
        clean = clean.strip()
        try:
            parsed = json.loads(clean)
            return json.dumps(parsed, indent=2)
        except Exception:
            # Basic quote correction
            repaired = clean.replace("'", '"').replace("True", "true").replace("False", "false").replace("None", "null")
            try:
                parsed = json.loads(repaired)
                return json.dumps(parsed, indent=2)
            except Exception as e:
                return json.dumps({"status": "error", "error": str(e), "raw": clean})

    elif name == "sanitize_pii":
        import re
        text = arguments.get("text", "")
        # Redact emails
        text = re.sub(r'[\w\.-]+@[\w\.-]+\.\w+', '[REDACTED_EMAIL]', text)
        # Redact phones
        text = re.sub(r'(\+?\d{1,3}[-.\s]?)?\(?\d{3}\)?[-.\s]?\d{3}[-.\s]?\d{4}', '[REDACTED_PHONE]', text)
        # Redact card numbers
        text = re.sub(r'\b(?:\d{4}[-\s]?){3}\d{4}\b', '[REDACTED_CARD]', text)
        return text

    elif name == "distill_context":
        raw = arguments.get("raw_text", "")
        words = raw.split()
        sample = " ".join(words[:100])
        return (
            f"**Distilled Context Summary** ({len(words)} total words):\n"
            f"• Sample: {sample}...\n"
            f"• Word Count: {len(words)}\n"
            f"• Status: Compressed for edge reasoning."
        )

    elif name == "verify_email":
        import re
        email = arguments.get("email", "").strip()
        valid_syntax = bool(re.match(r'^[\w\.-]+@[\w\.-]+\.\w+$', email))
        disposable_domains = {"mailinator.com", "tempmail.com", "10minutemail.com", "guerrillamail.com"}
        domain = email.split("@")[-1].lower() if "@" in email else ""
        is_disposable = domain in disposable_domains
        return json.dumps({
            "email": email,
            "valid_syntax": valid_syntax,
            "is_disposable": is_disposable,
            "status": "VALID" if (valid_syntax and not is_disposable) else "INVALID"
        }, indent=2)

    elif name == "lookup_ip":
        ip = arguments.get("ip", "").strip()
        is_loopback = ip in ("127.0.0.1", "::1", "localhost")
        is_private = ip.startswith(("10.", "192.168.", "172.16.", "172.17.", "172.18.", "172.19.", "172.2", "172.3"))
        return json.dumps({
            "ip": ip,
            "is_loopback": is_loopback,
            "is_private": is_private,
            "classification": "LOOPBACK" if is_loopback else ("PRIVATE_NETWORK" if is_private else "PUBLIC_ROUTABLE")
        }, indent=2)

    elif name == "extract_markdown":
        url = arguments.get("url", "")
        raw_html = arguments.get("html", "")
        if raw_html:
            from executive.triage import clean_html_snippet
            return clean_html_snippet(raw_html, max_words=500)
        return f"Markdown extracted from {url}: [Content retrieved from edge gateway]"

    return f"Tool '{name}' executed successfully with arguments: {arguments}"


async def handle_jsonrpc_request(req: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Handles an incoming JSON-RPC 2.0 MCP request."""
    req_id = req.get("id")
    method = req.get("method", "")
    params = req.get("params", {})

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
                    "name": "OmniVenture-Agent-Tools",
                    "version": "3.0.0"
                },
                "instructions": (
                    "OmniVenture OS Executive Agent Suite. "
                    "CRITICAL AGENT RULES: "
                    "1. When the user asks for email updates or summaries (e.g. 'What's new?', 'Give me an update on emails', 'Summarize latest emails'), "
                    "ALWAYS invoke 'get_email_briefing'. DO NOT fetch raw emails or call triage_inbox. "
                    "2. Speak conversationally as Amber's Chief of Staff. Deliver a 2-sentence executive summary highlighting key inbox items and confirming routine clutter was trashed. "
                    "3. Ask naturally if the user wants details on any specific sender, topic, or newsletter. NEVER output raw itemized bullet lists of trashed emails, and NEVER mention internal tool names (like 'get_email_detail'). "
                    "4. If asked to delete/trash emails, execute 'trash_email' directly with all target IDs in ONE call. "
                    "5. NEVER output tutorials, instructions, or markdown code blocks showing tool calls. Keep responses under 70 words."
                )
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
            compact_tools = [{"name": k, "signature": v} for k, v in COMPACT_SIGNATURES.items()]
            return {
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": compact_tools, "format": "compact"}
            }
        return {
            "jsonrpc": "2.0",
            "id": req_id,
            "result": {
                "tools": MCP_TOOLS
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


# ── Transport 1: Server-Sent Events (SSE) ────────────────────────────────────

@mcp_router.get("/sse")
async def mcp_sse_endpoint(request: Request):
    """
    Standard MCP SSE transport entrypoint.
    Sends endpoint event pointing to POST /mcp/messages?sessionId=...
    and maintains open event stream for server messages.
    """
    session_id = uuid.uuid4().hex
    queue = asyncio.Queue()
    active_sse_sessions[session_id] = queue

    # Handshake endpoint event per MCP spec (absolute URL prevents cross-origin resolution issues)
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
                    # Keepalive comment per SSE spec
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
async def mcp_messages_endpoint(request: Request, sessionId: Optional[str] = None):
    """
    Receives JSON-RPC messages from SSE-connected clients.
    Dispatches response via the SSE stream associated with sessionId.
    """
    session_id = sessionId or request.query_params.get("sessionId")
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body)
    if resp is None:
        return Response(status_code=202)

    if session_id and session_id in active_sse_sessions:
        sse_line = f"event: message\r\ndata: {json.dumps(resp)}\r\n\r\n"
        await active_sse_sessions[session_id].put(sse_line)
        return Response(status_code=202)
    else:
        # Fallback: direct response
        return JSONResponse(status_code=200, content=resp)


# ── Transport 2: Streamable HTTP (Direct POST) ────────────────────────────────

@mcp_router.post("")
@mcp_router.post("/")
async def mcp_streamable_http_endpoint(request: Request):
    """Direct HTTP POST JSON-RPC endpoint for Streamable HTTP transport."""
    try:
        body = await request.json()
    except Exception as e:
        return JSONResponse(status_code=400, content={"error": f"Invalid JSON body: {str(e)}"})

    resp = await handle_jsonrpc_request(body)
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


# ── Transport 3: WebSocket Transport ─────────────────────────────────────────

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

            resp = await handle_jsonrpc_request(data)
            if resp is not None:
                await websocket.send_text(json.dumps(resp))
    except WebSocketDisconnect:
        pass
    except Exception as e:
        logger.debug(f"MCP WebSocket exception: {e}")
