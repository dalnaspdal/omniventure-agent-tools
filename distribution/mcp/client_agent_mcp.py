#!/usr/bin/env python3
"""
OmniVenture Client-Facing Model Context Protocol (MCP) Server
(distribution/mcp/client_agent_mcp.py)

Exposes OmniVenture's sub-15ms edge intelligence tools over stdio for Claude Desktop,
Cursor, Zed, Antigravity, and autonomous agent swarms.

Tools:
1. extract_markdown: Scrapes URLs or parses HTML into dense Markdown, cutting 80-95% prompt tokens.
2. repair_json: Instantly fixes broken LLM JSON (code fences, single quotes, trailing commas) in <1ms.
3. sanitize_pii: Redacts emails, phone numbers, and payment cards before data leaves trust boundaries.
4. distill_context: Compresses long text into structured entities, numbers, and top keywords.
5. verify_email: Validates RFC syntax, live DNS MX records, and detects burner/disposable inboxes.
6. lookup_ip: Edge network taxonomy, loopback/private CIDR detection, and reverse PTR lookups.
"""

import sys
import json
import os
import urllib.request
import urllib.error
from typing import Dict, Any, Optional

DEFAULT_GATEWAY_URL = os.environ.get(
    "OMNIVENTURE_GATEWAY_URL",
    "https://automatically-welcome-dad-extensive.trycloudflare.com"
).rstrip("/")

RAPIDAPI_KEY = os.environ.get("RAPIDAPI_KEY", "")
RAPIDAPI_HOST = os.environ.get("RAPIDAPI_HOST", "omniventure-m2m-micro-utilities.p.rapidapi.com")
PAYMENT_TOKEN = os.environ.get("OMNIVENTURE_TOKEN", "")
PROXY_SECRET = os.environ.get("X_RAPIDAPI_PROXY_SECRET", "")

TOOLS = [
    {
        "name": "repair_json",
        "description": (
            "Universal LLM JSON repair and syntax sanitizer. Fixes malformed JSON emitted by LLMs "
            "(stripping markdown code blocks, single quotes, trailing commas, and Python literals like True/False/None) "
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
                    "description": "Public web URL to fetch and compress into Markdown."
                },
                "raw_html": {
                    "type": "string",
                    "description": "Optional raw HTML string to convert if URL is not provided."
                },
                "include_links": {
                    "type": "boolean",
                    "description": "Whether to retain hyperlinks in [text](url) format (default: false).",
                    "default": False
                }
            }
        }
    },
    {
        "name": "sanitize_pii",
        "description": (
            "Text privacy, toxicity & PII redactor. Masks Personally Identifiable Information "
            "(emails, phone numbers, payment credit card numbers, and SSNs) from text before sending "
            "data to external public LLMs. Enforces GDPR, HIPAA, and DLP boundaries."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "text": {
                    "type": "string",
                    "description": "Sensitive input text string to sanitize."
                }
            },
            "required": ["text"]
        }
    },
    {
        "name": "distill_context",
        "description": (
            "Cognitive context distiller and entity extractor. Distills dense structured entities, "
            "numerical metrics, and salient keywords from raw text at zero cloud LLM token cost. "
            "Compresses 5,000+ words into ~150 structured tokens (>95% compression)."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "raw_text": {
                    "type": "string",
                    "description": "Raw article, transcript, or document text to distill."
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
            "record verification, detects disposable/burner domains (Mailinator, TempMail), and scores "
            "corporate deliverability reputation."
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
            "and performs reverse DNS PTR lookups in sub-5ms latency."
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
    },
    {
        "name": "triage_inbox",
        "description": (
            "Executive email & inbox triage tool powered by local edge Qwen 1.5B. "
            "Fetches recent emails, sanitizes HTML/PII, and categorizes into Action Required, "
            "Commercial/Revenue, and Routine tiers in sub-2s edge time."
        ),
        "inputSchema": {
            "type": "object",
            "properties": {
                "limit": {
                    "type": "integer",
                    "description": "Number of recent emails to fetch and triage (default: 10, max: 25).",
                    "default": 10
                }
            }
        }
    }
]


def _call_gateway(endpoint_path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    """Posts payload to OmniVenture live gateway and returns JSON response."""
    url = f"{DEFAULT_GATEWAY_URL}{endpoint_path}"
    headers = {"Content-Type": "application/json"}

    # Inject auth header and resolve gateway domain
    if RAPIDAPI_KEY:
        url = f"https://{RAPIDAPI_HOST}{endpoint_path}"
        headers["X-RapidAPI-Key"] = RAPIDAPI_KEY
        headers["X-RapidAPI-Host"] = RAPIDAPI_HOST
    elif PAYMENT_TOKEN:
        headers["Authorization"] = f"Bearer {PAYMENT_TOKEN}"
    elif PROXY_SECRET:
        headers["X-RapidAPI-Proxy-Secret"] = PROXY_SECRET
    else:
        # Fallback to guest authorization header for local testing
        headers["Authorization"] = "Bearer mcp_client_guest"

    req_data = json.dumps(payload).encode("utf-8")
    req = urllib.request.Request(url, data=req_data, headers=headers, method="POST")

    try:
        with urllib.request.urlopen(req, timeout=12.0) as resp:
            body = resp.read().decode("utf-8")
            return json.loads(body)
    except urllib.error.HTTPError as e:
        err_body = e.read().decode("utf-8")
        try:
            return json.loads(err_body)
        except Exception:
            return {"error": f"HTTP {e.code}: {e.reason}", "raw_response": err_body}
    except Exception as e:
        return {"error": f"Gateway request failed: {str(e)}", "target_url": url}


def execute_tool(tool_name: str, args: Dict[str, Any]) -> str:
    """Dispatches tool call to the corresponding OmniVenture endpoint."""
    endpoint_map = {
        "repair_json": "/v1/m2m/repair/json",
        "extract_markdown": "/v1/m2m/extract/markdown",
        "sanitize_pii": "/v1/m2m/sanitize/pii",
        "distill_context": "/v1/m2m/distill",
        "verify_email": "/v1/m2m/verify/email",
        "lookup_ip": "/v1/m2m/lookup/ip",
        "triage_inbox": "/api/email/triage"
    }


    path = endpoint_map.get(tool_name)
    if not path:
        return json.dumps({"error": f"Unknown tool: {tool_name}"})

    res = _call_gateway(path, args)
    return json.dumps(res, indent=2)


def send_response(response_dict: dict):
    """Writes a JSON-RPC response to stdout followed by newline."""
    line = json.dumps(response_dict)
    sys.stdout.write(line + "\n")
    sys.stdout.flush()


def run_stdio_server():
    """Main JSON-RPC stdio event loop."""
    for raw_line in sys.stdin:
        line = raw_line.strip()
        if not line:
            continue
        try:
            req = json.loads(line)
        except json.JSONDecodeError:
            continue

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
                        "name": "omniventure-agent-tools",
                        "version": "1.0.0"
                    }
                }
            })
        elif method == "notifications/initialized":
            pass
        elif method == "tools/list":
            send_response({
                "jsonrpc": "2.0",
                "id": req_id,
                "result": {"tools": TOOLS}
            })
        elif method == "tools/call":
            tool_name = params.get("name")
            tool_args = params.get("arguments", {})
            try:
                result_text = execute_tool(tool_name, tool_args)
                send_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": result_text}],
                        "isError": False
                    }
                })
            except Exception as e:
                send_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "result": {
                        "content": [{"type": "text", "text": f"Execution error in {tool_name}: {str(e)}"}],
                        "isError": True
                    }
                })
        else:
            if req_id is not None:
                send_response({
                    "jsonrpc": "2.0",
                    "id": req_id,
                    "error": {"code": -32601, "message": f"Method {method} not implemented"}
                })


if __name__ == "__main__":
    run_stdio_server()
