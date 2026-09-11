# OmniVenture Edge Agent Tools (MCP & LangChain)

> **Sub-15ms edge intelligence tools for Claude Desktop, Cursor, LangChain, and autonomous AI agents.**
> Reduce LLM prompt context tokens by up to 90%, repair malformed JSON in <1ms, and enforce privacy boundaries on bare-metal dual Xeon edge infrastructure.
>
> 🌐 **Canonical Developer Hub & Agent Discovery**: [https://omniventure-api.web.app](https://omniventure-api.web.app)

---

## 🚀 Key Advantages for AI Agents

| Tool | Capability | Token / Latency Advantage |
| :--- | :--- | :--- |
| **`extract_markdown`** | Scrapes web URLs or parses raw HTML into dense, clean Markdown without boilerplate. | **Cuts context tokens by 80% to 95%** (e.g. converts 60KB HTML into ~800 clean tokens). Sub-15ms. |
| **`repair_json`** | Fixes malformed JSON emitted by LLMs (code fences, single quotes, trailing commas, Python literals). | **0.37ms edge execution**. Saves 100% tokens and 2–5s latency vs re-prompting. |
| **`sanitize_pii`** | Redacts emails, phone numbers, payment cards, and SSNs before sending data to external public LLMs. | Sub-5ms regex masking. Enforces GDPR, HIPAA, and DLP boundaries. |
| **`distill_context`** | Extracts dense structured entities, financial metrics, and top keywords from documents. | Compresses 5,000+ words into ~150 structured tokens (>95% compression). |
| **`verify_email`** | Validates RFC 5322 syntax, verifies live DNS MX records, and flags burner/temporary domains. | Sub-15ms direct DNS MX check. Zero third-party SaaS rate limits. |
| **`lookup_ip`** | Resolves IP routing taxonomy, private/loopback CIDRs, and reverse PTR hostnames. | Sub-5ms native socket resolution. |

---

## ⚡ 1-Click Install: Smithery.ai

Install automatically into Claude Desktop or Cursor with the Smithery CLI:

```bash
npx -y @smithery/cli install @dalnaspdal/omniventure-agent-tools --client claude
```
Or for Cursor:
```bash
npx -y @smithery/cli install @dalnaspdal/omniventure-agent-tools --client cursor
```

---

## ⚡ Quickstart: Claude Desktop (Manual Config)

Add this to your `claude_desktop_config.json`:

```json
{
  "mcpServers": {
    "omniventure-agent-tools": {
      "command": "python3",
      "args": ["-m", "distribution.mcp.fleet_runner", "--fleet", "all"],
      "env": {
        "OMNIVENTURE_GATEWAY_URL": "http://localhost:8950"
      }
    }
  }
}
```

---

## ⚡ Quickstart: Cursor IDE (Manual Config)

In Cursor: **Settings** → **Features** → **MCP Servers** → **Add New MCP Server**:
- **Name**: `omniventure-fleets`
- **Type**: `command`
- **Command**: `python3 -m distribution.mcp.fleet_runner --fleet all`

---

## ⚡ Quickstart: LangChain & Python

```python
from distribution.python_sdk.omniventure_tools import (
    create_markdown_tool,
    create_json_repair_tool,
    create_pii_sanitizer_tool
)

# 1. Instantiate drop-in LangChain tools
tools = [
    create_markdown_tool(api_key="YOUR_RAPIDAPI_KEY").as_langchain_tool(),
    create_json_repair_tool(api_key="YOUR_RAPIDAPI_KEY").as_langchain_tool(),
    create_pii_sanitizer_tool(api_key="YOUR_RAPIDAPI_KEY").as_langchain_tool()
]

# 2. Attach to your LangChain / CrewAI / smolagents agent
# agent = create_react_agent(llm, tools=tools)
```

---

## ⚡ Native RapidAPI Remote MCP Gateway

If you prefer not running local Python scripts, you can connect directly to RapidAPI's hosted MCP proxy:

```bash
npx mcp-remote https://mcp.rapidapi.com \
  --header "x-api-host: omniventure-m2m-micro-utilities.p.rapidapi.com" \
  --header "x-api-key: <YOUR_RAPIDAPI_KEY>"
```

---

## 💳 Settlement & Pricing

- Endpoints are priced at **£0.01 to £0.04 GBP per request**.
- Supported rails:
  - **Credit Card via RapidAPI**: Monthly metered billing settled to PayPal.
  - **HTTP 402 / L402 Lightning**: Direct programmatic preimage tokens for crypto-native agent swarms.

Marketplace Listing: [RapidAPI Hub - OmniVenture M2M Micro-Utilities](https://rapidapi.com/dalnaspidal/api/omniventure-m2m-micro-utilities)
