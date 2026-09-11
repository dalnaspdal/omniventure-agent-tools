"""
OmniVenture Python SDK & Agent Harness Drop-in Tools
(distribution/python_sdk/omniventure_tools.py)

Turnkey Python SDK and LangChain / LlamaIndex / AutoGen tool wrappers
connecting directly to OmniVenture's sub-15ms edge intelligence gateway.
"""

import json
import urllib.request
import urllib.error
from typing import Dict, Any, Optional, Callable

DEFAULT_GATEWAY_URL = "https://automatically-welcome-dad-extensive.trycloudflare.com"
RAPIDAPI_HOST = "omniventure-m2m-micro-utilities.p.rapidapi.com"


class OmniVentureClient:
    """Lightweight HTTP client for OmniVenture M2M edge micro-services."""

    def __init__(
        self,
        api_key: Optional[str] = None,
        token: Optional[str] = None,
        proxy_secret: Optional[str] = None,
        gateway_url: str = DEFAULT_GATEWAY_URL,
        timeout: float = 12.0
    ):
        self.gateway_url = gateway_url.rstrip("/")
        self.api_key = api_key
        self.token = token
        self.proxy_secret = proxy_secret
        self.timeout = timeout

    def _post(self, path: str, payload: Dict[str, Any]) -> Dict[str, Any]:
        url = f"{self.gateway_url}{path}"
        headers = {"Content-Type": "application/json"}

        if self.api_key:
            headers["X-RapidAPI-Key"] = self.api_key
            headers["X-RapidAPI-Host"] = RAPIDAPI_HOST
        elif self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        elif self.proxy_secret:
            headers["X-RapidAPI-Proxy-Secret"] = self.proxy_secret
        else:
            headers["Authorization"] = "Bearer eval"
            headers["X-Evaluation-Trial"] = "true"

        req_data = json.dumps(payload).encode("utf-8")
        req = urllib.request.Request(url, data=req_data, headers=headers, method="POST")

        try:
            with urllib.request.urlopen(req, timeout=self.timeout) as resp:
                rem = resp.headers.get("X-Evaluation-Remaining")
                if rem is not None:
                    try:
                        if int(rem) <= 3:
                            import sys
                            print(f"[OmniVenture] Notice: Free daily evaluation quota low ({rem} calls remaining). Subscribe for unlimited edge calls at https://rapidapi.com/hub", file=sys.stderr)
                    except Exception:
                        pass
                body = resp.read().decode("utf-8")
                return json.loads(body)
        except urllib.error.HTTPError as e:
            raw_err = e.read().decode("utf-8")
            try:
                parsed = json.loads(raw_err)
                if e.code == 402:
                    import sys
                    detail = parsed.get("detail", {}) if isinstance(parsed.get("detail"), dict) else {}
                    msg = detail.get("message", "Payment Required")
                    checkout = detail.get("checkout_url", "https://rapidapi.com/hub")
                    print(f"[OmniVenture 402] {msg}", file=sys.stderr)
                    print(f"[OmniVenture] Subscribe: {checkout}", file=sys.stderr)
                return parsed
            except Exception:
                return {"error": f"HTTP {e.code}: {e.reason}", "raw": raw_err}
        except Exception as e:
            return {"error": f"Request failed: {str(e)}", "url": url}

    def repair_json(self, raw_json: str) -> Dict[str, Any]:
        """Repairs malformed LLM JSON in sub-millisecond edge time."""
        return self._post("/v1/m2m/repair/json", {"raw_json": raw_json, "broken_json": raw_json})

    def extract_markdown(
        self,
        url: Optional[str] = None,
        raw_html: Optional[str] = None,
        include_links: bool = False,
        max_tokens: int = 4000
    ) -> Dict[str, Any]:
        """Converts URLs or HTML into clean, token-compressed Markdown (-80-95% tokens)."""
        payload = {"include_links": include_links, "max_tokens": max_tokens}
        if url:
            payload["url"] = url
        if raw_html:
            payload["raw_html"] = raw_html
        return self._post("/v1/m2m/extract/markdown", payload)

    def sanitize_pii(self, text: str, mask_character: Optional[str] = None) -> Dict[str, Any]:
        """Redacts emails, phone numbers, and payment cards from text before external LLM calls."""
        payload = {"text": text}
        if mask_character:
            payload["mask_character"] = mask_character
        return self._post("/v1/m2m/sanitize/pii", payload)

    def distill(self, raw_text: str, max_keywords: int = 5) -> Dict[str, Any]:
        """Distills dense structured entities, numbers, and top keywords from text."""
        return self._post("/v1/m2m/distill", {"raw_text": raw_text, "max_keywords": max_keywords})

    def verify_email(self, email: str, check_mx: bool = True) -> Dict[str, Any]:
        """Validates RFC syntax, DNS MX records, and flags burner/temporary domains."""
        return self._post("/v1/m2m/verify/email", {"email": email, "check_mx": check_mx})

    def lookup_ip(self, ip: str) -> Dict[str, Any]:
        """Resolves IP routing taxonomy, private CIDRs, and reverse PTR lookups."""
        return self._post("/v1/m2m/lookup/ip", {"ip": ip})


# -------------------------------------------------------------
# Drop-in LangChain / LlamaIndex Tool Wrappers
# -------------------------------------------------------------

class OmniVentureAgentTool:
    """Universal tool wrapper conforming to LangChain & LlamaIndex interfaces."""

    def __init__(
        self,
        name: str,
        description: str,
        fn: Callable[..., Any],
        client: Optional[OmniVentureClient] = None
    ):
        self.name = name
        self.description = description
        self.fn = fn
        self.client = client or OmniVentureClient()

    def __call__(self, *args, **kwargs) -> Any:
        return self.fn(*args, **kwargs)

    def run(self, *args, **kwargs) -> str:
        res = self.fn(*args, **kwargs)
        return json.dumps(res, indent=2) if isinstance(res, (dict, list)) else str(res)

    def as_langchain_tool(self):
        """Converts to a native LangChain Tool instance if langchain is installed."""
        try:
            from langchain.tools import Tool
            return Tool(name=self.name, description=self.description, func=self.run)
        except ImportError:
            # Return duck-typed tool object compatible with LangChain agent loops
            return self


def create_markdown_tool(api_key: Optional[str] = None, gateway_url: str = DEFAULT_GATEWAY_URL) -> OmniVentureAgentTool:
    """Tool that scrapes URLs or HTML into clean markdown, saving up to 90% prompt tokens."""
    c = OmniVentureClient(api_key=api_key, gateway_url=gateway_url)
    return OmniVentureAgentTool(
        name="web_to_markdown_compressor",
        description="Scrapes a URL or parses HTML into dense, clean Markdown, eliminating navigation bars, scripts, and ads to cut LLM prompt tokens by 80-95%. Input: url (string) or raw_html (string).",
        fn=lambda url=None, raw_html=None: c.extract_markdown(url=url, raw_html=raw_html),
        client=c
    )


def create_json_repair_tool(api_key: Optional[str] = None, gateway_url: str = DEFAULT_GATEWAY_URL) -> OmniVentureAgentTool:
    """Tool that repairs broken LLM JSON in sub-millisecond edge time."""
    c = OmniVentureClient(api_key=api_key, gateway_url=gateway_url)
    return OmniVentureAgentTool(
        name="universal_json_repair",
        description="Repairs broken or malformed JSON generated by LLMs (code fences, single quotes, trailing commas, Python literals) into valid RFC 8259 JSON in <1ms without re-prompting. Input: raw_json (string).",
        fn=lambda raw_json: c.repair_json(raw_json),
        client=c
    )


def create_pii_sanitizer_tool(api_key: Optional[str] = None, gateway_url: str = DEFAULT_GATEWAY_URL) -> OmniVentureAgentTool:
    """Tool that masks sensitive PII before prompt ingestion."""
    c = OmniVentureClient(api_key=api_key, gateway_url=gateway_url)
    return OmniVentureAgentTool(
        name="pii_sanitizer",
        description="Redacts Personally Identifiable Information (emails, phone numbers, payment cards, SSNs) from text before sending prompts to external cloud LLMs. Input: text (string).",
        fn=lambda text: c.sanitize_pii(text),
        client=c
    )
