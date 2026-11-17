"""The only way this server reaches WHG: a read-only, allow-listed HTTP client.

Design rules (docs: place MCP plan):
- GET only, and only paths matching ALLOWED_PATHS. A tool can never reach /api/users/, admin,
  write endpoints or another host, whatever arguments a model supplies.
- Calls the PUBLIC /api anonymously (decision 2026-11-17: anonymous tier), so only public data is
  visible and WHG's own visibility rules (place#310) apply. NO token is ever sent: a server-wide
  token would hand every internet caller that account's visibility the day an API view honoured it.
- Identifies itself (User-Agent WHG-MCP/<version>).
- Responses are read with a byte cap (MAX_BYTES).
- No redirects are followed (a redirect off-host would defeat the allowlist).
"""
import re
from typing import Any, Optional

import httpx

from . import __version__

MAX_BYTES = 1_500_000

ALLOWED_PATHS = [
    re.compile(r"^/api/db/$"),
    re.compile(r"^/api/spatial/$"),
    re.compile(r"^/api/sources/$"),
    re.compile(r"^/api/attribution/$"),
    re.compile(r"^/api/place/\d+(?:-\d+)*/$"),
]


class WHGError(Exception):
    """A failure to report to the model in plain words (never a stack trace or a URL with a token)."""


class DailyBudget:
    """This server's own daily allowance of upstream calls (decision 2026-11-17: separate MCP budget),
    so agent traffic can never use up the web/reconciliation API quota. Resets at 00:00 UTC; in-memory
    (a restart gives a fresh allowance, which the per-IP nginx limits bound)."""

    def __init__(self, limit: int, clock=None):
        import time
        self.limit, self._clock, self._day, self.used = limit, clock or time.time, None, 0

    def take(self) -> bool:
        day = int(self._clock() // 86400)
        if day != self._day:
            self._day, self.used = day, 0
        if self.used >= self.limit:
            return False
        self.used += 1
        return True


class WHGClient:
    def __init__(self, base_url: str, timeout: float = 30.0,
                 transport: Optional[httpx.AsyncBaseTransport] = None, budget: Optional[DailyBudget] = None):
        self.budget = budget
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self.base_url, timeout=timeout, follow_redirects=False, transport=transport,
            headers={"User-Agent": f"WHG-MCP/{__version__}", "Accept": "application/json"})

    async def aclose(self):
        await self._http.aclose()

    async def get(self, path: str, params: Optional[dict] = None) -> Any:
        if not any(p.fullmatch(path) for p in ALLOWED_PATHS):
            raise WHGError(f"Path not permitted: {path}")
        if self.budget is not None and not self.budget.take():
            raise WHGError("This service's daily request allowance is used up; try again after 00:00 UTC.")
        clean = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        try:
            async with self._http.stream("GET", path, params=clean) as r:
                body = b""
                async for chunk in r.aiter_bytes():
                    body += chunk
                    if len(body) > MAX_BYTES:
                        raise WHGError("WHG's answer is too large; narrow the query or ask for fewer places.")
        except WHGError:
            raise
        except httpx.TimeoutException:
            raise WHGError("WHG did not answer in time; try again or narrow the query.")
        except Exception:        # httpx.HTTPError, InvalidURL, ...: never leak internals
            raise WHGError("Could not reach WHG.")
        if r.status_code == 429:
            raise WHGError("WHG's daily API quota is exhausted; try again later.")
        if r.status_code in (401, 403):
            raise WHGError("WHG refused the request (authentication or permission).")
        if r.status_code == 404:
            raise WHGError("Not found.")
        if 300 <= r.status_code < 400:
            raise WHGError("Unexpected redirect from WHG.")
        if r.status_code >= 400:
            try:
                import json as _json
                j = _json.loads(body)
                detail = j.get("error") or j.get("detail")
            except Exception:
                detail = None
            raise WHGError(f"WHG returned {r.status_code}" + (f": {detail}" if detail else "."))
        try:
            import json as _json
            return _json.loads(body)
        except ValueError:
            raise WHGError("WHG returned something that was not JSON.")
