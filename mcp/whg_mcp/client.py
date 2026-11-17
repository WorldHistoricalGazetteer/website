"""The only way this server reaches WHG: a read-only, allow-listed HTTP client.

Design rules (docs: place MCP plan):
- Only the (method, path) pairs in ALLOWED: GET /api/sources/, GET /api/attribution/,
  GET /entity/place:<id>/api, and POST /reconcile with a JSON body (a read: reconciliation
  writes nothing). Each path is a fullmatch, so a tool can never reach the legacy v3
  endpoints (/api/db/, /api/place/, /api/spatial/, ... deprecated 2026-11-17), /api/users/,
  admin, write endpoints or another host, whatever arguments a model supplies.
- AUTH (SG 2026-11-17): callers of the MCP server stay anonymous, but the server itself
  authenticates upstream with its OWN service token (WHG_API_TOKEN), because the current API
  (/reconcile, the Entity API) requires one. The token belongs to a dedicated NON-STAFF service
  account, never a person's: its visibility is what every internet caller gets, so it must see
  only public data. It also gives the MCP its own WHG quota bucket. The token is sent only as
  ``Authorization: Bearer`` to the configured base URL (no redirects, relative allow-listed
  paths only), never logged, never put in an error message, and never taken from a caller.
- Identifies itself (User-Agent WHG-MCP/<version>).
- Responses are read with a byte cap (MAX_BYTES).
- No redirects are followed (a redirect off-host would defeat the allowlist and leak the token).
"""
import json
import re
from typing import Any, Optional

import httpx

from . import __version__

MAX_BYTES = 1_500_000

# A WHG place id as the Entity API takes it after ``place:``: a WHG database id, or a namespaced
# authority id as /reconcile returns them (candidate ``place:gn:745044`` -> ``gn:745044``).
PLACE_ID = r"(?:\d+|[a-z][a-z0-9_]{0,31}:[A-Za-z0-9._-]{1,64})"

ALLOWED = [
    ("GET", re.compile(r"/api/sources/")),
    ("GET", re.compile(r"/api/attribution/")),
    ("GET", re.compile(r"/entity/place:" + PLACE_ID + r"/api")),
    ("POST", re.compile(r"/reconcile")),
]


class WHGError(Exception):
    """A failure to report to the model in plain words (never a stack trace, a header or a token)."""


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


def permitted(method: str, path: str) -> bool:
    return any(m == method and p.fullmatch(path) for m, p in ALLOWED)


class WHGClient:
    def __init__(self, base_url: str, token: str, timeout: float = 30.0,
                 transport: Optional[httpx.AsyncBaseTransport] = None, budget: Optional[DailyBudget] = None):
        if not token or not token.strip():
            raise ValueError("A WHG service token is required.")
        self.budget = budget
        self._secret = token.strip()
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self.base_url, timeout=timeout, follow_redirects=False, transport=transport,
            headers={"User-Agent": f"WHG-MCP/{__version__}", "Accept": "application/json",
                     "Authorization": f"Bearer {token.strip()}"})

    def __repr__(self):        # never show the token, e.g. in a traceback's locals
        return f"WHGClient({self.base_url!r})"

    async def aclose(self):
        await self._http.aclose()

    async def get(self, path: str, params: Optional[dict] = None) -> Any:
        clean = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        return await self._send("GET", path, params=clean)

    async def post(self, path: str, body: dict) -> Any:
        return await self._send("POST", path, json_body=body)

    async def _send(self, method: str, path: str, params=None, json_body=None) -> Any:
        if not permitted(method, path):
            raise WHGError(f"Not permitted: {method} {path}")
        if self.budget is not None and not self.budget.take():
            raise WHGError("This service's daily request allowance is used up; try again after 00:00 UTC.")
        kwargs = {"params": params} if method == "GET" else {"json": json_body}
        try:
            async with self._http.stream(method, path, **kwargs) as r:
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
            raise WHGError("WHG's API quota for this service is exhausted; try again later.")
        if r.status_code in (401, 403):
            # Nothing from the upstream body: it could name the account or echo a header.
            raise WHGError("WHG rejected this service's token. The service operator needs to fix it; "
                           "please try again later.")
        if r.status_code == 404:
            raise WHGError("Not found.")
        if r.status_code == 451:
            raise WHGError("This place's source does not allow WHG to redistribute it.")
        if 300 <= r.status_code < 400:
            raise WHGError("Unexpected redirect from WHG.")
        if r.status_code >= 400:
            try:
                j = json.loads(body)
                detail = (j.get("error") or j.get("detail")) if isinstance(j, dict) else None
            except Exception:
                detail = None
            detail = str(detail).replace(self._secret, "[redacted]")[:200] if detail else None
            raise WHGError(f"WHG returned {r.status_code}" + (f": {detail}" if detail else "."))
        try:
            return json.loads(body)
        except ValueError:
            raise WHGError("WHG returned something that was not JSON.")
