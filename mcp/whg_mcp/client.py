"""The only way this server reaches WHG: a read-only, allow-listed HTTP client.

Design rules (docs: place MCP plan):
- GET only, and only paths matching ALLOWED_PATHS. A tool can never reach /api/users/, admin,
  write endpoints or another host, whatever arguments a model supplies.
- Calls the PUBLIC /api through the normal front door, so WHG's own visibility rules (place#310),
  licence handling and quotas apply to this server exactly as to any other client.
- Identifies itself (User-Agent WHG-MCP/<version>) and forwards the caller's token if there is one.
- No redirects are followed (a redirect off-host would defeat the allowlist).
"""
import re
from typing import Any, Optional

import httpx

from . import __version__

ALLOWED_PATHS = [
    re.compile(r"^/api/db/$"),
    re.compile(r"^/api/spatial/$"),
    re.compile(r"^/api/sources/$"),
    re.compile(r"^/api/attribution/$"),
    re.compile(r"^/api/place/\d+(?:-\d+)*/$"),
]


class WHGError(Exception):
    """A failure to report to the model in plain words (never a stack trace or a URL with a token)."""


class WHGClient:
    def __init__(self, base_url: str, token: Optional[str] = None, timeout: float = 30.0,
                 transport: Optional[httpx.AsyncBaseTransport] = None):
        self.base_url = base_url.rstrip("/")
        self._http = httpx.AsyncClient(
            base_url=self.base_url, timeout=timeout, follow_redirects=False, transport=transport,
            headers={"User-Agent": f"WHG-MCP/{__version__}", "Accept": "application/json"})
        self._token = token

    async def aclose(self):
        await self._http.aclose()

    async def get(self, path: str, params: Optional[dict] = None, token: Optional[str] = None) -> Any:
        if not any(p.match(path) for p in ALLOWED_PATHS):
            raise WHGError(f"Path not permitted: {path}")
        headers = {}
        tok = token or self._token
        if tok:
            headers["Authorization"] = f"Bearer {tok}"
        clean = {k: v for k, v in (params or {}).items() if v not in (None, "")}
        try:
            r = await self._http.get(path, params=clean, headers=headers)
        except httpx.TimeoutException:
            raise WHGError("WHG did not answer in time; try again or narrow the query.")
        except httpx.HTTPError:
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
                detail = r.json().get("error") or r.json().get("detail")
            except Exception:
                detail = None
            raise WHGError(f"WHG returned {r.status_code}" + (f": {detail}" if detail else "."))
        try:
            return r.json()
        except ValueError:
            raise WHGError("WHG returned something that was not JSON.")
