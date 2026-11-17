# whg-mcp

Read-only MCP server over WHG's current API: `POST /reconcile` (name and spatial search),
`GET /entity/place:<id>/api` (full records), `GET /api/sources/` and `GET /api/attribution/`.
It never calls the legacy v3 endpoints (`/api/db/`, `/api/index/`, `/api/place/…`, `/api/spatial/`,
`/api/geojson/`), which are deprecated (2026-11-17).
Separate from the Django app: own container (Python 3.12, `mcp` SDK), own deps.

## Authentication

Callers of the MCP server are anonymous. The server itself authenticates upstream with its OWN
service token, because `/reconcile` and the Entity API require one:

- Set `WHG_API_TOKEN` in the environment. **Without it the server refuses to start.**
- The token must belong to a **dedicated, non-staff, non-superuser service account**, never a
  person's. Whatever that account may see, every internet caller of the MCP server can see, so it
  must see public data only. WHG's API does not expose an account's staff flag, so the server cannot
  check this itself: whoever creates the token must.
- The account's own WHG quota is the MCP's quota bucket (decision 3, 2026-11-17). Give it a daily
  limit that matches `WHG_MCP_DAILY_BUDGET`, since a profile at its daily limit gets 429s.
- The token is sent only as `Authorization: Bearer`, only to `WHG_BASE_URL`. Redirects are never
  followed. It is never logged, never put in an error message, and never accepted from a caller.

## Run

`pip install -e .[test] && WHG_API_TOKEN=... WHG_BASE_URL=https://whgazetteer.org python -m whg_mcp.server` (stdio),
or `WHG_MCP_TRANSPORT=streamable-http` for HTTP at `/`. Tests: `pytest` (no network; the token there is a dummy).

Guarantees (each pinned by a test):

- Exact (method, path) allowlist. The legacy endpoints are unreachable, and an attempt sends nothing.
- No redirects are followed.
- Every call carries an identifiable `WHG-MCP/<version>` User-Agent.
- Bearer token only, and it never appears in errors, logs or tool output.

Served at the root of `mcp.whgazetteer.org` (subdomain, decision 2026-11-17). Stateless JSON transport,
Host/Origin checks on, bbox <= 10 deg, radius <= 100 km, at most 10 ids per `get_place`, responses capped
at 1.5 MB. A place geometry over 20 kB is replaced by its type and bbox.

Deployment (compose project, nginx block with per-IP limits, token env file, rollback, logs): see `deploy/README.md`.

Not yet done: `place_at_date`, a field whitelist. Open decisions are listed in the project memory note.
