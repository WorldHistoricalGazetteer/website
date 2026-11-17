# whg-mcp

Read-only MCP server over the public WHG API (`/api/db`, `/api/place`, `/api/spatial`, `/api/sources`, `/api/attribution`).
Separate from the Django app: own container (Python 3.12, `mcp` SDK), own deps.

Run locally: `pip install -e .[test] && WHG_BASE_URL=https://whgazetteer.org python -m whg_mcp.server` (stdio), or
`WHG_MCP_TRANSPORT=streamable-http` for HTTP at `/`. Tests: `pytest`.

Guarantees (each pinned by a test): GET only; paths allow-listed; no redirects followed; identifiable `WHG-MCP/<version>`
User-Agent; WHG's own visibility, licence and quota rules apply because it calls the public API.

Served at the root of `mcp.whgazetteer.org` (subdomain, decision 2026-11-17), anonymous: NO token is ever sent to WHG.
Stateless JSON transport, Host/Origin checks on, bbox <= 10 deg, radius <= 100 km, responses capped at 1.5 MB.

Deployment (compose project, nginx block with per-IP limits, rollback, logs): see `deploy/README.md`.

Not yet done: a separate MCP daily budget on the Django side (the server's own budget is in-process),
`place_at_date`, a field whitelist. Open decisions are listed in the project memory note.
