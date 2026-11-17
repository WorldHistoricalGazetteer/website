# whg-mcp

Read-only MCP server over the public WHG API (`/api/db`, `/api/place`, `/api/spatial`, `/api/sources`, `/api/attribution`).
Separate from the Django app: own container (Python 3.12, `mcp` SDK), own deps.

Run locally: `pip install -e .[test] && WHG_BASE_URL=https://whgazetteer.org python -m whg_mcp.server` (stdio), or
`WHG_MCP_TRANSPORT=streamable-http` for HTTP at `/mcp/`. Tests: `pytest`.

Guarantees (each pinned by a test): GET only; paths allow-listed; no redirects followed; identifiable `WHG-MCP/<version>`
User-Agent; WHG's own visibility, licence and quota rules apply because it calls the public API.

Not yet done: per-caller token passthrough, quota policy, nginx `/mcp/` location, compose project (`~/stacks/mcp`),
`place_at_date` tool. Open decisions are listed in the project memory note.
