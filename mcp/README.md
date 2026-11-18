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

## Notes from the first live run (2026-11-18)
- A spatial-only `/reconcile` query (no text) had to name the entity type (`type: ...#Place`), or WHG treated it
  as an OpenRefine type-guessing probe and returned placeholder "Guessing Result" candidates. Fixed on WHG
  2026-11-18 (a circle or bounds now stands on its own); the server still sends `type`, which is accepted.
- The circle-only form (`lat`/`lng`/`radius`) returned no results in production, so up to 0.2.4 `places_near`
  queried the bounding box around the circle. Fixed on WHG 2026-11-18; since 0.2.5 `places_near` sends the
  circle (`relation: within`, `limit` 500). WHG's answer is unranked, capped at 500 and drawn from an H3 disc
  reaching up to ~1.3x the radius, so the server still filters on exact distance to `repr_point` and sorts
  nearest first (`distance_km`). When all 500 come back it sets `may_be_incomplete` and a `note`: the nearest
  places may be among those WHG did not send. A per-query `error` (e.g. a radius over WHG's 500 km) is a
  tool error, not an empty answer, for every tool.
- The production service account is `whg-mcp-service` (non-staff, no groups, unusable password, unverified email,
  daily limit 2000). Its token lives only in `~/stacks/mcp/mcp.env` on the droplet.
