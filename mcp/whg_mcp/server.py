"""WHG MCP server: v1 read-only tools over the public WHG API.

Run:  WHG_BASE_URL=https://whgazetteer.org WHG_MCP_TRANSPORT=streamable-http python -m whg_mcp.server
"""
import os
from typing import Annotated, Optional

from mcp.server.fastmcp import FastMCP
from pydantic import Field

from . import __version__
from .client import WHGClient, WHGError

BASE_URL = os.environ.get("WHG_BASE_URL", "https://whgazetteer.org")
MAX_LIMIT = 50

mcp = FastMCP(
    "World Historical Gazetteer",
    instructions=(
        "Look up historical places in the World Historical Gazetteer (whgazetteer.org). "
        "Results carry attribution and licence information: when you quote or reuse a place, "
        "cite its source dataset (see get_attribution). Read-only."),
    host=os.environ.get("WHG_MCP_HOST", "127.0.0.1"),
    port=int(os.environ.get("WHG_MCP_PORT", "8000")),
    streamable_http_path="/mcp/",
)

_client: Optional[WHGClient] = None


def client() -> WHGClient:
    global _client
    if _client is None:
        _client = WHGClient(BASE_URL, token=os.environ.get("WHG_API_TOKEN") or None)
    return _client


def _limit(n: int) -> int:
    return max(1, min(int(n), MAX_LIMIT))


@mcp.tool()
async def search_places(
        name: Annotated[Optional[str], Field(description="Place name to match (exact name or variant)")] = None,
        name_contains: Annotated[Optional[str], Field(description="Substring of a place name")] = None,
        ccode: Annotated[Optional[str], Field(description="ISO 3166-1 alpha-2 country code(s), comma-separated, e.g. 'GB,IE'")] = None,
        fclass: Annotated[Optional[str], Field(description="Feature class letters, comma-separated (A admin, P populated, H water, T terrain, S site, ...)")] = None,
        year: Annotated[Optional[int], Field(description="Restrict to places attested in this year")] = None,
        dataset: Annotated[Optional[str], Field(description="Dataset label to restrict to")] = None,
        limit: Annotated[int, Field(description=f"Maximum results (1-{MAX_LIMIT})")] = 20,
) -> dict:
    """Search WHG places by name, with optional country, feature class, year and dataset filters.
    Needs at least `name` or `name_contains`."""
    if not (name or name_contains):
        raise WHGError("Give at least `name` or `name_contains`.")
    return await client().get("/api/db/", {
        "name": name, "name_contains": name_contains, "ccode": ccode, "fc": fclass,
        "year": year, "dataset": dataset, "pagesize": _limit(limit)})


@mcp.tool()
async def get_place(
        place_ids: Annotated[list[int], Field(description="One or more WHG place ids (max 50)", min_length=1, max_length=50)],
) -> dict:
    """Fetch full records (names, geometry, types, links, sources) for WHG place ids."""
    return await client().get("/api/place/" + "-".join(str(int(i)) for i in place_ids) + "/")


@mcp.tool()
async def places_near(
        lon: Annotated[float, Field(ge=-180, le=180)], lat: Annotated[float, Field(ge=-90, le=90)],
        km: Annotated[float, Field(gt=0, le=500, description="Radius in kilometres")] = 25,
        limit: Annotated[int, Field(description=f"Maximum results (1-{MAX_LIMIT})")] = 20,
) -> dict:
    """Places within a radius of a point."""
    return await client().get("/api/spatial/", {"type": "nearby", "lon": lon, "lat": lat, "km": km,
                                                 "pagesize": _limit(limit)})


@mcp.tool()
async def places_in_bbox(
        west: Annotated[float, Field(ge=-180, le=180)], south: Annotated[float, Field(ge=-90, le=90)],
        east: Annotated[float, Field(ge=-180, le=180)], north: Annotated[float, Field(ge=-90, le=90)],
        limit: Annotated[int, Field(description=f"Maximum results (1-{MAX_LIMIT})")] = 20,
) -> dict:
    """Places inside a bounding box (west, south, east, north in degrees)."""
    return await client().get("/api/spatial/", {"type": "bbox", "sw": f"{float(west)},{float(south)}", "ne": f"{float(east)},{float(north)}",
                                                 "pagesize": _limit(limit)})


@mcp.tool()
async def list_sources() -> dict:
    """The source gazetteers/namespaces WHG draws on, with their identifiers."""
    return await client().get("/api/sources/")


@mcp.tool()
async def get_attribution() -> dict:
    """Attribution and licence information for the data WHG serves. Cite sources when reusing results."""
    return await client().get("/api/attribution/")


@mcp.tool()
async def status() -> dict:
    """This server's version and the WHG instance it talks to."""
    return {"server": "whg-mcp", "version": __version__, "whg": BASE_URL, "read_only": True}


def main():
    transport = os.environ.get("WHG_MCP_TRANSPORT", "stdio")
    mcp.run(transport=transport)


if __name__ == "__main__":
    main()
