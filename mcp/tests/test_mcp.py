import httpx
import pytest

from whg_mcp import server
from whg_mcp.client import WHGClient, WHGError

SEEN = []


def handler(request: httpx.Request) -> httpx.Response:
    SEEN.append(request)
    if request.url.path == "/api/place/999/":
        return httpx.Response(404, json={"error": "nope"})
    if request.url.path == "/api/db/" and request.url.params.get("name") == "quota":
        return httpx.Response(429)
    if request.url.path == "/api/db/" and request.url.params.get("name") == "redir":
        return httpx.Response(302, headers={"Location": "http://evil.example/"})
    return httpx.Response(200, json={"ok": True, "path": request.url.path})


@pytest.fixture(autouse=True)
def fake_client(monkeypatch):
    SEEN.clear()
    c = WHGClient("https://whg.test", token="tok", transport=httpx.MockTransport(handler))
    monkeypatch.setattr(server, "_client", c)
    return c


async def test_search_sends_identifiable_ua_token_and_clean_params():
    await server.search_places(name="York", ccode="GB", limit=500)
    r = SEEN[0]
    assert r.method == "GET" and r.url.path == "/api/db/"
    assert r.headers["user-agent"].startswith("WHG-MCP/")
    assert r.headers["authorization"] == "Bearer tok"
    assert r.url.params["pagesize"] == "50"            # clamped
    assert "name_contains" not in r.url.params          # empty params dropped
    assert r.url.params["ccode"] == "GB"


async def test_search_needs_a_name():
    with pytest.raises(WHGError):
        await server.search_places()
    assert SEEN == []


async def test_get_place_joins_ids():
    await server.get_place([1, 2, 3])
    assert SEEN[0].url.path == "/api/place/1-2-3/"


async def test_spatial_queries():
    await server.places_near(lon=-1.08, lat=53.96, km=10)
    await server.places_in_bbox(west=-2, south=53, east=0, north=54)
    assert SEEN[0].url.params["type"] == "nearby"
    assert SEEN[1].url.params["sw"] == "-2.0,53.0" and SEEN[1].url.params["ne"] == "0.0,54.0"


@pytest.mark.parametrize("path", [
    "/api/users/", "/api/user/1/", "/admin/", "/api/db/../users/", "/api/place/1/extra/",
    "/api/place/abc/", "//evil.example/api/db/", "/datasets/1/", "/api/db", "/api/geojson/"])
async def test_allowlist_blocks_everything_else(path, fake_client):
    with pytest.raises(WHGError, match="not permitted"):
        await fake_client.get(path)
    assert SEEN == []      # nothing was even sent


async def test_allowlist_can_fail(fake_client):
    """Control: a permitted path does go through, so the block test above is not vacuous."""
    await fake_client.get("/api/sources/")
    assert len(SEEN) == 1


async def test_error_mapping_hides_internals(fake_client):
    with pytest.raises(WHGError, match="quota"):
        await server.search_places(name="quota")
    with pytest.raises(WHGError, match="Not found"):
        await server.get_place([999])


async def test_redirects_not_followed(fake_client):
    with pytest.raises(WHGError, match="redirect"):
        await server.search_places(name="redir")
    assert len(SEEN) == 1          # never contacted the redirect target


async def test_tools_registered_and_no_write_tools():
    names = {t.name for t in await server.mcp.list_tools()}
    assert names == {"search_places", "get_place", "places_near", "places_in_bbox",
                     "list_sources", "get_attribution", "status"}
