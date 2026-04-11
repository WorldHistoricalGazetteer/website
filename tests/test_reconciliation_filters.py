#!/usr/bin/env python3
"""
Live integration tests for the WHG Reconciliation API filter parameters
and the Extend endpoint.

Exercises every filter parameter (namespaces, countries, fclasses, types)
across Reconcile, Suggest, and Extend endpoints.

Run against dev before promoting to production.

Usage:
    python tests/test_reconciliation_filters.py [--base-url URL] [--token TOKEN]

Defaults to dev.whgazetteer.org.
"""
import argparse
import json
import sys
import textwrap

import requests

# ── CLI ───────────────────────────────────────────────────────────
parser = argparse.ArgumentParser(description=__doc__)
parser.add_argument(
    "--base-url",
    default="https://dev.whgazetteer.org",
    help="Base URL of the WHG server (default: %(default)s)",
)
parser.add_argument(
    "--token",
    default=None,
    help="API token for authenticated endpoints",
)
args = parser.parse_args()

base = args.base_url.rstrip("/")
token = args.token

session = requests.Session()
session.headers["User-Agent"] = "WHG-FilterTests/1.0"

PASS = 0
FAIL = 0


# ── Helpers ───────────────────────────────────────────────────────
def section(title):
    print(f"\n{'=' * 60}")
    print(f"  {title}")
    print(f"{'=' * 60}")


def check(label, response, expect_status=200, expect_results=None):
    """Print a pass/fail line.

    expect_results: True=non-empty, False=empty, None=don't check.
    """
    global PASS, FAIL
    ok = response.status_code == expect_status
    body = {}
    try:
        body = response.json()
    except Exception:
        pass

    result_list = None
    # Dig out result list from reconcile or suggest response shapes
    if "q0" in body:
        result_list = body["q0"].get("result", [])
    elif "result" in body:
        result_list = body["result"]
    elif "rows" in body:
        result_list = body["rows"]

    if expect_results is True and result_list is not None:
        ok = ok and len(result_list) > 0
    elif expect_results is False and result_list is not None:
        ok = ok and len(result_list) == 0

    count = f" ({len(result_list)} results)" if result_list is not None else ""
    status = "PASS ✓" if ok else "FAIL ✗"
    if ok:
        PASS += 1
    else:
        FAIL += 1
    print(f"  [{status}]  {label}  — HTTP {response.status_code}{count}")
    if not ok and response.text:
        print(f"           Body: {response.text[:300]}")
    if result_list and expect_results is True:
        for r in (result_list if isinstance(result_list, list) else [])[:3]:
            name = r.get("name", r.get("id", ""))
            desc = r.get("description", "")
            score = r.get("score", "")
            print(f"           • {r.get('id', '')}  {name}  score={score}  {desc[:80]}")
    return body


def _params(**kw):
    """Build query-string params, injecting token when available."""
    if token:
        kw["token"] = token
    return kw


# ------------------------------------------------------------------
section("0. Service metadata")
# ------------------------------------------------------------------
meta_resp = session.get(f"{base}/reconcile")
meta = check("GET /reconcile (metadata)", meta_resp)
schema_space = meta.get("schemaSpace", f"{base}/vocab")
place_type = f"{schema_space}#Place"
print(f"       Schema space: {schema_space}")


# ------------------------------------------------------------------
section("1. Reconcile — baseline (no filters)")
# ------------------------------------------------------------------
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5}}},
)
baseline = check("Edinburgh — no filters", r, expect_results=True)
all_ids = [x["id"] for x in baseline.get("q0", {}).get("result", [])]


# ------------------------------------------------------------------
section("2. Reconcile — namespaces filter")
# ------------------------------------------------------------------
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "namespaces": "whg"}}},
)
check("Edinburgh namespaces=whg  (legacy only)", r, expect_results=True)

r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "namespaces": "gn"}}},
)
check("Edinburgh namespaces=gn   (GeoNames only)", r)

r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "namespaces": "whg,gn"}}},
)
check("Edinburgh namespaces=whg,gn (both)", r, expect_results=True)


# ------------------------------------------------------------------
section("3. Reconcile — countries filter")
# ------------------------------------------------------------------
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "countries": ["GB"]}}},
)
check("Edinburgh countries=[GB]", r, expect_results=True)

r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "countries": ["JP"]}}},
)
check("Edinburgh countries=[JP]  (expect fewer/no results)", r)


# ------------------------------------------------------------------
section("4. Reconcile — fclasses filter")
# ------------------------------------------------------------------
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "fclasses": ["P"]}}},
)
check("Edinburgh fclasses=[P] (populated places)", r, expect_results=True)

r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "fclasses": ["H"]}}},
)
check("Edinburgh fclasses=[H] (hydrographic — expect fewer)", r)


# ------------------------------------------------------------------
section("5. Reconcile — types (AAT) filter")
# ------------------------------------------------------------------
# aat:300008347 = inhabited places
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "types": ["aat:300008347"]}}},
)
check("Edinburgh types=[aat:300008347] (inhabited places)", r)

# aat:300008804 = lakes — should yield few/no Edinburgh matches
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {"query": "Edinburgh", "type": place_type, "limit": 5, "types": ["aat:300008804"]}}},
)
check("Edinburgh types=[aat:300008804] (lakes — expect fewer)", r)


# ------------------------------------------------------------------
section("6. Reconcile — combined filters")
# ------------------------------------------------------------------
r = session.post(
    f"{base}/reconcile",
    params=_params(),
    json={"queries": {"q0": {
        "query": "London",
        "type": place_type,
        "limit": 5,
        "namespaces": "whg",
        "countries": ["GB"],
        "fclasses": ["P"],
    }}},
)
check("London  namespaces=whg + countries=GB + fclasses=P", r, expect_results=True)


# ------------------------------------------------------------------
section("7. Suggest — baseline")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5),
)
check("GET /suggest/entity?prefix=Edin", r, expect_results=True)


# ------------------------------------------------------------------
section("8. Suggest — namespaces filter")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, namespaces="whg"),
)
check("prefix=Edin  namespaces=whg", r, expect_results=True)

r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, namespaces="gn"),
)
check("prefix=Edin  namespaces=gn", r)


# ------------------------------------------------------------------
section("9. Suggest — countries filter")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, countries="GB"),
)
check("prefix=Edin  countries=GB", r, expect_results=True)

r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, countries="JP"),
)
check("prefix=Edin  countries=JP  (expect fewer)", r)


# ------------------------------------------------------------------
section("10. Suggest — fclasses filter")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, fclasses="P"),
)
check("prefix=Edin  fclasses=P", r, expect_results=True)

r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, fclasses="H"),
)
check("prefix=Edin  fclasses=H  (expect fewer)", r)


# ------------------------------------------------------------------
section("11. Suggest — types (AAT) filter")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, types="aat:300008347"),
)
check("prefix=Edin  types=aat:300008347", r)

r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Edin", limit=5, types="aat:300008804"),
)
check("prefix=Edin  types=aat:300008804 (lakes — expect fewer)", r)


# ------------------------------------------------------------------
section("12. Suggest — combined filters")
# ------------------------------------------------------------------
r = session.get(
    f"{base}/suggest/entity",
    params=_params(prefix="Lon", limit=5,
                   namespaces="whg", countries="GB", fclasses="P"),
)
check("prefix=Lon  namespaces=whg + countries=GB + fclasses=P", r, expect_results=True)


# ------------------------------------------------------------------
section("13. Extend — fetch properties for top reconcile result")
# ------------------------------------------------------------------
place_id = all_ids[0] if all_ids else None
if place_id:
    extend_payload = {
        "ids": [place_id],
        "properties": [
            {"id": "whg:names_canonical"},
            {"id": "whg:names_summary"},
            {"id": "whg:countries_codes"},
            {"id": "whg:countries_objects"},
            {"id": "whg:geometry_centroid"},
            {"id": "whg:geometry_geojson"},
            {"id": "whg:types_objects"},
            {"id": "whg:id_short"},
            {"id": "whg:id_object"},
        ],
    }
    r = session.post(
        f"{base}/reconcile",
        params=_params(),
        data={"extend": json.dumps(extend_payload)},
        headers={"Content-Type": "application/x-www-form-urlencoded"},
    )
    body = check(f"Extend {place_id}", r)
    # Print the row data for the first id
    rows = body.get("rows", {})
    if rows:
        first_key = next(iter(rows))
        print(f"\n       Extend row for {first_key}:")
        print(textwrap.indent(json.dumps(rows[first_key], indent=2, ensure_ascii=False), "           "))
else:
    print("  [SKIP]  No place_id from reconcile — skipping extend")


# ------------------------------------------------------------------
section("14. Entity API — legacy place feature (LPF)")
# ------------------------------------------------------------------
if all_ids:
    # Strip "place:" prefix if present to get the raw numeric ID
    raw_id = all_ids[0]
    if raw_id.startswith("place:"):
        raw_id = raw_id[len("place:"):]
    entity_id = f"place:{raw_id}"
    r = session.get(f"{base}/entity/{entity_id}/api", params=_params())
    body = check(f"GET /entity/{entity_id}/api  (legacy LPF)", r)
    if body:
        print(f"           title = {body.get('title', body.get('properties', {}).get('title', ''))}")
else:
    print("  [SKIP]  No place_id — skipping legacy entity feature test")


# ------------------------------------------------------------------
section("15. Entity API — legacy place preview")
# ------------------------------------------------------------------
if all_ids:
    r = session.get(f"{base}/entity/{entity_id}/preview", params=_params())
    ok = r.status_code == 200 and "text/html" in r.headers.get("Content-Type", "")
    PASS += 1 if ok else 0
    FAIL += 0 if ok else 1
    tag = "PASS ✓" if ok else "FAIL ✗"
    print(f"  [{tag}]  GET /entity/{entity_id}/preview  — HTTP {r.status_code}")
    if ok:
        # Show a snippet of the HTML
        snippet = r.text[:200].replace("\n", " ").strip()
        print(f"           HTML snippet: {snippet}…")
else:
    print("  [SKIP]  No place_id — skipping legacy entity preview test")


# ------------------------------------------------------------------
section("16. Entity API — CRC place feature (LPF)")
# ------------------------------------------------------------------
# Use a well-known GeoNames ID to test CRC path
crc_entity_id = "place:gn:745044"  # Istanbul
r = session.get(f"{base}/entity/{crc_entity_id}/api", params=_params())
body = check(f"GET /entity/{crc_entity_id}/api  (CRC LPF)", r)
if body and body.get("@id"):
    print(f"           @id = {body.get('@id')}")
    print(f"           title = {body.get('properties', {}).get('title', '')}")


# ------------------------------------------------------------------
section("17. Entity API — CRC place preview")
# ------------------------------------------------------------------
r = session.get(f"{base}/entity/{crc_entity_id}/preview", params=_params())
ok = r.status_code == 200 and "text/html" in r.headers.get("Content-Type", "")
PASS += 1 if ok else 0
FAIL += 0 if ok else 1
tag = "PASS ✓" if ok else "FAIL ✗"
print(f"  [{tag}]  GET /entity/{crc_entity_id}/preview  — HTTP {r.status_code}")
if ok:
    snippet = r.text[:200].replace("\n", " ").strip()
    print(f"           HTML snippet: {snippet}…")


# ------------------------------------------------------------------
section("18. Entity API — CRC place detail (expect redirect to feature)")
# ------------------------------------------------------------------
r = session.get(f"{base}/entity/{crc_entity_id}/", params=_params(), allow_redirects=False)
ok = r.status_code in (301, 302, 307, 308)
PASS += 1 if ok else 0
FAIL += 0 if ok else 1
tag = "PASS ✓" if ok else "FAIL ✗"
location = r.headers.get("Location", "")
print(f"  [{tag}]  GET /entity/{crc_entity_id}/  — HTTP {r.status_code}  Location: {location}")


# ------------------------------------------------------------------
section("SUMMARY")
# ------------------------------------------------------------------
total = PASS + FAIL
print(f"\n  {PASS}/{total} passed", end="")
if FAIL:
    print(f",  {FAIL} FAILED")
    sys.exit(1)
else:
    print(" — all good! 🎉")
print()

