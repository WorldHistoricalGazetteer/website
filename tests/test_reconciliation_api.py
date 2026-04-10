#!/usr/bin/env python3
"""
Comprehensive test suite for the WHG Reconciliation Service API v0.2.

Validates all endpoints against a live server, checking response shapes
match what OpenRefine expects.  Run against dev before promoting to production.

Usage:
    python tests/test_reconciliation_api.py [--base-url URL] [--token TOKEN]

Defaults to dev.whgazetteer.org.
"""

import argparse
import json
import re
import sys
import time

import requests

# ── Defaults ─────────────────────────────────────────────────────────────────

DEFAULT_BASE = "https://dev.whgazetteer.org"
DEFAULT_TOKEN = "IwZ5-626F25qXVRn--IYxJPgH-DKPBwZw8xa8BSgnJ4"

# ── Test infrastructure ──────────────────────────────────────────────────────

PASS = 0
FAIL = 0
ERRORS = []


def check(name: str, condition: bool, detail: str = ""):
    """Record a test result."""
    global PASS, FAIL
    if condition:
        PASS += 1
        print(f"  ✅ {name}")
    else:
        FAIL += 1
        msg = f"  ❌ {name}"
        if detail:
            msg += f"  — {detail}"
        print(msg)
        ERRORS.append(name)


def section(title: str):
    print(f"\n{'─' * 60}")
    print(f"  {title}")
    print(f"{'─' * 60}")


# ── Helpers ──────────────────────────────────────────────────────────────────

def is_recon_result(r: dict, entity_prefix: str = "place") -> list[str]:
    """
    Validate a single reconciliation result dict against the v0.2 spec.
    Returns a list of problems (empty = valid).
    """
    problems = []

    # Required keys
    for key in ("id", "name", "score", "match", "type"):
        if key not in r:
            problems.append(f"missing key '{key}'")

    if "id" in r:
        if not isinstance(r["id"], str):
            problems.append(f"id should be str, got {type(r['id']).__name__}")

    if "name" in r:
        if not isinstance(r["name"], str) or not r["name"]:
            problems.append(f"name should be non-empty str")

    if "score" in r:
        if not isinstance(r["score"], (int, float)):
            problems.append(f"score should be numeric, got {type(r['score']).__name__}")
        elif not (0 <= r["score"] <= 100):
            problems.append(f"score {r['score']} not in 0–100")

    if "match" in r:
        if not isinstance(r["match"], bool):
            problems.append(f"match should be bool, got {type(r['match']).__name__}")

    if "type" in r:
        if not isinstance(r["type"], list):
            problems.append(f"type should be list, got {type(r['type']).__name__}")
        elif r["type"]:
            t = r["type"][0]
            if not isinstance(t, dict) or "id" not in t or "name" not in t:
                problems.append(f"type[0] should have {{id, name}}, got {t}")

    return problems


# ── Test groups ──────────────────────────────────────────────────────────────

def test_service_metadata(s: requests.Session, base: str, token: str):
    section("1. Service Metadata (GET /reconcile)")

    # Without token
    resp = s.get(f"{base}/reconcile")
    check("GET /reconcile returns 200", resp.status_code == 200, f"got {resp.status_code}")
    meta = resp.json()

    check("has 'versions'", "versions" in meta)
    check("versions includes '0.2'", "0.2" in meta.get("versions", []))
    check("has 'name'", isinstance(meta.get("name"), str) and len(meta["name"]) > 0)
    check("has 'identifierSpace'", isinstance(meta.get("identifierSpace"), str))
    check("has 'schemaSpace'", isinstance(meta.get("schemaSpace"), str))

    # defaultTypes
    dt = meta.get("defaultTypes", [])
    check("has defaultTypes (list)", isinstance(dt, list) and len(dt) > 0)
    if dt:
        check("defaultTypes[0] has {id, name}",
              isinstance(dt[0], dict) and "id" in dt[0] and "name" in dt[0])
        type_names = [t["name"] for t in dt]
        check("defaultTypes includes Place", "Place" in type_names)
        check("defaultTypes includes Period", "Period" in type_names)

    # suggest
    sug = meta.get("suggest", {})
    check("has suggest.entity", "entity" in sug)
    check("has suggest.property", "property" in sug)
    if "entity" in sug:
        check("suggest.entity has service_url + service_path",
              "service_url" in sug["entity"] and "service_path" in sug["entity"])

    # extend
    ext = meta.get("extend", {})
    check("has extend.propose_properties", "propose_properties" in ext)
    check("has extend.property_settings", "property_settings" in ext)

    # preview
    check("has preview with url, width, height",
          "preview" in meta and "url" in meta.get("preview", {})
          and "width" in meta.get("preview", {}) and "height" in meta.get("preview", {}))

    # view + feature_view
    check("has view.url", "url" in meta.get("view", {}))
    check("has feature_view.url", "url" in meta.get("feature_view", {}))

    # authentication
    auth = meta.get("authentication", {})
    check("authentication.type is 'apiKey'", auth.get("type") == "apiKey")
    check("authentication.name is 'token'", auth.get("name") == "token")
    check("authentication.in is 'query'", auth.get("in") == "query")

    check("has batch_size (int)", isinstance(meta.get("batch_size"), int))

    # With token — verify {{token}} substitution
    resp2 = s.get(f"{base}/reconcile", params={"token": token})
    meta2 = resp2.json()
    preview_url = meta2.get("preview", {}).get("url", "")
    check("token injected into preview URL",
          token in preview_url, f"preview.url = {preview_url}")
    suggest_path = meta2.get("suggest", {}).get("entity", {}).get("service_path", "")
    check("token injected into suggest.entity.service_path",
          token in suggest_path, f"service_path = {suggest_path}")

    return meta.get("schemaSpace", "")


def test_place_reconciliation(s: requests.Session, base: str, token: str, schema_space: str):
    section("2. Place Reconciliation (POST /reconcile)")
    place_type = f"{schema_space}#Place"

    # ── 2a. Form-encoded (OpenRefine style)
    print("\n  2a. Form-encoded query")
    queries_json = json.dumps({
        "q0": {"query": "Edinburgh", "type": place_type, "limit": 5}
    })
    resp = s.post(f"{base}/reconcile", params={"token": token},
                  data={"queries": queries_json},
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("form-encoded POST returns 200", resp.status_code == 200, f"got {resp.status_code}")
    data = resp.json()

    check("response has key 'q0'", "q0" in data)
    results = data.get("q0", {}).get("result", [])
    check("q0.result is non-empty list", isinstance(results, list) and len(results) > 0,
          f"got {len(results)} results")

    if results:
        r0 = results[0]
        probs = is_recon_result(r0)
        check("first result has valid v0.2 shape", len(probs) == 0,
              "; ".join(probs) if probs else "")
        check("first result id starts with 'place:'",
              r0.get("id", "").startswith("place:"), f"id = {r0.get('id')}")
        check("first result type[0].name is 'Place'",
              r0.get("type", [{}])[0].get("name") == "Place")

        # Check for CRC results mixed in
        crc_results = [r for r in results if re.match(r"place:\w+:", r.get("id", ""))]
        legacy_results = [r for r in results if re.match(r"place:\d+$", r.get("id", ""))]
        print(f"    ℹ️  {len(legacy_results)} legacy + {len(crc_results)} CRC results")

    # ── 2b. JSON body
    print("\n  2b. JSON body query")
    resp2 = s.post(f"{base}/reconcile", params={"token": token},
                   json={"queries": {"q0": {"query": "Athens", "type": place_type, "limit": 5}}})
    check("JSON POST returns 200", resp2.status_code == 200, f"got {resp2.status_code}")
    data2 = resp2.json()
    results2 = data2.get("q0", {}).get("result", [])
    check("JSON body query returns results", len(results2) > 0)
    if results2:
        probs = is_recon_result(results2[0])
        check("JSON result has valid shape", len(probs) == 0, "; ".join(probs))

    # ── 2c. Batch queries
    print("\n  2c. Batch queries")
    batch = {
        "q0": {"query": "London", "type": place_type, "limit": 3},
        "q1": {"query": "Paris", "type": place_type, "limit": 3},
        "q2": {"query": "Tokyo", "type": place_type, "limit": 3},
    }
    resp3 = s.post(f"{base}/reconcile", params={"token": token},
                   data={"queries": json.dumps(batch)},
                   headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("batch POST returns 200", resp3.status_code == 200)
    data3 = resp3.json()
    check("batch response has all 3 query keys",
          all(k in data3 for k in ("q0", "q1", "q2")),
          f"keys = {list(data3.keys())}")
    for qk in ("q0", "q1", "q2"):
        res = data3.get(qk, {}).get("result", [])
        check(f"batch {qk} has results", len(res) > 0, f"{len(res)} results")

    # ── 2d. Score normalization across batch
    print("\n  2d. Score normalization")
    all_scores = []
    for qk in ("q0", "q1", "q2"):
        for r in data3.get(qk, {}).get("result", []):
            all_scores.append(r.get("score", -1))
    check("all scores in 0–100 range",
          all(0 <= sc <= 100 for sc in all_scores),
          f"min={min(all_scores)}, max={max(all_scores)}" if all_scores else "no scores")

    # Return a known LEGACY place ID for extension test (CRC IDs won't work)
    first_legacy_id = None
    for r in results:
        if re.match(r"place:\d+$", r.get("id", "")):
            first_legacy_id = r["id"]
            break
    return first_legacy_id


def test_type_guessing(s: requests.Session, base: str, token: str, schema_space: str):
    section("3. Type Guessing (OpenRefine discovery)")

    # OpenRefine sends a query without a 'type' to discover available types
    queries_json = json.dumps({
        "q0": {"query": "test", "limit": 5}
    })
    resp = s.post(f"{base}/reconcile", params={"token": token},
                  data={"queries": queries_json},
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("type-guessing POST returns 200", resp.status_code == 200, f"got {resp.status_code}")
    data = resp.json()
    results = data.get("q0", {}).get("result", [])
    check("type-guessing returns results", len(results) > 0)

    if results:
        type_names_seen = set()
        for r in results:
            for t in r.get("type", []):
                type_names_seen.add(t.get("name", ""))
        check("type-guessing covers Place type", "Place" in type_names_seen, f"types seen: {type_names_seen}")
        check("type-guessing covers Period type", "Period" in type_names_seen)
        check("all dummy results have score 100",
              all(r.get("score") == 100 for r in results))
        check("all dummy results have match True",
              all(r.get("match") is True for r in results))
        check("dummy ids are prefixed 'dummy:'",
              all(r.get("id", "").startswith("dummy:") for r in results))


def test_period_reconciliation(s: requests.Session, base: str, token: str, schema_space: str):
    section("4. Period Reconciliation")
    period_type = f"{schema_space}#Period"

    resp = s.post(f"{base}/reconcile", params={"token": token},
                  data={"queries": json.dumps({
                      "q0": {"query": "Medieval", "type": period_type, "limit": 5}
                  })},
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("period POST returns 200", resp.status_code == 200, f"got {resp.status_code}")
    data = resp.json()
    results = data.get("q0", {}).get("result", [])
    check("period query returns results", len(results) > 0, f"{len(results)} results")

    if results:
        r0 = results[0]
        probs = is_recon_result(r0, "period")
        check("period result has valid v0.2 shape", len(probs) == 0, "; ".join(probs))
        check("period result id starts with 'period:'",
              r0.get("id", "").startswith("period:"), f"id = {r0.get('id')}")
        check("period result type[0].name is 'Period'",
              r0.get("type", [{}])[0].get("name") == "Period")
        check("period result has description",
              isinstance(r0.get("description"), str) and len(r0["description"]) > 0)


def test_data_extension(s: requests.Session, base: str, token: str, place_id: str | None, schema_space: str):
    section("5. Data Extension (POST /reconcile with extend)")

    # If no legacy ID from earlier, try a targeted query
    if not place_id:
        print("    ℹ️  No legacy ID from earlier test; running bootstrap query...")
        place_type = f"{schema_space}#Place"
        resp0 = s.post(f"{base}/reconcile", params={"token": token},
                       json={"queries": {"q0": {"query": "London", "type": place_type, "limit": 50}}})
        if resp0.status_code == 200:
            for r in resp0.json().get("q0", {}).get("result", []):
                if re.match(r"place:\d+$", r.get("id", "")):
                    place_id = r["id"]
                    break

    if not place_id:
        print("  ⚠️  Skipping — no legacy place ID available")
        return

    print(f"    ℹ️  Using place ID: {place_id}")

    extend_payload = {
        "ids": [place_id],
        "properties": [
            {"id": "whg:names_canonical"},
            {"id": "whg:countries_codes"},
        ]
    }

    resp = s.post(f"{base}/reconcile", params={"token": token},
                  data={"extend": json.dumps(extend_payload)},
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("extend POST returns 200", resp.status_code == 200, f"got {resp.status_code}")

    if resp.status_code != 200:
        print(f"    ⚠️  Response body: {resp.text[:200]}")
        return

    try:
        data = resp.json()
    except Exception as e:
        check("extend response is valid JSON", False, str(e))
        return

    check("response has 'meta' key", "meta" in data)
    check("response has 'rows' key", "rows" in data)

    meta = data.get("meta", [])
    check("meta is a list", isinstance(meta, list))
    if meta:
        check("meta[0] has {id, name}", "id" in meta[0] and "name" in meta[0],
              f"meta[0] = {meta[0]}")

    rows = data.get("rows", {})
    check("rows is a dict", isinstance(rows, dict))
    check("rows has entry for our entity", len(rows) > 0,
          f"keys = {list(rows.keys())}")

    if rows:
        first_key = list(rows.keys())[0]
        row = rows[first_key]
        check("row is a dict of property values", isinstance(row, dict))
        for pid in ("whg:names_canonical", "whg:countries_codes"):
            val = row.get(pid)
            check(f"row has '{pid}'", val is not None, f"keys = {list(row.keys())}")
            if val is not None:
                check(f"'{pid}' is a list", isinstance(val, list))
                # OpenRefine expects each cell to be {str: value} or {id: value, name: label}
                if val:
                    cell = val[0]
                    check(f"'{pid}' cell is a dict",
                          isinstance(cell, dict),
                          f"got {type(cell).__name__}: {cell}")


def test_propose_properties(s: requests.Session, base: str):
    section("6. Propose Properties (GET /reconcile/properties)")

    resp = s.get(f"{base}/reconcile/properties")
    check("GET /reconcile/properties returns 200", resp.status_code == 200,
          f"got {resp.status_code}")
    data = resp.json()
    check("response has 'properties' key", "properties" in data)

    props = data.get("properties", [])
    check("properties is a non-empty list", isinstance(props, list) and len(props) > 0)

    if props:
        p0 = props[0]
        check("property has 'id'", "id" in p0)
        check("property has 'name'", "name" in p0)


def test_suggest_entity(s: requests.Session, base: str, token: str):
    section("7. Suggest Entity (GET /suggest/entity)")

    resp = s.get(f"{base}/suggest/entity",
                 params={"token": token, "prefix": "Lon", "limit": 5})
    check("GET /suggest/entity returns 200", resp.status_code == 200,
          f"got {resp.status_code}")
    data = resp.json()
    check("response has 'result' key", "result" in data)

    results = data.get("result", [])
    check("suggest returns results", len(results) > 0, f"{len(results)} results")

    if results:
        r0 = results[0]
        probs = is_recon_result(r0)
        check("suggest result has v0.2 shape", len(probs) == 0, "; ".join(probs))
        check("suggest result has description",
              "description" in r0, f"keys = {list(r0.keys())}")


def test_suggest_property(s: requests.Session, base: str, token: str):
    section("8. Suggest Property (GET /suggest/property)")

    resp = s.get(f"{base}/suggest/property",
                 params={"token": token, "prefix": "name", "limit": 5})
    check("GET /suggest/property returns 200", resp.status_code == 200,
          f"got {resp.status_code}")
    data = resp.json()
    check("response has 'result' key", "result" in data)

    results = data.get("result", [])
    check("property suggest returns results", len(results) > 0, f"{len(results)} results")

    if results:
        p0 = results[0]
        check("property result has 'id'", "id" in p0)
        check("property result has 'name'", "name" in p0)


def test_filters(s: requests.Session, base: str, token: str, schema_space: str):
    section("9. Spatial / Country / Temporal Filters")
    place_type = f"{schema_space}#Place"

    # ── 9a. Nearby circle (London area)
    print("\n  9a. Nearby circle filter")
    resp = s.post(f"{base}/reconcile", params={"token": token},
                  json={"queries": {"q0": {
                      "query": "Westminster",
                      "type": place_type,
                      "lat": 51.5, "lng": -0.12, "radius": 50,
                      "limit": 5,
                  }}})
    check("nearby filter returns 200", resp.status_code == 200)
    results = resp.json().get("q0", {}).get("result", [])
    check("nearby filter returns results", len(results) > 0, f"{len(results)} results")

    # ── 9b. Country code filter
    print("\n  9b. Country code filter")
    resp2 = s.post(f"{base}/reconcile", params={"token": token},
                   json={"queries": {"q0": {
                       "query": "Athens",
                       "type": place_type,
                       "countries": ["GR"],
                       "limit": 5,
                   }}})
    check("country filter returns 200", resp2.status_code == 200)
    results2 = resp2.json().get("q0", {}).get("result", [])
    check("country filter returns results", len(results2) > 0)

    # ── 9c. Temporal filter
    print("\n  9c. Temporal filter")
    resp3 = s.post(f"{base}/reconcile", params={"token": token},
                   json={"queries": {"q0": {
                       "query": "Constantinople",
                       "type": place_type,
                       "start": 300, "end": 1453,
                       "limit": 5,
                   }}})
    check("temporal filter returns 200", resp3.status_code == 200)
    results3 = resp3.json().get("q0", {}).get("result", [])
    check("temporal filter returns results", len(results3) > 0, f"{len(results3)} results")


def test_batch_limit(s: requests.Session, base: str, token: str, schema_space: str):
    section("10. Batch Size Limit")
    place_type = f"{schema_space}#Place"

    # Build 55 queries (batch_size is 50)
    queries = {f"q{i}": {"query": f"city_{i}", "type": place_type, "limit": 1}
               for i in range(55)}

    resp = s.post(f"{base}/reconcile", params={"token": token},
                  data={"queries": json.dumps(queries)},
                  headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("oversized batch returns 200", resp.status_code == 200,
          f"got {resp.status_code}")

    if resp.status_code != 200:
        print(f"    ⚠️  Skipping remaining checks (status {resp.status_code})")
        return

    try:
        data = resp.json()
    except Exception as e:
        check("batch response is valid JSON", False, str(e))
        return

    # Should have at most 50 query keys + possibly a "messages" key
    query_keys = [k for k in data.keys() if k.startswith("q")]
    check("batch truncated to ≤50 queries", len(query_keys) <= 50,
          f"got {len(query_keys)} query keys")
    check("response includes 'messages' about truncation",
          "messages" in data, f"keys = {list(data.keys())}")


def test_auth_required(s_noauth: requests.Session, base: str):
    section("11. Authentication Enforcement")

    # POST without token should get 401
    resp = s_noauth.post(f"{base}/reconcile",
                         data={"queries": json.dumps({"q0": {"query": "test"}})},
                         headers={"Content-Type": "application/x-www-form-urlencoded"})
    check("POST without token returns 401", resp.status_code == 401,
          f"got {resp.status_code}")

    # GET (metadata) should still work without token
    resp2 = s_noauth.get(f"{base}/reconcile")
    check("GET metadata works without token", resp2.status_code == 200,
          f"got {resp2.status_code}")


def test_error_handling(s: requests.Session, base: str, token: str, schema_space: str):
    section("12. Error Handling")
    place_type = f"{schema_space}#Place"

    # Empty query without bounds
    resp = s.post(f"{base}/reconcile", params={"token": token},
                  json={"queries": {"q0": {"query": "", "type": place_type}}})
    check("empty query returns 200 with error", resp.status_code == 200,
          f"got {resp.status_code}")
    try:
        data = resp.json()
        q0 = data.get("q0", {})
        check("empty query result has 'error' field",
              "error" in q0, f"q0 keys = {list(q0.keys())}")
    except Exception:
        check("empty query response is valid JSON", False, "non-JSON response")

    # Missing queries and extend
    resp2 = s.post(f"{base}/reconcile", params={"token": token},
                   json={"bad_key": "test"})
    check("missing queries/extend returns error", resp2.status_code == 400,
          f"got {resp2.status_code}")


# ── Main ─────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="WHG Reconciliation API test suite")
    parser.add_argument("--base-url", default=DEFAULT_BASE, help="Base URL of WHG instance")
    parser.add_argument("--token", default=DEFAULT_TOKEN, help="API token")
    args = parser.parse_args()

    base = args.base_url.rstrip("/")
    token = args.token

    print(f"\n{'═' * 60}")
    print(f"  WHG Reconciliation API v0.2 — Test Suite")
    print(f"  Target: {base}")
    print(f"{'═' * 60}")

    # Authenticated session
    s = requests.Session()
    s.headers.update({"User-Agent": "Mozilla/5.0 (WHG API Test Suite)"})

    # Unauthenticated session (for auth tests)
    s_noauth = requests.Session()
    s_noauth.headers.update({"User-Agent": "Mozilla/5.0 (WHG API Test Suite)"})

    start = time.time()

    schema_space = test_service_metadata(s, base, token)
    place_id = test_place_reconciliation(s, base, token, schema_space)
    test_type_guessing(s, base, token, schema_space)
    test_period_reconciliation(s, base, token, schema_space)
    test_data_extension(s, base, token, place_id, schema_space)
    test_propose_properties(s, base)
    test_suggest_entity(s, base, token)
    test_suggest_property(s, base, token)
    test_filters(s, base, token, schema_space)
    test_batch_limit(s, base, token, schema_space)
    test_auth_required(s_noauth, base)
    test_error_handling(s, base, token, schema_space)

    elapsed = time.time() - start

    print(f"\n{'═' * 60}")
    print(f"  Results: {PASS} passed, {FAIL} failed  ({elapsed:.1f}s)")
    print(f"{'═' * 60}")

    if ERRORS:
        print(f"\n  Failed tests:")
        for e in ERRORS:
            print(f"    • {e}")
        print()

    sys.exit(1 if FAIL else 0)


if __name__ == "__main__":
    main()








