#!/usr/bin/env python3
"""
Reconciliation API diagnostic tests.

Tests both the CRC gateway directly (via SSH to DO server) and the
Django reconcile endpoint's internal logic.

Run from repo root:
    python tests/test_reconciliation_api.py          # full suite
    python tests/test_reconciliation_api.py gateway   # gateway-only
    python tests/test_reconciliation_api.py django     # django-only

Requires: requests, subprocess (ssh whg must be configured)
"""

import json
import subprocess
import sys
import textwrap

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------

GATEWAY_URL = "http://index.whgazetteer.org:9200"
WHG_URL = "https://whgazetteer.org"

WESTERN_EUROPE_BOUNDS = {
    "type": "Polygon",
    "coordinates": [[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]],
}

# Same bounds but wrapped in GeometryCollection (legacy WHG format)
WESTERN_EUROPE_BOUNDS_LEGACY = {
    "geometries": [WESTERN_EUROPE_BOUNDS],
}

TEST_PLACES = ["London", "Venice", "Paris", "Istanbul"]

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

class TestResult:
    def __init__(self, name):
        self.name = name
        self.passed = 0
        self.failed = 0
        self.errors = []

    def ok(self, msg):
        self.passed += 1
        print(f"  ✓ {msg}")

    def fail(self, msg, detail=""):
        self.failed += 1
        self.errors.append(msg)
        print(f"  ✗ {msg}")
        if detail:
            for line in textwrap.wrap(detail, width=100):
                print(f"      {line}")

    def summary(self):
        total = self.passed + self.failed
        status = "PASS" if self.failed == 0 else "FAIL"
        print(f"\n  [{status}] {self.name}: {self.passed}/{total} passed")
        return self.failed == 0


def gateway_curl(method, path, body=None, timeout=15):
    """
    Run a curl command against the gateway via ssh whg.
    Returns (status_code, response_body_dict_or_str).
    """
    # Build the curl command string for remote execution
    curl_parts = ["curl", "-s", "-w", "'|||%{http_code}'", "-X", method]
    if body:
        body_json = json.dumps(body)
        # Escape single quotes in JSON for the shell
        body_json_escaped = body_json.replace("'", "'\\''")
        curl_parts += ["-H", "'Content-Type: application/json'", "-d", f"'{body_json_escaped}'"]
    curl_parts.append(f"'{GATEWAY_URL}{path}'")

    remote_cmd = " ".join(curl_parts)
    cmd = ["ssh", "whg", remote_cmd]

    try:
        result = subprocess.run(
            cmd, capture_output=True, text=True, timeout=timeout + 10
        )
        output = result.stdout.strip()

        # Split on our custom separator
        if "|||" in output:
            body_str, code_str = output.rsplit("|||", 1)
        else:
            body_str = output
            code_str = "0"

        try:
            status = int(code_str.strip())
        except ValueError:
            status = 0
        try:
            data = json.loads(body_str)
        except json.JSONDecodeError:
            data = body_str
        return status, data
    except subprocess.TimeoutExpired:
        return 0, "TIMEOUT"
    except Exception as e:
        return 0, str(e)


# ---------------------------------------------------------------------------
# Gateway tests
# ---------------------------------------------------------------------------

def test_gateway_health():
    t = TestResult("Gateway Health")
    status, data = gateway_curl("GET", "/api/health")
    if status == 200 and isinstance(data, dict):
        t.ok(f"Gateway: {data.get('gateway', '?')}, ES: {data.get('elasticsearch', '?')}")
    else:
        t.fail(f"Health check returned {status}", str(data)[:200])
    t.summary()
    return t


def test_gateway_reconcile_modes():
    """Test all search modes against the gateway."""
    t = TestResult("Gateway Reconcile Modes")

    for mode in ("exact", "starts", "fuzzy"):
        body = {"query": "London", "mode": mode, "size": 5}
        status, data = gateway_curl("POST", "/api/reconcile", body)

        if status != 200:
            t.fail(f"mode={mode}: HTTP {status}", str(data)[:200])
            continue

        hits = data.get("hits", []) if isinstance(data, dict) else []
        if hits:
            titles = [h.get("title", "?") for h in hits[:3]]
            t.ok(f"mode={mode}: {len(hits)} hits ({', '.join(titles)})")
        else:
            t.fail(f"mode={mode}: 0 hits", json.dumps(data)[:300])

    t.summary()
    return t


def test_gateway_bounds():
    """Test spatial bounds filtering (plain GeoJSON Polygon)."""
    t = TestResult("Gateway Bounds Filtering")

    for place in TEST_PLACES:
        body = {
            "query": place,
            "mode": "starts",
            "size": 5,
            "bounds": WESTERN_EUROPE_BOUNDS,
            "namespaces": ["gn", "wd"],
        }
        status, data = gateway_curl("POST", "/api/reconcile", body)

        if status != 200:
            t.fail(f"{place}: HTTP {status}", str(data)[:200])
            continue

        hits = data.get("hits", []) if isinstance(data, dict) else []
        if hits:
            first = hits[0]
            t.ok(f"{place}: {len(hits)} hits, best={first.get('title')} ({first.get('place_id')})")
        else:
            t.fail(f"{place} with W.Europe bounds: 0 hits")

    t.summary()
    return t


def test_gateway_namespaces():
    """Test namespace filtering."""
    t = TestResult("Gateway Namespace Filter")

    # Only GeoNames
    body = {"query": "London", "mode": "starts", "size": 10, "namespaces": ["gn"]}
    status, data = gateway_curl("POST", "/api/reconcile", body)
    if status == 200 and isinstance(data, dict):
        hits = data.get("hits", [])
        non_gn = [h for h in hits if not h.get("place_id", "").startswith("gn:")]
        if non_gn:
            t.fail(f"Namespace gn filter leaked: {[h['place_id'] for h in non_gn[:3]]}")
        elif hits:
            t.ok(f"namespaces=[gn]: {len(hits)} hits, all gn:*")
        else:
            t.fail("namespaces=[gn]: 0 hits")
    else:
        t.fail(f"HTTP {status}", str(data)[:200])

    # Wikidata + GeoNames
    body = {"query": "Venice", "mode": "starts", "size": 10, "namespaces": ["gn", "wd"]}
    status, data = gateway_curl("POST", "/api/reconcile", body)
    if status == 200 and isinstance(data, dict):
        hits = data.get("hits", [])
        namespaces = {h.get("namespace", "") for h in hits}
        if hits:
            t.ok(f"namespaces=[gn,wd]: {len(hits)} hits, namespaces={namespaces}")
        else:
            t.fail("namespaces=[gn,wd]: 0 hits")
    else:
        t.fail(f"HTTP {status}", str(data)[:200])

    t.summary()
    return t


def test_gateway_search():
    """Test the /api/search endpoint."""
    t = TestResult("Gateway /api/search")

    for mode in ("starts", "fuzzy"):
        body = {"query": "London", "mode": mode, "size": 5}
        status, data = gateway_curl("POST", "/api/search", body)

        if status != 200:
            t.fail(f"/api/search mode={mode}: HTTP {status}", str(data)[:200])
            continue

        hits = data.get("hits", []) if isinstance(data, dict) else []
        if hits:
            t.ok(f"/api/search mode={mode}: {len(hits)} hits, facets present={bool(data.get('facets'))}")
        else:
            t.fail(f"/api/search mode={mode}: 0 hits")

    t.summary()
    return t


def test_gateway_suggest():
    """Test the /api/suggest endpoint."""
    t = TestResult("Gateway /api/suggest")

    status, data = gateway_curl("GET", "/api/suggest?q=Lond&size=5")
    if status == 200 and isinstance(data, dict):
        suggestions = data.get("suggestions", [])
        if suggestions:
            names = [s.get("name", "?") for s in suggestions]
            t.ok(f"/api/suggest: {len(suggestions)} suggestions ({', '.join(names[:3])})")
        else:
            t.fail("/api/suggest: 0 suggestions")
    else:
        t.fail(f"HTTP {status}", str(data)[:200])

    t.summary()
    return t


# ---------------------------------------------------------------------------
# Django normalise_query_params tests (offline unit tests)
# ---------------------------------------------------------------------------

def test_django_normalise_bounds():
    """Test the Django-side bounds parsing handles all formats."""
    t = TestResult("Django normalise_query_params — Bounds Parsing")

    # Import the function (needs Django setup)
    try:
        import django
        import os
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "whg.settings")
        django.setup()
        from api.reconcile import normalise_query_params
    except Exception as e:
        t.fail(f"Cannot import Django: {e}")
        t.summary()
        return t

    # Test 1: Plain GeoJSON Polygon (the user's format)
    params = {
        "query": "London",
        "bounds": {"type": "Polygon", "coordinates": [[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]]},
    }
    try:
        result = normalise_query_params(params)
        if result["bounds"] and result["bounds"]["type"] == "Polygon":
            t.ok("Plain Polygon bounds parsed correctly")
        else:
            t.fail(f"Plain Polygon bounds: unexpected result {result['bounds']}")
    except Exception as e:
        t.fail(f"Plain Polygon bounds raised: {e}")

    # Test 2: GeometryCollection format (legacy)
    params = {
        "query": "London",
        "bounds": {"geometries": [{"type": "Polygon", "coordinates": [[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]]}]},
    }
    try:
        result = normalise_query_params(params)
        if result["bounds"] and result["bounds"]["type"] == "Polygon":
            t.ok("GeometryCollection bounds parsed correctly")
        else:
            t.fail(f"GeometryCollection bounds: unexpected result {result['bounds']}")
    except Exception as e:
        t.fail(f"GeometryCollection bounds raised: {e}")

    # Test 3: MultiPolygon
    params = {
        "query": "London",
        "bounds": {
            "type": "MultiPolygon",
            "coordinates": [[[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]]],
        },
    }
    try:
        result = normalise_query_params(params)
        if result["bounds"] and result["bounds"]["type"] == "MultiPolygon":
            t.ok("MultiPolygon bounds parsed correctly")
        else:
            t.fail(f"MultiPolygon bounds: unexpected result {result['bounds']}")
    except Exception as e:
        t.fail(f"MultiPolygon bounds raised: {e}")

    # Test 4: Invalid bounds should raise ValueError
    params = {"query": "London", "bounds": {"type": "Point", "coordinates": [0, 0]}}
    try:
        normalise_query_params(params)
        t.fail("Point bounds should have raised ValueError")
    except ValueError:
        t.ok("Point bounds correctly rejected")
    except Exception as e:
        t.fail(f"Point bounds raised unexpected: {type(e).__name__}: {e}")

    # Test 5: Empty bounds object should raise ValueError
    params = {"query": "London", "bounds": {}}
    try:
        normalise_query_params(params)
        t.fail("Empty bounds should have raised ValueError")
    except ValueError:
        t.ok("Empty bounds correctly rejected")
    except Exception as e:
        t.fail(f"Empty bounds raised unexpected: {type(e).__name__}: {e}")

    # Test 6: limit parameter maps to size
    params = {"query": "London", "limit": 5}
    try:
        result = normalise_query_params(params)
        if result["size"] == 5:
            t.ok("'limit' parameter correctly mapped to size=5")
        else:
            t.fail(f"'limit' parameter: expected size=5, got size={result['size']}")
    except Exception as e:
        t.fail(f"'limit' parameter raised: {e}")

    t.summary()
    return t


def test_django_build_es_query_bounds():
    """Test build_es_query handles both bounds formats."""
    t = TestResult("Django build_es_query — Bounds Handling")

    try:
        import django
        import os
        os.environ.setdefault("DJANGO_SETTINGS_MODULE", "whg.settings")
        django.setup()
        from api.reconcile_helpers import build_es_query
    except Exception as e:
        t.fail(f"Cannot import Django: {e}")
        t.summary()
        return t

    # Test 1: Plain Polygon bounds
    params = {
        "qstr": "London",
        "bounds": {"type": "Polygon", "coordinates": [[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]]},
    }
    try:
        query = build_es_query(params)
        query_str = json.dumps(query)
        if "geo_shape" in query_str:
            t.ok("Plain Polygon bounds → geo_shape filter present")
        else:
            t.fail("Plain Polygon bounds → no geo_shape filter in query", query_str[:300])
    except Exception as e:
        t.fail(f"Plain Polygon bounds raised: {e}")

    # Test 2: GeometryCollection bounds
    params = {
        "qstr": "London",
        "bounds": {"geometries": [{"type": "Polygon", "coordinates": [[[-15, 25], [40, 25], [40, 65], [-15, 65], [-15, 25]]]}]},
    }
    try:
        query = build_es_query(params)
        query_str = json.dumps(query)
        if "geo_shape" in query_str:
            t.ok("GeometryCollection bounds → geo_shape filter present")
        else:
            t.fail("GeometryCollection bounds → no geo_shape filter in query", query_str[:300])
    except Exception as e:
        t.fail(f"GeometryCollection bounds raised: {e}")

    t.summary()
    return t


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    scope = sys.argv[1] if len(sys.argv) > 1 else "all"

    all_results = []
    total_passed = 0
    total_failed = 0

    if scope in ("all", "gateway"):
        print("\n" + "=" * 60)
        print("GATEWAY TESTS (via ssh whg → CRC)")
        print("=" * 60)

        for test_fn in [
            test_gateway_health,
            test_gateway_reconcile_modes,
            test_gateway_bounds,
            test_gateway_namespaces,
            test_gateway_search,
            test_gateway_suggest,
        ]:
            print(f"\n--- {test_fn.__doc__ or test_fn.__name__} ---")
            result = test_fn()
            all_results.append(result)
            total_passed += result.passed
            total_failed += result.failed

    if scope in ("all", "django"):
        print("\n" + "=" * 60)
        print("DJANGO UNIT TESTS (offline)")
        print("=" * 60)

        for test_fn in [
            test_django_normalise_bounds,
            test_django_build_es_query_bounds,
        ]:
            print(f"\n--- {test_fn.__doc__ or test_fn.__name__} ---")
            result = test_fn()
            all_results.append(result)
            total_passed += result.passed
            total_failed += result.failed

    # Summary
    print("\n" + "=" * 60)
    print("SUMMARY")
    print("=" * 60)
    for r in all_results:
        status = "✓" if r.failed == 0 else "✗"
        print(f"  {status} {r.name}: {r.passed}/{r.passed + r.failed}")
    print(f"\n  Total: {total_passed} passed, {total_failed} failed")

    sys.exit(1 if total_failed else 0)


if __name__ == "__main__":
    main()


