"""The unminted locus page (place#325 Stage 2, item 3; place#172).

An *unminted* locus is a recipe carried in the URL itself:

    /atlas/locus/?r=<token>        token = places.locus_recipe.encode_recipe_token(recipe)

It is shareable but NOT permanent: nothing is stored, nothing is minted, and
the page re-runs the recipe against the index on every visit. Minting (the
registry, ``/entity/locus:<PID>``) is Stage 4 and is not here.

Division of labour, and why
---------------------------
* **Retrieval runs server-side**, through ``api.crc_client.crc_search_status``
  exactly as the Atlas does, because the CRC gateway admits only the app
  server (``/atlas/search/`` exists for the same reason).
* **Clustering runs in the browser**, with ``whg/webpack/js/clustering.js``,
  the one implementation of ``clusterHits`` there is. It mirrors the offline
  calibration maths and is what the Atlas ran when the recipe was made; a
  Python port would be a second implementation whose parity with the first
  would have to be proved and kept (exactly the twin-drift risk the recipe
  module already carries once). The browser already re-runs it on every Atlas
  render, so a page re-run costs nothing new.
* **The selection rule** (which of the re-run's clusters the recipe meant:
  the component holding the plurality of surviving members) is implemented
  beside the clusterer, in ``whg/webpack/js/locus_resolve.js``, and is a FIXED
  contract (place#172): change it and every recipe may resolve differently.

This module holds the server half: turning a recipe's ``retrieval`` block
back into the ``options`` that ``crc_search_status`` consumes, and describing
a recipe for people. The round trip is asserted at resolution time: the body
``crc_search_status`` actually sent is re-read through
``retrieval_from_search_body`` and must canonicalise to the recipe's own
``retrieval``; if it does not, the resolver refuses rather than serve an
answer to a different question.
"""
from __future__ import annotations

from typing import Optional

from places.locus_recipe import (RecipeError, decode_recipe_token, encode_recipe_token,
                                 parse_recipe, retrieval_from_search_body)

# The query-string parameter that carries the token. One name, one place.
LOCUS_TOKEN_PARAM = "r"

MODE_LABELS = {
    "exact": "exactly",
    "starts": "starts with",
    "in": "contains",
    "fuzzy": "fuzzy",
    "phonetic": "sounds like",
}
TEMPORAL_MODE_LABELS = {
    "possibly": "possibly (anything the record's dates do not rule out)",
    "definitely": "definitely (attested throughout the window)",
}
WEIGHT_LABELS = (("name", "Name"), ("spatial", "Location"), ("temporal", "Time"),
                 ("type", "Type"), ("link", "Links"))


def search_options_for_retrieval(retrieval: dict) -> dict:
    """The ``options`` dict that makes ``crc_search_status`` send exactly
    ``retrieval`` (plus the fixed-contract flags it always adds).

    The inverse of ``retrieval_from_search_body`` through the body builder in
    ``crc_search_status``; ``tests_locus`` proves the round trip against the
    REAL builder for every retrieval field.
    """
    out = {
        "qstr": retrieval.get("query") or "",
        "mode": retrieval["mode"],
        "size": retrieval["size"],
    }
    if retrieval.get("ccodes"):
        out["countries"] = list(retrieval["ccodes"])
    if retrieval.get("types"):
        out["types"] = list(retrieval["types"])
    if retrieval.get("aat_types"):
        out["aat_types"] = list(retrieval["aat_types"])
    if retrieval.get("namespaces"):
        out["namespaces"] = list(retrieval["namespaces"])
    if retrieval.get("bounds"):
        out["bounds"] = retrieval["bounds"]
    if retrieval.get("contained_in"):
        out["contained_in"] = list(retrieval["contained_in"])
        out["containment"] = retrieval["containment"]
    if retrieval.get("relation"):
        out["relation"] = retrieval["relation"]
    t = retrieval.get("temporal")
    if t:
        out["temporal"] = True
        if t.get("start") is not None:
            out["start"] = t["start"]
        if t.get("end") is not None:
            out["end"] = t["end"]
        out["undated"] = bool(t.get("undated"))
        out["temporal_mode"] = t["mode"]
    return out


def retrieval_round_trips(recipe: dict, body_sent: dict) -> Optional[str]:
    """None when ``body_sent`` (what ``crc_search_status`` posted) canonicalises
    to ``recipe["retrieval"]``; otherwise a sentence saying what differed."""
    try:
        got = parse_recipe({**recipe, "retrieval": retrieval_from_search_body(body_sent)})["retrieval"]
    except RecipeError as exc:
        return f"the body sent to the gateway is not a valid retrieval: {exc}"
    want = recipe["retrieval"]
    if got == want:
        return None
    keys = sorted(set(got) | set(want))
    diff = [k for k in keys if got.get(k) != want.get(k)]
    return "the body sent to the gateway differs from the recipe in: " + ", ".join(diff)


# --------------------------------------------------------------------------- description

def _bbox(geometry: dict):
    """[W, S, E, N] over every position in a Polygon/MultiPolygon/GeometryCollection."""
    xs, ys = [], []

    def walk(node):
        if isinstance(node, dict):
            if node.get("type") == "GeometryCollection":
                for g in node.get("geometries", []):
                    walk(g)
            else:
                walk(node.get("coordinates"))
        elif isinstance(node, list):
            if len(node) == 2 and all(isinstance(v, (int, float)) for v in node):
                xs.append(node[0])
                ys.append(node[1])
            else:
                for n in node:
                    walk(n)

    walk(geometry)
    if not xs:
        return None
    return [min(xs), min(ys), max(xs), max(ys)]


def _vertex_count(geometry: dict) -> int:
    n = 0

    def walk(node):
        nonlocal n
        if isinstance(node, dict):
            if node.get("type") == "GeometryCollection":
                for g in node.get("geometries", []):
                    walk(g)
            else:
                walk(node.get("coordinates"))
        elif isinstance(node, list):
            if len(node) == 2 and all(isinstance(v, (int, float)) for v in node):
                n += 1
            else:
                for m in node:
                    walk(m)

    walk(geometry)
    return n


def _year(y) -> str:
    if y is None:
        return "…"
    y = int(y)
    return f"{-y} BCE" if y < 0 else str(y)


def describe_recipe(recipe: dict) -> dict:
    """Human-readable rows for the page. ``recipe`` is canonical (parsed).

    There is no stoplist row here: the stoplist is not part of a recipe
    (place#325 ruling 3). The page shows the gateway's CURRENT list, from the
    resolver's response, in a row ``locus.js`` fills."""
    r = recipe["retrieval"]
    c = recipe["clustering"]
    retrieval = []
    if r.get("query"):
        retrieval.append(("Name", f"{MODE_LABELS.get(r['mode'], r['mode'])} “{r['query']}”"))
    else:
        retrieval.append(("Name", f"any (a spatial sweep); match mode {MODE_LABELS.get(r['mode'], r['mode'])}"))
    retrieval.append(("Candidates", f"the {r['size']} best-scoring records"))
    if r.get("ccodes"):
        retrieval.append(("Countries", ", ".join(r["ccodes"])))
    if r.get("namespaces"):
        retrieval.append(("Sources", ", ".join(r["namespaces"])))
    if r.get("types"):
        retrieval.append(("Source types", ", ".join(r["types"])))
    if r.get("aat_types"):
        retrieval.append(("AAT types", ", ".join(f"aat:{a}" for a in r["aat_types"])))
    if r.get("bounds"):
        bb = _bbox(r["bounds"])
        kind = r["bounds"].get("type")
        n = _vertex_count(r["bounds"])
        where = (f"{kind} with {n} vertices; extent "
                 f"{bb[0]:.4f}, {bb[1]:.4f} to {bb[2]:.4f}, {bb[3]:.4f} (lon, lat)") if bb else kind
        retrieval.append(("Within", where))
    if r.get("contained_in"):
        retrieval.append(("Contained in", f"{', '.join(r['contained_in'])} ({r['containment']} containment)"))
    if r.get("relation"):
        retrieval.append(("Spatial relation", r["relation"]))
    t = r.get("temporal")
    if t:
        span = f"{_year(t.get('start'))} to {_year(t.get('end'))}"
        mode = TEMPORAL_MODE_LABELS.get(t["mode"], t["mode"])
        undated = "; undated records included" if t.get("undated") else ""
        retrieval.append(("When", f"{span}, {mode}{undated}"))

    weights = [(label, f"{c['weights'][k]:g}") for k, label in WEIGHT_LABELS]
    clustering = [
        ("Merge threshold (θ)", f"{c['theta']:g}"),
        ("Same-gazetteer split margin", f"{c['same_ns_penalty']:g}"),
        ("Name threshold (τ name)", f"{c['tau_name']:g}"),
        ("Link threshold (τ link)", f"{c['tau_link']:g}"),
        ("Respect 'distinct' assertions", "yes" if c["respect_distinct"] else "no"),
    ]
    return {
        "retrieval": retrieval,
        "weights": weights,
        "clustering": clustering,
        "members": list(recipe["members"]),
    }


def load_token(token: str) -> tuple[Optional[dict], Optional[str], Optional[dict]]:
    """``(recipe, canonical_token, error)``. ``error`` is ``{"path", "message"}``
    for a token that is not a valid recipe; the page shows it as an error,
    never as a 500."""
    if not token:
        return None, None, {"path": "", "message": "no recipe token was given"}
    try:
        recipe = decode_recipe_token(token)
    except RecipeError as exc:
        return None, None, {"path": exc.path, "message": exc.message}
    return recipe, encode_recipe_token(recipe), None
