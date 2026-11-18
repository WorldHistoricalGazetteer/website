"""What the legacy v3 JSON endpoints do at and after the v4 cutover.

The endpoints (``/api/spatial/``, ``/api/db/``, ``/api/index/``, ``/api/place/...``,
``/api/geojson/``) are deprecated (``api.deprecation``). One setting decides what
they do, read on every request, so a flip needs only a settings reload:

``LEGACY_API_MODE``
    ``"deprecated"`` (the default): the legacy views answer, with the deprecation
    headers and notice. Exactly the behaviour before this module existed.

    ``"compat"``: the old URL and parameters are TRANSLATED to the successor API and
    the successor's own code answers, in the old envelope where that is feasible
    (a GeoJSON FeatureCollection for the searches), still carrying the deprecation
    headers and notice plus a ``compat`` member saying what was run, what was
    dropped, and why. A plain redirect cannot do this for the searches: ``POST
    /reconcile`` takes a JSON body and a token, and a 301/307 keeps GET and the
    query string. The successor needs a token, so compat does too: the caller adds
    ``?token=<token>`` (or ``Authorization: Bearer``) to the SAME old URL. A signed-in
    session counts. ``LEGACY_API_COMPAT_ANONYMOUS = True`` admits callers with no
    token, each IP limited to ``LEGACY_API_COMPAT_ANON_RATE`` requests a minute.

    ``"gone"``: HTTP 410 with a JSON body naming the replacement, the docs, the
    token requirement and, where the old request translates, the exact successor
    request to send instead.

``LEGACY_API_MODE_OVERRIDES``
    ``{endpoint key: mode}`` for one endpoint at a time (keys: ``spatial``, ``db``,
    ``index``, ``place``, ``place_src``, ``geojson``), e.g. ``{"geojson": "gone"}``.

``LEGACY_API_INAPP_PASSTHROUGH`` (default True)
    In ``compat`` and ``gone`` modes, a request from WHG's own pages (same-origin
    Referer or a session cookie, ``api.deprecation.request_origin``) still gets the
    legacy view, because the site itself still calls three of these endpoints
    (``whg/webpack/js/portal.js`` -> /api/spatial/?geometry=full,
    ``whg/webpack/js/getPlace.js`` -> /api/place/<id>, ``places/templates/places/
    place_detail.html`` -> /api/db/?id=). Set it False once those are migrated.

What each endpoint becomes in ``compat``:

=============  ==============================================================
spatial        ``POST /reconcile`` spatial-only query (``spatial_legacy``)
db ?name=      ``POST /reconcile`` name query; ``?id=`` -> Entity API place LPF
index ?name=   ``POST /reconcile`` name query; ``?pid=`` -> Entity API place LPF;
               ``?whgid=`` has no successor (410)
place/<id>     Entity API place LPF (one Feature; a list -> FeatureCollection)
place/<ds>/<src_id>  resolved to the place, then as place/<id>
geojson ?id=   307 to ``/entity/dataset:<id>/api`` (token carried over);
               ``?coll=`` has no successor (410)
=============  ==============================================================

The Entity API's place LPF is built in-process from the same queryset and
serializers ``EntityFeatureView`` uses (``api.schemas.TYPE_MAP['place']``), so one
compat request is one request against the caller's daily allowance, not one per id.
"""

import logging

from django.conf import settings
from django.core import checks
from django.http import HttpResponseRedirect, JsonResponse
from rest_framework.authentication import SessionAuthentication
from rest_framework.permissions import AllowAny
from rest_framework.throttling import BaseThrottle
from rest_framework.views import APIView

from api import spatial as spatial_api
from api.authentication import TokenQueryOrBearerAuthentication
from api.deprecation import PROFILE, TOKEN_DOCS, _ENDPOINTS, _TOKEN_TAIL
from api.spatial_legacy import DATASET_NOTE, NO_EQUIVALENT, to_reconcile_query
from api.throttling import consume_budget, consume_query_budget, throttle_message

logger = logging.getLogger("api.deprecation")

MODES = ("deprecated", "compat", "gone")
DEFAULT_MODE = "deprecated"
DEFAULT_ANON_RATE = 30
MAX_IDS = 50  # as /api/place/<id>-<id>-.../ always allowed

_RETIRED_LEAD = "This endpoint was retired when WHG moved to v4. "
# Query parameters that are about the transport, never the query.
_TRANSPORT_PARAMS = ("token", "format")


class LegacyParamError(ValueError):
    """A legacy request the old endpoint itself would have refused (400)."""


# ---------------------------------------------------------------------------
# Settings
# ---------------------------------------------------------------------------

def endpoint_mode(key):
    """The mode for one endpoint key. An unknown value is logged and treated as
    ``deprecated``: a typo must not retire an API (the system check below also
    reports it at startup)."""
    overrides = getattr(settings, "LEGACY_API_MODE_OVERRIDES", None) or {}
    mode = overrides.get(key) or getattr(settings, "LEGACY_API_MODE", None) or DEFAULT_MODE
    if mode not in MODES:
        logger.error("LEGACY_API_MODE %r for endpoint %s is not one of %s; serving 'deprecated'",
                     mode, key, MODES)
        return DEFAULT_MODE
    return mode


def inapp_passthrough():
    return bool(getattr(settings, "LEGACY_API_INAPP_PASSTHROUGH", True))


@checks.register()
def check_legacy_api_settings(app_configs=None, **kwargs):
    errors = []
    mode = getattr(settings, "LEGACY_API_MODE", None) or DEFAULT_MODE
    if mode not in MODES:
        errors.append(checks.Error(f"LEGACY_API_MODE is {mode!r}; it must be one of {MODES}.",
                                   id="api.E101"))
    overrides = getattr(settings, "LEGACY_API_MODE_OVERRIDES", None) or {}
    if not isinstance(overrides, dict):
        errors.append(checks.Error("LEGACY_API_MODE_OVERRIDES must be a dict {endpoint: mode}.",
                                   id="api.E102"))
    else:
        for key, value in overrides.items():
            if key not in _ENDPOINTS:
                errors.append(checks.Error(f"LEGACY_API_MODE_OVERRIDES key {key!r} is not one of "
                                           f"{tuple(_ENDPOINTS)}.", id="api.E103"))
            if value not in MODES:
                errors.append(checks.Error(f"LEGACY_API_MODE_OVERRIDES[{key!r}] is {value!r}; it must "
                                           f"be one of {MODES}.", id="api.E101"))
    return errors


# ---------------------------------------------------------------------------
# Translation: old request -> plan (pure apart from the dataset label lookup)
# ---------------------------------------------------------------------------

def _params(request):
    """The old query parameters as a flat dict (last value wins, as QueryDict.get
    does), without the token, which must never be echoed back."""
    return {k: request.GET.get(k) for k in request.GET if k not in _TRANSPORT_PARAMS}


def _split_upper(value):
    return sorted({x.strip().upper() for x in str(value).split(",") if x.strip()})


def _ccodes(value):
    codes = _split_upper(value)
    if not all(len(c) == 2 and c.isalpha() for c in codes):
        raise LegacyParamError("Each 'ccode' must be a 2-letter uppercase ISO code.")
    return codes


def _year(value):
    try:
        return int(value)
    except (TypeError, ValueError):
        raise LegacyParamError("'year' must be an integer.")


def _dataset_pk_for_label(label):
    from datasets.models import Dataset
    return Dataset.objects.filter(label=label).values_list("id", flat=True).first()


def _ids(raw):
    try:
        ids = [int(x) for x in raw if str(x).strip()]
    except ValueError:
        raise LegacyParamError("Place IDs must be integers.")
    if not ids:
        raise LegacyParamError("No place ID(s) provided.")
    if len(ids) > MAX_IDS:
        raise LegacyParamError(f"Too many IDs requested: {MAX_IDS} or fewer places at a time.")
    return ids


def _name_query(p, *, fc_param, size_default, size_max, dataset_is_label):
    """A name search on /api/db/ or /api/index/ as one /reconcile query object."""
    q, dropped, notes = {}, {}, []
    name = p.get("name") or p.get("name_contains") or p.get("namestartswith")
    q["query"] = name
    if p.get("name_contains") and not p.get("name"):
        notes.append("'name_contains' was a substring match on the title; /reconcile matches names "
                     "(fuzzy by default), so a fragment that is not a name may match nothing.")
    if p.get(fc_param):
        q["fclasses"] = _split_upper(p[fc_param])
    if p.get("ccode"):
        q["countries"] = _ccodes(p["ccode"])
    if p.get("year"):
        y = _year(p["year"])
        q["start"], q["end"] = y, y
    raw = str(p.get("pagesize") or size_default)
    if not raw.isdigit():
        raise LegacyParamError("'pagesize' must be an integer.")
    q["limit"] = max(1, min(int(raw), size_max))
    dataset_pk = None
    if p.get("dataset"):
        ds = p["dataset"]
        dataset_pk = _dataset_pk_for_label(ds) if dataset_is_label else (int(ds) if str(ds).isdigit() else None)
        if dataset_pk is None:
            raise LegacyParamError(f"The requested dataset {ds!r} does not exist.")
        q["dataset"] = dataset_pk
        q["namespaces"] = ["whg"]
        notes.append(DATASET_NOTE)
    for gone in ("context", "area"):
        if p.get(gone):
            dropped[gone] = f"No '{gone}' filter on /reconcile."
    return q, dataset_pk, dropped, notes


def plan_for(key, request, kwargs):
    """Translate one legacy request. Returns a plan dict whose ``kind`` is one of
    ``reconcile``, ``entity_places``, ``entity_src``, ``redirect``, ``none``
    (no successor for this request) or ``legacy`` (serve the legacy view: only
    the /api/place/ malformed-URL trap, which is an error message, not data).
    Raises LegacyParamError for a request the old endpoint would have refused."""
    p = _params(request)

    if key == "spatial":
        try:
            t = to_reconcile_query(p)
        except spatial_api.SpatialParamError as exc:
            raise LegacyParamError(str(exc))
        q = t["body"]["queries"]["q0"]
        return {"kind": "reconcile", "query": q, "dataset_pk": q.get("dataset"), "shape": "spatial",
                "dropped": t["dropped"], "notes": t["notes"], "params": p}

    if key == "db":
        if p.get("id"):
            return {"kind": "entity_places", "ids": _ids(p["id"].split(",")), "single": False, "shape": "db",
                    "dropped": {k: "'id' given, other parameters ignored (as before)." for k in p if k != "id"},
                    "notes": [], "params": p}
        if not (p.get("name") or p.get("name_contains")):
            raise LegacyParamError('Needs either a "name", a "name_contains", or "id" parameter at minimum '
                                   '(e.g. ?name=myplacename or ?name_contains=astring or ?id=integer).')
        raw = str(p.get("pagesize") or 20)
        if not raw.isdigit() or not 1 <= int(raw) <= 200:
            raise LegacyParamError("'pagesize' must be between 1 and 200.")
        q, ds_pk, dropped, notes = _name_query(p, fc_param="fc", size_default=20, size_max=200,
                                               dataset_is_label=True)
        return {"kind": "reconcile", "query": q, "dataset_pk": ds_pk, "shape": "db",
                "dropped": dropped, "notes": notes, "params": p}

    if key == "index":
        if p.get("whgid"):
            return {"kind": "none", "params": p,
                    "reason": "'whgid' (an index cluster of records) has no successor: v4 has no union "
                              "records. Search by name with POST /reconcile, or fetch one place with "
                              "GET /entity/place:<id>/api."}
        if p.get("pid"):
            return {"kind": "entity_places", "ids": _ids([p["pid"]]), "single": False, "shape": "index",
                    "dropped": {}, "notes": [], "params": p}
        if not p.get("name"):
            raise LegacyParamError("Query requires either name, pid, or whgid.")
        q, ds_pk, dropped, notes = _name_query(p, fc_param="fclass", size_default=100, size_max=1000,
                                               dataset_is_label=True)
        return {"kind": "reconcile", "query": q, "dataset_pk": ds_pk, "shape": "index",
                "dropped": dropped, "notes": notes, "params": p}

    if key == "place":
        if kwargs.get("pk") is not None:
            return {"kind": "entity_places", "ids": _ids([kwargs["pk"]]), "single": True, "shape": "place",
                    "dropped": {}, "notes": [], "params": p}
        if kwargs.get("pk_list") is not None:
            return {"kind": "entity_places", "ids": _ids(kwargs["pk_list"].split("-")), "single": False,
                    "shape": "place", "dropped": {}, "notes": [], "params": p}
        return {"kind": "legacy", "params": p}

    if key == "place_src":
        return {"kind": "entity_src", "dslabel": kwargs.get("dslabel"), "src_id": kwargs.get("src_id"),
                "shape": "place", "dropped": {}, "notes": [], "params": p}

    if key == "geojson":
        if p.get("id"):
            if not str(p["id"]).isdigit():
                raise LegacyParamError("'id' must be an integer dataset id.")
            return {"kind": "redirect", "path": f"/entity/dataset:{int(p['id'])}/api", "params": p,
                    "notes": ["The Entity API returns the whole dataset as one Linked Places (LPF) file, "
                              "not a paginated list of geometries."]}
        if p.get("coll"):
            return {"kind": "none", "params": p,
                    "reason": "No documented API yet serves a collection's geometries (?coll=)."}
        raise LegacyParamError("Query is missing a required parameter ('id' or 'coll').")

    raise KeyError(key)


def successor_request(plan, request):
    """The request a caller should send instead, as {method, url, body?}, or None."""
    kind = plan["kind"]
    if kind == "reconcile":
        return {"method": "POST", "url": request.build_absolute_uri("/reconcile"),
                "headers": {"Authorization": "Bearer <token>", "Content-Type": "application/json"},
                "body": {"queries": {"q0": plan["query"]}}}
    if kind == "entity_places":
        urls = [request.build_absolute_uri(f"/entity/place:{pk}/api") for pk in plan["ids"]]
        return {"method": "GET", "url": urls[0] if len(urls) == 1 else urls,
                "headers": {"Authorization": "Bearer <token>"}}
    if kind == "redirect":
        return {"method": "GET", "url": request.build_absolute_uri(plan["path"]),
                "headers": {"Authorization": "Bearer <token>"}}
    if kind == "entity_src":
        return {"method": "GET", "url": request.build_absolute_uri("/entity/place:<id>/api"),
                "note": "Look the place up by its WHG id; no API addresses it by dataset label and src_id."}
    return None


# ---------------------------------------------------------------------------
# Responses
# ---------------------------------------------------------------------------

def _json(data, status=200):
    return JsonResponse(data, status=status, json_dumps_params={"ensure_ascii": False})


def _replacement_block(key):
    switch, replacement, _successor, docs = _ENDPOINTS[key]
    return {"replacement": replacement, "docs": docs, "token_docs": TOKEN_DOCS, "token_required": True,
            "profile": PROFILE}, switch


def gone_response(key, request, kwargs):
    """HTTP 410: what replaced the endpoint and, when the old request translates,
    the successor request itself."""
    block, switch = _replacement_block(key)
    body = {"error": "gone", "retired": True, "endpoint": request.path,
            "message": _RETIRED_LEAD + switch + _TOKEN_TAIL, **block}
    try:
        plan = plan_for(key, request, kwargs)
    except LegacyParamError as exc:
        body.update({"successor_request": None, "translation_error": str(exc)})
    else:
        body["successor_request"] = successor_request(plan, request)
        if plan["kind"] == "none":
            body["no_successor"] = plan["reason"]
        for field in ("dropped", "notes"):
            if plan.get(field):
                body[field] = plan[field]
    return _json(body, status=410)


def _compat_member(plan, request):
    member = {"served_by": "compat", "successor_request": successor_request(plan, request)}
    if plan.get("dropped"):
        member["dropped"] = plan["dropped"]
    if plan.get("notes"):
        member["notes"] = plan["notes"]
    if plan["kind"] == "reconcile":
        member["no_equivalent"] = {k: v for k, v in NO_EQUIVALENT.items() if k != "auth"}
    return member


def _candidate_feature(c, request):
    pid = str(c.get("id") or "")
    place_id = pid[len("place:"):] if pid.startswith("place:") else pid
    point = c.get("repr_point")
    props = {"title": c.get("name"), "place_id": place_id, "namespace": c.get("namespace"),
             "ccodes": c.get("ccodes") or [], "score": c.get("score"), "match": c.get("match"),
             "alt_names": c.get("alt_names") or [], "place_types": c.get("place_types") or [],
             "uri": request.build_absolute_uri(f"/entity/place:{place_id}/api")}
    if "confidence" in c:
        props["confidence"] = c["confidence"]
    return {"type": "Feature", "id": pid,
            "geometry": {"type": "Point", "coordinates": list(point)} if point else None,
            "properties": props}


def _run_reconcile(plan, request):
    # Imported here: api.reconcile pulls in the gateway client and ES helpers.
    from api.reconcile import process_queries
    out = process_queries({"q0": dict(plan["query"])}, user=request.user)
    res = out.get("q0") or {}
    if res.get("error"):
        return _json({"error": res["error"], "compat": _compat_member(plan, request)}, status=400)
    candidates = res.get("result") or []
    if plan.get("dataset_pk"):
        prefix = f"place:whg:{plan['dataset_pk']}:"
        candidates = [c for c in candidates if str(c.get("id") or "").startswith(prefix)]
    features = [_candidate_feature(c, request) for c in candidates]
    body = {"type": "FeatureCollection", "features": features, "count": None,
            "pagesize": len(features), "parameters": plan["params"], "errors": None}
    if plan["shape"] == "spatial":
        body.update({"geometry_mode": "point", "match": plan["query"].get("relation")})
    if res.get("gateway"):
        body["gateway"] = res["gateway"]
    body["attribution"] = out.get("attribution")
    body["compat"] = _compat_member(plan, request)
    return _json(body)


def _place_features(request, ids):
    """Entity API LPF Features for these WHG place pks, in the order asked, built
    as EntityFeatureView builds them, for the places this user may see."""
    from api.schemas import TYPE_MAP
    from api.views_entity import _legacy_place_to_lpf
    cfg = TYPE_MAP["place"]
    objs = {o.pk: o for o in cfg["feature_queryset"](request.user).filter(pk__in=ids)}
    return [_legacy_place_to_lpf(cfg["feature_serializer"](objs[pk], context={"request": request}).data,
                                 request=request)
            for pk in ids if pk in objs]


def _not_found(plan, request, ids):
    return _json({"error": "No places found", "requested_ids": ids,
                  "message": f"No places found with ID(s): {', '.join(map(str, ids))}",
                  "compat": _compat_member(plan, request)}, status=404)


def _run_entity_places(plan, request):
    ids = plan["ids"]
    features = _place_features(request, ids)
    if not features:
        return _not_found(plan, request, ids)
    if plan["single"]:
        body = dict(features[0])
        body["compat"] = _compat_member(plan, request)
        return _json(body)
    body = {"type": "FeatureCollection", "features": features, "count": len(features),
            "pagesize": len(features), "requested_ids": ids, "parameters": plan["params"],
            "compat": _compat_member(plan, request)}
    return _json(body)


def _run_entity_src(plan, request):
    from api.dataset_access import visible_places_q
    from places.models import Place
    pks = list(Place.objects.filter(dataset__label=plan["dslabel"], src_id=plan["src_id"])
               .filter(visible_places_q(request.user)).order_by("id").values_list("id", flat=True)[:MAX_IDS])
    if not pks:
        return _json({"detail": "No Place matches the given query.",
                      "compat": _compat_member(plan, request)}, status=404)
    plan = {**plan, "ids": pks, "single": len(pks) == 1}
    if len(pks) > 1:
        plan["notes"] = plan["notes"] + [f"src_id {plan['src_id']!r} names {len(pks)} places in this "
                                         f"dataset; all are returned."]
    return _run_entity_places(plan, request)


def _redirect(plan, request):
    url = plan["path"]
    token = request.GET.get("token")
    if token:
        from urllib.parse import urlencode
        url += "?" + urlencode({"token": token})
    return HttpResponseRedirect(url, status=307)


def _client_ip(request):
    return BaseThrottle().get_ident(request)


class LegacyCompatView(APIView):
    """Serves one legacy endpoint in ``compat`` mode (see the module docstring).
    Authentication is the successor's: a token as ``?token=`` or Bearer, or a
    session."""
    authentication_classes = [TokenQueryOrBearerAuthentication, SessionAuthentication]
    permission_classes = [AllowAny]
    legacy_key = None
    legacy_view = None

    def get(self, request, *args, **kwargs):
        key = self.legacy_key
        try:
            plan = plan_for(key, request, kwargs)
        except LegacyParamError as exc:
            return _json({"error": str(exc)}, status=400)

        if plan["kind"] == "legacy":
            return self.legacy_view(request._request, *args, **kwargs)
        if plan["kind"] == "none":
            return gone_response(key, request, kwargs)

        if not getattr(request.user, "is_authenticated", False):
            if not getattr(settings, "LEGACY_API_COMPAT_ANONYMOUS", False):
                block, switch = _replacement_block(key)
                return _json({
                    "error": "API token required",
                    "message": ("This legacy endpoint is now served by its successor, which requires an API "
                                "token. Add ?token=<your token> to this same URL (or send "
                                "'Authorization: Bearer <token>')" + _TOKEN_TAIL),
                    **block, "successor_request": successor_request(plan, request)}, status=401)
            rate = int(getattr(settings, "LEGACY_API_COMPAT_ANON_RATE", DEFAULT_ANON_RATE) or 0)
            allowed, retry_after = consume_budget(f"legacy:anon:{_client_ip(request)}", rate)
            if not allowed:
                r = _json({"error": f"Rate limit exceeded: at most {rate} requests a minute without an API "
                                    f"token. Retry after {retry_after}s, or use a token" + _TOKEN_TAIL},
                          status=429)
                r["Retry-After"] = str(retry_after)
                return r

        if plan["kind"] == "reconcile":
            if request.user.is_authenticated:
                allowed, retry_after = consume_query_budget(request.user, 1)
                if not allowed:
                    r = _json({"error": throttle_message(retry_after)}, status=429)
                    r["Retry-After"] = str(retry_after)
                    return r
            return _run_reconcile(plan, request)
        if plan["kind"] == "entity_places":
            return _run_entity_places(plan, request)
        if plan["kind"] == "entity_src":
            return _run_entity_src(plan, request)
        if plan["kind"] == "redirect":
            return _redirect(plan, request)
        raise AssertionError(plan["kind"])


def compat_view(key, legacy_view):
    return LegacyCompatView.as_view(legacy_key=key, legacy_view=legacy_view)
