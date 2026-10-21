"""The group a Record page's enrichment describes (place#325 Stage 3).

Ruling (a): the gloss and the EDOP signature describe the whole group, so they need a
stated subset: the record itself plus the concordance AGGREGATE (tiers 1-2,
exactMatch/closeMatch, resolved). Tier 3 (co-citation) never contributes. This module
turns ``places.concordance.build_concordance`` into one normalised member list that
``places.gloss`` and ``api.edop_client`` both read, so the two sections cannot disagree
about who is in the group.

Member (plain, JSON-able)::

    {"key": "whg:<pk>" | "<ns>:<id>", "place_id", "title", "source", "via": "record" |
     "postgres" | "gateway", "is_record": bool, "names": [str], "types": [str],
     "ccodes": [str], "years": [start, end] | None, "part_of": [str],
     "repr_point": [lon, lat] | None, "hull_wkt": str | None, "detail": "full" |
     "unavailable" | "disabled"}

``detail`` says whether the attributes were read: a gateway member whose details the
index could not return is ``unavailable``, and the gloss must say "not known", never
"none". The record itself and Postgres members are always ``full``.

Cost: ``build_concordance`` (0.3-3 s on prod) plus one gateway call for the gateway
members' details. Two endpoints read the group, so it is cached ``GROUP_CACHE_TTL``
seconds per (record, user): the user matters only for gateway namespace embargoes.
"""
from __future__ import annotations

import json
import logging
from typing import Optional

logger = logging.getLogger(__name__)

GROUP_CACHE_TTL = 300
GROUP_CACHE_VERSION = 2  # 2: members carry ``offsite`` (mark_offsite)
GATEWAY_BATCH = 50

# GeoNames feature classes: the only gateway namespace whose `types.label` is a place
# type at all (place#267: for every other namespace it is the source-vocabulary tag).
GN_FEATURE_CLASSES = {
    "a": "administrative area", "h": "water feature", "l": "area or locality",
    "p": "populated place", "r": "road or railway", "s": "spot, building or farm",
    "t": "terrain feature", "u": "undersea feature", "v": "vegetation",
}
PART_OF_RELATIONS = {"gvp:broaderpartitive", "broaderpartitive", "partof", "part of", "broader",
                     "gvp:broader", "within"}


def _clean(s) -> str:
    return " ".join(str(s or "").split())


def _years_from_minmax(minmax, timespans=None) -> Optional[list]:
    vals = []
    if isinstance(minmax, (list, tuple)):
        vals += [v for v in minmax if isinstance(v, int) and not isinstance(v, bool)]
    for ts in timespans or []:
        if isinstance(ts, (list, tuple)):
            vals += [v for v in ts if isinstance(v, int) and not isinstance(v, bool)]
    return [min(vals), max(vals)] if vals else None


def years_from_gateway(rec: dict) -> Optional[list]:
    vals = []
    tr = rec.get("temporal_range")
    if isinstance(tr, (list, tuple)):
        vals += [v for v in tr if isinstance(v, int) and not isinstance(v, bool)]
    for ts in rec.get("timespans") or []:
        if isinstance(ts, dict):
            for b in ("start", "end"):
                v = ts.get(b)
                if isinstance(v, dict) and v:
                    v = next(iter(v.values()))
                try:
                    if v is not None and not isinstance(v, bool):
                        vals.append(int(v))
                except (TypeError, ValueError):
                    pass
    return [min(vals), max(vals)] if vals else None


def gateway_types(rec: dict, namespace: str) -> list:
    """Place types from a gateway record, or [] when its labels are not types.

    ``types.label`` is the source-vocabulary tag for every namespace except ``gn``
    (place#267), so only GeoNames feature classes are read, mapped to words; any other
    label is kept only if it is not a namespace tag. Honest, and thin, until #267's
    durable fix."""
    out = []
    ns = (namespace or "").lower()
    for t in rec.get("types") or []:
        label = t.get("label") if isinstance(t, dict) else t
        label = _clean(label)
        if not label:
            continue
        low = label.lower()
        if ns == "gn":
            if low in GN_FEATURE_CLASSES:
                out.append(GN_FEATURE_CLASSES[low])
            continue
        if low == ns or low in ("tgn", "wikidata", "wd", "trismegistos", "tm", "pleiades", "osm", "ohm"):
            continue
        out.append(label)
    return list(dict.fromkeys(out))


def gateway_names(rec: dict) -> list:
    out = []
    for n in rec.get("names") or []:
        label = n.get("label") if isinstance(n, dict) else n
        label = _clean(label)
        if label:
            out.append(label)
    return list(dict.fromkeys(out))


def _point_and_hull(geoms):
    """(repr_point [lon, lat] | None, convex hull WKT | None) of GEOS geometries."""
    from django.contrib.gis.geos import GeometryCollection
    gs = [g for g in geoms if g is not None and not g.empty]
    if not gs:
        return None, None
    try:
        gc = GeometryCollection(*[g.clone() for g in gs], srid=4326)
        return list(gc.centroid.coords), gc.convex_hull.wkt
    except Exception:  # noqa: BLE001 — a bad geometry is "no location", not a crash
        return None, None


def postgres_members(pks: list, *, record_pk: int) -> dict:
    """Full members for WHG records, by pk, in four queries."""
    from places.models import Place, PlaceGeom, PlaceName, PlaceRelated, PlaceType
    pks = list(dict.fromkeys(int(p) for p in pks))
    if not pks:
        return {}
    places = {p.pk: p for p in Place.objects.filter(pk__in=pks).select_related("dataset")}
    names, types, parts, geoms = {}, {}, {}, {}
    for pid, j, top in PlaceName.objects.filter(place_id__in=pks).values_list("place_id", "jsonb", "toponym"):
        label = _clean((j or {}).get("toponym") if isinstance(j, dict) else None) or _clean(top)
        if label:
            names.setdefault(pid, []).append(label)
    for pid, j in PlaceType.objects.filter(place_id__in=pks).values_list("place_id", "jsonb"):
        j = j if isinstance(j, dict) else {}
        label = _clean(j.get("label") or j.get("sourceLabel") or j.get("source_label") or j.get("src_label"))
        if label:
            types.setdefault(pid, []).append(label)
    for pid, j in PlaceRelated.objects.filter(place_id__in=pks).values_list("place_id", "jsonb"):
        j = j if isinstance(j, dict) else {}
        rel = _clean(j.get("relationType") or j.get("relation_type")).lower()
        label = _clean(j.get("label") or j.get("relationTo") or j.get("relation_to"))
        if rel in PART_OF_RELATIONS and label:
            parts.setdefault(pid, []).append(label)
    for pid, g in PlaceGeom.objects.filter(place_id__in=pks, geom__isnull=False).values_list("place_id", "geom"):
        geoms.setdefault(pid, []).append(g)
    out = {}
    for pk, p in places.items():
        rp, hull = _point_and_hull(geoms.get(pk, []))
        out[pk] = {
            "key": f"whg:{pk}", "place_id": pk, "title": p.title,
            "source": getattr(p.dataset, "title", None) or "WHG dataset",
            "via": "record" if pk == record_pk else "postgres", "is_record": pk == record_pk,
            "names": list(dict.fromkeys([_clean(p.title)] + names.get(pk, []))),
            "types": list(dict.fromkeys(types.get(pk, []))),
            "ccodes": sorted({c for c in (p.ccodes or []) if c}),
            "years": _years_from_minmax(p.minmax, p.timespans),
            "part_of": list(dict.fromkeys(parts.get(pk, []))),
            "repr_point": rp, "hull_wkt": hull, "detail": "full",
        }
    return out


def gateway_members(aggregate_entries: list, user=None) -> list:
    """Members for gateway-held records, with their details from ``POST /api/places``.
    On failure each member keeps its title and point from the concordance and is marked
    ``detail: "unavailable"`` (or ``"disabled"``)."""
    from api.crc_client import crc_fetch_places
    from api.models import GazetteerRegistryEntry
    from django.contrib.gis.geos import GEOSGeometry

    ids = [str(m["place_id"]) for m in aggregate_entries]
    got, status = {}, "full"
    for i in range(0, len(ids), GATEWAY_BATCH):
        meta: dict = {}
        got.update(crc_fetch_places(ids[i:i + GATEWAY_BATCH], user=user, meta=meta) or {})
        if meta.get("disabled"):
            status = "disabled"
        elif meta.get("error"):
            status = "unavailable"
    names_by_ns = dict(GazetteerRegistryEntry.objects.visible_to(user)
                       .filter(namespace__in={i.split(":", 1)[0] for i in ids})
                       .exclude(entry_class="dataset").values_list("namespace", "name"))
    out = []
    for m in aggregate_entries:
        pid = str(m["place_id"])
        ns = m.get("namespace") or pid.split(":", 1)[0]
        rec = got.get(pid)
        base = {"key": pid, "place_id": pid, "title": m.get("title") or pid,
                "source": names_by_ns.get(ns) or ns, "namespace": ns, "via": "gateway", "is_record": False,
                "names": [], "types": [], "ccodes": [], "years": None, "part_of": [],
                "repr_point": m.get("repr_point"), "hull_wkt": None}
        if rec is None:
            out.append({**base, "names": [_clean(base["title"])], "detail": status if status != "full" else "unavailable"})
            continue
        geoms = []
        for g in rec.get("geometries") or []:
            if isinstance(g, dict):
                gj = g.get("geometry") if isinstance(g.get("geometry"), dict) else g
                if gj.get("type") and gj.get("coordinates"):
                    try:
                        geoms.append(GEOSGeometry(json.dumps({"type": gj["type"], "coordinates": gj["coordinates"]}),
                                                  srid=4326))
                    except Exception:  # noqa: BLE001
                        pass
        _rp, hull = _point_and_hull(geoms)
        out.append({**base,
                    "title": rec.get("title") or base["title"],
                    "names": list(dict.fromkeys([_clean(rec.get("title") or base["title"])] + gateway_names(rec))),
                    "types": gateway_types(rec, ns),
                    "ccodes": sorted({c for c in (rec.get("ccodes") or []) if c}),
                    "years": years_from_gateway(rec),
                    "repr_point": rec.get("repr_point") or base["repr_point"],
                    "hull_wkt": hull, "detail": "full"})
    return out


def mark_offsite(members: list) -> list:
    """Set ``offsite`` on each member, in place: True when the member's data may be
    sent off-site (to EDOPS, computingplace.org, where requests are cached 30 days),
    which is when an ANONYMOUS caller could see it. Returns ``members``.

    🛑 The rule: nothing sent to EDOPS may derive from a record or namespace an
    anonymous caller cannot see. A beta user sees embargoed gateway namespaces
    (``visible_to(user)`` admits them), so they reach the concordance and the group;
    they may stay on the page for that user, but their geometry must not leave WHG.

    * ``whg:<pk>`` (Record page, Postgres members): the record passes
      ``visible_places_q(None)``; ``whg:<ds>:<src>`` (a locus page's posted hit):
      its dataset is public and not embargoed; any other ``whg:`` form cannot be
      checked and is withheld;
    * ``<ns>:<id>``: ``ns`` has a non-dataset registry row visible to an anonymous
      caller. A namespace with no registry row is withheld (fail closed).

    Fails closed: a member whose key cannot be read is withheld."""
    from api.dataset_access import hidden_datasets, visible_places_q
    from api.models import GazetteerRegistryEntry
    from places.models import Place

    def ns_of(m):
        key = str(m.get("key") or "")
        return (m.get("namespace") or key.split(":", 1)[0]).lower() if ":" in key else ""

    pks, nss = {}, set()
    for m in members:
        key = str(m.get("key") or "")
        if ns_of(m) == "whg":
            tail = key.split(":", 1)[1]
            if tail.isdigit():
                pks[key] = int(tail)
        elif ns_of(m):
            nss.add(ns_of(m))
    visible_pks = set(Place.objects.filter(pk__in=set(pks.values())).filter(visible_places_q(None))
                      .values_list("pk", flat=True)) if pks else set()
    anon_ns = set(GazetteerRegistryEntry.objects.visible_to(None).filter(namespace__in=nss)
                  .exclude(entry_class="dataset").values_list("namespace", flat=True)) if nss else set()
    hidden = None
    for m in members:
        key, ns = str(m.get("key") or ""), ns_of(m)
        if ns == "whg":
            if key in pks:
                ok = pks[key] in visible_pks
            else:
                hidden = hidden if hidden is not None else hidden_datasets(None)
                ok = not hidden.hides_contributed_id(key)
        else:
            ok = bool(ns) and ns in anon_ns
        m["offsite"] = ok
    return members


def offsite_members(members: list) -> list:
    """The members ``mark_offsite`` cleared; an unmarked member is NOT cleared."""
    return [m for m in members if m.get("offsite") is True]


def build_record_group(place, user=None) -> dict:
    """``{"public": bool, "members": [member], "n_equivalents": int}``; the record
    first. Not public -> ``{"public": False, "members": []}``: like the concordance,
    nothing is looked up (and no coordinate is sent anywhere)."""
    from places.concordance import build_concordance
    conc = build_concordance(place, user=user)
    if conc is None:
        return {"public": False, "members": [], "n_equivalents": 0, "unresolved": 0}
    agg = conc.get("aggregate") or []
    pg_ids = [m["place_id"] for m in agg if m.get("via") == "postgres" and isinstance(m.get("place_id"), int)]
    gw = [m for m in agg if m.get("via") == "gateway"]
    pg = postgres_members([place.pk] + pg_ids, record_pk=place.pk)
    for pk in pg_ids:
        if pk in pg:
            pg[pk]["tiers"] = next((m.get("tiers") for m in agg if m.get("place_id") == pk), [])
    members = [pg[place.pk]] if place.pk in pg else []
    members += [pg[pk] for pk in dict.fromkeys(pg_ids) if pk in pg and pk != place.pk]
    gws = gateway_members(gw, user=user) if gw else []
    for m in gws:
        m["tiers"] = next((a.get("tiers") for a in gw if str(a.get("place_id")) == m["key"]), [])
    members += gws
    # An aggregating link whose target the index could not be asked about is MISSING from
    # the aggregate, not absent from the world: the group is then known to be incomplete,
    # and both sections must say so rather than describe a smaller group as if it were all.
    unresolved = sum(1 for e in conc.get("outbound") or []
                     if e.get("aggregates") and (e.get("target") or {}).get("status")
                     in ("gateway_unavailable", "gateway_disabled"))
    # Every member is marked for EDOPS by the ANONYMOUS rule, whoever is asking: the
    # beta user's own view (the concordance, the gloss) keeps embargoed members.
    mark_offsite(members)
    return {"public": True, "members": members, "n_equivalents": len(members) - 1, "unresolved": unresolved}


def record_group(place, user=None) -> dict:
    """``build_record_group``, cached ``GROUP_CACHE_TTL`` s per (record, user)."""
    from django.core.cache import cache
    uid = getattr(user, "pk", None) if getattr(user, "is_authenticated", False) else "anon"
    key = f"record_group:v{GROUP_CACHE_VERSION}:{place.pk}:{uid}"
    try:
        hit = cache.get(key)
    except Exception:  # noqa: BLE001
        hit = None
    if isinstance(hit, dict):
        return hit
    group = build_record_group(place, user=user)
    # A gateway outage must not pin a thin group in the cache for five minutes.
    if not group.get("unresolved") and not any(m.get("detail") == "unavailable" for m in group["members"]):
        try:
            cache.set(key, group, GROUP_CACHE_TTL)
        except Exception:  # noqa: BLE001
            pass
    return group


def group_span(members: list) -> Optional[list]:
    spans = [m["years"] for m in members if m.get("years")]
    if not spans:
        return None
    return [min(s[0] for s in spans), max(s[1] for s in spans)]
