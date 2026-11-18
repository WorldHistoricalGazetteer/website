"""A location for a record that has none (place#350): adopted from a matched place, or placed
on the map by the dataset owner.

Two acts, both the dataset OWNER's alone (decision B; ``Dataset.owner``, not co-owners or
collaborators, who see the matched place's location as a reference), both separate from the match
itself (constraint 4: "Same place" never writes a geometry, and adopting never changes the match):

* **adopt** writes one ``PlaceGeom`` from the ``repr_point`` the matched suggestion carried in the
  feed (never a point sent by the browser), with ``geom_src`` = the source gazetteer,
  ``task_id`` = ``gen:<the candidate's generation id>`` and ``reviewer`` = the owner. Its ``jsonb``
  is the LPF geometry plus ``citation`` and ``provenance`` (match id, terms, date), which the
  dataset's downloads carry with the geometry (constraint 6).
* **place** writes one ``PlaceGeom`` that is the owner's own data: ``geom_src`` empty, a Point
  rounded to what the map's zoom supports, ``certainty`` ``less-certain`` and ``provenance`` saying
  it was placed on the map. It never snaps to a marker and never names a suggestion (decision G):
  the request has no way to name one, and one that tries is refused.

Both are refused when the record already has a location of any kind, when the dataset is declared
sensitive (either sensitivity question answered "yes" or "not sure"), and for anyone but the owner.
Adoption is further gated per source (decision C, #240): the namespace must be in the safe group,
its registry terms must not be share-alike, non-commercial or no-derivatives (an unstated value
counts as a restriction), it must be a source of points (v1 is points only: a centroid of a
boundary is not offered), the suggestion must be marked "Same place" and not withheld, and the
dataset must have a licence. Anything that fails is a reference: id, link and citation, no copy.

``remove`` deletes exactly one row written here (by pk, and only a row carrying this module's
``provenance``); a geometry the contributor loaded can never be removed through it.
"""
import math

from django.db import transaction
from django.utils import timezone

# Decision C: public domain / attribution-only sources (place#240's measured safe group).
SAFE_SOURCES = frozenset({"wd", "gn", "ohm", "po", "pl", "ofs", "clio", "un", "tgn", "ukhc"})
# Points only in v1: the safe sources whose records are points. The others in the safe group
# (ohm, po, clio, un, ukhc: boundaries; ofs: not known here) describe places mostly by polygons,
# and a centroid of a polygon is not a point the source published (constraint 3).
POINT_SOURCES = frozenset({"wd", "gn", "tgn", "pl"})
SOURCE_LABELS = {"gn": "GeoNames", "wd": "Wikidata", "tgn": "Getty TGN", "pl": "Pleiades",
                 "osm": "OpenStreetMap", "ohm": "OpenHistoricalMap"}
SENSITIVE_ANSWERS = ("yes", "unsure")
ADOPTED, PLACED = "adopted", "placed"
PLACED_TASK = "feed-placed"
MAX_ZOOM = 17          # the feed map's maxZoom
MAX_DECIMALS = 5       # about a metre; the feed never stores finer

MESSAGES = {
    "owner_only": "Only the dataset owner can adopt or place a location.",
    "sensitive": "This dataset is declared sensitive, so no location is offered.",
    "has_location": "This record already has a location.",
    "withheld": "This location is not available to you.",
    "not_safe_source": "This source's location can be kept as a reference, not adopted.",
    "not_point_source": "Only point locations can be adopted for now; this source describes places by boundaries.",
    "no_terms": "This source's terms are not recorded, so its location can be kept as a reference only.",
    "terms": "This source's terms do not allow its location to be adopted into your dataset; it is kept as a reference.",
    "dataset_licence": "Choose a licence for your dataset before adopting a location.",
    "no_location": "This suggestion has no location.",
    "not_matched": "Mark this suggestion as the same place first.",
}
HINT_TERMS = "hint only: location not adoptable (terms)"


class LocationRefused(Exception):
    def __init__(self, status, reason):
        super().__init__(reason)
        self.status, self.reason, self.message = status, reason, MESSAGES.get(reason, reason)


# ---------------------------------------------------------------------------
# Who, and on what
# ---------------------------------------------------------------------------

def is_owner(user, ds):
    return bool(user and user.is_authenticated and ds.owner_id == user.pk)


def is_sensitive(ds):
    return (ds.sensitivity_indigenous in SENSITIVE_ANSWERS
            or ds.sensitivity_restricted in SENSITIVE_ANSWERS)


def provenance(g):
    j = g.jsonb if isinstance(g.jsonb, dict) else {}
    p = j.get("provenance")
    return p if isinstance(p, dict) and p.get("kind") in (ADOPTED, PLACED) else None


def location_info(place, geoms=None):
    """What the record's location is: None (no location), ``own`` (one the contributor loaded),
    or the adopted / placed row with its source, date and person."""
    geoms = list(place.geoms.all()) if geoms is None else list(geoms)
    if not geoms:
        return None
    for g in geoms:
        p = provenance(g)
        if p:
            return {"kind": p["kind"], "geom": g.pk, "source": p.get("source"),
                    "source_label": p.get("source_label"), "match": p.get("match"),
                    "terms": p.get("terms"),
                    "date": g.create_date.isoformat() if g.create_date else None,
                    "by": getattr(g.reviewer, "name", None) or getattr(g.reviewer, "username", None)}
    return {"kind": "own", "geom": None}


# ---------------------------------------------------------------------------
# The per-source gate
# ---------------------------------------------------------------------------

def registry_entries(namespaces):
    from api.models import GazetteerRegistryEntry
    ns = {n for n in namespaces if n}
    if not ns:
        return {}
    return {e.id: e for e in GazetteerRegistryEntry.objects.filter(id__in=ns, entry_class="authority")
            .select_related("license")}


def source_reason(ns, entry):
    """None when ``ns``'s location may be copied into a contributor's dataset, else why not."""
    if ns not in SAFE_SOURCES:
        return "terms" if entry is not None and entry.license_id else "not_safe_source"
    if entry is None or entry.license is None:
        return "no_terms"
    lic = entry.license
    # NULL is "not evidenced either way": not a permission, so it is treated as a restriction.
    if lic.share_alike or lic.permits_commercial is not True or lic.no_derivatives is not False:
        return "terms"
    if ns not in POINT_SOURCES:
        return "not_point_source"
    return None


def _local_id(candidate_id):
    return str(candidate_id).split(":", 1)[1] if ":" in str(candidate_id) else str(candidate_id)


def reference(c, entry):
    """Id, link and citation of a suggestion's place: what a reference carries instead of a copy."""
    link = None
    tpl = getattr(entry, "web_item", None)
    if tpl and "<id>" in tpl:
        link = tpl.replace("<id>", _local_id(c.candidate_id))
    citation = (getattr(entry, "citation_text", None) or "").strip() or None
    lic = getattr(entry, "license", None)
    terms = None if lic is None else {"id": lic.spdx_id, "label": lic.label,
                                      "url": getattr(entry, "license_url", None) or lic.url or None}
    return {"id": c.candidate_id, "link": link, "citation": citation, "terms": terms,
            "source_label": source_label(c.namespace, entry)}


def source_label(ns, entry=None):
    return SOURCE_LABELS.get(ns) or (getattr(entry, "name", None) or ns)


def adopt_info(c, location, withheld, entry, ds, user, record_info, hidden_ns=frozenset()):
    """What the feed says about adopting ``c``'s location, for ``user``. ``adoptable`` is about
    the source alone (it decides the map's hint label); ``offer`` is whether the button is shown."""
    if withheld or (c.namespace or "") in hidden_ns:
        return {"adoptable": False, "offer": False, "reason": "withheld", "hint": None}
    reason = source_reason(c.namespace or "", entry)
    if reason is None and location is None:
        reason = "no_location"
    out = {"adoptable": reason is None, **reference(c, entry)}
    out["hint"] = HINT_TERMS if reason in ("terms", "no_terms", "not_safe_source") else (
        "hint only: location not adoptable (boundary source)" if reason == "not_point_source" else None)
    if reason is None:
        if not is_owner(user, ds):
            reason = "owner_only"
        elif is_sensitive(ds):
            reason = "sensitive"
        elif record_info is not None:
            reason = "has_location"
        elif ds.license_id is None:
            reason = "dataset_licence"
        elif c.status != "accepted":
            reason = "not_matched"
    if reason == "sensitive":
        return {"adoptable": False, "offer": False, "reason": "sensitive", "hint": None}
    out.update(offer=reason is None, reason=reason, message=MESSAGES.get(reason) if reason else None)
    # "not_matched" is the last gate: everything else allows it, so the page may offer it as soon
    # as the person marks the suggestion "Same place" (the server checks again when asked).
    if reason in (None, "not_matched"):
        lic = entry.license
        out["statement"] = statement(source_label(c.namespace, entry), lic.spdx_id)
    return out


def statement(label, terms):
    return (f"This location is adopted from {label} under {terms}. It is labelled as coming from "
            f"{label}, and downloads of the dataset say so.")


# ---------------------------------------------------------------------------
# Writes
# ---------------------------------------------------------------------------

def _locked_place(ds, place_pk):
    from places.models import Place
    place = Place.objects.select_for_update().filter(pk=place_pk, dataset=ds.label).first()
    if place is None:
        raise LocationRefused(404, "not found")
    return place


def _common_gates(user, ds, place):
    if not is_owner(user, ds):
        raise LocationRefused(403, "owner_only")
    if is_sensitive(ds):
        raise LocationRefused(409, "sensitive")
    if place.geoms.exists():
        raise LocationRefused(409, "has_location")


def _authority_source(ns, entry):
    """The ``sources`` row ``geom_src`` points at for an authority (created once, ownerless)."""
    from places.models import Source
    src, _ = Source.objects.get_or_create(
        src_id=ns[:30], defaults={"label": source_label(ns, entry)[:255], "owner": None,
                                  "uri": getattr(entry, "source_url", None) or None,
                                  "citation": ((getattr(entry, "citation_text", None) or "")[:500] or None)})
    return src


def _clear_download_cache(ds):
    def clear():
        try:
            from api.download_file import FileCache
            for ft in ("lpf", "tsv"):
                FileCache.delete_cache("dataset", ds.pk, filetype=ft)
        except Exception:  # noqa: BLE001 - a stale cache is rebuilt later; never fail the write
            pass
    transaction.on_commit(clear)
    from datasets.location_index import location_changed  # the index re-reads the dataset too
    transaction.on_commit(lambda: location_changed(ds.pk))


def adopt(user, ds, place_pk, candidate_pk):
    """Adopt the location of one matched suggestion. Returns the new ``PlaceGeom``."""
    from django.contrib.gis.geos import Point
    from api.models import ContributorAttestation, GazetteerRegistryEntry
    from datasets.models_matching import MatchCandidate
    from datasets.views_feed import candidate_location, visible_contributed_pks
    from places.models import PlaceGeom
    with transaction.atomic():
        place = _locked_place(ds, place_pk)
        c = MatchCandidate.objects.filter(pk=candidate_pk, place=place).exclude(status="stale").first()
        if c is None:
            raise LocationRefused(404, "not found")
        if not is_owner(user, ds):
            raise LocationRefused(403, "owner_only")
        if is_sensitive(ds):
            raise LocationRefused(409, "sensitive")
        location, withheld = candidate_location(c, visible_contributed_pks(user, [c]))
        if withheld or (c.namespace or "") in GazetteerRegistryEntry.objects.hidden_namespaces_for(user):
            raise LocationRefused(409, "withheld")
        entry = registry_entries([c.namespace]).get(c.namespace)
        reason = source_reason(c.namespace or "", entry)
        if reason:
            raise LocationRefused(409, reason)
        if place.geoms.exists():
            raise LocationRefused(409, "has_location")
        if ds.license_id is None:
            raise LocationRefused(409, "dataset_licence")
        att = (ContributorAttestation.objects.filter(candidate=c, status__in=("active", "pending"))
               .exclude(relation_type="distinct").order_by("-id").first())
        if c.status != "accepted" or att is None:
            raise LocationRefused(409, "not_matched")
        if location is None:
            raise LocationRefused(409, "no_location")
        label = source_label(c.namespace, entry)
        terms = entry.license.spdx_id
        now = timezone.now()
        jsonb = {"type": "Point", "coordinates": [location[0], location[1]],
                 "citation": {"id": c.candidate_id, "label": label},
                 "provenance": {"kind": ADOPTED, "source": c.namespace, "source_label": label,
                                "match": c.candidate_id, "terms": terms,
                                "terms_url": entry.license_url or entry.license.url or None,
                                "attribution": (entry.citation_text or None),
                                "adopted": now.date().isoformat(),
                                "statement": statement(label, terms),
                                "candidate": c.pk, "attestation": att.pk, "generation": c.generation_id}}
        g = PlaceGeom.objects.create(place=place, src_id=place.src_id, jsonb=jsonb,
                                     geom=Point(location[0], location[1], srid=4326),
                                     geom_src=_authority_source(c.namespace, entry),
                                     task_id=f"gen:{c.generation_id}", reviewer=user)
        _clear_download_cache(ds)
    return g


def decimals_for_zoom(zoom):
    """Decimal places a click at ``zoom`` can support: about one screen pixel (256 px tiles)."""
    z = max(0.0, min(float(MAX_ZOOM), float(zoom)))
    return int(max(0, min(MAX_DECIMALS, round(math.log10(256 * 2 ** z / 360.0)))))


PLACE_KEYS = {"action", "place", "lon", "lat", "zoom"}


def _finite(v):
    if isinstance(v, bool):
        raise ValueError
    f = float(v)
    if not math.isfinite(f):
        raise ValueError
    return f


def place_by_hand(user, ds, place_pk, body):
    """The owner's own point for a record, from a click on the map. ``body`` may carry only
    ``lon``, ``lat`` and ``zoom``: anything naming a suggestion or a source is refused."""
    from django.contrib.gis.geos import Point
    from places.models import PlaceGeom
    extra = set(body) - PLACE_KEYS
    if extra:
        raise LocationRefused(400, "only lon, lat and zoom may be sent: " + ", ".join(sorted(extra)))
    try:
        lon, lat, zoom = _finite(body.get("lon")), _finite(body.get("lat")), _finite(body.get("zoom"))
    except (TypeError, ValueError):
        raise LocationRefused(400, "lon, lat and zoom must be numbers")
    if not (-180 <= lon <= 180 and -90 <= lat <= 90) or not (0 <= zoom <= 24):
        raise LocationRefused(400, "lon, lat or zoom out of range")
    d = decimals_for_zoom(zoom)
    lon, lat = round(lon, d), round(lat, d)
    if d == 0:
        lon, lat = float(lon), float(lat)
    with transaction.atomic():
        place = _locked_place(ds, place_pk)
        _common_gates(user, ds, place)
        now = timezone.now()
        jsonb = {"type": "Point", "coordinates": [lon, lat], "certainty": "less-certain",
                 "provenance": {"kind": PLACED, "method": "placed on the map", "approximate": True,
                                "zoom": int(zoom) if float(zoom).is_integer() else zoom, "decimals": d,
                                "placed": now.date().isoformat()}}
        g = PlaceGeom.objects.create(place=place, src_id=place.src_id, jsonb=jsonb,
                                     geom=Point(lon, lat, srid=4326), geom_src=None,
                                     task_id=PLACED_TASK, reviewer=user)
        _clear_download_cache(ds)
    return g


def remove(user, ds, place_pk, geom_pk):
    """Delete exactly one adopted or placed row. A geometry the contributor loaded is not found."""
    from places.models import PlaceGeom
    with transaction.atomic():
        place = _locked_place(ds, place_pk)
        g = PlaceGeom.objects.filter(pk=geom_pk, place=place).first()
        if g is None or provenance(g) is None:
            raise LocationRefused(404, "not found")
        if not is_owner(user, ds):
            raise LocationRefused(403, "owner_only")
        PlaceGeom.objects.filter(pk=g.pk).delete()
        _clear_download_cache(ds)


def adopted_rows(place):
    return [g for g in place.geoms.all() if (provenance(g) or {}).get("kind") == ADOPTED]


def adopted_from(place):
    """The suggestion ids a record's adopted locations came from (decision D's guard)."""
    return {str(p.get("match")) for p in (provenance(g) for g in place.geoms.all()) if p and p.get("kind") == ADOPTED and p.get("match")}
