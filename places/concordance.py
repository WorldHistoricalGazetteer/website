"""The concordance of a gazetteer record (place#325, Stage 1).

A concordance is a VIEW of identity attestations about one record, one step
from it. It is not an entity and mints no identifier. Three tiers:

1. **outbound**: this record's own links (``place_link``), plus record-to-record
   pairs (``close_matches``) that this record's dataset attested;
2. **inbound**: other records' attestations that they are the same as this one:
   their ``place_link`` rows naming one of this record's own identifiers, and
   ``close_matches`` pairs attested from the partner's side or of unknown
   direction. Each is attributed to the attesting record and dataset;
3. **cocited**: records that cite one of the same identifiers this record
   cites. Co-citation, NOT identity: nobody asserted A = B. Listed, never
   aggregated (``aggregates`` is always False on tier 3).

Rules, from the rulings of 2026-10-10:

* **Self-links are not attestations.** A link whose identifier names the record
  itself (its local part equals the record's own ``src_id``, the whole
  identifier equals ``src_id``, or it is ``whg:<own pk>``) is excluded from
  every tier. Self-links still identify the record as a link TARGET: they are
  how an authority record held in WHG (TGN, by ``src_id``) is found.
* **Link types.** ``exactMatch`` and ``closeMatch`` aggregate; ``sameAs`` is an
  alias for ``exactMatch``. Anything else (``related``, ``seeAlso``, untyped,
  ...) is listed with ``aggregates: False``.
* **Provenance.** Every entry carries ``provenance.basis``. For
  ``close_matches``: ``reviewed`` (owner-reviewed), ``imported`` (2024
  migration, provenance unknown), ``authid``, or ``unknown`` when null. For
  ``place_link``: ``reviewed`` when the row came from a reviewed reconciliation
  task, otherwise ``contributed`` (in the data as uploaded).
* **One step, no closure.** Nothing here follows a link from a linked record.
* **Private datasets never render.** Every record that appears (as target,
  attester or co-citer) passes the ANONYMOUS visibility rule
  (``api.dataset_access.visible_places_q(None)``: public and not embargoed),
  whoever is asking. A non-public record has no concordance (``None``).

Resolution of an outbound target:

* ``whg:<pk>`` and authority ids WHG holds in Postgres (a record whose self-link
  is that identifier, e.g. TGN by ``src_id``) resolve in Postgres;
* other ids in a namespace the CRC gateway holds (the visible
  ``GazetteerRegistryEntry`` namespaces: ``gn``, ``wd``, ``pl`` ...) resolve via
  ``api.crc_client.crc_fetch_places``;
* anything else (``viaf:``, ``loc:``, unrecognised URLs) is listed with
  ``status: "not_held"``.

Cost: tiers 2-3 need one scan of ``place_link`` by identifier, which has no
index, so ``build_concordance`` took 0.3-3.0 s per record on prod (read-only
trial, 2026-10-21, five records, gateway time included). An expression index on
``(jsonb->>'identifier')`` is the fix before this backs a page.

The transitive groupings in ``utils/mapdata.py`` and
``collection/views.update_collection_components`` are a different thing (a
collection's families) and deliberately do NOT use this module (ruling D2).
"""
from __future__ import annotations

import ast
import json
import logging
import re
from typing import Iterable, Optional

from django.db.models import F, Func, Q, TextField

logger = logging.getLogger(__name__)

AGGREGATING_TYPES = ("exactMatch", "closeMatch")
_TYPE_ALIASES = {
    "exactmatch": "exactMatch",
    "exact": "exactMatch",
    "sameas": "exactMatch",
    "closematch": "closeMatch",
    "close": "closeMatch",
}
WHG_NS = "whg"


class _LinkIdentifier(Func):
    """``place_link.jsonb ->> 'identifier'`` as plain text. Not
    ``KeyTextTransform``: in Django 4.1 its ``__in`` lookup JSON-encodes the
    right-hand values (``'"gn:100"'``), so nothing matches; and not
    ``jsonb__identifier__in``, a jsonb comparison ~10x slower on prod."""
    template = "(%(expressions)s ->> 'identifier')"
    output_field = TextField()
GATEWAY_BATCH = 50
# A widely cited id (wd:Q90) may be co-cited by hundreds of records; tier 3 is
# a list for people to read, so it is capped and says when it was.
COCITED_LIMIT = 200
INBOUND_LIMIT = 500

PROVENANCE_LABELS = {
    "reviewed": "reviewed by the dataset owner",
    "imported": "2024 migration; provenance unknown",
    "authid": "matched on a shared authority id",
    "unknown": "provenance unknown",
    "contributed": "in the contributed data",
}

# URL forms of the identifiers the resolver can do something with. Others are
# kept verbatim and listed as not held.
_URL_PATTERNS = (
    (re.compile(r"^https?://(?:www\.|m\.)?wikidata\.org/(?:wiki|entity)/(Q\d+)", re.I), "wd"),
    (re.compile(r"^https?://(?:www\.|sws\.)?geonames\.org/(\d+)", re.I), "gn"),
    (re.compile(r"^https?://vocab\.getty\.edu/(?:page/)?tgn/(\d+)", re.I), "tgn"),
    (re.compile(r"^https?://pleiades\.stoa\.org/places/(\d+)", re.I), "pl"),
    (re.compile(r"^https?://(?:www\.|dev\.)?whgazetteer\.org/places/(\d+)", re.I), WHG_NS),
)
_NS_ALIASES = {"wikidata": "wd", "geonames": "gn", "pleiades": "pl", "getty": "tgn", "whgazetteer": WHG_NS}
_CURIE = re.compile(r"^([A-Za-z][A-Za-z0-9_.-]*):(?!//)(.+)$")


# ---------------------------------------------------------------------------
# Identifiers and link types
# ---------------------------------------------------------------------------

def normalise_identifier(raw) -> tuple[str, Optional[str], Optional[str]]:
    """``(identifier, namespace, local)`` for a link identifier. Known URL
    forms become CURIEs (``https://www.wikidata.org/wiki/Q90`` -> ``wd:Q90``);
    an unrecognised URL or bare string comes back verbatim with no namespace."""
    s = str(raw or "").strip()
    if not s:
        return "", None, None
    for pat, ns in _URL_PATTERNS:
        m = pat.match(s)
        if m:
            local = m.group(1)
            return f"{ns}:{local}", ns, local
    m = _CURIE.match(s)
    if m:
        ns = m.group(1).lower()
        ns = _NS_ALIASES.get(ns, ns)
        local = m.group(2).strip()
        if ns == "wd" and re.fullmatch(r"q\d+", local, re.I):
            local = local.upper()
        return f"{ns}:{local}", ns, local
    return s, None, None


def identifier_variants(curie: str) -> list[str]:
    """Spellings under which ``curie`` may be stored in ``place_link`` (the
    stored strings are not normalised), for exact-match SQL lookups."""
    ident, ns, local = normalise_identifier(curie)
    out = {ident}
    if ns == "wd":
        out |= {f"https://www.wikidata.org/wiki/{local}", f"http://www.wikidata.org/entity/{local}",
                f"http://www.wikidata.org/wiki/{local}", f"https://www.wikidata.org/entity/{local}"}
    elif ns == "gn":
        out |= {f"https://www.geonames.org/{local}", f"http://www.geonames.org/{local}",
                f"http://sws.geonames.org/{local}", f"https://sws.geonames.org/{local}/",
                f"http://sws.geonames.org/{local}/"}
    elif ns == "tgn":
        out |= {f"http://vocab.getty.edu/tgn/{local}", f"http://vocab.getty.edu/page/tgn/{local}",
                f"https://vocab.getty.edu/tgn/{local}", f"https://vocab.getty.edu/page/tgn/{local}"}
    elif ns == "pl":
        out |= {f"https://pleiades.stoa.org/places/{local}", f"http://pleiades.stoa.org/places/{local}"}
    return sorted(out)


def normalise_link_type(raw) -> tuple[Optional[str], bool]:
    """``(type, aggregates)``. ``sameAs`` is an alias of ``exactMatch``; a
    prefix (``skos:``, ``owl:``) is ignored. Only exact/closeMatch aggregate;
    every other type is returned as given (or None) and does not."""
    s = str(raw or "").strip()
    if not s:
        return None, False
    bare = s.rsplit(":", 1)[-1].rsplit("#", 1)[-1]
    t = _TYPE_ALIASES.get(bare.lower())
    if t:
        return t, True
    return bare, False


def is_self_link(identifier_raw, place_id: int, src_id) -> bool:
    """True when a link names the record that carries it (see module doc).

    Same test as the place#325 measurement (1,805,748 self-links on prod):
    the identifier's local part equals the record's own ``src_id``. A numeric
    ``src_id`` that coincides with an unrelated authority id would also match;
    that is accepted as the price of not needing a per-dataset authority map."""
    raw = str(identifier_raw or "").strip()
    src = str(src_id or "").strip()
    ident, ns, local = normalise_identifier(raw)
    if ns == WHG_NS and local == str(place_id):
        return True
    if not src:
        return False
    if raw == src:
        return True
    if local is not None and local == src:
        return True
    # A raw CURIE whose local part was not normalised (case etc.)
    if ":" in raw and raw.split(":", 1)[1] == src:
        return True
    return False


# ---------------------------------------------------------------------------
# Shared with Place.matches
# ---------------------------------------------------------------------------

def close_match_rows(place_id: int):
    """The ``close_matches`` rows touching one record: one step, no closure.
    ``Place.matches`` and the concordance both read this."""
    from places.models import CloseMatch
    return CloseMatch.objects.filter(Q(place_a_id=place_id) | Q(place_b_id=place_id))


# ---------------------------------------------------------------------------
# Visibility (anonymous, always)
# ---------------------------------------------------------------------------

def _visible_places_q(prefix: str = ""):
    from api.dataset_access import visible_datasets_q
    return visible_datasets_q(None, prefix=f"{prefix}dataset__")


def _record_summary(p) -> dict:
    return {
        "place_id": p.id,
        "title": p.title,
        "src_id": p.src_id,
        "dataset": p.dataset_id,  # the FK is the label
        "dataset_id": getattr(p.dataset, "id", None),
        "dataset_title": getattr(p.dataset, "title", None),
    }


def _repr_points(place_ids: Iterable[int]) -> dict[int, list]:
    """Representative point ([lon, lat]) per place that has any geometry, as
    ``Place.repr_point`` computes it, in one query."""
    from django.contrib.gis.geos import GeometryCollection
    from places.models import PlaceGeom
    ids = list({int(i) for i in place_ids})
    if not ids:
        return {}
    by_place: dict[int, list] = {}
    for pid, g in PlaceGeom.objects.filter(place_id__in=ids, geom__isnull=False).values_list("place_id", "geom"):
        by_place.setdefault(pid, []).append(g)
    out = {}
    for pid, geoms in by_place.items():
        try:
            out[pid] = list(GeometryCollection(geoms).centroid.coords)
        except Exception:  # noqa: BLE001 — a bad geometry is "no point", not a crash
            continue
    return out


# ---------------------------------------------------------------------------
# Target resolution
# ---------------------------------------------------------------------------

def gateway_namespaces(user=None) -> set[str]:
    """Namespaces the CRC gateway holds that ``user`` may see (embargoed rows
    drop out via ``visible_to``). ``whg`` is excluded: contributed records
    resolve in Postgres."""
    from api.models import GazetteerRegistryEntry
    return set(
        GazetteerRegistryEntry.objects.visible_to(user)
        .exclude(namespace__in=["", WHG_NS])
        .values_list("namespace", flat=True)
    )


def _gateway_has_geometry(rec: dict) -> bool:
    if rec.get("repr_point"):
        return True
    for g in rec.get("geometries") or []:
        if isinstance(g, dict) and (g.get("repr_point") or g.get("geometry") or g.get("coordinates")):
            return True
    return False


def _gateway_point(rec: dict):
    rp = rec.get("repr_point")
    if rp:
        return rp
    for g in rec.get("geometries") or []:
        if isinstance(g, dict) and g.get("repr_point"):
            return g["repr_point"]
    return None


def resolve_targets(curies: Iterable[str], user=None, use_gateway: bool = True,
                    exclude_place_id: Optional[int] = None) -> dict[str, dict]:
    """Resolve normalised identifiers to records WHG holds.

    Returns ``{curie: resolution}``; every input appears. ``resolution`` is::

        {"status": "resolved" | "not_found" | "not_held" | "gateway_unavailable"
                   | "gateway_disabled",
         "via": "postgres" | "gateway" | None,
         "records": [ {place_id|place_id(gateway), title, has_geometry, repr_point, ...} ],
         "has_geometry": bool}

    ``not_found``: asked and definitively absent. ``not_held``: no store WHG
    has could hold it (``viaf:``, an unknown URL, or a namespace not in the
    gateway). ``gateway_unavailable`` / ``gateway_disabled`` are NOT absence
    (place#272): the gateway could not be, or was not, asked.
    """
    from places.models import Place, PlaceLink

    curies = [c for c in dict.fromkeys(curies) if c]
    out: dict[str, dict] = {}
    parsed = {c: normalise_identifier(c) for c in curies}

    def blank(status, via=None):
        return {"status": status, "via": via, "records": [], "has_geometry": False}

    # 1. whg:<pk>
    whg_pks = {}
    for c, (_i, ns, local) in parsed.items():
        if ns == WHG_NS and local and local.isdigit():
            whg_pks[c] = int(local)
    pg_hits: dict[str, list] = {}
    if whg_pks:
        qs = (Place.objects.filter(id__in=set(whg_pks.values())).filter(_visible_places_q())
              .select_related("dataset"))
        by_pk = {p.id: p for p in qs}
        for c, pk in whg_pks.items():
            if pk in by_pk and pk != exclude_place_id:
                pg_hits.setdefault(c, []).append(by_pk[pk])

    # 2. authority ids held in Postgres: a visible record whose src_id is the
    #    local part AND which carries the identifier as its own self-link.
    auth = {c: (ns, local) for c, (_i, ns, local) in parsed.items()
            if ns and ns != WHG_NS and local}
    if auth:
        locals_ = {local for _ns, local in auth.values()}
        cands = list(Place.objects.filter(src_id__in=locals_).filter(_visible_places_q())
                     .select_related("dataset"))
        if cands:
            cand_by_id = {p.id: p for p in cands}
            self_ids: dict[int, set] = {}
            for pid, j in PlaceLink.objects.filter(place_id__in=cand_by_id).values_list("place_id", "jsonb"):
                ident = normalise_identifier((j or {}).get("identifier"))[0]
                p = cand_by_id[pid]
                if is_self_link((j or {}).get("identifier"), p.id, p.src_id):
                    self_ids.setdefault(pid, set()).add(ident)
            for c in auth:
                for pid, idents in self_ids.items():
                    if c in idents and pid != exclude_place_id:
                        pg_hits.setdefault(c, []).append(cand_by_id[pid])

    if pg_hits:
        points = _repr_points(p.id for ps in pg_hits.values() for p in ps)
        for c, ps in pg_hits.items():
            recs = []
            for p in sorted(ps, key=lambda x: x.id):
                r = _record_summary(p)
                r["repr_point"] = points.get(p.id)
                r["has_geometry"] = p.id in points
                recs.append(r)
            out[c] = {"status": "resolved", "via": "postgres", "records": recs,
                      "has_geometry": any(r["has_geometry"] for r in recs)}

    # 3. the gateway, for the rest in a namespace it holds
    rest = [c for c in curies if c not in out]
    ns_held = gateway_namespaces(user) if (use_gateway and rest) else set()
    to_ask = [c for c in rest if parsed[c][1] in ns_held]
    for c in rest:
        if c not in to_ask:
            out[c] = blank("not_held")
    if to_ask:
        from api.crc_client import crc_fetch_places
        for i in range(0, len(to_ask), GATEWAY_BATCH):
            batch = to_ask[i:i + GATEWAY_BATCH]
            meta: dict = {}
            got = crc_fetch_places(batch, user=user, meta=meta)
            if meta.get("disabled"):
                for c in batch:
                    out[c] = blank("gateway_disabled", "gateway")
                continue
            if meta.get("error"):
                for c in batch:
                    r = blank("gateway_unavailable", "gateway")
                    r["error"] = meta["error"]
                    out[c] = r
                continue
            for c in batch:
                rec = got.get(c)
                if rec is None:
                    out[c] = blank("not_found", "gateway")
                    continue
                hg = _gateway_has_geometry(rec)
                out[c] = {"status": "resolved", "via": "gateway", "has_geometry": hg, "records": [{
                    "place_id": rec.get("place_id", c),
                    "title": rec.get("title"),
                    "namespace": rec.get("namespace"),
                    "repr_point": _gateway_point(rec),
                    "has_geometry": hg,
                }]}
    return out


# ---------------------------------------------------------------------------
# The concordance
# ---------------------------------------------------------------------------

def _task_dataset_id(task) -> Optional[int]:
    """The dataset a reconciliation task ran for (``task_kwargs['ds']``), or
    None. ``task_kwargs`` is stored as a Python repr, sometimes JSON-quoted."""
    if task is None:
        return None
    raw = getattr(task, "task_kwargs", None)
    val = raw
    for _ in range(2):
        if isinstance(val, dict):
            break
        if not isinstance(val, str):
            return None
        try:
            val = json.loads(val)
        except (ValueError, TypeError):
            try:
                val = ast.literal_eval(val)
            except (ValueError, SyntaxError):
                return None
    if not isinstance(val, dict):
        return None
    try:
        return int(val.get("ds"))
    except (TypeError, ValueError):
        return None


def _link_provenance(row) -> dict:
    basis = "reviewed" if (row.task_id and row.reviewer_id) else "contributed"
    return {"basis": basis, "label": PROVENANCE_LABELS[basis]}


def build_concordance(place, user=None, use_gateway: bool = True) -> Optional[dict]:
    """The concordance of ``place`` as a plain dict, or None when the record
    is not public (private and embargoed datasets never render).

    ``user`` is used ONLY to call the CRC gateway (which refuses anonymous
    callers) and to apply namespace embargoes there; it never widens which
    WHG records appear. Shape::

        {"record": {...}, "self_identifiers": [...], "excluded_self_links": int,
         "outbound": [entry], "inbound": [entry], "cocited": [entry],
         "aggregate": [member], "truncated": {"inbound": bool, "cocited": bool}}

    entry: ``tier, source ("place_link"|"close_matches"), identifier,
    identifier_raw, namespace, link_type, link_type_raw, aggregates,
    provenance {basis, label}, attested_by {record...}`` plus, for tier 1,
    ``target`` (a ``resolve_targets`` resolution) and, for tiers 2-3,
    ``record`` (the attesting / co-citing record).

    ``aggregate``: the distinct resolved records of tiers 1-2 whose entries
    aggregate (exact/closeMatch), with ``has_geometry`` - the subset a map,
    gloss or EDOP signature may use. Tier 3 never contributes.
    """
    from places.models import Place, PlaceLink

    if not Place.objects.filter(id=place.id).filter(_visible_places_q()).exists():
        return None

    record = _record_summary(place)
    out = {"record": record, "self_identifiers": [], "excluded_self_links": 0,
           "outbound": [], "inbound": [], "cocited": [], "aggregate": [],
           "truncated": {"inbound": False, "cocited": False}}

    # ---- tier 1: outbound place_link ------------------------------------
    self_idents = {f"{WHG_NS}:{place.id}"}
    outbound_links = []
    for row in PlaceLink.objects.filter(place_id=place.id).order_by("id"):
        j = row.jsonb or {}
        raw_ident = j.get("identifier")
        ident, ns, _local = normalise_identifier(raw_ident)
        if not ident:
            continue
        if is_self_link(raw_ident, place.id, place.src_id):
            out["excluded_self_links"] += 1
            self_idents.add(ident)
            continue
        ltype, agg = normalise_link_type(j.get("type"))
        outbound_links.append({
            "tier": 1, "source": "place_link", "identifier": ident, "identifier_raw": raw_ident,
            "namespace": ns, "link_type": ltype, "link_type_raw": j.get("type"), "aggregates": agg,
            "provenance": _link_provenance(row), "attested_by": record,
        })
    out["self_identifiers"] = sorted(self_idents)

    resolutions = resolve_targets([e["identifier"] for e in outbound_links], user=user,
                                  use_gateway=use_gateway, exclude_place_id=place.id)
    for e in outbound_links:
        e["target"] = resolutions.get(e["identifier"])
    out["outbound"].extend(outbound_links)

    # ---- close_matches: tier 1 or 2 by who attested ---------------------
    cm_rows = list(close_match_rows(place.id).select_related("task"))
    partner_ids = {(r.place_b_id if r.place_a_id == place.id else r.place_a_id) for r in cm_rows}
    partner_ids.discard(place.id)
    partners = {p.id: p for p in Place.objects.filter(id__in=partner_ids)
                .filter(_visible_places_q()).select_related("dataset")}
    points = _repr_points(partners)
    own_ds = record["dataset_id"]
    for r in cm_rows:
        pid = r.place_b_id if r.place_a_id == place.id else r.place_a_id
        p = partners.get(pid)
        if p is None:  # private / embargoed partner, or a degenerate self-pair
            continue
        basis = r.basis or "unknown"
        prov = {"basis": basis, "label": PROVENANCE_LABELS.get(basis, PROVENANCE_LABELS["unknown"])}
        partner = _record_summary(p)
        partner["repr_point"] = points.get(p.id)
        partner["has_geometry"] = p.id in points
        task_ds = _task_dataset_id(r.task)
        if task_ds is not None and task_ds == own_ds:
            direction, tier = "outbound", 1
        elif task_ds is not None and task_ds == partner["dataset_id"]:
            direction, tier = "inbound", 2
        else:
            direction, tier = "unknown", 2
        entry = {
            "tier": tier, "source": "close_matches", "identifier": f"{WHG_NS}:{p.id}",
            "identifier_raw": None, "namespace": WHG_NS, "link_type": "closeMatch",
            "link_type_raw": None, "aggregates": True, "provenance": prov, "direction": direction,
            "attested_by": record if tier == 1 else partner,
        }
        if tier == 1:
            entry["target"] = {"status": "resolved", "via": "postgres", "records": [partner],
                               "has_geometry": partner["has_geometry"]}
            out["outbound"].append(entry)
        else:
            entry["record"] = partner
            out["inbound"].append(entry)

    # ---- tiers 2 and 3 from one scan of place_link ----------------------
    inbound_strings = set()
    for s in self_idents:
        inbound_strings.update(identifier_variants(s))
    cocite_map: dict[str, str] = {}
    for e in outbound_links:
        for v in identifier_variants(e["identifier"]):
            cocite_map[v] = e["identifier"]
    wanted = inbound_strings | set(cocite_map)
    rows = []
    if wanted:
        # A seq scan of place_link: it has no index on the identifier. An
        # expression index on (jsonb->>'identifier') would make this cheap.
        rows = list(
            PlaceLink.objects.annotate(_ident=_LinkIdentifier(F("jsonb")))
            .filter(_ident__in=sorted(wanted))
            .exclude(place_id=place.id)
            .filter(_visible_places_q(prefix="place__"))
            .select_related("place__dataset")
            .order_by()[: INBOUND_LIMIT + COCITED_LIMIT + 1]
        )
        # Ordered here, not in SQL: ORDER BY place_id + LIMIT lets the planner
        # walk the place_id index over the whole table (1.7 s vs 0.4 s on prod).
        rows.sort(key=lambda r: (r.place_id, r.id))
    if len(rows) > INBOUND_LIMIT + COCITED_LIMIT:
        out["truncated"]["inbound"] = out["truncated"]["cocited"] = True
        rows = rows[: INBOUND_LIMIT + COCITED_LIMIT]

    seen_inbound = {e["record"]["place_id"] for e in out["inbound"] if e.get("record")}
    tier1_ids = {rec["place_id"] for e in out["outbound"] for rec in (e.get("target") or {}).get("records", [])
                 if isinstance(rec.get("place_id"), int)}
    point_ids = {row.place_id for row in rows}
    points = _repr_points(point_ids)
    inbound_rows, cocited_rows = [], []
    for row in rows:
        j = row.jsonb or {}
        raw_ident = j.get("identifier")
        p = row.place
        if is_self_link(raw_ident, p.id, p.src_id):
            # The other record naming ITSELF: that is how it is a target
            # (tier 1, resolved above), not an attestation about this record.
            continue
        ident, ns, _l = normalise_identifier(raw_ident)
        ltype, agg = normalise_link_type(j.get("type"))
        rec = _record_summary(p)
        rec["repr_point"] = points.get(p.id)
        rec["has_geometry"] = p.id in points
        base = {"source": "place_link", "identifier": ident, "identifier_raw": raw_ident,
                "namespace": ns, "link_type": ltype, "link_type_raw": j.get("type"),
                "provenance": _link_provenance(row), "attested_by": rec, "record": rec}
        if raw_ident in inbound_strings:
            inbound_rows.append({"tier": 2, "aggregates": agg, "direction": "inbound", **base})
        elif raw_ident in cocite_map and p.id not in tier1_ids:
            cocited_rows.append({"tier": 3, "aggregates": False, "cocited_identifier": cocite_map[raw_ident],
                                 **base})
    if len(inbound_rows) > INBOUND_LIMIT:
        out["truncated"]["inbound"] = True
        inbound_rows = inbound_rows[:INBOUND_LIMIT]
    inbound_ids = seen_inbound | {e["record"]["place_id"] for e in inbound_rows}
    # A record that is already an identity attester is not ALSO "co-cited".
    cocited_rows = [e for e in cocited_rows if e["record"]["place_id"] not in inbound_ids]
    if len(cocited_rows) > COCITED_LIMIT:
        out["truncated"]["cocited"] = True
        cocited_rows = cocited_rows[:COCITED_LIMIT]
    out["inbound"].extend(inbound_rows)
    out["cocited"].extend(cocited_rows)

    # ---- the aggregated subset: tiers 1-2, exact/closeMatch, resolved ----
    agg: dict = {}
    for e in out["outbound"]:
        if not e["aggregates"]:
            continue
        t = e.get("target") or {}
        if t.get("status") != "resolved":
            continue
        for rec in t["records"]:
            agg.setdefault((t["via"], rec["place_id"]), {**rec, "via": t["via"], "tiers": set()})["tiers"].add(1)
    for e in out["inbound"]:
        if not e["aggregates"]:
            continue
        rec = e["record"]
        agg.setdefault(("postgres", rec["place_id"]), {**rec, "via": "postgres", "tiers": set()})["tiers"].add(2)
    out["aggregate"] = [{**m, "tiers": sorted(m["tiers"])} for m in agg.values()]
    return out
