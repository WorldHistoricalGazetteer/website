"""The attributes of the places a contributor matched, for the "Download matches" CSV.

A Link stores only the matched place's id, so everything else is read back from the gateway
(``POST /api/places``: the full record) at download time. That covers every Link however it was
made: a suggestion the feed stored a trimmed card for, or a hand search that stored nothing.

Three rules, because the file leaves WHG:

* a namespace the registry says is not ``redistributable`` (its licence forbids re-hosting a copy),
  or is under an embargo the viewer cannot see, or a contributed record the viewer may not see
  now, gets only its id, title, source, licence and a ``matched_detail`` that says why;
* silence is never absence: ``matched_detail`` says ``full`` (the gateway answered), ``card only``
  (it did not, so the stored suggestion is used), ``not found`` (it answered without that id) or
  ``gateway unavailable`` (nothing could be said), so an empty cell is never ambiguous;
* every row that carries attributes also carries its source's licence and citation.
"""
import logging
from types import SimpleNamespace

logger = logging.getLogger(__name__)

CHUNK = 40
CELL_MAX = 30000   # Excel's cell limit is 32,767 characters

COLUMNS = ["matched_id", "matched_title", "matched_source", "matched_source_name",
           "matched_lon", "matched_lat", "matched_ccodes", "matched_types", "matched_aat_ids",
           "matched_names", "matched_dates", "matched_links", "matched_related",
           "matched_description", "matched_population", "matched_elevation",
           "matched_url", "matched_licence", "matched_citation", "matched_detail"]


def _cell(parts, sep="; "):
    """Join non-empty strings, dropping repeats in order, and keep the cell inside Excel's limit."""
    seen, out = set(), []
    for p in parts:
        p = str(p).strip() if p is not None else ""
        if p and p not in seen:
            seen.add(p)
            out.append(p)
    s = sep.join(out)
    return s if len(s) <= CELL_MAX else s[:CELL_MAX - 1] + "…"


def _span(t):
    if isinstance(t, dict):
        a, b = t.get("start"), t.get("end")
        if a is None and b is None:
            d = t.get("definite")
            return "/".join(str(x) for x in d) if isinstance(d, (list, tuple)) else ""
        return f"{'' if a is None else a}/{'' if b is None else b}"
    return str(t or "")


def _text(d):
    if isinstance(d, dict):
        return d.get("value") or d.get("label") or d.get("text") or d.get("description") or ""
    return d


def _point(place):
    from datasets.views_feed import lon_lat
    return lon_lat(place.get("repr_point")) or next(
        (ll for ll in (lon_lat(g.get("repr_point")) for g in place.get("geometries") or []
                       if isinstance(g, dict)) if ll), None)


def flatten(place):
    """The columns that come from one gateway ``PlaceDetail``."""
    ll = _point(place)
    names = [f"{n['label']} ({n['lang']})" if n.get("lang") else n["label"]
             for n in place.get("names") or [] if isinstance(n, dict) and n.get("label")]
    types = [t.get("sourceLabel") or t.get("label") for t in place.get("types") or [] if isinstance(t, dict)]
    aat = [a for t in place.get("types") or [] if isinstance(t, dict) for a in t.get("aat_ids") or []]
    spans = list(place.get("timespans") or [])
    spans += [s for n in place.get("names") or [] if isinstance(n, dict) for s in n.get("timespans") or []]
    links = [l.get("identifier") if isinstance(l, dict) else l for l in place.get("links") or []]
    related = [f"{r.get('relation_type', '')} {r.get('related_place_id', '')}".strip()
               for r in place.get("relations") or [] if isinstance(r, dict)]
    return {
        "matched_title": place.get("title") or "",
        "matched_lon": ll[0] if ll else "", "matched_lat": ll[1] if ll else "",
        "matched_ccodes": _cell(place.get("ccodes") or []),
        "matched_types": _cell(types), "matched_aat_ids": _cell(aat),
        "matched_names": _cell(names), "matched_dates": _cell(_span(s) for s in spans),
        "matched_links": _cell(links), "matched_related": _cell(related),
        "matched_description": _cell((_text(d) for d in place.get("descriptions") or []), sep=" | "),
        "matched_population": "" if place.get("population") in (None, 0) else place["population"],
        "matched_elevation": "" if place.get("elevation") is None else place["elevation"],
    }


def from_card(card, title=""):
    """What a stored suggestion remembers of the place (see ``candidates.card``)."""
    ll = (card or {}).get("repr_point")
    return {
        "matched_title": (card or {}).get("title") or title,
        "matched_lon": ll[0] if ll else "", "matched_lat": ll[1] if ll else "",
        "matched_ccodes": _cell((card or {}).get("ccodes") or []),
        "matched_types": _cell((card or {}).get("types") or []),
        "matched_names": _cell(n.get("label", "") for n in (card or {}).get("names") or []),
        "matched_dates": _cell([_span((card or {}).get("temporal_range"))]
                               if (card or {}).get("temporal_range") else []),
    }


def namespace_of(pid):
    return str(pid).split(":", 1)[0] if ":" in str(pid) else ""


def _licence_bits(entry, local_id):
    if entry is None:
        return {}
    lic = entry.license
    tpl = getattr(entry, "web_item", None)
    return {"matched_source_name": entry.name or entry.id,
            "matched_url": tpl.replace("<id>", local_id) if tpl and "<id>" in tpl else "",
            "matched_licence": (lic.spdx_id if lic else "") or "",
            "matched_citation": (getattr(entry, "citation_text", None) or "").strip()}


def details_for(user, matches, fetch=None):
    """``{matched_id: row}`` for ``matches``, a list of ``(matched_id, candidate_or_None)``.

    ``fetch`` is ``crc_client.crc_fetch_places`` (injected by tests)."""
    from api.models import GazetteerRegistryEntry
    from datasets.views_feed import candidate_visible, visible_contributed_pks
    if fetch is None:
        from api.crc_client import crc_fetch_places as fetch

    ids = list(dict.fromkeys(m for m, _ in matches))
    cand_of = {m: c for m, c in matches if c is not None}
    namespaces = {namespace_of(i) for i in ids}
    entries = {e.id: e for e in GazetteerRegistryEntry.objects.filter(id__in=namespaces).select_related("license")}
    hidden = GazetteerRegistryEntry.objects.hidden_namespaces_for(user)
    visible = visible_contributed_pks(user, [SimpleNamespace(candidate_id=i) for i in ids if i.startswith("whg:")])

    out, to_fetch = {}, []
    for pid in ids:
        ns = namespace_of(pid)
        entry = entries.get(ns)
        local = pid.split(":", 1)[1] if ":" in pid else pid
        base = {"matched_id": pid, "matched_source": ns, **_licence_bits(entry, local)}
        why = None
        if ns in hidden:
            why = "withheld: this source is under embargo"
        elif entry is not None and not entry.redistributable:
            why = "withheld: this source's licence does not allow its data to be re-distributed"
        elif pid.startswith("whg:") and not candidate_visible(SimpleNamespace(candidate_id=pid), visible):
            why = "withheld: a contributed record you cannot see"
        if why:
            c = cand_of.get(pid)
            # The title was shown to the person when they decided; nothing else is copied.
            base.update(matched_title="" if pid.startswith("whg:") or ns in hidden else
                        (c.candidate_title if c is not None else ""), matched_detail=why)
            out[pid] = base
        else:
            out[pid] = base
            to_fetch.append(pid)

    got, failed = {}, set()
    for i in range(0, len(to_fetch), CHUNK):
        chunk = to_fetch[i:i + CHUNK]
        meta = {}
        try:
            res = fetch(chunk, user=user, meta=meta)
        except Exception:  # noqa: BLE001 — a failed fetch is reported per row, never a 500
            logger.exception("match export: gateway fetch failed")
            res, meta = {}, {"error": "unexpected"}
        if meta.get("error") or meta.get("disabled"):
            failed.update(chunk)
        got.update(res or {})

    for pid in to_fetch:
        c = cand_of.get(pid)
        if pid in got:
            out[pid].update(flatten(got[pid]), matched_detail="full")
        elif c is not None:
            out[pid].update(from_card(c.card, c.candidate_title),
                            matched_detail="card only: the gateway could not be asked" if pid in failed
                            else "card only: the gateway no longer returns this place")
        else:
            out[pid]["matched_detail"] = ("gateway unavailable: download again later" if pid in failed
                                          else "not found: the gateway returns no such place")
    return out
