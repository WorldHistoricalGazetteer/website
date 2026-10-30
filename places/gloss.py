"""A short gloss of a group of records, generated from their data (place#299, limited;
place#325 Stage 3).

#299 asks for prose saying "what kind of place, what it is inside, what the
constituent records disagree about". Its inputs are known to be incomplete:
containment is read where present, never inferred (#265), and gateway place types are
source-vocabulary tags except for GeoNames feature classes (#267). So this is the
smallest honest version:

* **Only what the records carry.** Names, types, country codes, dates and contributed
  part-of relations, counted across the members. No language model, no inference, no
  outside knowledge: every clause can be traced to fields in the member list, and the
  ``facts`` block carries the counts each sentence was built from.
* **Agreement and disagreement are stated as counts** ("2 of 3 records"), never
  resolved: if country codes differ, the gloss lists them; it does not pick one.
* **What is unknown is said.** Untyped, undated and unplaced members are counted;
  members whose details could not be read are reported as not known, which is not
  "none"; and the absence of a containing place is reported with the reason it may be
  absent (containment is incomplete).

The input is the normalised member list of ``places.record_group`` (Record page) or the
locus endpoint's validated members. Server-side, so an API consumer such as EDOPS could
read the same output (the #299 open question; chosen here because it is the one place
the wording and the counting can be tested).
"""
from __future__ import annotations

import unicodedata
from collections import Counter
from typing import Optional

GLOSS_VERSION = 1
MAX_NAMES_SHOWN = 6
MAX_TYPES_SHOWN = 5
MAX_SOURCES_SHOWN = 8

# place#325 Stage 5: the same sentences, grouped by what they are about, so a page can
# show one card per aspect. Order and titles are the cards'; ``text`` is the sentence(s)
# ``sentences`` already says for that aspect, verbatim (``countries`` also carries the
# contributed part-of sentence: both say where). ``unknown`` carries the
# "Not known from these records" bullets as ``items``.
FACET_KEYS = ("sources", "names", "type", "countries", "dates", "unknown")
FACET_TITLES = {"sources": "Sources", "names": "Names", "type": "Type", "countries": "Countries",
                "dates": "Dates", "unknown": "Not known from these records"}


def _norm(s: str) -> str:
    return " ".join(unicodedata.normalize("NFC", str(s or "")).casefold().split())


def _year(y: int) -> str:
    return f"{-y} BCE" if y < 0 else f"{y} CE" if y < 1000 else str(y)


def _span(s, e) -> str:
    return _year(s) if s == e else f"{_year(s)} to {_year(e)}"


def _of(k: int, n: int) -> str:
    return "every record" if k == n and n > 1 else ("the record" if n == 1 else f"{k} of {n} records")


def _list(items: list[str]) -> str:
    items = [i for i in items if i]
    if len(items) <= 1:
        return "".join(items)
    return ", ".join(items[:-1]) + " and " + items[-1]


def _counted(counter: Counter, limit: int) -> list[str]:
    return [f"{label} ({n})" for label, n in counter.most_common(limit)]


def _overlap(a, b) -> bool:
    return a[0] <= b[1] and b[0] <= a[1]


def build_gloss(members: list, *, kind: str = "concordance", unresolved: int = 0,
                truncated: bool = False) -> dict:
    """``{"version", "kind", "n_records", "sentences", "unknown", "limits", "facts", "facets"}``.

    ``facets`` (additive, place#325 Stage 5): ``[{"key", "title", "text", "items"?}]`` in
    ``FACET_KEYS`` order, one per aspect that has something to say; ``text`` is that
    aspect's sentence(s) from ``sentences`` joined by a space (``""`` for ``unknown``),
    ``items`` a short ``[{"label", "count"?}]`` list where one is natural.

    ``kind``: ``"concordance"`` (the record plus its attested equivalents; the record is
    the member with ``is_record``) or ``"locus"`` (a soft cluster's current members).
    ``unresolved``: attested equivalents that could not be looked up (the index was
    unreachable), so are missing from ``members``; the gloss says the group is incomplete.
    ``truncated``: more records attest this one than the concordance reads
    (``places.concordance.INBOUND_LIMIT``), so ``members`` is part of the group."""
    n = len(members)
    out = {"version": GLOSS_VERSION, "kind": kind, "n_records": n, "sentences": [], "unknown": [],
           "limits": [], "facts": {}, "facets": []}
    if not n:
        out["unknown"].append("There are no records to describe.")
        out["facets"] = _facets({}, {}, out["unknown"])
        return out
    known = [m for m in members if m.get("detail", "full") == "full"]
    unread = n - len(known)
    s = out["sentences"]
    said: dict[str, list] = {}      # facet key -> its sentences, in the order said
    items: dict[str, list] = {}

    def say(key, sentence):
        s.append(sentence)
        said.setdefault(key, []).append(sentence)

    # ---- who: records and sources ------------------------------------------
    sources = Counter(m.get("source") or m.get("namespace") or "unknown source" for m in members)
    out["facts"]["sources"] = dict(sources)
    src = _list(_counted(sources, MAX_SOURCES_SHOWN)) if len(sources) > 1 else next(iter(sources))
    items["sources"] = [{"label": label, "count": c} for label, c in sources.most_common(MAX_SOURCES_SHOWN)]
    if kind == "concordance":
        eq = n - 1
        if eq == 0:
            say("sources", f"This record alone, from {src}: no other record is linked as the same place, "
                           "so this describes one contributor's account.")
        else:
            say("sources", f"This record and {eq} linked equivalent{'s' if eq != 1 else ''} "
                           f"(exactMatch or closeMatch), {n} records from {len(sources)} source"
                           f"{'s' if len(sources) != 1 else ''}: {src}.")
    else:
        say("sources", f"{n} record{'s' if n != 1 else ''} currently in this cluster, from {len(sources)} source"
                       f"{'s' if len(sources) != 1 else ''}: {src}.")

    # ---- names -------------------------------------------------------------
    per_record = Counter()
    display: dict[str, Counter] = {}
    for m in known or members:
        seen = set()
        for name in m.get("names") or ([m.get("title")] if m.get("title") else []):
            k = _norm(name)
            if not k or k in seen:
                continue
            seen.add(k)
            per_record[k] += 1
            display.setdefault(k, Counter())[name] += 1
    if per_record:
        base = len(known or members)
        top_k, top_n = per_record.most_common(1)[0]
        top = display[top_k].most_common(1)[0][0]
        others = [display[k].most_common(1)[0][0] for k, _ in per_record.most_common() if k != top_k]
        clause = f"Named “{top}” in {_of(top_n, base)}"
        if others:
            shown = [f"“{o}”" for o in others[:MAX_NAMES_SHOWN]]
            more = len(others) - len(shown)
            clause += f"; also {_list(shown)}" + (f" and {more} other name{'s' if more != 1 else ''}" if more else "")
        say("names", clause + ".")
        items["names"] = [{"label": display[k].most_common(1)[0][0], "count": c}
                          for k, c in per_record.most_common(MAX_NAMES_SHOWN + 1)]
        out["facts"]["names"] = {"most_common": top, "in_records": top_n, "distinct": len(per_record)}

    # ---- types -------------------------------------------------------------
    types = Counter()
    labels: dict[str, str] = {}
    untyped = 0
    for m in known:
        ts = {_norm(t): t for t in (m.get("types") or []) if _norm(t)}
        if not ts:
            untyped += 1
        for k, t in ts.items():
            types[k] += 1
            labels.setdefault(k, t)
    out["facts"]["types"] = {labels[k]: v for k, v in types.items()}
    out["facts"]["untyped"] = untyped
    if types:
        shown = [f"{labels[k]} ({v})" for k, v in types.most_common(MAX_TYPES_SHOWN)]
        more = len(types) - len(shown)
        clause = f"Typed as {_list(shown)}" + (f", and {more} other type{'s' if more != 1 else ''}" if more else "")
        if len(types) > 1:
            clause += "; the records do not all use the same type"
        say("type", clause + ".")
        items["type"] = [{"label": labels[k], "count": v} for k, v in types.most_common(MAX_TYPES_SHOWN)]
    if untyped:
        out["unknown"].append("No record carries a place type." if untyped == len(known) > 1 else
                              f"{_of(untyped, len(known)).capitalize()} carr{'ies' if untyped == 1 else 'y'} no place type.")

    # ---- where: country codes and containment -------------------------------
    cc_sets = [tuple(m.get("ccodes") or []) for m in known]
    with_cc = [c for c in cc_sets if c]
    codes = Counter(c for cs in with_cc for c in cs)
    out["facts"]["ccodes"] = dict(codes)
    if codes:
        if len(set(with_cc)) == 1:
            say("countries", f"Country code{'s' if len(with_cc[0]) > 1 else ''} {', '.join(with_cc[0])} in "
                             f"{_of(len(with_cc), len(known))}" + (" that has one" if len(with_cc) < len(known) else "") + ".")
        else:
            say("countries", f"The records disagree on country: {_list(_counted(codes, 8))}.")
        items["countries"] = [{"label": c, "count": v} for c, v in codes.most_common(8)]
    if len(with_cc) < len(known):
        k = len(known) - len(with_cc)
        out["unknown"].append("No record has a country code." if k == len(known) > 1 else
                              f"{_of(k, len(known)).capitalize()} {'has' if k == 1 else 'have'} no country code.")
    parts = Counter()
    for m in known:
        for p in dict.fromkeys(m.get("part_of") or []):
            parts[p] += 1
    out["facts"]["part_of"] = dict(parts)
    if parts:
        say("countries", "Part of " + _list([f"{p} (according to {v} record{'s' if v != 1 else ''})"
                                                  for p, v in parts.most_common(5)]) + ".")
    else:
        out["unknown"].append("No record names a place that contains it. That is not evidence that nothing "
                              "does: containment in WHG is read only where a source states it, and is incomplete.")

    # ---- when --------------------------------------------------------------
    spans = [tuple(m["years"]) for m in known if m.get("years")]
    out["facts"]["dated"] = len(spans)
    if spans:
        lo, hi = min(a for a, _ in spans), max(b for _, b in spans)
        clause = f"Dated {_span(lo, hi)} across {_of(len(spans), len(known))}"
        disjoint = any(not _overlap(spans[i], spans[j]) for i in range(len(spans)) for j in range(i + 1, len(spans)))
        if disjoint:
            clause += "; some records' dates do not overlap at all"
        say("dates", clause + ".")
        out["facts"]["span"] = [lo, hi]
        out["facts"]["dates_disjoint"] = disjoint
    if len(spans) < len(known):
        k = len(known) - len(spans)
        out["unknown"].append("No record is dated." if k == len(known) > 1 else
                              f"{_of(k, len(known)).capitalize()} {'is' if k == 1 else 'are'} undated.")

    if unresolved:
        out["unknown"].append(f"{unresolved} linked equivalent{'s' if unresolved != 1 else ''} could not be looked "
                              "up in WHG's index just now, so this describes an incomplete group. Reload to try again.")
    if truncated:
        from places.concordance import INBOUND_LIMIT
        out["unknown"].append(f"More records attest that they are this place than WHG reads for one page (the "
                              f"first {INBOUND_LIMIT}, by record id), so this describes part of the group, not "
                              "all of it.")
    out["facts"]["truncated"] = bool(truncated)
    if unread:
        out["unknown"].append(f"The details of {unread} record{'s' if unread != 1 else ''} held in WHG's index "
                              "could not be read just now, so they are counted above by name only. That is not a "
                              "finding that they have no types, dates or country codes.")

    out["limits"] = [
        "Generated from the records' own fields: nothing is inferred, and nothing comes from outside them.",
        "Place types from WHG's search index are GeoNames feature classes only; other sources' types there are not "
        "yet readable (place#267).",
    ]
    out["facets"] = _facets(said, items, out["unknown"])
    return out


def _facets(said: dict, items: dict, unknown: list) -> list:
    """``build_gloss``'s ``facets``: the sentences already said, grouped by aspect."""
    out = []
    for key in FACET_KEYS:
        if key == "unknown":
            if unknown:
                out.append({"key": key, "title": FACET_TITLES[key], "text": "",
                            "items": [{"label": u} for u in unknown]})
            continue
        if not said.get(key):
            continue
        f = {"key": key, "title": FACET_TITLES[key], "text": " ".join(said[key])}
        if items.get(key):
            f["items"] = items[key]
        out.append(f)
    return out


def member_from_hit(hit: dict) -> Optional[dict]:
    """A gloss/EDOP member from a locus page's posted hit, or None if it is unusable.
    Only plain values are kept, each capped, so the endpoint never echoes arbitrary
    structures."""
    from places.record_group import gateway_names, gateway_types, years_from_gateway

    if not isinstance(hit, dict):
        return None
    pid = str(hit.get("place_id") or "").strip()[:200]
    if not pid:
        return None
    ns = str(hit.get("namespace") or (pid.split(":", 1)[0] if ":" in pid else "")).strip()[:40]
    title = " ".join(str(hit.get("title") or pid).split())[:300]
    rp = hit.get("repr_point")
    from api.edop_client import valid_point
    point = [float(rp[0]), float(rp[1])] if valid_point(rp) else None
    years = years_from_gateway(hit)
    ccodes = sorted({str(c)[:3] for c in (hit.get("ccodes") or [])[:20] if isinstance(c, str) and c})
    names = gateway_names({"names": (hit.get("names") or [])[:50]})
    out = {"key": pid, "place_id": pid, "title": title, "source": ns or "unknown source", "namespace": ns,
           "via": "gateway", "is_record": False,
           "names": list(dict.fromkeys([title] + [x[:300] for x in names])),
           "types": gateway_types({"types": (hit.get("types") or [])[:30]}, ns)[:30],
           "ccodes": ccodes, "years": years, "part_of": [], "repr_point": point, "hull_wkt": None,
           "detail": "full"}
    # place#325 Stage 5: a wd: hit's Wikipedia sitelinks, only if the hit carried `links`
    # (the gateway SEARCH model does not declare them today, so none do: see
    # places.wikipedia_links). Read through the same strict URL parser, capped.
    if ns.lower() == "wd" and isinstance(hit.get("links"), list):
        from places.wikipedia_links import member_wikipedia
        out["wikipedia"] = member_wikipedia(hit["links"][:400])
    return out
