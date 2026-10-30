"""The "Links" section of the public Record page (place#343, Phase 6 of place#334; ruling Q7).

Shows, server-side, the ACTIVE Links (``api.ContributorAttestation``) that name this record:
who made each one, the relation in plain words, the date, where it came from (a reviewed
v3 match carried over, or one made in v4), the licence (CC BY 4.0, ruling Q6) and, for a
signed-in viewer, "Dispute this Link", which opens the existing Link sheet (``attest.js``)
pre-set to the opposite answer and writes through ``POST /api/links`` like any other Link.
There is no write path here.

Shown only with the v4 release switch on (``places.v4_landing.v4_landing_enabled``), so it is
dark wherever the switch is off (prod, today).

What it shows is the same for every viewer (the anonymous rule), whoever is looking:

* only ``status == "active"``: pending, rejected, superseded and withdrawn rows never appear;
* only when THIS record passes the anonymous visibility rule (public, not embargoed): an
  owner viewing their own private record sees nothing here (their pending Links are in the
  Concordance, which is circle-aware);
* the other side must pass the same rule: a WHG record in a public, unembargoed dataset
  (checked through its stored Place pk, in the same query), or an authority namespace with a
  registry row visible to an anonymous caller. A row whose other side fails is dropped
  WHOLE, so nothing of it (id, title, dataset name) reaches the page. Active rows should
  never name a private record (making a dataset private returns its Links to pending), but
  this does not rely on that.

Cost: ONE query for the rows (the other side's visibility, title and dataset title are
correlated subqueries in it; the attester comes by ``select_related``), plus the embargo
list the anonymous rule reads, one check that this record passes it, and, only when some
row names an authority, one registry query. None of it grows with the number of rows.
"""
from __future__ import annotations

from urllib.parse import quote

from django.conf import settings
from django.db.models import OuterRef, Q, Subquery

# What each relation is called on screen (place#334 Q9: Link / Same place / Not the same
# place). Mirrors attest.js RELATIONS; sameAs is the exactMatch alias.
RELATION_LABELS = {
    "closeMatch": "Same place",
    "exactMatch": "Same place, exactly",
    "sameAs": "Same place, exactly",
    "distinct": "Not the same place",
}
# The answer a dispute preselects in the Link sheet: the opposite of the Link's own.
DISPUTE_RELATION = {"closeMatch": "distinct", "exactMatch": "distinct", "sameAs": "distinct",
                    "distinct": "closeMatch"}
ANONYMOUS_ATTESTER = "a WHG contributor"
WHG_NS = "whg"
MAX_ROWS = 200


def shows_attester_name(attester, viewer=None, per_user=True) -> bool:
    """Whether a Link's maker is named to ``viewer``. THE one place this is decided
    (also used by ``api.attestations.attester``).

    A maker is named to EVERYONE when they opted in (``User.show_name_on_links``, set on the
    Profile page); otherwise only to beta users (the pre-release state, unchanged) and to
    themselves; everyone else reads "a WHG contributor".

    ``per_user=False`` (a result cached per viewer CLASS, e.g. the Concordance) answers on
    viewer-class facts ONLY: it drops the "themselves" case, which would otherwise put one
    person's view in a shared entry, AND the maker's opt-in, which they can change at any
    time and which would otherwise stay in the cache after they withdrew it. The cached
    Concordance applies the opt-in after the cache instead
    (``places.concordance.decorate_for_user``).
    """
    if attester is None:
        return False
    if per_user and getattr(attester, "show_name_on_links", False):
        return True
    if viewer is not None and getattr(viewer, "is_authenticated", False):
        if getattr(viewer, "can_access_beta", False):
            return True
        if per_user and getattr(viewer, "id", None) == getattr(attester, "id", object()):
            return True
    return False


def _licence():
    lic = getattr(settings, "WHG_OVERLAY_LICENSE", None) or {}
    return {"label": "CC BY 4.0", "url": lic.get("url") or "https://creativecommons.org/licenses/by/4.0/"}


def record_links(place, viewer=None) -> list[dict]:
    """The Links to show in the Record page's "Links" section for ``place``: a list of
    display dicts (see the module docstring for the rule). ``viewer`` only decides ``mine``
    and the maker's name (``shows_attester_name``), never which rows appear."""
    from api.attestations import record_ids
    from api.dataset_access import visible_datasets_q
    from api.models import ContributorAttestation, GazetteerRegistryEntry
    from places.models import Place

    if place is None or place.pk is None:
        return []
    # The anonymous rule (public, not embargoed), built once: it reads the embargo list.
    anon_q = visible_datasets_q(None, prefix="dataset__")
    visible_places = Place.objects.filter(anon_q)
    if not visible_places.filter(pk=place.pk).exists():
        return []

    def side(field, col):
        return Subquery(visible_places.filter(pk=OuterRef(field)).values(col)[:1])

    ids = sorted(record_ids(place))
    rows = list(ContributorAttestation.objects
                .filter(status="active")
                .filter(Q(place_a__in=ids) | Q(place_b__in=ids))
                .annotate(a_title=side("place_a_pk", "title"), a_ds=side("place_a_pk", "dataset__title"),
                          a_ok=side("place_a_pk", "id"),
                          b_title=side("place_b_pk", "title"), b_ds=side("place_b_pk", "dataset__title"),
                          b_ok=side("place_b_pk", "id"))
                .select_related("user")
                .order_by("-asserted_at", "id")[:MAX_ROWS])

    candidates = []
    for r in rows:
        a_is_me = r.place_a in ids
        if a_is_me and r.place_b in ids:
            continue  # both ids name this record: not a Link to anything else
        other = r.place_b if a_is_me else r.place_a
        s = "b" if a_is_me else "a"
        ns = other.split(":", 1)[0]
        if ns == WHG_NS:
            ok = getattr(r, f"{s}_ok")
            if ok is None:  # no stored record, or a record the anonymous rule does not admit
                continue
            other_view = {"kind": "record", "id": f"{WHG_NS}:{ok}", "pk": ok,
                          "title": getattr(r, f"{s}_title") or "(untitled record)",
                          "dataset": getattr(r, f"{s}_ds") or "",
                          "href": f"/atlas/place/{ok}/"}
        else:
            other_view = {"kind": "authority", "id": other, "namespace": ns, "title": other,
                          "href": "/atlas/?place=" + quote(other, safe=":")}
        candidates.append((r, other_view))

    namespaces = {o["namespace"] for _, o in candidates if o["kind"] == "authority"}
    if namespaces:
        visible_ns = set(GazetteerRegistryEntry.objects.visible_to(None)
                         .filter(id__in=namespaces).values_list("id", flat=True))
        candidates = [(r, o) for r, o in candidates
                      if o["kind"] != "authority" or o["namespace"] in visible_ns]

    licence = _licence()
    uid = getattr(viewer, "id", None) if getattr(viewer, "is_authenticated", False) else None
    out = []
    for r, other in candidates:
        mine = uid is not None and r.user_id == uid
        named = shows_attester_name(r.user, viewer)
        orcid = (getattr(r.user, "orcid", None) or "") if named else ""
        out.append({
            "id": r.id,
            "relation": r.relation_type,
            "relation_label": RELATION_LABELS.get(r.relation_type, r.relation_type),
            "dispute_relation": DISPUTE_RELATION.get(r.relation_type, "distinct"),
            "dispute_label": RELATION_LABELS[DISPUTE_RELATION.get(r.relation_type, "distinct")],
            "other": other,
            "by": "you" if mine else ((getattr(r.user, "name", "") or ANONYMOUS_ATTESTER) if named
                                      else ANONYMOUS_ATTESTER),
            "orcid": orcid if orcid.startswith("https://orcid.org/") else "",
            "mine": mine,
            "date": r.asserted_at.date().isoformat() if r.asserted_at else "",
            "legacy": bool(r.legacy_v3_2),
            "source_label": ("Carried over from a reviewed WHG v3 match" if r.legacy_v3_2
                             else "Made in WHG"),
            "justification": (r.justification or "").strip(),
            "licence": licence,
        })
    return out

