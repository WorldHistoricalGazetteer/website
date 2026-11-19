"""The contributor feed: "Match your places" (place#334 sections 4c, 4d, 4i, 4j; Phase 2: place#338).

One page (``/datasets/<id>/matches/``) and its JSON:

* ``GET  matches/candidates/?tier=&offset=`` a page of records with their suggestions, in the
  plan's order (4c): confident, ambiguous, weak, none; later and unqueried on their own tabs.
  Each record and each suggestion carries ``location`` ([lon, lat] or null) for the feed map.
  A contributed suggestion whose dataset the viewer may not see now is left out entirely;
* ``POST matches/review/`` one decision about one record: ``link`` (Same place; ``candidates`` are
  stored suggestions, ``target`` one hand-search result, ``targets`` up to ``MAX_HAND_TARGETS`` of them
  at once, all or none: the same place often sits in several sources), ``reject``
  (Not the same place; optionally also recorded as a ``distinct`` Link), ``none`` (None of
  these fits), ``later`` (Not sure yet), ``reopen`` (undo), and ``decide``: every row of a record
  decided in one request (``same`` / ``not`` / ``unsure`` per suggestion, each with how the
  decision arose: ``explicit``, ``prefilled`` or ``default``);
* ``GET  matches/search/?place=&q=&relax=`` a hand search under the run's constraints, each a
  relaxable chip as in Map your Data's review (place#208);
* ``GET  matches/progress/`` and ``POST matches/regenerate/``;
* ``GET  matches/export/`` the viewer's own decisions as a CSV (one row per Link; a record with
  none has one row stating where its review stands).

Every Link is written by ``api.attestations.create`` with its visibility and cap rules; nothing
here writes an attestation any other way. Access: signed in, ``can_access_beta`` (or, with the v4
release switch on, any signed-in user: ``places.v4_landing.linking_open_to``, place#343), and inside the
dataset's circle (``user_has_private_access``); anyone else gets the same 404 as for a dataset
that does not exist. Link counts are shown only here and on the owner's dataset page (Q11).
"""
import json
import logging
import math
import re
import unicodedata

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Count, Q
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from datasets import candidates, location_adoption, match_export
from datasets.models import Dataset
from datasets.models_matching import CandidateGeneration, MatchCandidate, RecordReviewState
from places.v4_landing import linking_open_to

logger = logging.getLogger("reconciliation")

PAGE_SIZE = 25
TABS = ["confident", "ambiguous", "weak", "none", "later", "unqueried"]
TAB_LABELS = {"confident": "Likely the same place", "ambiguous": "Which one, if any?",
              "weak": "Weak suggestions", "none": "Nothing found", "later": "Not sure yet",
              "unqueried": "Waiting for search"}
MAX_HAND_TARGETS = 10   # places that can be linked from one hand search in one action
RELATIONS = ("closeMatch", "exactMatch")   # Q1: closeMatch by default, exactMatch under "advanced"


def _dataset_for(request, id):
    user = request.user
    ds = Dataset.objects.filter(pk=id).first()
    if ds is None or not linking_open_to(user) or not ds.user_has_private_access(user):
        raise Http404("not found")
    return ds


def _tab_q(tab):
    if tab == "later":
        return Q(state="later")
    return Q(tier=tab, state="unreviewed")


def tab_counts(ds):
    states = RecordReviewState.objects.filter(dataset=ds)
    out = {t: states.filter(_tab_q(t)).count() for t in TABS}
    return out


def progress(ds):
    """What the progress section shows: per-tab counts, decisions, Links made, and the latest
    generation. Contributor-only (Q11)."""
    from api.models import ContributorAttestation
    from places.models import Place
    states = RecordReviewState.objects.filter(dataset=ds)
    by_state = dict(states.values_list("state").annotate(n=Count("id")))
    total = Place.objects.filter(dataset=ds.label).count()
    reviewed = sum(n for s, n in by_state.items() if s != "unreviewed")
    links = (ContributorAttestation.objects.filter(dataset_id=f"whg:{ds.pk}", status__in=("active", "pending"))
             .exclude(relation_type="distinct").count())
    gen = CandidateGeneration.objects.filter(dataset=ds).first()
    return {
        "records": total, "reviewed": reviewed, "linked_records": by_state.get("linked", 0),
        "no_good_match": by_state.get("no_good_match", 0), "later": by_state.get("later", 0),
        "links": links, "tabs": tab_counts(ds),
        "generation": None if gen is None else {
            "id": gen.id, "status": gen.status, "counts": gen.counts,
            "created": gen.created.isoformat(), "finished": gen.finished.isoformat() if gen.finished else None},
    }


def lon_lat(v):
    """``[lon, lat]`` (rounded to 5 places, about a metre) from a 2+ list of finite numbers in
    range, else None. The feed map is given nothing it would have to second-guess."""
    import math
    if not isinstance(v, (list, tuple)) or len(v) < 2:
        return None
    try:
        lon, lat = float(v[0]), float(v[1])
    except (TypeError, ValueError):
        return None
    if not (math.isfinite(lon) and math.isfinite(lat)) or not (-180 <= lon <= 180 and -90 <= lat <= 90):
        return None
    return [round(lon, 5), round(lat, 5)]


def visible_contributed_pks(user, cands):
    """Of the contributed (``whg:<ds>:<src>``) suggestions in ``cands``, the dataset pks whose
    content ``user`` may see NOW, in one query (``api.dataset_access``: public and not embargoed,
    or inside the circle). A suggestion was filtered for whoever generated it, at that time; the
    person looking may be a different collaborator, and a dataset may have gone private or under
    embargo since. A dataset that no longer exists is not in the answer, so its point is withheld."""
    from api.dataset_access import parse_contributed_id, visible_dataset_pks
    pks = set()
    for c in cands:
        parsed = parse_contributed_id(c.candidate_id) if str(c.candidate_id).startswith("whg:") else None
        if parsed:
            pks.add(parsed[0])
    if not pks:
        return set()
    # ``visible_dataset_pks`` hands the ids back unchecked to a user who is not constrained (staff),
    # so a dataset that no longer exists would count as visible to them: intersect with what exists.
    return set(Dataset.objects.filter(pk__in=visible_dataset_pks(user, pks)).values_list("pk", flat=True))


def candidate_visible(c, visible_pks):
    """False for a contributed (``whg:``) suggestion whose dataset the viewer may not see now
    (private, embargoed, deleted, or an id that names no dataset). Such a suggestion is left out
    of the feed ENTIRELY: no title, id, dataset or point, no number in the list. Authority
    suggestions are always shown."""
    cid = str(c.candidate_id or "")
    if not cid.startswith("whg:"):
        return True
    from api.dataset_access import parse_contributed_id
    parsed = parse_contributed_id(cid)
    return parsed is not None and parsed[0] in visible_pks


def candidate_location(c, visible_pks):
    """``(location, withheld)`` for one suggestion: its representative point from the card the
    gateway answered with, unless it is a contributed record the viewer may not see now (then
    ``(None, True)``: never its coordinates)."""
    cid = str(c.candidate_id or "")
    if cid.startswith("whg:"):
        from api.dataset_access import parse_contributed_id
        parsed = parse_contributed_id(cid)
        if parsed is None or parsed[0] not in visible_pks:
            return None, True
    return lon_lat((c.card or {}).get("repr_point")), False


def record_location(place):
    """The record's own representative point (centroid of all its geometries), or None."""
    try:
        return lon_lat(place.repr_point)
    except Exception:  # noqa: BLE001 — a broken geometry is "no location", never a 500
        return None


GROUP_KM = 2.0   # suggestions from different sources this close, with the same name, are one place


def _norm_name(v):
    """A name for comparing, not showing: no accents, no case, letters and digits only."""
    v = unicodedata.normalize("NFKD", str(v or "")).casefold()
    return re.sub(r"[\W_]+", "", "".join(ch for ch in v if not unicodedata.combining(ch)))


def _km(a, b):
    r = 6371.0088
    p1, p2 = math.radians(a[1]), math.radians(b[1])
    h = (math.sin((p2 - p1) / 2) ** 2
         + math.cos(p1) * math.cos(p2) * math.sin(math.radians(b[0] - a[0]) / 2) ** 2)
    return 2 * r * math.asin(math.sqrt(h))


def group_candidates(cands):
    """Mark suggestions that are probably ONE place described by more than one source (the same
    town in GeoNames and in Wikidata). ``cands`` are the serialised suggestions of one record;
    each gets ``group``: None, or the 1-based rank of the best-ranked member of its group. Two
    suggestions are joined when they come from different sources, both have a location no more
    than ``GROUP_KM`` apart, and one's title is a name of the other. This is presentation only:
    nothing is merged, and every member keeps its own decision and its own Link."""
    n = len(cands)
    parent = list(range(n))

    def find(i):
        while parent[i] != i:
            parent[i] = parent[parent[i]]
            i = parent[i]
        return i

    def names(c):
        card = c.get("card") or {}
        out = {_norm_name(card.get("title") or c.get("title"))}
        out.update(_norm_name(x.get("label")) for x in (card.get("names") or []) if isinstance(x, dict))
        out.discard("")
        return out

    nm = [names(c) for c in cands]
    spaces = [{c.get("namespace") or ""} for c in cands]   # per root: the sources already in the group
    for i in range(n):
        for j in range(i + 1, n):
            a, b = cands[i], cands[j]
            if (a.get("namespace") or "") == (b.get("namespace") or ""):
                continue
            if not (isinstance(a.get("location"), list) and isinstance(b.get("location"), list)):
                continue
            if _km(a["location"], b["location"]) > GROUP_KM or not (nm[i] & nm[j]):
                continue
            ri, rj = find(i), find(j)
            # One source never has two members in a group: two GeoNames entries are two places
            # however close they lie, so a Wikidata entry cannot make them one.
            if ri == rj or spaces[ri] & spaces[rj]:
                continue
            parent[rj] = ri
            spaces[ri] |= spaces[rj]
    members = {}
    for i in range(n):
        members.setdefault(find(i), []).append(i)
    for idx in members.values():
        first = min(cands[i]["rank"] for i in idx)
        for i in idx:
            cands[i]["group"] = first if len(idx) > 1 else None
    return cands


def _record_details(p):
    """What the person needs to compare against: the record's other names, its types and country
    codes, from rows already loaded (``feed_candidates`` prefetches them)."""
    names, seen = [], {p.title}
    for n in p.names.all():
        if n.toponym and n.toponym not in seen:
            seen.add(n.toponym)
            names.append(n.toponym)
    types = []
    for t in p.types.all():
        j = t.jsonb or {}
        label = j.get("sourceLabel") or j.get("label")
        if label and label not in types:
            types.append(label)
    return {"names": names[:8], "types": types[:4], "ccodes": list(p.ccodes or [])[:4]}


def _candidate_json(c, visible_pks=frozenset(), loc=None, record_info=None):
    sig = c.signals or {}
    location, withheld = candidate_location(c, visible_pks)
    card = dict(c.card or {})
    if withheld:
        card.pop("repr_point", None)
    out = {"id": c.id, "candidate_id": c.candidate_id, "title": c.candidate_title, "namespace": c.namespace,
           "tier": c.tier, "rank": c.rank, "status": c.status, "confidence": c.gateway_confidence,
           "km": sig.get("km"), "composite": sig.get("composite"),
           "demoted": str(sig.get("type_sanity", "ok")) != "ok",
           "location": location, "location_withheld": withheld,
           "explanation": c.explanation or {}, "card": card}
    if loc is not None:
        # place#350: whether this place's location may be adopted (owner only), else a reference.
        out["adopt"] = location_adoption.adopt_info(
            c, location, withheld, loc["entries"].get(c.namespace), loc["ds"], loc["user"], record_info,
            loc["hidden"])
    return out


def location_context(ds, user, cands):
    """What ``_record_json`` needs to say, per suggestion, whether its location can be adopted:
    the registry rows of the sources in view, and the namespaces under an embargo for ``user``."""
    from api.models import GazetteerRegistryEntry
    return {"ds": ds, "user": user,
            "entries": location_adoption.registry_entries({c.namespace for c in cands}),
            "hidden": GazetteerRegistryEntry.objects.hidden_namespaces_for(user)}


def _record_json(st, cands, visible_pks=frozenset(), loc=None):
    p = st.place
    info = location_adoption.location_info(p)
    can_place = bool(loc and info is None and location_adoption.is_owner(loc["user"], loc["ds"])
                     and not location_adoption.is_sensitive(loc["ds"]))
    return {"place": p.pk, "title": p.title, "src_id": p.src_id, "subject_id": f"whg:{p.pk}",
            "tier": st.tier, "state": st.state, "has_geometry": st.has_geometry,
            "location": record_location(p), "location_info": info, "can_place": can_place,
            "minmax": p.minmax, "note": st.note, "last_failure": st.last_failure,
            "decided_at": st.decided_at.isoformat() if st.decided_at else None,
            **_record_details(p),
            # Ranks are renumbered over what is shown, so a hidden suggestion leaves no gap.
            "candidates": group_candidates([{**_candidate_json(c, visible_pks, loc, info), "rank": n}
                                            for n, c in enumerate(cands, start=1)])}


@login_required
@require_GET
def feed_page(request, id):
    ds = _dataset_for(request, id)
    tab = request.GET.get("tier") or ""
    counts = tab_counts(ds)
    if tab not in TABS:   # resume (4d): the first non-empty tab, in the plan's order
        tab = next((t for t in TABS if counts[t]), "confident")
    return render(request, "datasets/match_feed.html", {
        "ds": ds, "tab": tab, "tabs": [(t, TAB_LABELS[t], counts[t]) for t in TABS],
        "record": request.GET.get("record") or "", "progress": progress(ds)})


@login_required
@require_GET
def feed_candidates(request, id):
    ds = _dataset_for(request, id)
    tab = request.GET.get("tier") or "confident"
    if tab not in TABS:
        return JsonResponse({"error": f"tier must be one of {TABS}"}, status=400)
    try:
        offset = max(0, int(request.GET.get("offset") or 0))
    except ValueError:
        offset = 0
    order = {"confident": ("has_geometry", "-shared_count", "place_id"),
             "ambiguous": ("-shared_count", "place_id")}.get(tab, ("place_id",))
    qs = (RecordReviewState.objects.filter(dataset=ds).filter(_tab_q(tab))
          .select_related("place").prefetch_related("place__geoms__reviewer", "place__names", "place__types")
          .order_by(*order))
    total = qs.count()
    page = list(qs[offset:offset + PAGE_SIZE])
    cands = {}
    for c in (MatchCandidate.objects.filter(place_id__in=[s.place_id for s in page])
              .exclude(status="stale").order_by("place_id", "rank")):
        cands.setdefault(c.place_id, []).append(c)
    # One visibility query per load; hidden suggestions are dropped before anything is serialised.
    every = [c for cs in cands.values() for c in cs]
    visible_pks = visible_contributed_pks(request.user, every)
    loc = location_context(ds, request.user, every)
    records = [_record_json(s, [c for c in cands.get(s.place_id, [])
                                if c.generation_id == s.generation_id and candidate_visible(c, visible_pks)],
                            visible_pks, loc)
               for s in page]
    return JsonResponse({"tier": tab, "offset": offset, "total": total, "records": records,
                         "next": offset + PAGE_SIZE if offset + PAGE_SIZE < total else None,
                         "counts": tab_counts(ds)})


def _refused(exc):
    return JsonResponse({"error": exc.message}, status=exc.status)


# "prefilled" was retired 2026-11-17: the page never fills in a decision for the person, so a
# decision claiming that source is refused (rows written earlier keep their stored value).
SOURCES = ("explicit", "default")
DECISIONS = ("same", "not", "unsure")


def _link_one(request, ds, st, c, target, relation, certainty, note, source=""):
    """One Link from a suggestion (``c``) or a hand-search target, through the attestation rules.
    A hand-search Link has no candidate, so it is marked in its frozen evidence with the feed
    context that made it; Undo matches on this. Returns the attestation id."""
    from api import attestations
    subject = f"whg:{st.place_id}"
    evidence = {"feed_dataset": ds.pk, "feed_place": st.place_id, "hand_search": True} if c is None else {
        "generation": c.generation_id, "tier": c.tier, "rank": c.rank,
        "confidence": c.gateway_confidence, "signals": c.signals,
        "explanation": (c.explanation or {}).get("sentence", ""),
        "feed_dataset": ds.pk, "feed_place": st.place_id, "decision_source": source or "explicit"}
    row, _created = attestations.create(
        request.user, {"place_a": subject, "place_b": target, "relation_type": relation,
                       "certainty": certainty, "justification": note or None},
        candidate=c, evidence=evidence)
    if c is not None:
        c.status, c.decided_at, c.decided_by = "accepted", timezone.now(), request.user
        c.decision_source = source or "explicit"
        c.save(update_fields=["status", "decided_at", "decided_by", "decision_source"])
    return row.id


def _reject_one(request, st, c, note, distinct=False, source="explicit"):
    """Set one suggestion aside ("not this one"), and with ``distinct`` also say so as a
    statement others can see. Returns the attestation id or None."""
    from api import attestations
    made = None
    if distinct:
        row, _ = attestations.create(
            request.user, {"place_a": f"whg:{st.place_id}", "place_b": c.candidate_id,
                           "relation_type": "distinct", "justification": note or None},
            candidate=c, evidence={"generation": c.generation_id, "tier": c.tier,
                                   "decision_source": source})
        made = row.id
    c.status, c.decided_at, c.decided_by = "rejected", timezone.now(), request.user
    c.decision_source = source
    c.save(update_fields=["status", "decided_at", "decided_by", "decision_source"])
    return made


@login_required
@require_POST
def feed_review(request, id):
    """One decision about one record (see module docstring)."""
    from api import attestations
    ds = _dataset_for(request, id)
    try:
        body = json.loads(request.body or b"{}")
    except ValueError:
        return JsonResponse({"error": "body must be JSON"}, status=400)
    action = body.get("action")
    st = (RecordReviewState.objects.filter(dataset=ds, place_id=body.get("place"))
          .select_related("place").first())
    if st is None:
        return JsonResponse({"error": "not found"}, status=404)
    note = str(body.get("note") or "").strip()[:2000]
    now = timezone.now()
    cand_ids = [int(x) for x in (body.get("candidates") or []) if str(x).isdigit()]
    cands = list(MatchCandidate.objects.filter(place=st.place, pk__in=cand_ids).exclude(status="stale"))
    if len(cands) != len(set(cand_ids)):
        return JsonResponse({"error": "not found"}, status=404)
    made = []

    def reply():
        return JsonResponse({"place": st.place_id, "state": st.state, "attestations": made,
                             "counts": tab_counts(ds), "progress": progress(ds)})

    if action == "link":
        relation = body.get("relation") or "closeMatch"
        if relation not in RELATIONS:
            return JsonResponse({"error": f"relation must be one of {list(RELATIONS)}"}, status=400)
        certainty = body.get("certainty") or ""
        targets = [(c, c.candidate_id) for c in cands]
        if body.get("target"):   # a hand-search result: a Link with no suggestion behind it
            targets.append((None, str(body["target"])))
        extra = body.get("targets")   # several hand-search results at once (the same place in other sources)
        if extra is not None:
            if not isinstance(extra, list) or not all(isinstance(t, str) and 0 < len(t.strip()) <= 128 for t in extra):
                return JsonResponse({"error": "targets must be a list of place ids"}, status=400)
            have = {t for _, t in targets}
            wanted = list(dict.fromkeys(t.strip() for t in extra))
            if len(wanted) > MAX_HAND_TARGETS:
                return JsonResponse({"error": f"link at most {MAX_HAND_TARGETS} places at a time"}, status=400)
            targets += [(None, t) for t in wanted if t not in have]
        if not targets:
            return JsonResponse({"error": "nothing to link"}, status=400)
        if any(c.status == "rejected" for c in cands):
            # Setting a suggestion aside and then linking it would leave the record saying both.
            return JsonResponse({"error": "A suggestion you set aside cannot be marked Same place until you undo that."},
                                status=409)
        try:
            with transaction.atomic():
                for c, target in targets:
                    made.append(_link_one(request, ds, st, c, target, relation, certainty, note,
                                          source=body.get("source") or "explicit"))
        except attestations.AttestationRefused as exc:
            return _refused(exc)
        st.state = "linked"
    elif action == "reject":
        if not cands:
            return JsonResponse({"error": "nothing to reject"}, status=400)
        try:
            with transaction.atomic():
                for c in cands:
                    made_id = _reject_one(request, st, c, note, distinct=bool(body.get("distinct")))
                    if made_id:
                        made.append(made_id)
        except attestations.AttestationRefused as exc:
            return _refused(exc)
        return reply()
    elif action == "decide":
        # Every row of the record decided at once. "unsure" leaves a suggestion as it is.
        relation = body.get("relation") or "closeMatch"
        if relation not in RELATIONS:
            return JsonResponse({"error": f"relation must be one of {list(RELATIONS)}"}, status=400)
        certainty = body.get("certainty") or ""
        publish = bool(body.get("distinct"))
        raw = body.get("decisions")
        if not isinstance(raw, list) or not raw:
            return JsonResponse({"error": "decisions must be a non-empty list"}, status=400)
        pool = {c.pk: c for c in MatchCandidate.objects.filter(place=st.place, generation_id=st.generation_id)
                .exclude(status="stale")}
        picked, seen = [], set()
        for d in raw:
            try:
                cid = int(d.get("candidate"))
            except (TypeError, ValueError, AttributeError):
                return JsonResponse({"error": "each decision needs a candidate id"}, status=400)
            dec, src = d.get("decision"), d.get("source") or "explicit"
            if dec not in DECISIONS or src not in SOURCES:
                return JsonResponse({"error": "decision must be one of "
                                              f"{list(DECISIONS)} and source one of {list(SOURCES)}"}, status=400)
            if cid not in pool or cid in seen:
                return JsonResponse({"error": "not found"}, status=404)
            seen.add(cid)
            picked.append((pool[cid], dec, src))
        same = [(c, src) for c, dec, src in picked if dec == "same"]
        nots = [(c, src) for c, dec, src in picked if dec == "not"]
        if not same and not nots:
            return JsonResponse({"error": "nothing decided"}, status=400)
        if any(c.status == "rejected" for c, _ in same):
            return JsonResponse({"error": "A suggestion you set aside cannot be marked Same place until you undo that."},
                                status=409)
        if any(c.status == "accepted" for c, _ in nots):
            return JsonResponse({"error": "A suggestion you marked Same place cannot be set aside until you undo that."},
                                status=409)
        try:
            with transaction.atomic():
                for c, src in same:
                    made.append(_link_one(request, ds, st, c, c.candidate_id, relation, certainty, note,
                                          source=src))
                for c, src in nots:
                    # A statement others can see is made only for a negative the person marked
                    # themselves, never for one the page filled in.
                    made_id = _reject_one(request, st, c, note, distinct=publish and src == "explicit",
                                          source=src)
                    if made_id:
                        made.append(made_id)
        except attestations.AttestationRefused as exc:
            return _refused(exc)
        if same:
            st.state = "linked"
        else:
            visible_pks = visible_contributed_pks(request.user, pool.values())
            shown = [c for c in pool.values() if candidate_visible(c, visible_pks)]
            if shown and all(c.status == "rejected" for c in shown):
                st.state = "no_good_match"
    elif action == "none":
        (MatchCandidate.objects.filter(place=st.place, generation_id=st.generation_id, status="suggested")
         .update(status="rejected", decided_at=now, decided_by=request.user, decision_source="none_fits"))
        st.state = "no_good_match"
    elif action == "later":
        st.state = "later"
    elif action == "reopen":
        # place#350: a location adopted from one of this record's matches goes with the match
        # only if the owner says so; it is never silently kept. Asked first, nothing undone yet.
        adopted = location_adoption.adopted_rows(st.place)
        choice = body.get("adopted_location")
        if adopted:
            if not location_adoption.is_owner(request.user, ds):
                return JsonResponse({"error": "The dataset owner adopted this place's location from the match; "
                                              "only they can undo it.", "reason": "owner_only"}, status=409)
            if choice not in ("remove", "keep"):
                return JsonResponse({
                    "error": "This record's location was adopted from the match. Remove it too, or keep it?",
                    "reason": "adopted_location",
                    "adopted": [{"geom": g.pk, **location_adoption.location_info(st.place, [g])} for g in adopted]},
                    status=409)
            if choice == "remove":
                for g in adopted:
                    location_adoption.remove(request.user, ds, st.place_id, g.pk)
            else:
                # place#343: kept by the owner's explicit choice, so the Link's withdrawal below
                # does not take it back (datasets.location_lifecycle).
                from datasets.location_lifecycle import keep_after_undo
                keep_after_undo(request.user, adopted)
        # Undo: take back the Links this person made from this record's suggestions, and put
        # the record back in its tab. Rows are withdrawn, never deleted.
        # That includes hand-search Links (no candidate), found by the feed marker that
        # ``link`` writes into their evidence; Links made elsewhere carry no marker.
        from django.db.models import Q
        from api.models import ContributorAttestation
        for row in ContributorAttestation.objects.filter(
                Q(candidate__place=st.place)
                | Q(evidence__feed_dataset=ds.pk, evidence__feed_place=st.place_id),
                user=request.user, status__in=("active", "pending")):
            attestations.withdraw(row, by=request.user)
        (MatchCandidate.objects.filter(place=st.place, generation_id=st.generation_id)
         .exclude(status="stale").update(status="suggested", decided_at=None, decided_by=None,
                                         decision_source=""))
        st.state = "unreviewed"
    else:
        return JsonResponse(
            {"error": "action must be link, reject, decide, none, later or reopen"}, status=400)

    st.decided_at, st.decided_by = now, request.user
    if note:
        st.note = note
    st.save()
    return reply()


def constraint_chips(generation, place):
    """The run's constraints for this record, as relaxable chips (MyD place#208)."""
    recipe = (generation.recipe if generation else None) or {}
    chips = []
    if recipe.get("countries"):
        chips.append({"id": "countries", "label": "in " + ", ".join(recipe["countries"]),
                      "title": "Only places in the countries chosen for this run"})
    if recipe.get("contained_in"):
        chips.append({"id": "contained_in", "label": "inside the chosen region",
                      "title": "Only places inside the region chosen for this run"})
    if recipe.get("namespaces"):
        chips.append({"id": "namespaces", "label": "sources: " + ", ".join(recipe["namespaces"]),
                      "title": "Only the sources chosen for this run"})
    mm = place.minmax or []
    if len(mm) == 2 and (mm[0] is not None or mm[1] is not None):
        chips.append({"id": "dates", "label": f"dates {mm[0] if mm[0] is not None else ''}–"
                                              f"{mm[1] if mm[1] is not None else ''}",
                      "title": "Only places whose dates may overlap this record's (undated places included)"})
    return chips


def hand_results(scored, user):
    """Hand-search results as the feed serves them, each with ``location`` ([lon, lat] or null)
    by the SAME rules as a stored suggestion (``candidate_visible`` / ``candidate_location``): a
    contributed (``whg:``) result is served only if ``user`` may see its dataset now, and is left
    out ENTIRELY otherwise (no title, id or point); authority results are always served. This
    holds even if the gateway filter upstream (``candidates.filter_hits``) is ever loosened."""
    from types import SimpleNamespace
    rows = [SimpleNamespace(candidate_id=c["id"], card=c["card"], scored=c) for c in scored]
    visible_pks = visible_contributed_pks(user, rows)
    out = []
    for row in rows:
        if not candidate_visible(row, visible_pks):
            continue
        c = row.scored
        location, withheld = candidate_location(row, visible_pks)
        out.append({"candidate_id": c["id"], "title": c["card"]["title"], "namespace": c["card"]["namespace"],
                    "confidence": c["confidence"], "km": c["km"], "demoted": not c["sane"],
                    "location": location, "location_withheld": withheld,
                    "explanation": c["explanation"], "card": c["card"]})
    return out


@login_required
@require_GET
def feed_search(request, id):
    """Search by hand for one record. Results are scored and explained like suggestions but not
    stored; linking one sends ``target`` to ``feed_review``."""
    ds = _dataset_for(request, id)
    st = (RecordReviewState.objects.filter(dataset=ds, place_id=request.GET.get("place"))
          .select_related("place", "generation").first())
    if st is None:
        return JsonResponse({"error": "not found"}, status=404)
    place = st.place
    q = (request.GET.get("q") or place.title).strip()[:200]
    relax = {x for x in (request.GET.get("relax") or "").split(",") if x}
    recipe = dict((st.generation.recipe if st.generation else None) or candidates.build_recipe(ds))
    for key in ("countries", "contained_in", "namespaces"):
        if key in relax:
            recipe.pop(key, None)
    opts = candidates.search_options(place, recipe)
    opts["qstr"] = q
    if "dates" in relax:
        for k in ("temporal", "start", "end", "temporal_mode", "undated"):
            opts.pop(k, None)
    chips = [{**c, "applied": c["id"] not in relax} for c in constraint_chips(st.generation, place)]
    try:
        data, failure = candidates.gateway_search(opts, request.user)
    except Exception:  # noqa: BLE001
        data, failure = None, "error"
    if failure is not None or not isinstance(data, dict):
        # Silence, said as silence: never an empty result list.
        return JsonResponse({"answered": False, "failure": failure or "error", "constraints": chips,
                             "results": []}, status=200)
    _tier, scored, _rec = candidates.assess(place, data, request.user)
    results = hand_results(scored[:20], request.user)
    return JsonResponse({"answered": True, "q": q, "constraints": chips, "results": results})


EXPORT_COLUMNS = (["src_id", "title", "review_state", "relation", "certainty", "link_status"]
                  + match_export.COLUMNS + ["confidence", "decided_at", "note"])


@login_required
@require_GET
def feed_export(request, id):
    """The viewer's own decisions in this feed as CSV: every record the feed holds, with one row
    per Link made from it (``active`` or ``pending``; ``distinct`` statements are not matches and
    withdrawn or rejected Links are no longer standing). Records without a Link have one row that
    says where their review stands. Every matched place carries its full record, read from the
    gateway now (``datasets.match_export``), so a hand-search Link is as complete as a suggestion;
    what may not leave WHG (a licence that forbids re-distribution, an embargo, a contributed record
    the viewer may not see) is withheld and ``matched_detail`` says so."""
    import csv
    from api.models import ContributorAttestation
    from django.http import HttpResponse
    ds = _dataset_for(request, id)
    atts = (ContributorAttestation.objects
            .filter(user=request.user, status__in=("active", "pending"), evidence__feed_dataset=ds.pk)
            .exclude(relation_type="distinct").select_related("candidate").order_by("asserted_at"))
    by_place, matches = {}, []
    for a in atts:
        pk = a.evidence.get("feed_place")
        if pk is None:
            continue
        subject = f"whg:{int(pk)}"
        matched = a.place_b if a.place_a == subject else a.place_a
        by_place.setdefault(int(pk), []).append((a, matched))
        matches.append((matched, a.candidate))
    detail = match_export.details_for(request.user, matches)
    blank = [""] * len(match_export.COLUMNS)

    response = HttpResponse(content_type="text/csv; charset=utf-8")
    response["Content-Disposition"] = f'attachment; filename="whg-matches-dataset-{ds.pk}.csv"'
    response.write("\ufeff")   # so Excel reads it as UTF-8
    out = csv.writer(response)
    out.writerow(EXPORT_COLUMNS)
    states = RecordReviewState.objects.filter(dataset=ds).select_related("place").order_by("place_id")
    for st in states.iterator():
        p = st.place
        rows = by_place.get(p.pk) or []
        if not rows:
            out.writerow([p.src_id, p.title, st.state, "", "", ""] + blank + ["", "", st.note])
            continue
        for a, matched in rows:
            d = detail.get(matched, {})
            c = a.candidate
            out.writerow([p.src_id, p.title, st.state, a.relation_type, a.certainty, a.status]
                         + [d.get(k, "") for k in match_export.COLUMNS]
                         + [c.gateway_confidence if c is not None and c.gateway_confidence is not None else "",
                            a.asserted_at.date().isoformat(), a.justification or st.note])
    return response


@login_required
@require_GET
def feed_progress(request, id):
    return JsonResponse(progress(_dataset_for(request, id)))


@login_required
@require_POST
def feed_regenerate(request, id):
    """"Look again": a new generation for the records nobody has decided yet."""
    from datasets import tasks_candidates
    ds = _dataset_for(request, id)
    try:
        body = json.loads(request.body or b"{}")
    except ValueError:
        body = {}
    try:
        gen = tasks_candidates.start_generation(ds, request.user, countries=body.get("countries"),
                                                contained_in=body.get("contained_in"))
    except candidates.GenerationRefused as exc:
        return JsonResponse({"error": str(exc)}, status=409)
    return JsonResponse({"generation": gen.id, "status": gen.status}, status=202)


@login_required
@require_POST
def feed_location(request, id):
    """place#350: ``adopt`` a matched place's location, ``place`` one by hand, or ``remove``
    either (``datasets.location_adoption`` holds every rule). Dataset owner only."""
    ds = _dataset_for(request, id)
    try:
        body = json.loads(request.body or b"{}")
    except ValueError:
        return JsonResponse({"error": "body must be JSON"}, status=400)
    if not isinstance(body, dict):
        return JsonResponse({"error": "body must be a JSON object"}, status=400)
    action = body.get("action")
    try:
        place_pk = int(body.get("place"))
    except (TypeError, ValueError):
        return JsonResponse({"error": "not found"}, status=404)
    LA = location_adoption
    try:
        if action == "adopt":
            try:
                cand = int(body.get("candidate"))
            except (TypeError, ValueError):
                return JsonResponse({"error": "not found"}, status=404)
            g = LA.adopt(request.user, ds, place_pk, cand)
        elif action == "place":
            g = LA.place_by_hand(request.user, ds, place_pk, body)
        elif action == "remove":
            try:
                geom = int(body.get("geom"))
            except (TypeError, ValueError):
                return JsonResponse({"error": "not found"}, status=404)
            LA.remove(request.user, ds, place_pk, geom)
            g = None
        else:
            return JsonResponse({"error": "action must be adopt, place or remove"}, status=400)
    except LA.LocationRefused as exc:
        return JsonResponse({"error": exc.message, "reason": exc.reason}, status=exc.status)
    from places.models import Place
    place = Place.objects.prefetch_related("geoms__reviewer").get(pk=place_pk)
    return JsonResponse({"place": place_pk, "location": record_location(place),
                         "location_info": LA.location_info(place),
                         "can_place": (LA.location_info(place) is None and LA.is_owner(request.user, ds)
                                       and not LA.is_sensitive(ds))},
                        status=201 if g is not None else 200)
