"""The contributor feed: "Link your records" (place#334 sections 4c, 4d, 4i, 4j; Phase 2: place#338).

One page (``/datasets/<id>/matches/``) and its JSON:

* ``GET  matches/candidates/?tier=&offset=`` a page of records with their suggestions, in the
  plan's order (4c): confident, ambiguous, weak, none; later and unqueried on their own tabs;
* ``POST matches/review/`` one decision about one record: ``link`` (Same place), ``reject``
  (Not the same place; optionally also recorded as a ``distinct`` Link), ``none`` (None of
  these fits), ``later`` (Not sure yet), ``reopen`` (undo);
* ``GET  matches/search/?place=&q=&relax=`` a hand search under the run's constraints, each a
  relaxable chip as in Map your Data's review (place#208);
* ``GET  matches/progress/`` and ``POST matches/regenerate/``.

Every Link is written by ``api.attestations.create`` with its visibility and cap rules; nothing
here writes an attestation any other way. Access: signed in, ``can_access_beta`` (or, with the v4
release switch on, any signed-in user: ``places.v4_landing.linking_open_to``, place#343), and inside the
dataset's circle (``user_has_private_access``); anyone else gets the same 404 as for a dataset
that does not exist. Link counts are shown only here and on the owner's dataset page (Q11).
"""
import json
import logging

from django.contrib.auth.decorators import login_required
from django.db import transaction
from django.db.models import Count, Q
from django.http import Http404, JsonResponse
from django.shortcuts import render
from django.utils import timezone
from django.views.decorators.http import require_GET, require_POST

from datasets import candidates
from datasets.models import Dataset
from datasets.models_matching import CandidateGeneration, MatchCandidate, RecordReviewState
from places.v4_landing import linking_open_to

logger = logging.getLogger("reconciliation")

PAGE_SIZE = 25
TABS = ["confident", "ambiguous", "weak", "none", "later", "unqueried"]
TAB_LABELS = {"confident": "Likely the same place", "ambiguous": "Which one, if any?",
              "weak": "Weak suggestions", "none": "Nothing found", "later": "Later",
              "unqueried": "Not yet checked"}
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


def _candidate_json(c):
    sig = c.signals or {}
    return {"id": c.id, "candidate_id": c.candidate_id, "title": c.candidate_title, "namespace": c.namespace,
            "tier": c.tier, "rank": c.rank, "status": c.status, "confidence": c.gateway_confidence,
            "km": sig.get("km"), "composite": sig.get("composite"),
            "demoted": str(sig.get("type_sanity", "ok")) != "ok",
            "explanation": c.explanation or {}, "card": c.card or {}}


def _record_json(st, cands):
    p = st.place
    return {"place": p.pk, "title": p.title, "src_id": p.src_id, "subject_id": f"whg:{p.pk}",
            "tier": st.tier, "state": st.state, "has_geometry": st.has_geometry,
            "minmax": p.minmax, "note": st.note, "last_failure": st.last_failure,
            "candidates": [_candidate_json(c) for c in cands]}


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
          .select_related("place").order_by(*order))
    total = qs.count()
    page = list(qs[offset:offset + PAGE_SIZE])
    cands = {}
    for c in (MatchCandidate.objects.filter(place_id__in=[s.place_id for s in page])
              .exclude(status="stale").order_by("place_id", "rank")):
        cands.setdefault(c.place_id, []).append(c)
    records = [_record_json(s, [c for c in cands.get(s.place_id, []) if c.generation_id == s.generation_id])
               for s in page]
    return JsonResponse({"tier": tab, "offset": offset, "total": total, "records": records,
                         "next": offset + PAGE_SIZE if offset + PAGE_SIZE < total else None,
                         "counts": tab_counts(ds)})


def _refused(exc):
    return JsonResponse({"error": exc.message}, status=exc.status)


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
    subject = f"whg:{st.place_id}"
    cand_ids = [int(x) for x in (body.get("candidates") or []) if str(x).isdigit()]
    cands = list(MatchCandidate.objects.filter(place=st.place, pk__in=cand_ids).exclude(status="stale"))
    if len(cands) != len(set(cand_ids)):
        return JsonResponse({"error": "not found"}, status=404)
    made = []

    if action == "link":
        relation = body.get("relation") or "closeMatch"
        if relation not in RELATIONS:
            return JsonResponse({"error": f"relation must be one of {list(RELATIONS)}"}, status=400)
        certainty = body.get("certainty") or ""
        targets = [(c, c.candidate_id) for c in cands]
        if body.get("target"):   # a hand-search result: a Link with no suggestion behind it
            targets.append((None, str(body["target"])))
        if not targets:
            return JsonResponse({"error": "nothing to link"}, status=400)
        try:
            with transaction.atomic():
                for c, target in targets:
                    # A hand-search Link has no candidate, so it is marked in its frozen
                    # evidence with the feed context that made it; Undo matches on this.
                    evidence = {"feed_dataset": ds.pk, "feed_place": st.place_id,
                                "hand_search": True} if c is None else {
                        "generation": c.generation_id, "tier": c.tier, "rank": c.rank,
                        "confidence": c.gateway_confidence, "signals": c.signals,
                        "explanation": (c.explanation or {}).get("sentence", ""),
                        "feed_dataset": ds.pk, "feed_place": st.place_id}
                    row, _created = attestations.create(
                        request.user, {"place_a": subject, "place_b": target, "relation_type": relation,
                                       "certainty": certainty, "justification": note or None},
                        candidate=c, evidence=evidence)
                    made.append(row.id)
                    if c is not None:
                        c.status, c.decided_at, c.decided_by = "accepted", now, request.user
                        c.save(update_fields=["status", "decided_at", "decided_by"])
        except attestations.AttestationRefused as exc:
            return _refused(exc)
        st.state = "linked"
    elif action == "reject":
        if not cands:
            return JsonResponse({"error": "nothing to reject"}, status=400)
        try:
            with transaction.atomic():
                if body.get("distinct"):   # also say so, as a statement others can see
                    for c in cands:
                        row, _ = attestations.create(
                            request.user, {"place_a": subject, "place_b": c.candidate_id,
                                           "relation_type": "distinct", "justification": note or None},
                            candidate=c, evidence={"generation": c.generation_id, "tier": c.tier})
                        made.append(row.id)
                for c in cands:
                    c.status, c.decided_at, c.decided_by = "rejected", now, request.user
                    c.save(update_fields=["status", "decided_at", "decided_by"])
        except attestations.AttestationRefused as exc:
            return _refused(exc)
        return JsonResponse({"place": st.place_id, "state": st.state, "attestations": made,
                             "counts": tab_counts(ds)})
    elif action == "none":
        (MatchCandidate.objects.filter(place=st.place, generation_id=st.generation_id, status="suggested")
         .update(status="rejected", decided_at=now, decided_by=request.user))
        st.state = "no_good_match"
    elif action == "later":
        st.state = "later"
    elif action == "reopen":
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
         .exclude(status="stale").update(status="suggested", decided_at=None, decided_by=None))
        st.state = "unreviewed"
    else:
        return JsonResponse({"error": "action must be link, reject, none, later or reopen"}, status=400)

    st.decided_at, st.decided_by = now, request.user
    if note:
        st.note = note
    st.save()
    return JsonResponse({"place": st.place_id, "state": st.state, "attestations": made,
                         "counts": tab_counts(ds)})


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
    results = [{"candidate_id": c["id"], "title": c["card"]["title"], "namespace": c["card"]["namespace"],
                "confidence": c["confidence"], "km": c["km"], "demoted": not c["sane"],
                "explanation": c["explanation"], "card": c["card"]} for c in scored[:20]]
    return JsonResponse({"answered": True, "q": q, "constraints": chips, "results": results})


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
