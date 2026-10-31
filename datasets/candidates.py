"""Match candidates for a dataset's records (place#334 section 4b, Phase 2: place#338).

WHG suggests; a person decides. This module turns one record into a ranked, tiered and explained
list of places the gateway thinks it may be, and nothing more: it never writes an attestation.

The steps, one record at a time:

1. **Recipe.** The run's shared retrieval settings (``build_recipe``): a fuzzy name search, 20
   hits, country codes ONLY when the owner declared a scope for the run (a wrong code hides the
   answer: memory project_consumer_defects_recur), an optional region, and the record's own
   years with ``temporal_mode=possibly`` and ``undated`` so undated authority records survive.
   No type filter: AAT coverage on candidates is ~0% outside TGN (#273); types are scored.
2. **Gateway call** through the existing client, ``api.crc_client.crc_search_status`` (``POST
   /api/search``), because it returns the clustering fuel (``repr_point``, ``temporal_range``,
   ``aat_paths``, name embeddings, ``edges``, ``clustering_params``) that ``score_pair`` needs
   and that ``crc_reconcile_search``'s adapted hits drop. Its failure kinds are kept: a record
   whose query timed out is ``unqueried``, never ``none`` (silence is not absence).
3. **Own-dataset filter, in Django.** ``exclude_namespaces=["whg:<id>"]`` is a ``must_not`` on
   the ``namespace`` field (``es_helpers.py:1351``) and does nothing to ``whg:<ds>:<src>`` ids,
   so the record's own dataset (and any contributed record the requester may not see) is
   dropped here.
4. **Pair scoring** with ``search.clustering.score_pair`` (the parity-tested twin of the
   Atlas clusterer) under the gateway's own ``clustering_params``.
5. **Type sanity** (``placetypes/data/non_place_types.json``): a sewage works, lake or war
   memorial named after a settlement is demoted. It only ever demotes.
6. **Tiers** (proposals to calibrate, never probabilities): confident / ambiguous / weak for a
   record with candidates, ``none`` when nothing usable came back.
7. **Explanation**: sentence pieces built only from fields of the response.

Gateway load: generation is paced by the Celery task (``RECON_BG_QPS``); outside production a
run needs an explicit small ``RECON_DEV_RECORD_LIMIT`` (``run_limit``), because dev shares
production's gateway and indexes.
"""
import json
import logging
import os
from functools import lru_cache

from django.conf import settings

from search import clustering

logger = logging.getLogger("reconciliation")

PRODUCTION_CONTEXT = "whgazetteer-org"
# Outside production a run may not exceed this many records whatever the setting says.
DEV_LIMIT_CEILING = 100
# Absolute cap on one run in any context (a 100,000-record dataset is ~9 h at 3 q/s).
DEFAULT_MAX_RECORDS = 250_000

SEARCH_SIZE = 20
# Lexical tiers on the gateway's absolute match quality (0-100, place#206): 58 is the exact
# lexical tier (2.5/4.25), 30 the gateway's recommended floor.
CONFIDENT_MIN_CONFIDENCE = 58.0
WEAK_BELOW_CONFIDENCE = 30.0
# A second candidate of a different place within this much composite makes the top one
# ambiguous rather than confident.
COMPETITOR_MARGIN = 0.1
# Above this distance a present spatial signal is reported as disagreeing.
FAR_KM = 100.0
NON_NAME_SIGNALS = ("spatial", "temporal", "type")

TYPES_FILE = os.path.join(settings.BASE_DIR, "placetypes", "data", "non_place_types.json")


class GenerationRefused(Exception):
    """A run this environment may not start (no dev cap, cap too large)."""


def run_limit(requested=None):
    """How many records a run may query here. Production: the request, capped at
    ``RECON_MAX_RECORDS``. Anywhere else: only with an explicit ``RECON_DEV_RECORD_LIMIT`` of at
    most ``DEV_LIMIT_CEILING``, because dev shares production's gateway (place#334 4j)."""
    hard = int(getattr(settings, "RECON_MAX_RECORDS", DEFAULT_MAX_RECORDS))
    if getattr(settings, "ENV_CONTEXT", "") == PRODUCTION_CONTEXT:
        return min(int(requested), hard) if requested else hard
    dev = getattr(settings, "RECON_DEV_RECORD_LIMIT", None)
    if not dev:
        raise GenerationRefused("outside production, candidate generation needs an explicit "
                                "RECON_DEV_RECORD_LIMIT")
    dev = int(dev)
    if dev < 1 or dev > DEV_LIMIT_CEILING:
        raise GenerationRefused(f"RECON_DEV_RECORD_LIMIT must be between 1 and {DEV_LIMIT_CEILING}")
    return min(int(requested), dev) if requested else dev


# ---------------------------------------------------------------------------
# Recipe and the record side
# ---------------------------------------------------------------------------

def build_recipe(dataset, countries=None, contained_in=None, namespaces=None):
    """The retrieval settings every record of one run shares. ``countries`` and
    ``contained_in`` are the owner's declared scope for the run, never the records' own codes."""
    recipe = {"mode": "fuzzy", "size": SEARCH_SIZE, "temporal_mode": "possibly", "undated": True,
              "exclude_dataset": int(dataset.pk)}
    if countries:
        recipe["countries"] = sorted({str(c).strip().upper() for c in countries if str(c).strip()})
    if contained_in:
        recipe["contained_in"] = [str(c).strip() for c in contained_in if str(c).strip()]
    if namespaces:
        recipe["namespaces"] = sorted({str(n).strip() for n in namespaces if str(n).strip()})
    return recipe


def subject_id(place):
    """The record's gateway id, ``whg:<dataset pk>:<src_id>``."""
    return f"whg:{place.dataset.pk}:{place.src_id}"


def search_options(place, recipe):
    """The ``crc_search_status`` options for one record under ``recipe``."""
    opts = {"qstr": place.title, "mode": recipe.get("mode", "fuzzy"),
            "size": int(recipe.get("size", SEARCH_SIZE))}
    for key in ("countries", "namespaces", "contained_in"):
        if recipe.get(key):
            opts[key] = list(recipe[key])
    mm = place.minmax or []
    if len(mm) == 2 and (mm[0] is not None or mm[1] is not None):
        opts.update({"temporal": True, "start": mm[0], "end": mm[1],
                     "temporal_mode": recipe.get("temporal_mode", "possibly"),
                     "undated": bool(recipe.get("undated", True))})
    return opts


def record_types(place):
    """``(fclasses, aat_paths)`` the record itself carries."""
    from placetypes.models import Type
    fclasses = set(place.fclasses or [])
    aat_ids = set()
    for pt in place.types.all():
        if pt.fclass:
            fclasses.add(pt.fclass)
        if pt.aat_id:
            aat_ids.add(pt.aat_id)
    paths = [p for p in Type.objects.filter(aat_id__in=aat_ids).values_list("path", flat=True) if p]
    return fclasses, paths


def record_hit(place, sid=None):
    """The record in the hit shape ``score_pair`` reads: names, ``repr_point`` ([lon, lat]),
    ``temporal_range`` and ``aat_paths``. Anything the record lacks is left out, so the signal
    degrades out rather than scoring zero."""
    sid = sid or subject_id(place)
    hit = {"place_id": sid, "namespace": "whg", "title": place.title,
           "names": [{"label": n} for n in [place.title] + [x.toponym for x in place.names.all()
                                                              if x.toponym and x.toponym != place.title]]}
    rp = place.repr_point
    if rp and len(rp) >= 2:
        hit["repr_point"] = [rp[0], rp[1]]
    mm = place.minmax or []
    if len(mm) == 2 and (mm[0] is not None or mm[1] is not None):
        hit["temporal_range"] = [mm[0], mm[1]]
    _fc, paths = record_types(place)
    if paths:
        hit["aat_paths"] = paths
    return hit


# ---------------------------------------------------------------------------
# Gateway
# ---------------------------------------------------------------------------

def gateway_search(options, user):
    """One gateway search; ``(response, failure)``. The only door to the gateway in this module,
    and through the existing client (tests patch ``api.crc_client.crc_search_status``)."""
    from api import crc_client
    return crc_client.crc_search_status(options, user=user)


def query_record(place, recipe, user):
    """``(data, failure)`` for one record. ``failure`` is the client's reason ("timeout",
    "unreachable", "disabled", "error") or None when the gateway ANSWERED, even with nothing."""
    try:
        data, failure = gateway_search(search_options(place, recipe), user)
    except Exception as exc:  # noqa: BLE001 — an unexpected client error is still silence
        logger.warning("candidate search for place %s raised: %s", place.pk, exc)
        return None, "error"
    if failure is None and not isinstance(data, dict):
        return None, "error"
    return data, failure


# ---------------------------------------------------------------------------
# Filtering and type sanity
# ---------------------------------------------------------------------------

def filter_hits(hits, dataset_pk, sid, user, _visible=None):
    """Drop the record's own dataset (and the record), and any contributed record the requester
    may not see. Done here because the gateway's namespace exclusion cannot express it."""
    from api.dataset_access import dataset_visibility
    own = f"whg:{int(dataset_pk)}:"
    visible = {} if _visible is None else _visible
    out = []
    for h in hits or []:
        pid = str(clustering.hit_id(h) or "")
        if not pid or pid == sid or pid.startswith(own):
            continue
        if pid.startswith("whg:"):
            ds = pid.split(":")[1] if pid.count(":") >= 2 else ""
            if ds not in visible:
                visible[ds] = dataset_visibility(pid, user).allowed
            if not visible[ds]:
                continue
        out.append(h)
    return out


@lru_cache(maxsize=1)
def _non_place_types_cached(path, mtime):
    with open(path) as f:
        return json.load(f)


def non_place_types():
    return _non_place_types_cached(TYPES_FILE, os.path.getmtime(TYPES_FILE))


def _gn_code(t):
    """``S.SWT`` from a GeoNames type entry (``{"identifier": "SWT", "label": "S",
    "sourceLabel": "S.SWT"}``)."""
    src = str(t.get("sourceLabel") or "")
    if "." in src:
        return src
    label, ident = str(t.get("label") or ""), str(t.get("identifier") or "")
    return f"{label}.{ident}" if len(label) == 1 and ident else ""


def _osm_tag(t):
    src = str(t.get("sourceLabel") or "")
    return src if "=" in src else ""


def type_sanity(hit, record_fclasses, rules=None):
    """``(ok, reason)``: False with the offending type when the candidate is a named thing
    (``non_place_types.json``) and the record is a settlement. ``ok`` is True when nothing is
    known, so an untyped candidate is never demoted."""
    rules = rules or non_place_types()
    settlement_fc = set(rules.get("settlement_fclasses") or [])
    record_fclasses = set(record_fclasses or [])
    if record_fclasses and not (record_fclasses & settlement_fc):
        return True, ""
    ns = clustering.hit_namespace(hit) or str(clustering.hit_id(hit) or "").split(":", 1)[0]
    types = [t for t in (hit.get("types") or []) if isinstance(t, dict)]
    if not types:
        return True, ""
    if ns == "gn":
        r = rules.get("gn") or {}
        codes = [c for c in (_gn_code(t) for t in types) if c]
        if any(c.split(".", 1)[0] in set(r.get("keep_classes") or []) for c in codes):
            return True, ""
        for c in codes:
            cls = c.split(".", 1)[0]
            if (cls in set(r.get("deny_classes") or []) and c not in set(r.get("allow_codes") or [])
                    and cls not in record_fclasses):
                return False, c
        return True, ""
    if ns in ("osm", "ohm"):
        r = rules.get(ns) or {}
        tags = [x for x in (_osm_tag(t) for t in types) if x]
        keep, deny = set(r.get("keep_tags") or []), set(r.get("deny_tags") or [])
        if any(x.split("=", 1)[0] in keep or x in keep for x in tags):
            return True, ""
        for x in tags:
            if x in deny or x.split("=", 1)[0] in deny:
                return False, x
        return True, ""
    if ns in ("wd", "tgn"):
        r = rules.get(ns) or {}
        keep_key, deny_key = ("keep_qids", "deny_qids") if ns == "wd" else ("keep_aat", "deny_aat")
        ids = [str(t.get("identifier") or "") for t in types]
        if any(i in set(r.get(keep_key) or []) for i in ids):
            return True, ""
        for i in ids:
            if i in set(r.get(deny_key) or []):
                return False, f"{ns}:{i}"
    return True, ""


# ---------------------------------------------------------------------------
# Scoring, tiers, explanation
# ---------------------------------------------------------------------------

def _edge_index(edges):
    idx = {}
    for e in edges or []:
        if isinstance(e, dict) and e.get("a") is not None and e.get("b") is not None:
            idx[clustering.pair_key(e["a"], e["b"])] = e
    return idx


def _same_place(a, b, edge_index):
    e = edge_index.get(clustering.pair_key(a, b))
    return bool(e and e.get("relation_type") in clustering.MERGE_RELATIONS)


def _num(v):
    try:
        return None if v is None else float(v)
    except (TypeError, ValueError):
        return None


def score_candidates(rec, hits, params, edges=None, stoplist=None, record_fclasses=None, rules=None):
    """Score every hit against the record. Returns dicts ``{hit, id, composite, signals, km,
    confidence, score, name_ok, corroborated, sane, sanity_reason}``, best first: sane before
    demoted, then name-confident first, then composite, then the gateway's own order."""
    params = params if isinstance(params, dict) and params.get("weights") else clustering.DEFAULT_PARAMS
    weights = params["weights"]
    theta = float((params.get("thresholds") or {}).get("theta_query", 0.55))
    eidx = _edge_index(edges)
    stop = clustering._stop_set(stoplist)
    rl = clustering.hit_lat_lng(rec)
    out = []
    for order, h in enumerate(hits):
        res = clustering.score_pair(rec, h, weights, eidx, stop)
        sig = dict(res["signals"])
        hl = clustering.hit_lat_lng(h)
        km = clustering.haversine_km(rl[0], rl[1], hl[0], hl[1]) if rl and hl else None
        conf = _num(h.get("confidence"))
        sane, reason = type_sanity(h, record_fclasses, rules)
        name_ok = conf is not None and conf >= CONFIDENT_MIN_CONFIDENCE
        corroborated = (res["composite"] >= theta and any(k in sig for k in NON_NAME_SIGNALS))
        out.append({"hit": h, "id": str(clustering.hit_id(h)), "composite": res["composite"],
                    "signals": sig, "km": km, "confidence": conf, "score": _num(h.get("score")),
                    "name_ok": name_ok, "corroborated": corroborated, "sane": sane,
                    "sanity_reason": reason, "order": order, "theta": theta})
    out.sort(key=lambda c: (not c["sane"], not c["name_ok"], -c["composite"], c["order"]))
    return out, eidx


def assign_tiers(scored, eidx):
    """The record's tier and each candidate's, per the plan's table (4b):

    * confident: the top candidate is name-confident, corroborated by a non-name signal with
      composite >= theta_query, passes type sanity, and no candidate of a DIFFERENT place (not
      joined to it by a merge edge) is name-confident within ``COMPETITOR_MARGIN`` composite;
    * ambiguous: two or more different places are name-confident, or the top one is
      name-confident but nothing else corroborates it;
    * weak: nothing is name-confident (best confidence < 30, or only near misses);
    * none: no hits, or every hit fails type sanity.
    A demoted (type-sanity) candidate is always ``weak``."""
    sane = [c for c in scored if c["sane"]]
    if not sane:
        for c in scored:
            c["tier"] = "weak"
        return "none"
    top = sane[0]
    rivals = [c for c in sane[1:] if c["name_ok"] and not _same_place(top["id"], c["id"], eidx)]
    if top["name_ok"] and top["corroborated"] and not any(
            c["composite"] >= top["composite"] - COMPETITOR_MARGIN for c in rivals):
        record_tier = "confident"
    elif top["name_ok"]:
        record_tier = "ambiguous"
    else:
        record_tier = "weak"
    for i, c in enumerate(scored):
        if not c["sane"]:
            c["tier"] = "weak"
        elif c is top:
            c["tier"] = record_tier
        else:
            c["tier"] = "ambiguous" if c["name_ok"] else "weak"
    return record_tier


def _year(y):
    return "" if y is None else (f"{-y} BCE" if y < 0 else str(y))


def _type_words(hit):
    words = []
    for t in hit.get("types") or []:
        if not isinstance(t, dict):
            continue
        label = str(t.get("label") or "").strip()
        src = str(t.get("sourceLabel") or "").strip()
        w = label if len(label) > 1 and label not in ("wikidata", "osm", "ohm", "tgn") else (src or label)
        if w and w not in words:
            words.append(w)
    return words[:2]


def explain(rec, c):
    """Sentence pieces for one candidate, each built from response fields only, plus the one
    sentence the feed shows. Disagreement is shown as well as agreement."""
    h, sig = c["hit"], c["signals"]
    pieces = []
    qm = h.get("query_match") if isinstance(h.get("query_match"), dict) else {}
    matched = str(qm.get("name") or h.get("title") or "")
    title = str(rec.get("title") or "")
    if matched and title and matched.strip().lower() == title.strip().lower() and c["name_ok"]:
        pieces.append({"kind": "name", "agrees": True, "text": "exact name"})
    elif c["name_ok"]:
        pieces.append({"kind": "name", "agrees": True, "text": f"name {title} ~ {matched}"})
    elif c["confidence"] is not None:
        pieces.append({"kind": "name", "agrees": False, "text": "sounds alike"})
    if c["km"] is not None:
        km = c["km"]
        text = f"{km:.0f} km" if km >= 1 else "under 1 km"
        pieces.append({"kind": "distance", "agrees": km < FAR_KM,
                       "text": text + (" apart" if km >= FAR_KM else " away")})
    tr = clustering.hit_temporal(h)
    if tr and len(tr) >= 2 and (tr[0] is not None or tr[1] is not None):
        pieces.append({"kind": "dates", "agrees": bool(sig.get("temporal")) if "temporal" in sig else None,
                       "text": f"dates {_year(tr[0])}–{_year(tr[1])}".rstrip("–")})
    words = _type_words(h)
    if words:
        pieces.append({"kind": "type", "agrees": c["sane"], "text": ", ".join(words)})
    if not c["sane"]:
        pieces.append({"kind": "type_sanity", "agrees": False,
                       "text": "a feature named after a place, not the place itself"})
    cc = [x for x in (h.get("ccodes") or []) if x]
    if cc:
        pieces.append({"kind": "country", "agrees": None, "text": ", ".join(cc[:3])})
    agree = [p["text"] for p in pieces if p["agrees"] is True]
    disagree = [p["text"] for p in pieces if p["agrees"] is False]
    sentence = ""
    if agree:
        sentence = "Suggested because: " + "; ".join(agree) + "."
    if disagree:
        sentence += (" " if sentence else "") + "But: " + "; ".join(disagree) + "."
    return {"pieces": pieces, "sentence": sentence}


def card(h):
    """What the feed shows of a suggested place, trimmed from the hit."""
    names = []
    for n in h.get("names") or []:
        if isinstance(n, dict) and n.get("label") and len(names) < 3:
            names.append({"label": n["label"], "lang": n.get("lang")})
    out = {"title": h.get("title") or "", "names": names, "types": _type_words(h),
           "ccodes": [x for x in (h.get("ccodes") or []) if x][:5],
           "namespace": clustering.hit_namespace(h) or str(clustering.hit_id(h) or "").split(":", 1)[0]}
    # The feed map reads ``repr_point`` ([lon, lat]); a hit that carries only ``point``
    # ([lat, lon]) is turned round here, so it is shown rather than dropped.
    ll = clustering.hit_lat_lng(h)
    if ll:
        out["repr_point"] = [ll[1], ll[0]]
    tr = clustering.hit_temporal(h)
    if tr:
        out["temporal_range"] = tr[:2]
    return out


def assess(place, data, user, sid=None):
    """``(record_tier, candidates, rec)`` for an ANSWERED gateway response. ``candidates`` are
    scored dicts with ``tier``, ``rank`` (1-based), ``explanation`` and ``card``."""
    sid = sid or subject_id(place)
    rec = record_hit(place, sid)
    raw = list(data.get("hits") or [])
    # The record itself, when already indexed, lends its name embeddings to the record side, so
    # the name signal is measured instead of degrading out.
    me = next((h for h in raw if str(clustering.hit_id(h) or "") == sid), None)
    if me and isinstance(me.get("names"), list):
        rec["names"] = me["names"]
    hits = filter_hits(raw, place.dataset.pk, sid, user)
    fclasses, _paths = record_types(place)
    scored, eidx = score_candidates(rec, hits, data.get("clustering_params"), data.get("edges"),
                                    data.get("toponym_stoplist"), fclasses)
    tier = assign_tiers(scored, eidx)
    for i, c in enumerate(scored, start=1):
        c["rank"] = i
        c["explanation"] = explain(rec, c)
        c["card"] = card(c["hit"])
    return tier, scored, rec


def stored_signals(c):
    """The ``MatchCandidate.signals`` JSON for one scored candidate."""
    sig = {k: round(v, 6) for k, v in c["signals"].items()}
    sig["composite"] = round(c["composite"], 6)
    sig["km"] = None if c["km"] is None else round(c["km"], 3)
    sig["type_sanity"] = "ok" if c["sane"] else f"demoted:{c['sanity_reason']}"
    sig["name_confident"] = c["name_ok"]
    sig["corroborated"] = c["corroborated"]
    return sig


# ---------------------------------------------------------------------------
# Persistence: one record, one generation
# ---------------------------------------------------------------------------

# A person's decision is kept across generations; these records are not queried again.
DECIDED_STATES = ("linked", "excluded")


def record_candidates(place, generation, user):
    """Query, score and store one record for ``generation``; returns its record tier.

    Idempotent: a record already ANSWERED in this generation is not queried again (``None`` is
    returned); one left ``unqueried`` is. A gateway failure stores ``tier='unqueried'`` with the
    reason and writes no candidates, so it can never be read as "nothing found". An answer,
    even an empty one, replaces the record's candidates for this generation and marks the
    suggestions of older generations ``stale``."""
    from django.db import transaction
    from django.utils import timezone
    from datasets.models_matching import MatchCandidate, RecordReviewState

    state, _ = RecordReviewState.objects.get_or_create(place=place,
                                                       defaults={"dataset": generation.dataset})
    if state.generation_id == generation.id and state.tier != "unqueried":
        return None
    sid = subject_id(place)
    data, failure = query_record(place, generation.recipe or {}, user)
    state.attempts += 1
    state.generation = generation
    state.has_geometry = place.geoms.exists()
    if failure is not None:
        state.tier = "unqueried"
        state.last_failure = str(failure)[:32]
        state.save()
        return "unqueried"
    tier, scored, _rec = assess(place, data, user, sid)
    now = timezone.now()
    with transaction.atomic():
        MatchCandidate.objects.filter(place=place, generation=generation).delete()
        (MatchCandidate.objects.filter(place=place, status="suggested")
         .exclude(generation=generation).update(status="stale"))
        MatchCandidate.objects.bulk_create([
            MatchCandidate(dataset=generation.dataset, place=place, subject_id=sid,
                           candidate_id=c["id"][:160], candidate_title=str(c["card"]["title"])[:500],
                           namespace=str(c["card"]["namespace"] or "")[:32],
                           gateway_score=c["score"], gateway_confidence=c["confidence"],
                           signals=stored_signals(c), tier=c["tier"], rank=c["rank"],
                           generation=generation, explanation=c["explanation"], card=c["card"],
                           created=now)
            for c in scored])
        state.tier = tier
        state.last_failure = ""
        state.queried_at = now
        state.candidate_count = len(scored)
        state.top_candidate_id = scored[0]["id"][:160] if scored and tier != "none" else ""
        state.save()
    _stamp_generation(generation, data)
    return tier


def _stamp_generation(generation, data):
    """Record, once, what the run's candidates were computed against."""
    params = data.get("clustering_params") if isinstance(data, dict) else None
    changed = []
    if isinstance(params, dict) and not generation.clustering_params_version:
        generation.clustering_params = params
        generation.clustering_params_version = str(params.get("version", ""))[:32]
        changed += ["clustering_params", "clustering_params_version"]
    alias = (data or {}).get("index_generation") or (data or {}).get("generation")
    if alias and not generation.index_alias_generation:
        generation.index_alias_generation = str(alias)[:64]
        changed.append("index_alias_generation")
    if changed:
        generation.save(update_fields=changed)
