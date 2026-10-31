"""The editors' link check (place#334 §4e "link identifiers that resolve (sample 50 through the
gateway)"; Phase 3, place#340).

A reproducible sample of the link identifiers a dataset's own records carry (``place_link``:
``gn:``, ``wd:``, ``tgn:``, ``pl:`` ...) is looked up by id in the gazetteer service
(``api.crc_client.crc_fetch_places``, ``POST /api/places``, the same call the Record page uses to
resolve a link), so the editor can see whether the contributor's identifiers name real records.
It is advice, never a gate: ``ok``, ``warn`` ("look closer") or ``not_checked`` ("sample
unavailable", "nothing to check"); never ``fail`` and never in ``blocking``.

Rules this module keeps (the pattern of ``datasets/qc_sample.py``):

* **Editors only, no link counts (Q11).** The result and the text carry the sample's own
  figures (its size is the only denominator) and never how many links the dataset has. The
  number of links seeds the sample and marks it stale, but it is stored only as a keyed hash
  (``fingerprint``), never as a number. ``qc.qc_report`` puts this check in place of the
  ``link_resolution`` placeholder only in the editors' report, and only when an editor asks;
  the owner's report keeps the placeholder.
* **What is sampled.** Distinct identifiers after ``places.concordance.normalise_identifier``
  (so ``https://www.wikidata.org/wiki/Q90`` and ``wd:Q90`` are one). Placeholders (``wd:None``,
  ``gn:null``, empty) are left out: the placeholder check already reports them. Of the sampled
  identifiers, those in a namespace the gazetteer service does not hold (``whg:``, ``viaf:``, a
  bare string, an unrecognised URL) are not asked about and are counted separately, with their
  namespaces named.
* **Bounded and paced.** At most ``QC_LINK_SAMPLE_SIZE`` (default and ceiling 50, the API's
  batch cap) identifiers, asked in batches of ``BATCH`` with one request in flight, paced to
  ``RECON_BG_QPS`` identifiers per second; the number asked is charged once to the requesting
  editor's per-minute query budget (``api.throttling``). Outside production the size is also
  capped by ``candidates.run_limit``, so a server without an explicit ``RECON_DEV_RECORD_LIMIT``
  never calls the gateway it shares with production: it reports "sample unavailable".
* **Deterministic.** Identifiers are ordered by ``md5("<dataset pk>:<link count>:<identifier>")``:
  the same dataset with the same number of links always yields the same sample.
* **Never blocks the page.** The page reads the stored result; a missing or stale one is queued
  (``datasets.qc_link_resolution``, queue ``recon``) after commit, and the check says "being
  prepared". The result is kept on the latest ``submitted`` lifecycle event under
  ``payload["qc_links"]``, beside the sample check's ``qc_sample``.
* **Silence is not absence.** A batch the service did not answer (timeout, refusal, network) is
  counted as unanswered, never as "does not resolve"; two silent batches in a row end the
  sample; too few answers, or a service not configured here, give "sample unavailable".

The verdict's threshold (fewer than 90% of the answered identifiers resolve: "look closer") is
a proposal, not yet calibrated against real datasets; the text says so.
"""
import datetime
import hashlib
import hmac
import logging
import math
import time

from django.conf import settings
from django.db import transaction

from datasets.qc_sample import _age, _now, store as _store, submission_event

logger = logging.getLogger("reconciliation")

PAYLOAD_KEY = "qc_links"
KEY = "link_resolution"
LABEL = "Link identifiers resolve"
DEFAULT_SIZE = 50
MAX_SIZE = 50                      # the API's per-request batch cap
BATCH = 10                         # identifiers per request: one refused id costs at most ten
MIN_ANSWERED = 5                   # fewer answered (when more were asked): no verdict
OK_SHARE = 0.9                     # proposed, uncalibrated
SILENCE_STREAK = 2
EXAMPLES = 5
RUNNING_STALE = datetime.timedelta(minutes=10)
RETRY_UNAVAILABLE = datetime.timedelta(minutes=30)
REVIEW_STATES = ("submitted", "in_review")
QUEUE = "recon"
NO_NAMESPACE = "no namespace"


def configured_size():
    try:
        n = int(getattr(settings, "QC_LINK_SAMPLE_SIZE", DEFAULT_SIZE) or DEFAULT_SIZE)
    except (TypeError, ValueError):
        n = DEFAULT_SIZE
    return max(1, min(n, MAX_SIZE))


# ---------------------------------------------------------------------------
# The sample
# ---------------------------------------------------------------------------

def _link_rows(ds):
    from places.models import PlaceLink
    return (PlaceLink.objects.filter(place__dataset=ds.label)
            .order_by("place_id", "id").values_list("place_id", "jsonb__identifier"))


def fingerprint(ds, n_links=None):
    """A keyed hash of (dataset, number of links): changes when links are added or removed,
    and is not a count anyone can read back (Q11)."""
    from places.models import PlaceLink
    if n_links is None:
        n_links = PlaceLink.objects.filter(place__dataset=ds.label).count()
    msg = f"{int(ds.pk)}:{int(n_links)}".encode()
    return hmac.new(str(settings.SECRET_KEY).encode(), msg, hashlib.sha256).hexdigest()[:20]


def sample_identifiers(ds, k):
    """``(sample, fingerprint)``: up to ``k`` ``(identifier, namespace, place_id)`` tuples,
    distinct normalised identifiers with placeholders left out, in a fixed order for a fixed
    number of links. ``place_id`` is the first record carrying the identifier."""
    from places.concordance import is_placeholder_identifier, normalise_identifier
    first, n_links = {}, 0
    for place_id, raw in _link_rows(ds).iterator():
        n_links += 1
        if is_placeholder_identifier(raw):
            continue
        ident, ns, _local = normalise_identifier(raw)
        if ident and ident not in first:
            first[ident] = (ns, place_id)
    seed = f"{int(ds.pk)}:{n_links}:"
    order = sorted(first, key=lambda i: (hashlib.md5((seed + i).encode()).hexdigest(), i))
    return [(i, first[i][0], first[i][1]) for i in order[:int(k)]], fingerprint(ds, n_links)


def _unavailable(fp, reason, **extra):
    return {"status": "unavailable", "fingerprint": fp, "reason": reason,
            "finished_at": _now().isoformat(timespec="seconds"), **extra}


def run_sample(ds, user, sleep=time.sleep, clock=time.monotonic):
    """Take the sample now (the task body). Returns the result dict that is stored."""
    from api.crc_client import crc_fetch_places
    from api.throttling import consume_query_budget
    from datasets import candidates
    from datasets.tasks_candidates import _qps
    from places.concordance import gateway_namespaces

    try:
        size = min(configured_size(), candidates.run_limit(configured_size()))
    except candidates.GenerationRefused:
        return _unavailable(fingerprint(ds), "checking is switched off on this server")
    sample, fp = sample_identifiers(ds, size)
    if not sample:
        return {"status": "done", "fingerprint": fp, "size": 0, "asked": 0, "answered": 0,
                "resolved": 0, "unanswered": 0, "skipped": 0, "skipped_namespaces": [],
                "examples_unresolved": [], "finished_at": _now().isoformat(timespec="seconds")}
    held = gateway_namespaces(user)
    asked = [(i, pid) for i, ns, pid in sample if ns in held]
    skipped_ns = sorted({ns or NO_NAMESPACE for _i, ns, _p in sample if ns not in held})
    base = {"fingerprint": fp, "size": len(sample), "asked": len(asked),
            "skipped": len(sample) - len(asked), "skipped_namespaces": skipped_ns}
    if asked:
        allowed, _retry = consume_query_budget(user, len(asked))
        if not allowed:
            return _unavailable(fp, "the gazetteer service's per-minute allowance is used up", **base)

    rate = _qps()
    resolved, unresolved, unanswered, streak = [], [], [], 0
    for start in range(0, len(asked), BATCH):
        batch = asked[start:start + BATCH]
        ids = [i for i, _p in batch]
        t0 = clock()
        meta = {}
        got = crc_fetch_places(ids, user=user, meta=meta) or {}
        if meta.get("disabled"):
            return _unavailable(fp, "the gazetteer service is not available to this server", **base)
        if meta.get("error"):
            unanswered.extend(ids)
            streak += 1
        else:
            streak = 0
            for i, pid in batch:
                (resolved if i in got else unresolved).append((i, pid))
        if streak >= SILENCE_STREAK:
            break
        if start + BATCH < len(asked):
            pause = max(0.0, len(batch) / rate - (clock() - t0))
            if pause:
                sleep(pause)

    answered = len(resolved) + len(unresolved)
    # Identifiers the sample stopped before (two silent batches in a row) count as unanswered.
    base.update(answered=answered, resolved=len(resolved),
                unanswered=len(asked) - answered)
    if asked and answered < min(MIN_ANSWERED, len(asked)):
        return _unavailable(fp, "the gazetteer service did not answer enough of the sampled "
                                "identifiers", **base)
    return {"status": "done", **base,
            "examples_unresolved": [{"identifier": i, "place_id": pid}
                                    for i, pid in unresolved[:EXAMPLES]],
            "finished_at": _now().isoformat(timespec="seconds")}


# ---------------------------------------------------------------------------
# The check as the editor reads it
# ---------------------------------------------------------------------------

def verdict(result):
    """``ok`` when at least 90% of the answered identifiers resolve, else ``warn``."""
    answered = result.get("answered") or 0
    return "ok" if result.get("resolved", 0) >= math.ceil(OK_SHARE * answered) else "warn"


def check_from_result(result):
    from datasets.qc import _check
    if result.get("status") != "done":
        return _check(KEY, LABEL, "not_checked",
                      detail=f"Sample unavailable: {result.get('reason') or 'not taken'}.",
                      verdict="sample unavailable")
    size, asked = result["size"], result["asked"]
    if not size:
        return _check(KEY, LABEL, "not_checked",
                      detail="The records carry no link identifiers to check.",
                      verdict="nothing to check")
    lines = [f"{size} link identifiers sampled (the same ones each time while the links are "
             "unchanged)."]
    if result["skipped"]:
        lines.append(f"{result['skipped']} of {size} are in namespaces the gazetteer service "
                     f"does not hold ({', '.join(result['skipped_namespaces'])}) and were not "
                     "checked.")
    if not asked:
        return _check(KEY, LABEL, "not_checked", detail=" ".join(lines),
                      verdict="nothing to check")
    answered = result["answered"]
    if result["unanswered"]:
        lines.append(f"{result['unanswered']} of the {asked} asked about got no answer from the "
                     f"service; the figure below is of the {answered} answered.")
    lines.append(f"{result['resolved']} of {answered} sampled link identifiers resolve.")
    status = verdict(result)
    if status == "warn":
        lines.append("Worth a closer look: fewer than 90% resolve.")
    lines.append("(The 90% threshold is a proposal, not yet calibrated.)")
    examples = [{"place_id": e["place_id"], "title": e["identifier"], "identifier": e["identifier"]}
                for e in result.get("examples_unresolved") or []]
    return _check(KEY, LABEL, status, None, " ".join(lines), examples,
                  verdict="ok" if status == "ok" else "look closer")


def _pending():
    from datasets.qc import _check
    return _check(KEY, LABEL, "not_checked",
                  detail="The sample is being prepared; reload this page in a minute or two.",
                  verdict="being prepared")


def _not_now(why):
    from datasets.qc import _check
    return _check(KEY, LABEL, "not_checked", detail=why, verdict="sample unavailable")


def _usable(cached, fp):
    """What to do with a stored entry: ``show``, ``wait`` or ``run``."""
    if not isinstance(cached, dict):
        return "run"
    status = cached.get("status")
    if status == "running":
        return "wait" if _age(cached.get("started_at")) < RUNNING_STALE else "run"
    if cached.get("fingerprint") != fp:
        return "run"                  # links added or removed since: a different sample
    if status == "done":
        return "show"
    if status == "unavailable":
        return "show" if _age(cached.get("finished_at")) < RETRY_UNAVAILABLE else "run"
    return "run"


def link_check(ds, requested_by=None, enqueue=None):
    """The check for the editors' report: the stored result, or "being prepared" after queuing
    a run (only for a dataset waiting for review, and only for ``requested_by``, an editor).
    Reads one row and may write one; never calls the gateway itself."""
    enqueue = enqueue or _enqueue
    event = submission_event(ds)
    if event is None:
        return _not_now("Taken once the dataset has been submitted for review.")
    fp = fingerprint(ds)
    cached = (event.payload or {}).get(PAYLOAD_KEY)
    action = _usable(cached, fp)
    if action == "show":
        return check_from_result(cached)
    if action == "wait":
        return _pending()
    if ds.lifecycle_state not in REVIEW_STATES or requested_by is None:
        return (check_from_result(cached)
                if isinstance(cached, dict) and cached.get("status") in ("done", "unavailable")
                else _not_now("Taken while the dataset is waiting for review."))
    if not _claim(event.pk, fp):
        return _pending()
    transaction.on_commit(lambda: enqueue(event.pk, requested_by.pk))
    return _pending()


def _claim(event_pk, fp):
    """Mark the event's link sample as running, unless another request already did."""
    from datasets.models import DatasetLifecycleEvent
    with transaction.atomic():
        ev = DatasetLifecycleEvent.objects.select_for_update().filter(pk=event_pk).first()
        if ev is None:
            return False
        payload = dict(ev.payload or {})
        if _usable(payload.get(PAYLOAD_KEY), fp) != "run":
            return False
        payload[PAYLOAD_KEY] = {"status": "running", "fingerprint": fp,
                                "started_at": _now().isoformat(timespec="seconds")}
        DatasetLifecycleEvent.objects.filter(pk=event_pk).update(payload=payload)
    return True


def store(event_pk, result):
    _store(event_pk, result, key=PAYLOAD_KEY)


def take_and_store(event_pk, user_pk, sleep=time.sleep):
    """The task body. Any error is stored as "sample unavailable", never raised."""
    from django.contrib.auth import get_user_model
    from datasets.models import DatasetLifecycleEvent
    ev = DatasetLifecycleEvent.objects.filter(pk=event_pk).select_related("dataset").first()
    if ev is None or ev.dataset is None:
        return "missing"
    user = get_user_model().objects.filter(pk=user_pk).first() or ev.dataset.owner
    try:
        result = run_sample(ev.dataset, user, sleep=sleep)
    except Exception:  # noqa: BLE001 — a failed sample must read as unavailable, not an error page
        logger.exception("qc link sample for dataset %s failed", ev.dataset_pk)
        result = _unavailable(fingerprint(ev.dataset), "the sample could not be taken")
    store(event_pk, result)
    return result["status"]


def _enqueue(event_pk, user_pk):
    from datasets.tasks_qc import qc_link_resolution
    try:
        qc_link_resolution.apply_async(args=[event_pk, user_pk], queue=QUEUE)
    except Exception:  # noqa: BLE001 — a broker outage must not break the editor's page
        logger.exception("could not queue the qc link sample for event %s", event_pk)
        from datasets.models import DatasetLifecycleEvent
        ev = DatasetLifecycleEvent.objects.filter(pk=event_pk).select_related("dataset").first()
        fp = fingerprint(ev.dataset) if ev and ev.dataset else None
        store(event_pk, _unavailable(fp, "the sample could not be queued"))
