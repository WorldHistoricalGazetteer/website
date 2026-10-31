"""The editors' sample check (place#334 Phase 3, §4e "QC gateway sample").

A small, reproducible sample of a dataset in review is searched through the same path the
owner's suggestions use (``datasets.candidates``: ``query_record`` -> ``api.crc_client.
crc_search_status`` -> ``assess``), so the editor can see how well the records are likely to
find their counterparts in other gazetteers before accepting the dataset. It is advice, never a
gate: the check is ``ok`` or ``warn`` ("look closer"), or ``not_checked`` when no sample could
be taken; it is never ``fail`` and never appears in ``blocking``.

Rules this module keeps:

* **Editors only, no link counts (Q11).** The result counts suggestions (tiers of the sampled
  records), never the dataset's links or anyone's Links. ``qc.qc_report`` adds the check only
  to the editors' report, and only when an editor asks for it.
* **Bounded and paced.** At most ``QC_SAMPLE_SIZE`` (default 12, never above 50, the API batch
  cap) records, one gateway call each, one in flight, paced to ``RECON_BG_QPS``; the size is
  charged once to the requesting editor's per-minute query budget (``api.throttling``, 600/min).
  Outside production the size is also capped by ``candidates.run_limit``, so a dev server
  without an explicit ``RECON_DEV_RECORD_LIMIT`` never calls the gateway it shares with
  production; it reports "sample unavailable" instead.
* **Deterministic.** Records are ordered by ``md5("<dataset pk>:<record count>:<place pk>")``,
  so the same dataset with the same number of records always yields the same sample.
* **Never blocks the page.** The page only reads the stored result. A missing or stale one is
  queued as a Celery task (``datasets.qc_gateway_sample``, queue ``recon``) after commit and
  the check says "being prepared". The result is kept on the dataset's latest ``submitted``
  lifecycle event, under ``payload["qc_sample"]``, beside the summary of the automatic checks
  that event already carries: a new submission gets a fresh sample, and so does a change in the
  number of records. That key is the only thing written to the event after it is created.
* **Silence is not absence.** A record whose search timed out is counted as unanswered, never
  as "nothing suggested"; three silences in a row end the sample early; if too few records were
  answered the check is "sample unavailable", never a verdict.
"""
import datetime
import logging
import math
import time

from django.conf import settings
from django.db import connection, transaction

logger = logging.getLogger("reconciliation")

PAYLOAD_KEY = "qc_sample"
DEFAULT_SIZE = 12
MAX_SIZE = 50                      # the API's per-request batch cap
MIN_ANSWERED = 5                   # fewer answered records than this: no verdict
OK_CONFIDENT_SHARE = 0.5           # a proposal to calibrate, like the tiers themselves
SHARED_TOP_MIN = 3                 # this many sampled records with one top suggestion is odd
SILENCE_STREAK = 3
RUNNING_STALE = datetime.timedelta(minutes=10)
RETRY_UNAVAILABLE = datetime.timedelta(minutes=30)
REVIEW_STATES = ("submitted", "in_review")
QUEUE = "recon"

KEY = "gateway_sample"
LABEL = "Suggestions for a sample of records"


def _now():
    return datetime.datetime.now()


def configured_size():
    try:
        n = int(getattr(settings, "QC_SAMPLE_SIZE", DEFAULT_SIZE) or DEFAULT_SIZE)
    except (TypeError, ValueError):
        n = DEFAULT_SIZE
    return max(1, min(n, MAX_SIZE))


def sample_place_ids(ds, k, n_records):
    """``k`` place ids of ``ds``, the same ones every time for the same record count."""
    seed = f"{int(ds.pk)}:{int(n_records)}:"
    with connection.cursor() as cur:
        cur.execute("SELECT id FROM places WHERE dataset = %s "
                    "ORDER BY md5(%s || id::text), id LIMIT %s", [ds.label, seed, int(k)])
        return [r[0] for r in cur.fetchall()]


def _unavailable(n_records, reason, **extra):
    return {"status": "unavailable", "records": n_records, "reason": reason,
            "finished_at": _now().isoformat(timespec="seconds"), **extra}


def run_sample(ds, user, sleep=time.sleep, clock=time.monotonic):
    """Take the sample now (the task body). Returns the result dict that is stored."""
    from api.throttling import consume_query_budget
    from datasets import candidates
    from datasets.tasks_candidates import _qps
    from places.models import Place

    n_records = ds.places.count()
    if not n_records:
        return _unavailable(0, "the dataset has no records")
    try:
        size = min(configured_size(), n_records, candidates.run_limit(configured_size()))
    except candidates.GenerationRefused:
        return _unavailable(n_records, "searching is switched off on this server")
    allowed, _retry = consume_query_budget(user, size)
    if not allowed:
        return _unavailable(n_records, "the search service's per-minute allowance is used up")

    ids = sample_place_ids(ds, size, n_records)
    places = {p.pk: p for p in Place.objects.filter(pk__in=ids).select_related("dataset")
              .prefetch_related("names", "types")}
    recipe = candidates.build_recipe(ds)
    interval = 1.0 / _qps()
    tiers = {"confident": 0, "ambiguous": 0, "weak": 0, "none": 0}
    no_name, silent, located, streak = [], [], 0, 0
    nothing, tops, reached = [], {}, 0
    for pid in ids:
        place = places.get(pid)
        if place is None:
            continue
        reached += 1
        if place.geoms.exists():
            located += 1
        if not (place.title or "").strip():
            no_name.append(pid)
            continue
        t0 = clock()
        data, failure = candidates.query_record(place, recipe, user)
        if failure is not None:
            silent.append(pid)
            streak += 1
        else:
            streak = 0
            tier, scored, _rec = candidates.assess(place, data, user)
            tiers[tier] = tiers.get(tier, 0) + 1
            if tier == "none":
                nothing.append(pid)
            elif scored:
                top = scored[0]
                entry = tops.setdefault(top["id"], {"title": str(top["card"].get("title") or ""),
                                                     "n": 0})
                entry["n"] += 1
        if streak >= SILENCE_STREAK:
            break
        pause = max(0.0, interval - (clock() - t0))
        if pause:
            sleep(pause)

    answered = sum(tiers.values())
    base = {"records": n_records, "size": len(ids), "reached": reached, "answered": answered,
            # Records the sample stopped before (three silences in a row) count as unanswered.
            "unanswered": len(silent) + len(ids) - reached, "no_name": len(no_name), "with_location": located,
            "tiers": tiers}
    if answered < min(MIN_ANSWERED, len(ids) - len(no_name)) or (answered == 0 and not no_name):
        return _unavailable(n_records, "the search service did not answer enough of the "
                                       "sampled records", **base)
    shared = max(tops.items(), key=lambda kv: kv[1]["n"], default=(None, {"n": 0}))
    return {"status": "done", **base,
            "shared_top": ({"id": shared[0], "title": shared[1]["title"], "n": shared[1]["n"]}
                           if shared[1]["n"] >= SHARED_TOP_MIN else None),
            "examples_nothing": nothing[:5], "examples_no_name": no_name[:5],
            "finished_at": _now().isoformat(timespec="seconds")}


# ---------------------------------------------------------------------------
# The check as the editor reads it
# ---------------------------------------------------------------------------

def verdict(result):
    """``(status, reasons)``: ``ok``/``warn`` and why it is worth a closer look."""
    reasons = []
    answered = result["answered"]
    confident = result["tiers"].get("confident", 0)
    if confident < math.ceil(OK_CONFIDENT_SHARE * answered):
        reasons.append("fewer than half have a confident suggestion")
    if result.get("no_name"):
        reasons.append("some records have no name")
    if result.get("shared_top"):
        reasons.append("several records have the same top suggestion")
    return ("warn" if reasons else "ok"), reasons


def _of(n, d, what):
    return f"{n} of {d} {what}"


def check_from_result(result):
    from datasets.qc import _check, _examples
    if result.get("status") != "done":
        return _check(KEY, LABEL, "not_checked",
                      detail=f"Sample unavailable: {result.get('reason') or 'not taken'}.",
                      verdict="sample unavailable")
    status, reasons = verdict(result)
    size, answered, tiers = result["size"], result["answered"], result["tiers"]
    lines = [f"{_of(size, result['records'], 'records')} sampled (the same records each time "
             "for the same number of records)."]
    if result["unanswered"] or result["no_name"]:
        parts = []
        if result["unanswered"]:
            parts.append(f"{result['unanswered']} got no answer from the search service")
        if result["no_name"]:
            parts.append(f"{result['no_name']} have no name and were not searched")
        lines.append("Of these, " + " and ".join(parts) + f"; the figures below are of the "
                     f"{answered} answered.")
    if answered:
        lines.append(f"{_of(tiers.get('confident', 0), answered, 'sampled records')} have a "
                     f"confident suggestion; {tiers.get('ambiguous', 0)} have more than one "
                     f"possible; {tiers.get('weak', 0)} only weak ones; "
                     f"{tiers.get('none', 0)} nothing suggested.")
    lines.append(f"{_of(result['with_location'], result.get('reached', size), 'sampled records')} "
                 "have a location (a suggestion is confident only when a location or date "
                 "agrees with the name).")
    if result.get("shared_top"):
        st = result["shared_top"]
        lines.append(f"{_of(st['n'], answered, 'sampled records')} have the same top "
                     f"suggestion ({st['title'] or st['id']}).")
    if reasons:
        lines.append("Worth a closer look: " + "; ".join(reasons) + ".")
    examples = list(result.get("examples_no_name") or []) + list(result.get("examples_nothing") or [])
    return _check(KEY, LABEL, status, None, " ".join(lines), _examples(examples),
                  verdict="ok" if status == "ok" else "look closer")


def _pending():
    from datasets.qc import _check
    return _check(KEY, LABEL, "not_checked",
                  detail="The sample is being prepared; reload this page in a minute or two.",
                  verdict="being prepared")


def _not_now(why):
    from datasets.qc import _check
    return _check(KEY, LABEL, "not_checked", detail=why, verdict="sample unavailable")


def submission_event(ds):
    from datasets.models import DatasetLifecycleEvent
    return (DatasetLifecycleEvent.objects.filter(dataset_pk=ds.pk, to_state="submitted")
            .order_by("-at", "-id").first())


def _age(stamp):
    try:
        return _now() - datetime.datetime.fromisoformat(stamp)
    except (TypeError, ValueError):
        return datetime.timedelta.max


def _usable(cached, n_records):
    """What to do with a stored entry: ``show``, ``wait`` or ``run``."""
    if not isinstance(cached, dict):
        return "run"
    status = cached.get("status")
    if status == "running":
        return "wait" if _age(cached.get("started_at")) < RUNNING_STALE else "run"
    if cached.get("records") != n_records:
        return "run"                  # records added or removed since: a different sample
    if status == "done":
        return "show"
    if status == "unavailable":
        return "show" if _age(cached.get("finished_at")) < RETRY_UNAVAILABLE else "run"
    return "run"


def sample_check(ds, requested_by=None, enqueue=None):
    """The check for the editors' report: the stored result, or "being prepared" after queuing
    a run (only for a dataset waiting for review, and only for ``requested_by``, an editor).
    Reads one row and may write one; never calls the gateway itself."""
    enqueue = enqueue or _enqueue
    event = submission_event(ds)
    if event is None:
        return _not_now("Taken once the dataset has been submitted for review.")
    n_records = ds.places.count()
    cached = (event.payload or {}).get(PAYLOAD_KEY)
    action = _usable(cached, n_records)
    if action == "show":
        return check_from_result(cached)
    if action == "wait":
        return _pending()
    if ds.lifecycle_state not in REVIEW_STATES or requested_by is None:
        return (check_from_result(cached) if isinstance(cached, dict) and cached.get("status") in ("done", "unavailable")
                else _not_now("Taken while the dataset is waiting for review."))
    if not _claim(event.pk, n_records):
        return _pending()             # someone else queued it a moment ago
    transaction.on_commit(lambda: enqueue(event.pk, requested_by.pk))
    return _pending()


def _claim(event_pk, n_records):
    """Mark the event's sample as running, unless another request already did."""
    from datasets.models import DatasetLifecycleEvent
    with transaction.atomic():
        ev = DatasetLifecycleEvent.objects.select_for_update().filter(pk=event_pk).first()
        if ev is None:
            return False
        payload = dict(ev.payload or {})
        if _usable(payload.get(PAYLOAD_KEY), n_records) != "run":
            return False
        payload[PAYLOAD_KEY] = {"status": "running", "records": n_records,
                                "started_at": _now().isoformat(timespec="seconds")}
        DatasetLifecycleEvent.objects.filter(pk=event_pk).update(payload=payload)
    return True


def store(event_pk, result, key=PAYLOAD_KEY):
    """Write ``result`` under ``key`` of the event's payload (``qc_links`` uses its own key)."""
    from datasets.models import DatasetLifecycleEvent
    with transaction.atomic():
        ev = DatasetLifecycleEvent.objects.select_for_update().filter(pk=event_pk).first()
        if ev is None:
            return
        payload = dict(ev.payload or {})
        payload[key] = result
        DatasetLifecycleEvent.objects.filter(pk=event_pk).update(payload=payload)


def take_and_store(event_pk, user_pk, sleep=time.sleep):
    """The task body: take the sample for the event's dataset and store it. Any error is
    stored as "sample unavailable", never raised into the worker as a retry loop."""
    from django.contrib.auth import get_user_model
    from datasets.models import DatasetLifecycleEvent
    ev = DatasetLifecycleEvent.objects.filter(pk=event_pk).select_related("dataset").first()
    if ev is None or ev.dataset is None:
        return "missing"
    user = get_user_model().objects.filter(pk=user_pk).first() or ev.dataset.owner
    try:
        result = run_sample(ev.dataset, user, sleep=sleep)
    except Exception:  # noqa: BLE001 — a failed sample must read as unavailable, not an error page
        logger.exception("qc sample for dataset %s failed", ev.dataset_pk)
        result = _unavailable(ev.dataset.places.count(), "the sample could not be taken")
    store(event_pk, result)
    return result["status"]


def _enqueue(event_pk, user_pk):
    from datasets.tasks_qc import qc_gateway_sample
    try:
        qc_gateway_sample.apply_async(args=[event_pk, user_pk], queue=QUEUE)
    except Exception:  # noqa: BLE001 — a broker outage must not break the editor's page
        logger.exception("could not queue the qc sample for event %s", event_pk)
        from datasets.models import DatasetLifecycleEvent
        ev = DatasetLifecycleEvent.objects.filter(pk=event_pk).select_related("dataset").first()
        n = ev.dataset.places.count() if ev and ev.dataset else None
        store(event_pk, _unavailable(n, "the sample could not be queued"))
