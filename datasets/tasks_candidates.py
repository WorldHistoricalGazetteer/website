"""Background candidate generation (place#334 section 4b "Precompute and cache", Phase 2: place#338).

``generate_candidates(generation_id)`` runs on the dedicated Celery queue ``recon``
(``CELERY_TASK_ROUTES``; the worker listens with ``-Q celery,recon``). It walks a dataset's
records in pk order, one gateway call per record, paced to ``RECON_BG_QPS`` with one request in
flight, and is:

* **capped**: ``candidates.run_limit`` refuses a non-production run without an explicit small
  ``RECON_DEV_RECORD_LIMIT`` (dev shares production's gateway and indexes);
* **resumable and idempotent**: ``last_place_pk`` is the cursor, and a record answered in this
  generation is never queried again, so a crashed or re-delivered task picks up where it was;
* **outage-aware**: a failed call leaves the record ``unqueried`` (never ``none``); after
  ``OUTAGE_STREAK`` failures in a row the run pauses and reschedules itself; answered-but-slow
  calls (p95 of the last 50 over ``SLOW_P95_S``) make it yield between calls;
* **one at a time per environment**: a second run waits while another has a fresh heartbeat;
* **visible**: ``counts`` (total, done, unqueried, one per tier) is written every
  ``CHECKPOINT_EVERY`` records so the feed can open before the run finishes.
"""
import logging
import time
from collections import deque
from datetime import timedelta

from celery import shared_task
from django.conf import settings
from django.db.models import Count, Max
from django.utils import timezone

from datasets import candidates

logger = logging.getLogger("reconciliation")

CHECKPOINT_EVERY = 100
OUTAGE_STREAK = 5
OUTAGE_BACKOFF_S = 600
BUSY_RETRY_S = 300
STALE_HEARTBEAT = timedelta(minutes=15)
MAX_RETRY_ROUNDS = 3
SLOW_P95_S = 2.0
SLOW_WINDOW = 50
OUTAGE_FAILURES = ("timeout", "unreachable", "disabled", "error")


def _qps():
    try:
        q = float(getattr(settings, "RECON_BG_QPS", 3) or 3)
    except (TypeError, ValueError):
        q = 3.0
    return max(0.05, min(q, 10.0))


def _schedule(generation_id, countdown):
    generate_candidates.apply_async(args=[generation_id], countdown=countdown)


def tier_counts(generation):
    """Counts for the run so far, from the stored states (the truth, not a tally in memory)."""
    from datasets.models_matching import RecordReviewState
    from places.models import Place
    rows = (RecordReviewState.objects.filter(generation=generation)
            .values("tier").annotate(n=Count("id")))
    by_tier = {r["tier"]: r["n"] for r in rows}
    total = Place.objects.filter(dataset=generation.dataset.label).count()
    answered = sum(n for t, n in by_tier.items() if t != "unqueried")
    return {"total": total, "done": answered, "unqueried": by_tier.get("unqueried", 0),
            "confident": by_tier.get("confident", 0), "ambiguous": by_tier.get("ambiguous", 0),
            "weak": by_tier.get("weak", 0), "none": by_tier.get("none", 0)}


def _busy_elsewhere(generation):
    """Another run of this environment is live (fresh heartbeat)."""
    from datasets.models_matching import CandidateGeneration
    fresh = timezone.now() - STALE_HEARTBEAT
    return (CandidateGeneration.objects.filter(status="running", heartbeat__gte=fresh)
            .exclude(pk=generation.pk).exists())


def _checkpoint(generation, cursor, extra=None):
    counts = dict(generation.counts or {})
    counts.update(tier_counts(generation))
    counts.update(extra or {})
    generation.counts = counts
    generation.last_place_pk = cursor
    generation.heartbeat = timezone.now()
    generation.save(update_fields=["counts", "last_place_pk", "heartbeat"])


def _finalise(generation):
    """Ordering keys the feed reads (4c): how many records share the top suggestion (confident)
    or the title (ambiguous)."""
    from datasets.models_matching import RecordReviewState
    states = RecordReviewState.objects.filter(generation=generation)
    tops = dict(states.filter(tier="confident").exclude(top_candidate_id="")
                .values_list("top_candidate_id").annotate(n=Count("id")))
    titles = dict(states.filter(tier="ambiguous").values_list("place__title").annotate(n=Count("id")))
    for st in states.filter(tier__in=("confident", "ambiguous")).select_related("place"):
        n = tops.get(st.top_candidate_id, 1) if st.tier == "confident" else titles.get(st.place.title, 1)
        if st.shared_count != n:
            st.shared_count = n
            st.save(update_fields=["shared_count"])


def run_generation(generation, sleep=time.sleep, schedule=_schedule, clock=time.monotonic):
    """The task body, callable directly (tests pass a fake ``sleep`` and ``schedule``).
    Returns the generation's status when it stops. A run that finishes emails the owner once
    (``notify_owner``); a failed, paused or already-finished run sends nothing."""
    from datasets.models_matching import RecordReviewState
    from places.models import Place

    if generation.status in ("finished", "failed", "superseded"):
        return generation.status
    if _busy_elsewhere(generation):
        generation.status = "queued"
        generation.save(update_fields=["status"])
        schedule(generation.pk, BUSY_RETRY_S)
        return "queued"
    try:
        limit = candidates.run_limit(generation.record_limit)
    except candidates.GenerationRefused as exc:
        generation.status, generation.error = "failed", str(exc)
        generation.finished = timezone.now()
        generation.save(update_fields=["status", "error", "finished"])
        return "failed"

    dataset = generation.dataset
    user = generation.requested_by or dataset.owner
    generation.status = "running"
    generation.started = generation.started or timezone.now()
    generation.heartbeat = timezone.now()
    generation.save(update_fields=["status", "started", "heartbeat"])

    counts = dict(generation.counts or {})
    queried = int(counts.get("queried", 0))
    decided = set(RecordReviewState.objects.filter(dataset=dataset, state__in=candidates.DECIDED_STATES)
                  .values_list("place_id", flat=True))
    qs = Place.objects.filter(dataset=dataset.label).order_by("pk")
    if generation.last_place_pk:
        qs = qs.filter(pk__gt=generation.last_place_pk)
    interval = 1.0 / _qps()
    streak, latencies, since_checkpoint = 0, deque(maxlen=SLOW_WINDOW), 0
    cursor = generation.last_place_pk
    limited = False

    for place in qs.iterator(chunk_size=500):
        if place.pk in decided:
            cursor = place.pk
            continue
        if queried >= limit:
            limited = True
            break
        t0 = clock()
        tier = candidates.record_candidates(place, generation, user)
        if tier is not None:                      # None: already answered in this generation
            queried += 1
            elapsed = clock() - t0
            latencies.append(elapsed)
            streak = streak + 1 if tier == "unqueried" else 0
            pause = max(0.0, interval - elapsed)
            if len(latencies) >= 10 and sorted(latencies)[int(0.95 * (len(latencies) - 1))] > SLOW_P95_S:
                pause += SLOW_P95_S               # the gateway is struggling: yield
            if pause:
                sleep(pause)
        cursor = place.pk
        since_checkpoint += 1
        if streak >= OUTAGE_STREAK:
            # Five silences in a row: the gateway is down, not the records empty. Stop, keep the
            # cursor (the silent records stay 'unqueried' and are retried on resume).
            _checkpoint(generation, cursor, {"queried": queried, "paused_reason": "gateway"})
            generation.status = "paused"
            generation.save(update_fields=["status"])
            schedule(generation.pk, OUTAGE_BACKOFF_S)
            return "paused"
        if since_checkpoint >= CHECKPOINT_EVERY:
            _checkpoint(generation, cursor, {"queried": queried})
            since_checkpoint = 0

    _checkpoint(generation, cursor, {"queried": queried, "limited": limited, "paused_reason": ""})
    # Records the gateway did not answer get another pass, from the start, a few times.
    rounds = int((generation.counts or {}).get("retry_rounds", 0))
    if generation.counts.get("unqueried") and rounds < MAX_RETRY_ROUNDS and not limited:
        generation.counts = {**generation.counts, "retry_rounds": rounds + 1}
        generation.last_place_pk = None
        generation.status = "paused"
        generation.save(update_fields=["counts", "last_place_pk", "status"])
        schedule(generation.pk, OUTAGE_BACKOFF_S)
        return "paused"
    _finalise(generation)
    generation.status = "finished"
    generation.finished = timezone.now()
    generation.save(update_fields=["status", "finished"])
    notify_owner(generation)
    return "finished"


@shared_task(name="datasets.generate_candidates", acks_late=True)
def generate_candidates(generation_id):
    """Celery entry point (queue ``recon``)."""
    from datasets.models_matching import CandidateGeneration
    generation = CandidateGeneration.objects.filter(pk=generation_id).select_related("dataset").first()
    if generation is None:
        return "missing"
    return run_generation(generation)


def start_generation(dataset, user, countries=None, contained_in=None, namespaces=None, record_limit=None,
                     enqueue=True, trigger="manual"):
    """Create a generation for ``dataset`` and queue it. Refuses (``GenerationRefused``) before
    writing anything when this environment may not run one, or while one is already live for
    the dataset. Earlier generations are marked ``superseded``; their suggestions go ``stale``
    record by record as the new run reaches them.

    ``trigger`` (``manual``, ``auto``, ``refresh``) and the records' signature at the start
    (``records_signature``) are kept in ``counts``: the first decides the completion email, the
    second lets the automatic start tell "already prepared" from "the records changed"."""
    from datasets.models_matching import CandidateGeneration
    candidates.run_limit(record_limit)
    live = CandidateGeneration.objects.filter(dataset=dataset, status__in=LIVE_STATUSES)
    if live.exists():
        raise candidates.GenerationRefused("suggestions are already being prepared for this dataset")
    CandidateGeneration.objects.filter(dataset=dataset, status="finished").update(status="superseded")
    gen = CandidateGeneration.objects.create(
        dataset=dataset, requested_by=user, record_limit=record_limit,
        counts={"trigger": trigger, "records_signature": records_signature(dataset)},
        recipe=candidates.build_recipe(dataset, countries=countries, contained_in=contained_in,
                                       namespaces=namespaces))
    if enqueue:
        _schedule(gen.pk, 0)
    return gen


LIVE_STATUSES = ("queued", "running", "paused")
# Lifecycle states in which a dataset's records are settled enough to suggest Links for: every
# state after validation. NULL (a v3-era dataset not on the lifecycle) is never one of them.
READY_STATES = ("draft", "submitted", "in_review", "changes_requested", "declined", "accepted",
                "published", "withdrawn")


def records_signature(dataset):
    """``"<count>:<highest pk>"`` of the dataset's records. A re-upload replaces the records (new
    pks), an added or removed record changes the count. Edits to a record in place do not change
    it: the owner can still ask for fresh suggestions ("Look again")."""
    from places.models import Place
    agg = Place.objects.filter(dataset=dataset.label).aggregate(n=Count("id"), top=Max("id"))
    return f"{agg['n'] or 0}:{agg['top'] or 0}"


# ---------------------------------------------------------------------------
# (1) Automatic start when a dataset becomes ready for linking
# ---------------------------------------------------------------------------

def queue_auto_generation(dataset_pk):
    """Called by ``datasets.lifecycle`` INSIDE a transition's transaction: queues the automatic
    start for after the commit (never for a rolled-back transition)."""
    from django.db import transaction
    transaction.on_commit(lambda: auto_generate_candidates.delay(dataset_pk))


def auto_generation_skip_reason(dataset):
    """Why an automatic start would not happen for ``dataset`` ('' when it would)."""
    from datasets.models_matching import CandidateGeneration
    if dataset.lifecycle_state not in READY_STATES:
        return f"lifecycle state {dataset.lifecycle_state!r} is not ready for linking"
    from places.v4_landing import linking_open_to
    if not linking_open_to(dataset.owner):
        return "the owner is not offered linking (beta)"
    latest = CandidateGeneration.objects.filter(dataset=dataset).order_by("-id").first()
    if latest is not None and latest.status in LIVE_STATUSES:
        return f"generation {latest.pk} is {latest.status}"
    if latest is not None and latest.status == "finished":
        sig = (latest.counts or {}).get("records_signature")
        # A run from before signatures were kept counts as current: never re-run on a guess.
        if sig is None or sig == records_signature(dataset):
            return f"generation {latest.pk} finished and the records have not changed"
    return ""


@shared_task(name="datasets.auto_generate_candidates")
def auto_generate_candidates(dataset_pk):
    """Start suggestions for a dataset that has just become ready for linking (queue ``recon``).
    Idempotent: nothing happens while a run is queued, running or paused, or when the last run
    finished over the same records. The environment's caps apply through ``start_generation``
    (outside production: refused without ``RECON_DEV_RECORD_LIMIT``), and a refusal is logged,
    never raised. The dataset row is locked so two transitions cannot start two runs."""
    from django.db import transaction
    from datasets.models import Dataset
    with transaction.atomic():
        ds = Dataset.objects.select_for_update().filter(pk=dataset_pk).select_related("owner").first()
        if ds is None:
            return "missing"
        reason = auto_generation_skip_reason(ds)
        if reason:
            logger.info("automatic suggestions for dataset %s skipped: %s", dataset_pk, reason)
            return "skipped"
        try:
            gen = start_generation(ds, ds.owner, trigger="auto", enqueue=False)
        except candidates.GenerationRefused as exc:
            logger.info("automatic suggestions for dataset %s refused: %s", dataset_pk, exc)
            return "refused"
        transaction.on_commit(lambda: _schedule(gen.pk, 0))
    return "started"


# ---------------------------------------------------------------------------
# (3) Daily refresh of suggestions whose candidate pool has moved on
# ---------------------------------------------------------------------------

# A newly published dataset is searchable only once the gateway has indexed it (usually within a
# minute; ``sweep_index_pending`` retries for 24 hours). A run that started less than this long
# after a publication may have missed it, so it still counts as stale once indexing is confirmed.
REFRESH_INDEX_GRACE = timedelta(hours=24)
REFRESH_PER_RUN_DEFAULT = 2
REFRESH_PER_DAY_DEFAULT = 5


def _setting_int(name, default):
    try:
        return max(0, int(getattr(settings, name, default)))
    except (TypeError, ValueError):
        return default


def stale_generations():
    """The latest finished generation of each lifecycle dataset whose candidate pool is stale,
    oldest first. Stale = another dataset was published on the v4 lifecycle (a
    ``DatasetLifecycleEvent`` to ``published``) at or after ``REFRESH_INDEX_GRACE`` before the run
    started, and is still published, public and confirmed in the search index
    (``index_pending_since`` cleared). Skipped: NULL-lifecycle datasets, states before validation,
    datasets with a run queued/running/paused, and runs that did not finish (failed runs are left
    for the owner's "Look again"). Changes to the index or clustering parameters are not detected."""
    from datasets.models import Dataset, DatasetLifecycleEvent
    from datasets.models_matching import CandidateGeneration
    in_pool = set(Dataset.objects.filter(lifecycle_state="published", public=True,
                                         index_pending_since__isnull=True).values_list("pk", flat=True))
    pubs = list(DatasetLifecycleEvent.objects.filter(to_state="published", dataset_pk__in=in_pool)
                .values_list("dataset_pk", "at"))
    if not pubs:
        return []
    # Only each dataset's LATEST run is considered, and only when it finished: a run in flight is
    # always the latest (``start_generation`` refuses a second), so busy datasets drop out here.
    out = []
    latest_ids = (CandidateGeneration.objects.filter(dataset__lifecycle_state__in=READY_STATES)
                  .values("dataset_id").annotate(top=Max("id"))
                  .values_list("top", flat=True))
    for gen in (CandidateGeneration.objects.filter(pk__in=list(latest_ids), status="finished")
                .select_related("dataset", "dataset__owner").order_by("started", "id")):
        since = (gen.started or gen.created) - REFRESH_INDEX_GRACE
        if any(pk != gen.dataset_id and at >= since for pk, at in pubs):
            out.append(gen)
    return out


@shared_task(name="datasets.refresh_stale_suggestions")
def refresh_stale_suggestions():
    """Daily (beat): prepare suggestions again for lifecycle datasets whose candidate pool is
    stale (``stale_generations``), at most ``RECON_REFRESH_PER_RUN`` datasets per sweep and
    ``RECON_REFRESH_PER_DAY`` refresh runs in any 24 hours, so it cannot flood the gateway. Each
    run keeps the owner's scope and record limit from the run it replaces, goes through
    ``start_generation`` (the environment's caps) and is serialised with every other run by the
    task's one-at-a-time rule."""
    from datasets.models_matching import CandidateGeneration
    from places.v4_landing import linking_open_to
    per_run = _setting_int("RECON_REFRESH_PER_RUN", REFRESH_PER_RUN_DEFAULT)
    per_day = _setting_int("RECON_REFRESH_PER_DAY", REFRESH_PER_DAY_DEFAULT)
    today = CandidateGeneration.objects.filter(created__gte=timezone.now() - timedelta(hours=24),
                                               counts__trigger="refresh").count()
    budget = max(0, min(per_run, per_day - today))
    started, skipped = [], []
    for old in stale_generations():
        if len(started) >= budget:
            break
        ds = old.dataset
        if not linking_open_to(ds.owner):
            skipped.append(ds.pk)
            continue
        recipe = old.recipe or {}
        try:
            gen = start_generation(ds, ds.owner, countries=recipe.get("countries"),
                                   contained_in=recipe.get("contained_in"),
                                   namespaces=recipe.get("namespaces"), record_limit=old.record_limit,
                                   trigger="refresh")
        except candidates.GenerationRefused as exc:
            logger.info("refresh of suggestions for dataset %s refused: %s", ds.pk, exc)
            skipped.append(ds.pk)
            continue
        started.append(gen.pk)
    return {"started": started, "skipped": skipped, "budget": budget}


# ---------------------------------------------------------------------------
# (2) The owner's email when a run finishes
# ---------------------------------------------------------------------------

REFRESH_EMAIL_MIN_GAP = timedelta(days=7)


def site_url(path):
    import os
    return os.environ.get("URL_FRONT", "https://whgazetteer.org").rstrip("/") + path


def notify_owner(generation):
    """Email the dataset's owner (only the owner: the counts are theirs, Q11) that the
    suggestions are ready, once per run: the run is claimed by a conditional UPDATE on
    ``counts.owner_notified`` before anything is sent, so a re-run or re-delivery of the same
    run sends nothing. A daily refresh mails at most once a week per dataset. Returns whether a
    mail was sent; never raises."""
    from django.urls import reverse
    from datasets.models_matching import CandidateGeneration
    from whgmail.messaging import WHGmail
    try:
        gen = CandidateGeneration.objects.select_related("dataset", "dataset__owner").get(pk=generation.pk)
        if gen.status != "finished":
            return False
        ds, counts = gen.dataset, dict(gen.counts or {})
        owner = ds.owner
        if not getattr(owner, "email", None):
            return False
        if counts.get("trigger") == "refresh" and CandidateGeneration.objects.filter(
                dataset=ds, counts__has_key="owner_notified",
                finished__gte=timezone.now() - REFRESH_EMAIL_MIN_GAP).exclude(pk=gen.pk).exists():
            return False
        counts["owner_notified"] = timezone.now().isoformat()
        claimed = (CandidateGeneration.objects.filter(pk=gen.pk).exclude(counts__has_key="owner_notified")
                   .update(counts=counts))
        if not claimed:
            return False
        generation.counts = counts
        with_suggestions = sum(int(counts.get(t) or 0) for t in ("confident", "ambiguous", "weak"))
        return bool(WHGmail(context={
            "template": "suggestions_ready",
            "to_email": owner.email,
            "subject": "Suggested links for your WHG dataset are ready",
            "greeting_name": owner.name,
            "reply_to": settings.DEFAULT_FROM_EDITORIAL,
            "dataset_title": ds.title,
            "dataset_label": ds.label,
            "dataset_id": ds.id,
            "refresh": counts.get("trigger") == "refresh",
            "records_checked": int(counts.get("done") or 0),
            "with_suggestions": with_suggestions,
            "limited": bool(counts.get("limited")),
            "feed_url": site_url(reverse("datasets:ds_matches", args=[ds.id])),
        }))
    except Exception:  # noqa: BLE001 — the run has finished; a mail problem must not undo that
        logger.exception("suggestions-ready mail for generation %s failed", generation.pk)
        return False
