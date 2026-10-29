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
from django.db.models import Count
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
    Returns the generation's status when it stops."""
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
                     enqueue=True):
    """Create a generation for ``dataset`` and queue it. Refuses (``GenerationRefused``) before
    writing anything when this environment may not run one, or while one is already live for
    the dataset. Earlier generations are marked ``superseded``; their suggestions go ``stale``
    record by record as the new run reaches them."""
    from datasets.models_matching import CandidateGeneration
    candidates.run_limit(record_limit)
    live = CandidateGeneration.objects.filter(dataset=dataset, status__in=("queued", "running", "paused"))
    if live.exists():
        raise candidates.GenerationRefused("suggestions are already being prepared for this dataset")
    CandidateGeneration.objects.filter(dataset=dataset, status="finished").update(status="superseded")
    gen = CandidateGeneration.objects.create(
        dataset=dataset, requested_by=user, record_limit=record_limit,
        recipe=candidates.build_recipe(dataset, countries=countries, contained_in=contained_in,
                                       namespaces=namespaces))
    if enqueue:
        _schedule(gen.pk, 0)
    return gen
