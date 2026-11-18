"""An adopted or hand-placed location (place#350) outside the feed: its label wherever the record
is shown, and its way into the search index.

**Label.** ``location_labels(place)`` is the one wording for "this location is not the
contributor's own": the Atlas map popup (``/entity/place:<id>/api?variant=popup``, served from
Postgres since place#329) and the Atlas portal (``/atlas/place/?id=``) both carry it as
``location_provenance``, and it says exactly what the Record page says. It is read from the live
row, never from the index: the index document is the contributor's record (place#334, "changes in
the index: nothing on the record"), so provenance is not written into it, and a label read from
Postgres cannot outlive an Undo. Callers attach it only after their own visibility check.

**Index.** A contributed dataset reaches the gateway index only by being re-indexed whole
(``index_dataset_on_publish`` → gateway ``POST /api/registry/reingest`` ``whg:<id>``, which
re-reads the dataset's LPF from Django and replaces its documents). An adopted or placed point
written after publication therefore stayed out of the index, and a removed one stayed in, until
something re-indexed the dataset. ``location_changed`` flags a published, public dataset as
waiting for the index and schedules one re-index, debounced, so a run of adoptions costs one job.
A removal is retracted the same way (the replace drops it); a withdrawn or private dataset is
never touched here (``retract_dataset_from_index`` owns those).
"""
import logging

logger = logging.getLogger(__name__)

# One re-index per dataset per window, however many locations the owner adopts in it.
REFRESH_DELAY_S = 120


def _by(g):
    return getattr(g.reviewer, "name", None) or getattr(g.reviewer, "username", "") or ""


def location_label(kind, source_label, terms, date, by):
    """The sentence the Record page shows (places/templates/places/atlas_record.html)."""
    when = date.strftime("%-d %b %Y") if date else ""
    if kind == "adopted":
        return (f"Location from {source_label}, adopted {when} by {by}"
                + (f" ({terms})" if terms else ""))
    return f"Location placed on the map {when} by {by} (approximate)"


def location_labels(place, geoms=None):
    """The record's adopted or placed locations as ``[{kind, source, source_label, terms, date,
    by, label}]``; empty when every location is the contributor's own (or there is none).
    ``geoms`` lets a caller pass rows it has already prefetched."""
    from datasets.location_adoption import provenance
    out = []
    for g in (place.geoms.all() if geoms is None else geoms):
        p = provenance(g)
        if p is None:
            continue
        by = _by(g)
        out.append({"kind": p["kind"], "source": p.get("source"), "source_label": p.get("source_label"),
                    "terms": p.get("terms"),
                    "date": g.create_date.date().isoformat() if g.create_date else None,
                    "by": by,
                    "label": location_label(p["kind"], p.get("source_label"), p.get("terms"),
                                            g.create_date, by)})
    return out


def location_changed(ds_pk):
    """Called on commit after an adopted or placed location is written or removed. Returns True
    when a re-index was asked for (published and public datasets only)."""
    from django.core.cache import cache
    from django.utils import timezone
    from datasets.models import Dataset
    # Always move the flag forward: ``index_dataset_on_publish`` clears it only for a job that
    # started after it, so a job already in flight cannot answer for this change.
    n = Dataset.objects.filter(pk=ds_pk, lifecycle_state="published", public=True).update(
        index_pending_since=timezone.now())
    if not n:
        return False
    try:
        first = cache.add(f"location_index_refresh:{ds_pk}", 1, REFRESH_DELAY_S)
    except Exception:  # noqa: BLE001 - no cache: schedule anyway; the task tolerates repeats
        logger.warning("location_changed: cache unavailable for dataset %s", ds_pk, exc_info=True)
        first = True
    if first:
        from datasets.tasks import index_dataset_on_publish
        index_dataset_on_publish.apply_async(args=[ds_pk], countdown=REFRESH_DELAY_S)
    return True


def job_predates(job_id, pending_since):
    """True when gateway job ``job_id`` provably started before ``pending_since``, so it cannot
    have read a change made then. Local dataset jobs are named by their start time in epoch
    milliseconds (>= 10**12, ``gateway.dataset_jobs``); any other id proves nothing either way
    and is treated as current, as before."""
    s = str(job_id or "")
    if not s.isdigit() or int(s) < 10 ** 12 or pending_since is None:
        return False
    return int(pending_since.timestamp() * 1000) > int(s)
