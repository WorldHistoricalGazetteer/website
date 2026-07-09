"""
Dataset ("Gazetteer") check-out — materialise a bounded set of a published gazetteer's records into a
Workbench ``dataset_edit`` snapshot for editing (plan-dataset-checkout §3/§4).

Two entry points on the same machinery, differing only in how the record set is bounded:

  * **Filtered subset (§3, the workhorse)** — check out the records matching a title/country filter
    (and/or a page), bounded by ``limit``. "Fix these twelve records."
  * **Whole-dataset-that-fits (§4)** — the same call with no filter, allowed only when the dataset is
    small enough to materialise (the server cap here + the client's ``navigator.storage.estimate()``
    capacity gate together realise §1b's "steer by size"). Large gazetteers are refused and the user is
    steered to the subset path.

Each checked-out record is the same full-LPF snapshot the single-record editor speaks
(``checkout.record_snapshot``), plus a per-record ``base_version`` (``record_state_hash``) so
publish-back can optimistic-lock **each record individually** (plan §1d): a concurrent edit to one
record never blocks a correction to another; only a clash on the *same* record surfaces a conflict.

The published dataset stays authoritative; publish-back (``dataset_publish.publish_dataset_edit``) is a
per-record **delta** (only records the editor marked ``_dirty``) — never a full re-accession.
"""
import hashlib
import json
import logging

from .checkout import record_snapshot, record_state_hash

logger = logging.getLogger(__name__)

# Server-side hard cap on a single check-out. Protects the server from building a giant JSON snapshot /
# oversized project row for a huge gazetteer. The browser capacity gate (navigator.storage.estimate)
# sits on top of this; whichever is smaller wins. Filtered subsets almost always sit well under this.
MAX_CHECKOUT_RECORDS = 3000
# Rough serialised bytes per full-LPF record — used only to advise the client's capacity gate.
EST_BYTES_PER_RECORD = 2200


class DatasetCheckoutError(Exception):
    """Raised when a dataset can't be checked out (too large for whole-dataset, empty filter, …)."""


def _places_qs(dataset, q=None, ccode=None):
    """The dataset's places, narrowed by an optional title/name query and country code. Ordered by id
    for a stable, resumable page window."""
    from django.db.models import Q
    from places.models import Place, PlaceName
    qs = Place.objects.filter(dataset=dataset)
    if ccode:
        qs = qs.filter(ccodes__contains=[str(ccode).upper()[:2]])
    q = (q or '').strip()
    if q:
        # title match OR any variant-name match (so "Roma" finds a place titled "Rome").
        name_pids = PlaceName.objects.filter(place__dataset=dataset, toponym__icontains=q)\
            .values_list('place_id', flat=True)
        qs = qs.filter(Q(title__icontains=q) | Q(id__in=list(name_pids[:5000])))
    return qs.order_by('id')


def dataset_checkout_info(dataset):
    """Cheap pre-flight for the capacity chooser (no per-record serialisation): total record count, a
    per-record byte estimate, and the distinct country codes present (capped) so the client can offer a
    country filter. The client compares ``numrows × est_bytes`` against ``navigator.storage.estimate()``
    to decide whole-dataset vs subset."""
    from places.models import Place
    numrows = Place.objects.filter(dataset=dataset).count()
    ccodes = set()
    for row in (Place.objects.filter(dataset=dataset).exclude(ccodes=[])
                .values_list('ccodes', flat=True)[:5000]):
        for c in (row or []):
            ccodes.add(c)
        if len(ccodes) > 60:
            break
    return {
        'dataset_id': dataset.id, 'title': dataset.title, 'label': dataset.label,
        'numrows': numrows, 'est_bytes_per_record': EST_BYTES_PER_RECORD,
        'est_total_bytes': numrows * EST_BYTES_PER_RECORD,
        'max_records': MAX_CHECKOUT_RECORDS,
        'fits_whole': numrows <= MAX_CHECKOUT_RECORDS,
        'ccodes': sorted(ccodes),
    }


def checkout_dataset(dataset, q=None, ccode=None, limit=None, offset=0):
    """Serialise a bounded set of ``dataset``'s records → a ``dataset_edit`` snapshot.

    ``q``/``ccode`` filter the set (subset path); ``limit``/``offset`` page it. With no filter and no
    limit this is the whole-dataset path — allowed only if the dataset fits ``MAX_CHECKOUT_RECORDS``
    (else raise, steering the caller to a subset). Returns ``(snapshot, base_version)`` where
    ``base_version`` is a manifest hash over the per-record hashes (informational; the real guard is
    per-record at publish)."""
    filtered = bool((q or '').strip() or ccode)
    qs = _places_qs(dataset, q=q, ccode=ccode)
    total_matched = qs.count()
    if total_matched == 0:
        raise DatasetCheckoutError('No records match — adjust the filter and try again.')

    cap = MAX_CHECKOUT_RECORDS
    if limit is not None:
        try:
            cap = max(1, min(MAX_CHECKOUT_RECORDS, int(limit)))
        except (TypeError, ValueError):
            cap = MAX_CHECKOUT_RECORDS

    # Whole-dataset guard: an unfiltered, unpaged set that exceeds the cap is refused (steer to subset).
    if not filtered and limit is None and total_matched > MAX_CHECKOUT_RECORDS:
        raise DatasetCheckoutError(
            f'This gazetteer has {total_matched:,} records — too many to edit wholesale here. '
            f'Filter by name or country, or edit a page of up to {MAX_CHECKOUT_RECORDS:,} records.')

    try:
        offset = max(0, int(offset))
    except (TypeError, ValueError):
        offset = 0

    window = (qs.select_related('dataset')
              .prefetch_related('names', 'types', 'links', 'descriptions', 'geoms')[offset:offset + cap])
    records, manifest = [], []
    for place in window:
        rec = record_snapshot(place)
        rec['base_version'] = record_state_hash(place)
        rec['_dirty'] = False
        records.append(rec)
        manifest.append([place.id, rec['base_version']])

    snapshot = {
        'dataset_id': dataset.id, 'dataset_label': dataset.label, 'dataset_title': dataset.title,
        'filter': {'q': (q or '').strip(), 'ccode': (ccode or '')},
        'total_matched': total_matched, 'offset': offset, 'loaded': len(records),
        'records': records,
    }
    base_version = hashlib.sha1(
        json.dumps(sorted(manifest), sort_keys=True, default=str).encode('utf-8')).hexdigest()
    return snapshot, base_version
