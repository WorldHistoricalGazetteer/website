"""Namespace-aggregated gazetteer attribution (citations design §4.7).

Surfaces, per source present in a result set, the attribution recorded in the
gazetteer registry (for authority gazetteers) or on the dataset (for contributed
WHG data), plus WHG's own curation/aggregation licence overlay.

``attribution_block()`` is the envelope attached at the ROOT of every
multi-record API response, so that a consumer receiving results blended from
several differently-licensed sources can comply with each source's terms
without a second lookup (place#157).
"""
from django.conf import settings

from api.models import GazetteerRegistryEntry
from api.reconcile_helpers import get_namespace

# Registry fields spanned for the aggregated block. Kept separate from the flat
# ``license__*`` selection in ``registry_attribution()`` below: that shape is a
# private contract with the Atlas UI, whereas this one is a PUBLIC API contract
# and therefore nests the licence as an object.
_LICENSE_FIELDS = (
    'license__spdx_id', 'license__label', 'license__url',
    'license__permits_commercial', 'license__share_alike',
    'license__attribution_required', 'license__no_derivatives',
    'license__custom',
)


def _license_object(row, url_override=None):
    """Nest the spanned ``license__*`` values into a single ``license`` object,
    or return None when the source carries no resolved licence.

    ``url_override`` is the registry/dataset-level deed URL, used where a source
    deviates from the canonical licence deed.
    """
    spdx = row.get('license__spdx_id')
    if not spdx:
        return None
    return {
        'spdx_id': spdx,
        'label': row.get('license__label') or '',
        'url': url_override or row.get('license__url') or '',
        # `permits_commercial` and `no_derivatives` are TRI-STATE: null means the
        # rights holder grants nothing either way (e.g. UN Geospatial data), which
        # is not the same as a prohibition. Consumers must test for null before
        # treating either as a boolean (place#157).
        'permits_commercial': row.get('license__permits_commercial'),
        'share_alike': row.get('license__share_alike'),
        'attribution_required': row.get('license__attribution_required'),
        'no_derivatives': row.get('license__no_derivatives'),
        'custom': row.get('license__custom'),
    }


def attribution_for(namespaces):
    """Return ``{namespace: {name, citation, record_count, license, …}}`` for the
    given namespace codes, drawn from the registry's ``authority`` rows.

    The ``license`` object is what makes this block worth carrying: without it a
    consumer knows *which* sources a result set spans but not on what terms
    (place#157). ``permits_commercial`` / ``share_alike`` are the two flags a
    reuser actually has to act on.

    Unknown namespaces (and non-authority namespaces such as per-dataset ``whg``
    sub-namespaces — see ``attribution_for_datasets``) are omitted.
    """
    codes = {n.strip().lower() for n in namespaces if n and str(n).strip()}
    if not codes:
        return {}
    out = {}
    for row in (GazetteerRegistryEntry.objects
                .filter(namespace__in=codes, entry_class='authority')
                .values('namespace', 'name', 'description', 'citation_text',
                        'rights_holder', 'source_url', 'license_url',
                        'record_count', *_LICENSE_FIELDS)):
        # Prefer the structured ``citation_text`` (populated by the Phase 4
        # inventory push) over the legacy ``description`` blob, which older
        # rows crammed the citation into. Falls back to ``description`` for
        # rows pushed before the upgrade.
        out[row['namespace']] = {
            'name': row['name'],
            'citation': row['citation_text'] or row['description'] or '',
            'record_count': row['record_count'],
            'rights_holder': row['rights_holder'] or '',
            'source_url': row['source_url'] or '',
            'license': _license_object(row, url_override=row['license_url']),
        }
    return out


def attribution_for_datasets(dataset_refs):
    """Return ``{dataset_label: {name, citation, license, …}}`` for contributed
    WHG datasets.

    The authority registry covers namespaced sources (``gn:``, ``tgn:``, …), but
    records served from WHG's own database carry the pseudo-namespace ``whg`` and
    their rights live on the *dataset*, not the registry. Without this, the block
    for a DB-backed endpoint would report an empty source list and assert only
    the WHG overlay — which would state WHG's terms while silently omitting the
    contributor's, exactly the collapse the per-source model exists to prevent.

    ``dataset_refs`` may mix primary keys and labels.
    """
    from datasets.models import Dataset

    pks, labels = set(), set()
    for ref in dataset_refs or ():
        if ref is None:
            continue
        if isinstance(ref, int) or (isinstance(ref, str) and ref.isdigit()):
            pks.add(int(ref))
        else:
            labels.add(str(ref).strip())
    if not pks and not labels:
        return {}

    from django.db.models import Q
    q = Q(pk__in=pks) if pks else Q()
    if labels:
        q = q | Q(label__in=labels)
    out = {}
    for row in (Dataset.objects.filter(q)
                .values('label', 'title', 'citation', 'creator',
                        'rights_statement', 'webpage', *_LICENSE_FIELDS)):
        out[row['label']] = {
            'name': row['title'],
            'citation': row['citation'] or '',
            'rights_holder': row['creator'] or '',
            'source_url': row['webpage'] or '',
            # Free-text conditions attached to a custom licence, or extra terms
            # layered on a standard one — surfaced because a bespoke condition
            # cannot be inferred from the SPDX id alone.
            'rights_statement': row['rights_statement'] or '',
            'license': _license_object(row),
        }
    return out


def registry_attribution(namespace):
    """Full per-source attribution for a single namespace, drawn from the
    registry's ``authority`` row.

    Returns a flat dict ready to attach to a place as ``place["attribution"]``
    (or ``None`` when the namespace is unknown / not an authority). Shared by
    the Atlas portal modal (``search.views.atlas_place``) and the Atlas
    gazetteer-feature map popup (``api.views_entity.EntityFeatureView``), so
    both surface identical source/licence metadata. The ``license`` FK is
    exposed as flat spanned fields (``license__spdx_id`` etc.) — consistent
    with how it is consumed elsewhere — plus the permissiveness flags the
    client uses to colour-code the licence badge.
    """
    ns = (namespace or '').strip().lower()
    if not ns:
        return None
    return (GazetteerRegistryEntry.objects
            .filter(namespace=ns, entry_class='authority')
            .values('id', 'name', 'description', 'citation_text',
                    'rights_holder', 'source_url', 'license_url',
                    'license__spdx_id', 'license__label', 'license__url',
                    'license__permits_commercial', 'license__share_alike',
                    'license__attribution_required', 'license__custom',
                    # Download legality + volume (place#136) — lets the popup
                    # conditionally offer a source-data download.
                    'downloadable', 'redistributable',
                    'download_blocked_reason')
            .first())


def attribution_block(namespaces=None, datasets=None):
    """Full attribution envelope: per-source attribution + the WHG overlay.

    Attached at the ROOT of every multi-record API response (place#157) as::

        {"attribution": {"sources": {...}, "datasets": {...}, "whg": {...}},
         "features": [...]}

    ``sources`` is keyed by authority namespace, ``datasets`` by dataset label;
    a result set may legitimately span both. ``datasets`` is omitted entirely
    when no contributed data is present, so the common authority-only case stays
    compact.

    The WHG overlay (``settings.WHG_OVERLAY_LICENSE``) is WHG's own
    curation/aggregation licence, asserted *alongside* each source's rights —
    never instead of them.
    """
    block = {
        'sources': attribution_for(namespaces or ()),
        'whg': getattr(settings, 'WHG_OVERLAY_LICENSE', None),
    }
    ds = attribution_for_datasets(datasets or ())
    if ds:
        block['datasets'] = ds
    return block


def namespaces_from_ids(ids):
    """Derive the set of namespace codes present in a list of place ids
    (e.g. ``"gn:745044"`` -> ``"gn"``)."""
    return {get_namespace(str(i)) for i in ids if i}


def datasets_from_place_ids(place_ids):
    """Map WHG place ids to the labels of the datasets that contributed them, so
    ``whg``-namespace results can be attributed to a real source rather than only
    to the WHG overlay. Gateway ids (``gn:…``) are ignored.

    ``whg:<dataset_id>:<src_id>`` names its dataset outright, so those need no
    place lookup at all; ``whg:<place_pk>`` and the bare numeric form predate
    namespacing and are still resolved through Place. Two queries at most."""
    from datasets.models import Dataset
    from places.models import Place

    dataset_pks, place_pks = set(), set()
    for raw in place_ids:
        parts = str(raw).split(':')
        if len(parts) >= 3 and parts[0].lower() == 'whg' and parts[1].isdigit():
            dataset_pks.add(int(parts[1]))
        elif len(parts) == 2 and parts[0].lower() == 'whg' and parts[1].isdigit():
            place_pks.add(int(parts[1]))
        elif str(raw).isdigit():
            place_pks.add(int(raw))

    labels = set()
    if dataset_pks:
        labels |= {label for label in (Dataset.objects.filter(pk__in=dataset_pks)
                                       .values_list('label', flat=True)) if label}
    if place_pks:
        labels |= {label for label in (Place.objects.filter(pk__in=place_pks)
                                       .values_list('dataset__label', flat=True)) if label}
    return labels


def safe_attribution_block(namespaces=None, datasets=None):
    """``attribution_block`` that can never break a result payload.

    Attribution is supplementary metadata: if the registry is unreachable or a
    lookup fails, the caller should still get its results. Returns the bare WHG
    overlay on failure rather than raising.
    """
    try:
        return attribution_block(namespaces=namespaces, datasets=datasets)
    except Exception:   # noqa: BLE001 — never let attribution break a response
        import logging
        logging.getLogger(__name__).exception('attribution_block failed')
        return {'sources': {}, 'whg': getattr(settings, 'WHG_OVERLAY_LICENSE', None)}
