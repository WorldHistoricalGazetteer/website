"""Namespace-aggregated gazetteer attribution (citations design §4.7).

Surfaces, per namespace present in a result set, the attribution recorded in
the gazetteer registry, plus WHG's own curation/aggregation licence overlay.
Uses the registry fields available today (name, description-as-citation,
record_count); structured licence/rights_holder fields arrive with the
authorities source-of-truth phase and will slot into the same shape.
"""
from django.conf import settings

from api.models import GazetteerRegistryEntry
from api.reconcile_helpers import get_namespace


def attribution_for(namespaces):
    """Return {namespace: {name, citation, record_count}} for the given
    namespace codes, drawn from the registry's ``authority`` rows.

    Unknown namespaces (and non-authority namespaces such as per-dataset
    ``whg`` sub-namespaces, which are covered by the WHG overlay) are omitted.
    """
    codes = {n.strip().lower() for n in namespaces if n and str(n).strip()}
    if not codes:
        return {}
    out = {}
    for entry in GazetteerRegistryEntry.objects.filter(
            namespace__in=codes, entry_class='authority'):
        # Prefer the structured ``citation_text`` (populated by the Phase 4
        # inventory push) over the legacy ``description`` blob, which older
        # rows crammed the citation into. Falls back to ``description`` for
        # rows pushed before the upgrade.
        out[entry.namespace] = {
            'name': entry.name,
            'citation': entry.citation_text or entry.description or '',
            'record_count': entry.record_count,
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
                    'license__attribution_required', 'license__custom')
            .first())


def attribution_block(namespaces):
    """Full attribution envelope: per-source attribution + the WHG overlay.

    The WHG overlay (settings.WHG_OVERLAY_LICENSE) is WHG's own
    curation/aggregation licence, asserted *alongside* each source's rights.
    """
    return {
        'sources': attribution_for(namespaces),
        'whg': getattr(settings, 'WHG_OVERLAY_LICENSE', None),
    }


def namespaces_from_ids(ids):
    """Derive the set of namespace codes present in a list of place ids
    (e.g. ``"gn:745044"`` -> ``"gn"``)."""
    return {get_namespace(str(i)) for i in ids if i}
