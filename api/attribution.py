"""Namespace-aggregated gazetteer attribution (citations design §4.7).

Surfaces, per namespace present in a result set, the attribution recorded in
the gazetteer registry, plus WHG's own curation/aggregation licence overlay.

Since Phase 4 (authorities source-of-truth) the registry carries structured
``citation_text``, ``license`` (FK), ``license_url``, ``rights_holder`` and
``contributors_csl`` fields, populated by the indexing pipeline's inventory
push. ``citation`` prefers ``citation_text`` and falls back to the legacy
``description`` blob for rows pushed before the upgrade; ``license`` and the
other structured keys are emitted only when present, so the shape degrades
gracefully on un-upgraded data.
"""
from django.conf import settings

from api.models import GazetteerRegistryEntry
from api.reconcile_helpers import get_namespace


def _entry_attribution(entry):
    """Build the per-source attribution dict for one registry row."""
    data = {
        'name': entry.name,
        'citation': (entry.citation_text or entry.description or ''),
        'record_count': entry.record_count,
    }
    # Structured licence: prefer the resolved FK, allowing a per-source URL
    # override (``license_url``) over the canonical deed.
    if entry.license_id is not None:
        data['license'] = {
            'spdx_id': entry.license.spdx_id,
            'label': entry.license.label,
            'url': entry.license_url or entry.license.url,
        }
    elif entry.license_url:
        data['license'] = {'spdx_id': None, 'label': None,
                           'url': entry.license_url}
    if entry.rights_holder:
        data['rights_holder'] = entry.rights_holder
    if entry.source_url:
        data['source_url'] = entry.source_url
    if entry.contributors_csl:
        data['contributors'] = entry.contributors_csl
    return data


def attribution_for(namespaces):
    """Return ``{namespace: {name, citation, record_count, license?,
    rights_holder?, source_url?, contributors?}}`` for the given namespace
    codes, drawn from the registry's ``authority`` rows.

    Optional keys (``license``/``rights_holder``/``source_url``/
    ``contributors``) appear only when the registry has them. Unknown
    namespaces (and non-authority namespaces such as per-dataset ``whg``
    sub-namespaces, which are covered by the WHG overlay) are omitted.
    """
    codes = {n.strip().lower() for n in namespaces if n and str(n).strip()}
    if not codes:
        return {}
    out = {}
    qs = (GazetteerRegistryEntry.objects
          .filter(namespace__in=codes, entry_class='authority')
          .select_related('license'))
    for entry in qs:
        out[entry.namespace] = _entry_attribution(entry)
    return out


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
