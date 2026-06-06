"""Truthful, per-object rights/licence rendering (citations design §4.2–4.3).

Replaces the sitewide hard-coded CC-BY-NC-4.0 assertion. A WHG object's rights
are the *source* licence chosen by the contributor (``obj.license`` — a
``License`` row, optional) asserted **alongside** WHG's own aggregation/curation
overlay (``settings.WHG_OVERLAY_LICENSE``), never instead of it (design
decisions 1+2). Where no source licence is recorded we assert only the WHG
overlay — we never fabricate a source licence (which is exactly the bug this
replaces: stamping ``-NC`` over ODbL / ODC-By / public-domain source data).

One code path feeds DataCite DOI metadata (``utils/doi.py``), download
envelopes (``api/download_file.py``), and export READMEs (``utils/tasks.py``).
"""
from django.conf import settings


def _overlay():
    """WHG's aggregation/curation overlay licence, or None if unset."""
    return getattr(settings, 'WHG_OVERLAY_LICENSE', None)


def _license_of(obj):
    """Return the object's source ``License`` instance, or None.

    Tolerant of objects without the FK (e.g. place-type DOIs) and of the
    ``Link.license`` CharField (ignored here — it's not a ``License`` row).
    """
    lic = getattr(obj, 'license', None)
    # Only treat it as a source licence if it looks like a License row
    # (has an spdx_id) — guards against the legacy CharField on other models.
    return lic if getattr(lic, 'spdx_id', None) else None


def datacite_rights_list(obj):
    """Build a DataCite ``rightsList`` for ``obj``.

    Always includes the WHG overlay; prepends the object's own source licence
    when one is recorded. Returns a list of DataCite rights dicts.
    """
    out = []
    lic = _license_of(obj)
    if lic is not None:
        out.append({
            'rights': lic.label,
            'rightsURI': lic.url or '',
            'rightsIdentifier': lic.spdx_id,
            'rightsIdentifierScheme': 'SPDX',
            'schemeURI': lic.spdx_uri or 'https://spdx.org/licenses/',
            'lang': 'en',
        })
    overlay = _overlay()
    if overlay:
        out.append({
            'rights': overlay.get('label'),
            'rightsURI': overlay.get('url') or '',
            'rightsIdentifier': overlay.get('spdx_id'),
            'rightsIdentifierScheme': 'SPDX',
            'schemeURI': 'https://spdx.org/licenses/',
            'lang': 'en',
        })
    return out


def rights_statement_text(obj):
    """Human-readable licence statement for download/export envelopes.

    Names the object's source licence (when recorded), includes any free-text
    ``rights_statement`` on the object, and always states the WHG overlay.
    """
    lic = _license_of(obj)
    rights_stmt = (getattr(obj, 'rights_statement', '') or '').strip()
    overlay = _overlay() or {}
    noun = obj.__class__.__name__.lower()
    parts = []
    if lic is not None:
        suffix = f": {lic.url}" if lic.url else ""
        parts.append(
            f"This {noun} is licensed under {lic.label} "
            f"({lic.spdx_id}){suffix}."
        )
    if rights_stmt:
        parts.append(rights_stmt)
    if overlay:
        overlay_url = overlay.get('url') or ''
        parts.append(
            "Unless specified otherwise, content created for or uploaded to "
            "the World Historical Gazetteer is made available under WHG's "
            f"aggregation licence, {overlay.get('label')} "
            f"({overlay.get('spdx_id')})"
            + (f": {overlay_url}" if overlay_url else "") + ". "
            "Externally hosted datasets and content linked to by WHG remain "
            "under the copyrights and licences specified by their original "
            "contributors."
        )
    return "\n\n".join(parts)
