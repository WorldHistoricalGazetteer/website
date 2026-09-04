"""Rights statements for material leaving the platform.

WHG asserts no single blanket licence over what it holds. Each source carries
its own licence, that licence travels with the data wherever it leaves the
platform, and WHG's curation/aggregation licence is asserted *alongside* those
terms, never instead of them (place#157).

Every download surface used to say otherwise, in two incompatible directions:
the download panel claimed CC BY 4.0 over all public data, while the export
header, the zip README and the notification email claimed CC BY-NC-4.0 over
"contributed datasets and collections". The second is not merely inconsistent —
CC BY 4.0 §2(a)(5)(B) forbids a recipient from imposing downstream restrictions,
so applying WHG's NonCommercial overlay to data granted to us under CC BY 4.0
breaches the licence we ourselves received.

This module is the single place those statements are built, so the distinction
between "the contributor's terms" and "WHG's terms for WHG's own contribution"
cannot drift apart again across four templates.

Note what is deliberately absent: where no licence is recorded, nothing is
asserted. Roughly three quarters of contributed datasets are in that position,
and inventing terms for them — permissive or restrictive — would misrepresent
somebody's rights in either direction.
"""

from django.conf import settings


def _overlay():
    return getattr(settings, 'WHG_OVERLAY_LICENSE', None) or {}


def terms_for(obj):
    """Structured rights for one downloadable object.

    ``recorded`` is the field to branch on: False means we hold no licence for
    this item, which is a statement about our records, not about the item being
    unrestricted.
    """
    # Refuse None outright. Every branch below makes a positive statement about
    # somebody's rights, and "no object" would silently render as "no licence
    # recorded" — an authoritative-sounding claim drawn from nothing. Callers
    # that may not have an object must say so themselves.
    if obj is None:
        raise ValueError("terms_for() requires an object; rights cannot be stated for None")

    licence = getattr(obj, 'license', None)
    statement = (getattr(obj, 'rights_statement', '') or '').strip()

    source = None
    if licence:
        source = {
            'spdx_id': licence.spdx_id,
            'label': licence.label,
            'url': licence.url or licence.spdx_url or '',
            'permits_commercial': licence.permits_commercial,
            'attribution_required': licence.attribution_required,
            'no_derivatives': licence.no_derivatives,
            'custom': licence.custom,
        }

    return {
        'recorded': bool(licence),
        'source': source,
        'rights_statement': statement,
        'license_source': getattr(obj, 'license_source', None),
        'overlay': _overlay(),
    }


def terms_text(obj, kind=None):
    """The same thing as plain prose, for the LPF export header and the zip
    README. Deliberately explicit about which half of the statement covers what:
    a reader who takes only one sentence away should take away the source's
    terms, not WHG's.
    """
    t = terms_for(obj)
    kind = kind or obj.__class__.__name__.lower()
    title = (getattr(obj, 'title', '') or '').strip()
    named = f'This {kind}' + (f', "{title}",' if title else '')

    parts = []

    if t['recorded']:
        src = t['source']
        url = f' ({src["url"]})' if src['url'] else ''
        parts.append(
            f'{named} is made available by its contributor under the '
            f'{src["label"]} [{src["spdx_id"]}] licence{url}. Those terms govern the '
            f'records in this file, and they travel with the data: if you redistribute '
            f'these records, you must pass the same terms on.'
        )
    else:
        parts.append(
            f'{named} has no licence recorded in WHG. We therefore assert no terms '
            f'over its records on the contributor\'s behalf, and their absence here is '
            f'not a grant of permission. If you wish to reuse this material, please '
            f'contact the contributor named in the accompanying metadata.'
        )

    if t['rights_statement']:
        parts.append(f'Additional conditions stated by the rights holder: {t["rights_statement"]}')

    overlay = t['overlay']
    if overlay:
        url = f' ({overlay.get("url")})' if overlay.get('url') else ''
        parts.append(
            f'Separately, where WHG has added to this material — reconciliation links '
            f'to other gazetteers, editorial content, and the aggregation itself — that '
            f'WHG contribution is offered under the {overlay.get("label")} '
            f'[{overlay.get("spdx_id")}] licence{url}. This covers WHG\'s own work only '
            f'and does not restrict the terms above.'
        )

    parts.append(
        'Externally hosted datasets and content linked to by WHG remain under the '
        'copyrights and licences specified by their own publishers.'
    )

    return ' '.join(parts)
