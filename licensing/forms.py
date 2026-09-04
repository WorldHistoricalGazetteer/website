"""Shared form helpers for contributor licence selection.

The licence picker posts a bare SPDX id, so every form that offers it needs the
same validation. Keeping that in one place matters more than it might look:
the rule about non-selectable licences (below) is a rights constraint, not a
presentation detail, and a second divergent copy of it would eventually let a
contributor license their own data under some institution's bespoke terms.
"""

import logging

logger = logging.getLogger(__name__)


def clean_contributor_license(spdx, context="contribution"):
    """Return ``spdx`` if it is a licence a contributor may choose, else ''.

    An unusable id is dropped with a loud log rather than raising. Silently
    discarding a licence is exactly what place#157 was about, so this is noisy —
    but refusing a whole contribution over a licence we could not resolve would
    be a worse outcome than recording none.
    """
    spdx = (spdx or '').strip()
    if not spdx:
        return ''

    from licensing.models import License

    lic = License.objects.filter(spdx_id=spdx).first()
    if lic is None:
        logger.warning(
            "%s sent unknown licence id %r — dropped. Seed it in the licensing "
            "vocabulary if it is legitimate.", context, spdx)
        return ''

    # The picker never offers these, but the field is a plain POST value: a
    # contributor must not be able to license their own data under one named
    # institution's bespoke terms (e.g. the UK Data Service EULA).
    if not lic.contributor_selectable:
        logger.warning(
            "%s sent non-selectable licence id %r — dropped. These record one "
            "source's own terms and are not a contributor choice.", context, spdx)
        return ''

    return spdx
