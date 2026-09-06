"""Scheduled sync of the rule sets from the ``indexing`` repo.

Runs on a timer rather than on page load — see :mod:`phonetics.sync` for why
that ordering matters to whether a review can be trusted afterwards.
"""

import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task(name='phonetics.sync_rulesets')
def sync_rulesets():
    from .sync import sync_all
    summary = sync_all()
    logger.info('phonetics sync: %s rule sets, %s changed, %s error(s)',
                summary['rulesets'], summary['changed'], len(summary['errors']))
    for error in summary['errors']:
        logger.warning('phonetics sync error: %s', error)
    return summary
