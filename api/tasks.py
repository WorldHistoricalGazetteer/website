# api/tasks.py
import logging

from celery import shared_task

logger = logging.getLogger(__name__)


@shared_task
def release_embargoes():
    """Celery Beat counterpart of ``manage.py release_embargoes`` (place#162)
    — see ``api/management/commands/release_embargoes.py`` for why this is a
    convergence step rather than the sole enforcement. Scheduled in
    ``whg/celery.py``."""
    from api.models import GazetteerRegistryEntry
    released = GazetteerRegistryEntry.release_due_embargoes()
    if released:
        logger.info("release_embargoes: released %d embargoed gazetteer(s)", released)
    return released
