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


@shared_task
def forward_attestation_post(attestation_id):
    """Send one active attestation to the gateway's hard-link overlay. The row is re-read
    here, so one withdrawn between being queued and being run is not sent. Best-effort: a
    failure is logged by ``crc_post_link`` and reconciled by the next ``contributor_replay``
    run (the overlay is rebuildable from Postgres)."""
    from api.crc_client import crc_post_link
    from api.models import ContributorAttestation
    row = ContributorAttestation.objects.filter(pk=attestation_id, status="active").first()
    if row is None:
        return False
    return crc_post_link(row.gateway_payload())


@shared_task
def forward_attestation_revoke(key):
    """Remove one attestation from the gateway overlay (``key`` = ``gateway_key()``)."""
    from api.crc_client import crc_delete_link
    return crc_delete_link(key)
