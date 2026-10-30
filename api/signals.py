import logging

from django.db import transaction
from django.db.models.signals import post_save, post_delete, m2m_changed
from django.dispatch import receiver

from api.attestations import invalidate_concordances
from api.download_file import invalidate_and_rebuild_cache, FileCache
from api.models import ContributorAttestation
from api.tasks import forward_attestation_post, forward_attestation_revoke
from collection.models import Collection
from datasets.models import Dataset

logger = logging.getLogger("indexing")

@receiver(post_save, sender=Dataset)
def dataset_cache_saved(sender, instance, **kwargs):
    for filetype in ['lpf', 'tsv']:
        invalidate_and_rebuild_cache("dataset", instance.id, filetype=filetype)

@receiver(post_delete, sender=Dataset)
def dataset_cache_deleted(sender, instance, **kwargs):
    for filetype in ['lpf', 'tsv']:
        FileCache.delete_cache("dataset", instance.id, filetype=filetype)
        FileCache.cancel_current_build("dataset", instance.id, filetype=filetype)


# Collection signals (place collections only)
@receiver(post_save, sender=Collection)
def collection_cache_saved(sender, instance, **kwargs):
    if instance.collection_class == "place":
        for filetype in ['lpf', 'tsv']:
            invalidate_and_rebuild_cache("collection", instance.id, filetype=filetype)

@receiver(post_delete, sender=Collection)
def collection_cache_deleted(sender, instance, **kwargs):
    if instance.collection_class == "place":
        for filetype in ['lpf', 'tsv']:
            FileCache.delete_cache("collection", instance.id, filetype=filetype)
            FileCache.cancel_current_build("collection", instance.id, filetype=filetype)

@receiver(m2m_changed, sender=Collection.places.through)
def collection_places_changed(sender, instance, action, pk_set, **kwargs):
    if action in ["post_add", "post_remove", "post_clear"] and instance.collection_class == "place":
        for filetype in ['lpf', 'tsv']:
            # Throttling and deferred rebuild handled internally
            invalidate_and_rebuild_cache("collection", instance.id, filetype=filetype)


# ---------------------------------------------------------------------------
# ContributorAttestation forwarding (Master Plan §2a)
# ---------------------------------------------------------------------------
# Live writes/deletes are forwarded to the gateway so the Pitt-side SQLite
# hard-link overlay stays in sync without waiting for the next batch run.
# Forwarding is best-effort: a failure logs a warning and is reconciled by
# the next Batch 12 contributor_replay run.
#
# Only ``status='active'`` rows are published — pending/rejected rows live
# only in DO PG and are surfaced (to in-scope users) via Django-side
# scope filtering at request time, never via the public hard-link store.


def _forward(task, *args):
    """Queue a gateway forward after the transaction commits (a rolled-back write must
    not reach the overlay) and never raise into the caller: a broker outage must not fail
    an attestation write, and the next replay reconciles."""
    def run():
        try:
            task.delay(*args)
        except Exception as exc:  # noqa: BLE001
            logger.warning("attestation forward could not be queued: %s", exc)
    transaction.on_commit(run)


@receiver(post_save, sender=ContributorAttestation)
def attestation_saved(sender, instance, created, **kwargs):
    invalidate_concordances(instance)
    # Only the publishable subset is forwarded; pending/rejected stay in-platform only.
    if instance.status == "active":
        _forward(forward_attestation_post, instance.pk)
    elif not created:
        # A previously-active row has left 'active': revoke it from the gateway so the
        # public store reflects the new state. (Revoking a row never forwarded is a no-op.)
        _forward(forward_attestation_revoke, instance.gateway_key())


@receiver(post_delete, sender=ContributorAttestation)
def attestation_deleted(sender, instance, **kwargs):
    invalidate_concordances(instance)
    if instance.status == "active":
        _forward(forward_attestation_revoke, instance.gateway_key())
