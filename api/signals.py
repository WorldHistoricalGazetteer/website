import logging

from django.db.models.signals import post_save, post_delete, m2m_changed
from django.dispatch import receiver

from api.crc_client import crc_delete_link, crc_post_link
from api.download_file import invalidate_and_rebuild_cache, FileCache
from api.models import ContributorAttestation
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


def _attestation_payload(instance: ContributorAttestation) -> dict:
    asserted_at = instance.asserted_at.isoformat() if instance.asserted_at else None
    return {
        "place_a": instance.place_a,
        "place_b": instance.place_b,
        "relation_type": instance.relation_type,
        "source_category": "contributor",
        "source_id": instance.source_id(),
        "asserted_at": asserted_at,
        "justification": instance.justification or None,
    }


@receiver(post_save, sender=ContributorAttestation)
def attestation_saved(sender, instance, created, **kwargs):
    # Only the publishable subset is forwarded; pending/rejected stay
    # in-platform only.
    if instance.status != "active":
        # If a previously-active row has been transitioned out of 'active'
        # we revoke it from the gateway so the public store reflects the
        # new state.
        if not created:
            payload = {
                "place_a": instance.place_a,
                "place_b": instance.place_b,
                "relation_type": instance.relation_type,
                "source_id": instance.source_id(),
            }
            try:
                crc_delete_link(payload)
            except Exception as exc:
                logger.warning("attestation_saved revoke forward failed: %s", exc)
        return
    try:
        crc_post_link(_attestation_payload(instance))
    except Exception as exc:
        logger.warning("attestation_saved forward failed: %s", exc)


@receiver(post_delete, sender=ContributorAttestation)
def attestation_deleted(sender, instance, **kwargs):
    if instance.status != "active":
        # Never forwarded in the first place.
        return
    payload = {
        "place_a": instance.place_a,
        "place_b": instance.place_b,
        "relation_type": instance.relation_type,
        "source_id": instance.source_id(),
    }
    try:
        crc_delete_link(payload)
    except Exception as exc:
        logger.warning("attestation_deleted forward failed: %s", exc)
