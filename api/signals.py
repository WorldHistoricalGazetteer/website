from django.db.models.signals import post_save, post_delete, m2m_changed
from django.dispatch import receiver

from api.download_file import invalidate_and_rebuild_cache, FileCache
from collection.models import Collection
from datasets.models import Dataset

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
