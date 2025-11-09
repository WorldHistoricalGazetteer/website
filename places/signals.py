from django.apps import apps
from django.db.models.signals import pre_save
from django.dispatch import receiver

import logging

logger = logging.getLogger(__name__)


@receiver(pre_save, sender=apps.get_model('places', 'Place'))
def handle_index_change(sender, instance, **kwargs):
    from datasets.tasks import unindex_from_pub
    Place = apps.get_model('places', 'Place')

    # If the instance is not in the database yet, it's a new record, so no action is needed.
    if instance._state.adding or not instance.pk:
        return

    # Get the current value from the database for existing objects
    try:
        current_place = Place.objects.only('indexed').get(pk=instance.pk)
    except Place.DoesNotExist:
        return

    # Check if 'indexed' was True and is changing to False
    if current_place.indexed and not instance.indexed:
        # The place is being unindexed, so remove it from pub
        unindex_from_pub.delay(place_id=instance.pk)
        logger.info(f"Place {instance.pk} being removed from pub index")
