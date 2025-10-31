# datasets.signals.py
# when created; when public; when indexed
import logging

import requests
from django.conf import settings
from django.core.cache import caches
from django.db import transaction
from django.db.models.signals import pre_delete, pre_save, post_save
from django.dispatch import receiver

from utils.doi import doi
from .models import Dataset, DatasetFile
from .utils import compute_dataset_bbox

logger = logging.getLogger(__name__)


@receiver(pre_save, sender=Dataset)
def send_new_dataset_email(sender, instance, **kwargs):
    if instance.pk:  # if instance exists
        old_instance = Dataset.objects.get(pk=instance.pk)
        # if old_instance.ds_status != instance.ds_status and instance.ds_status == 'uploaded':
        # Check if the old_instance.ds_status is None, indicating a new instance
        owner_name = instance.owner.name if instance.owner.name else instance.owner.username
        if old_instance.ds_status is None and instance.ds_status == 'uploaded' and not instance.owner.groups.filter(
                name='whg_team').exists():
            notification = (
                f"*Subject:* New Dataset Created\n"
                f"*Owner Name:* {owner_name}\n"
                f"*Username:* {instance.owner.username}\n"
                f"*Dataset Title:* {instance.title}\n"
                f"*Dataset Label:* {instance.label}\n"
                f"*Dataset ID:* {instance.id}\n"
                f"----------------------------------------"
            )

            from whgmail.messaging import zulip_notification
            zulip_notification(notification, topic="New Dataset Created")

            try:
                response = requests.post(settings.SLACK_NOTIFICATION_WEBHOOK, json={"text": notification})
                if not response.status_code == 200:
                    logger.debug(f"Failed to send message to Slack: {response.status_code}, {response.text}")
            except Exception as e:
                logger.exception(f"Error occurred while sending Slack notification for new dataset: {e}")


def format_time(seconds):
    minutes, seconds = divmod(seconds, 60)
    return f"{minutes} minutes {seconds} seconds"


def handle_dataset_bbox(sender, instance, **kwargs):
    bbox = compute_dataset_bbox(instance.label)
    if bbox:
        instance.bbox = bbox


@receiver(pre_save, sender=Dataset)
def check_featured_field_change(sender, instance, **kwargs):
    """
    Signal handler to detect if the 'featured' field has been changed.
    """
    if instance.pk:
        try:
            existing_instance = sender.objects.get(pk=instance.pk)
            # Check if the 'featured' field has changed
            if existing_instance.featured != instance.featured:
                caches['property_cache'].delete(f"dataset:{instance.pk}:carousel_metadata")
                caches['property_cache'].delete(f"dataset:{instance.pk}:citation_csl")
        except sender.DoesNotExist:
            pass


@receiver(pre_save, sender=Dataset)
def handle_public_flag(sender, instance, **kwargs):
    from .tasks import index_to_pub, unindex_from_pub

    if instance.id:  # Check if it's an existing instance, not new
        old_instance = sender.objects.get(pk=instance.pk)
        if old_instance.public != instance.public:  # There's a change in 'public' status
            owner = instance.owner
            from whgmail.messaging import WHGmail
            if instance.public:
                WHGmail(context={
                    'template': 'dataset_published',
                    'to_email': owner.email,
                    'subject': 'Your WHG dataset has been published',
                    'reply_to': settings.DEFAULT_FROM_EDITORIAL,
                    'greeting_name': owner.name,
                    'dataset_title': instance.title if instance else 'N/A',
                    'dataset_label': instance.label if instance else 'N/A',
                    'dataset_id': instance.id if instance else 'N/A',
                })

                # Changed from False to True, index the records
                transaction.on_commit(lambda: index_to_pub.delay(instance.id))

                # remove from volunteers needed page
                # if instance.volunteers:
                #   instance.volunteers = False
                #   instance.save()
            else:
                # Changed from True to False, remove the records from the index
                transaction.on_commit(lambda: unindex_from_pub.delay(instance.id))
                # notify the owner
                owner = instance.owner
                WHGmail(context={
                    'template': 'dataset_unpublished',
                    'to_email': owner.email,
                    'subject': 'Your WHG dataset has been unpublished',
                    'greeting_name': owner.name,
                    'dataset_title': instance.title if instance else 'N/A',
                    'dataset_label': instance.label if instance else 'N/A',
                    'dataset_id': instance.id if instance else 'N/A',
                })


@receiver(post_save, sender=Dataset)
def handle_dataset_post_save(sender, instance, created, **kwargs):
    handle_dataset_bbox(sender, instance, **kwargs)
    doi(instance._meta.model_name, instance.id)


# notify the owner when status changes to 'wd-complete' or 'indexed'
@receiver(pre_save, sender=Dataset)
def handle_status_change(sender, instance, **kwargs):
    # Check if the handler is already running
    if hasattr(instance, '_updating'):
        return
    setattr(instance, '_updating', True)
    try:
        if instance.pk is not None:  # Check if it's an existing instance, not new
            from whgmail.messaging import WHGmail
            old_instance = sender.objects.get(pk=instance.pk)
            # Check whether 'ds_status' has been changed to 'wd-complete'
            # and notify the owner, bcc to editorial
            if old_instance.ds_status != instance.ds_status and instance.ds_status == 'wd-complete':
                owner = instance.owner
                WHGmail(context={
                    'template': 'wikidata_review_complete',
                    'to_email': owner.email,
                    'bcc': [settings.DEFAULT_FROM_EDITORIAL],
                    'subject': 'WHG reconciliation review complete',
                    'greeting_name': owner.name,
                    'dataset_title': instance.title if instance else 'N/A',
                    'dataset_label': instance.label if instance else 'N/A',
                    'dataset_id': instance.id if instance else 'N/A',
                    'slack_notify': True,
                })
                # print('handle_status_change: wd-complete')
                # remove from volunteers needed page
                instance.volunteers = False
                instance.save(update_fields=['volunteers'])

            # Check whether 'ds_status' has been changed to 'indexed'
            if old_instance.ds_status != instance.ds_status and instance.ds_status == 'indexed':
                # print('handle_status_change: ds_status changed to indexed')
                owner = instance.owner
                WHGmail(context={
                    'template': 'dataset_indexed',
                    'to_email': owner.email,
                    'bcc': [settings.DEFAULT_FROM_EDITORIAL],
                    'subject': 'Your WHG dataset is fully indexed',
                    'greeting_name': owner.name,
                    'dataset_title': instance.title if instance else 'N/A',
                    'dataset_label': instance.label if instance else 'N/A',
                    'dataset_id': instance.id if instance else 'N/A',
                    'slack_notify': True,
                })
                # remove from volunteers needed page
                instance.volunteers = False
                instance.save(update_fields=['volunteers'])

    finally:
        # Remove the flag once done
        delattr(instance, '_updating')
    # except Exception as e:
    #   print('send_dataset_email error:', e)
    #   logger.exception("Error occurred while sending dataset email")


@receiver(pre_delete, sender=Dataset)
def remove_files(**kwargs):
    ds_instance = kwargs.get('instance')
    files = DatasetFile.objects.filter(dataset_id_id=ds_instance.id)
    files.delete()
    doi(ds_instance._meta.model_name, ds_instance.id, 'hide')
