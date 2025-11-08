# users.signals.py

import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver

from accounts.orcid import revoke_orcid_token

logger = logging.getLogger('messaging')
User = get_user_model()


@receiver(post_save, sender=User)
def welcome_email(sender, instance, created, **kwargs):
    """
    Send welcome email to new user once they have a verified email.
    Notify admins via Zulip.
    """
    # Only send once
    if instance.welcome_email_sent:
        return

    # We need a verified email to send
    if not instance.has_verified_email:
        return

    # Send welcome email
    try:
        logger.debug(
            f"Sending welcome email to user {instance.id} | {instance.username} | {instance.name} | {instance.email}"
        )

        from whgmail.messaging import WHGmail
        WHGmail(context={
            'template': 'welcome',
            'subject': 'Welcome to WHG',
            'to_email': instance.email,
            'greeting_name': instance.name,
            'username': instance.username,
            'name': instance.name,
        })

        # Notify admins via Zulip
        notification = (
            f"*Subject:* New User Registered\n"
            f"*Username:* {instance.username}\n"
            f"*Name:* {instance.name}\n"
            f"*User ID:* {instance.id}\n"
            f"----------------------------------------"
        )
        from whgmail.messaging import zulip_notification
        zulip_notification(notification, topic="New User Registered")

        # Mark email as sent
        instance.welcome_email_sent = True
        instance.save(update_fields=["welcome_email_sent"])

    except Exception:
        logger.exception(f"Failed to send welcome email to user {instance.id} | {instance.username}")


@receiver(post_delete, sender=User)
def revoke_orcid_on_delete(sender, instance, **kwargs):
    if instance.orcid_refresh_token:
        revoke_orcid_token(instance.orcid_refresh_token)
