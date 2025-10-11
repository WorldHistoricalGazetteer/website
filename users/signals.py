# users.signals.py

import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models.signals import post_save, post_delete
from django.dispatch import receiver
from slack_sdk import WebClient
from slack_sdk.errors import SlackApiError

from accounts.orcid import revoke_orcid_token
from whgmail.messaging import WHGmail

logger = logging.getLogger('messaging')
User = get_user_model()


@receiver(post_save, sender=User)
def welcome_email(sender, instance, created, **kwargs):
    """Send welcome email to new user on registration w/reply-to editorial;
       separate message to admins vias Slack"""
    if not created:
        return  # only act on first save (registration)

    logger.debug(
        f"New user created: {instance.id} | {instance.username} | {instance.name}, sending welcome email to {instance.email}")

    WHGmail(context={
        'template': 'welcome',
        'subject': 'Welcome to WHG',
        'to_email': instance.email,
        'greeting_name': instance.name,
        'username': instance.username,
        'name': instance.name,
    })

    try:
        slack_message = (
            f"*Subject:* New User Registered\n"
            f"*Username:* {instance.username}\n"
            f"*Name:* {instance.name}\n"
            f"*User ID:* {instance.id}\n"
            f"----------------------------------------"
        )
        client = WebClient(token=settings.SLACK_BOT_OAUTH)
        response = client.chat_postMessage(
            channel='site-notifications',
            text=slack_message
        )

        if response["ok"]:
            logger.info("Message sent to Slack.")
        else:
            logger.error(f"Failed to send message to Slack: {response['error']}")

    except SlackApiError as e:
        logger.error(f"Slack API error: {e.response['error']}")
    except Exception as e:
        logger.exception("Error occurred while sending Slack notification for new user registration")


@receiver(post_delete, sender=User)
def revoke_orcid_on_delete(sender, instance, **kwargs):
    if instance.orcid_refresh_token:
        revoke_orcid_token(instance.orcid_refresh_token)
