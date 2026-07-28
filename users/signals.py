# users.signals.py

import logging

from django.conf import settings
from django.contrib.auth import get_user_model
from django.db.models.signals import pre_save, post_save, post_delete
from django.dispatch import receiver

from accounts.orcid import revoke_orcid_token

logger = logging.getLogger('messaging')
User = get_user_model()


@receiver(pre_save, sender=User)
def _remember_prior_confirmation(sender, instance, **kwargs):
    """
    Stash the DB's prior ``email_confirmed`` value so ``welcome_email`` can fire
    only on the genuine unverified->verified transition (or a brand-new verified
    account) — never on an incidental save of an already-verified user.
    """
    if not instance.pk:
        instance._was_confirmed = False
        return
    prev = sender.objects.filter(pk=instance.pk).only("email_confirmed").first()
    instance._was_confirmed = bool(prev and prev.email_confirmed)


@receiver(post_save, sender=User)
def welcome_email(sender, instance, created, **kwargs):
    """
    Send welcome email to new user once they have a verified email.
    Notify admins via Zulip.

    Fires exactly once per account, and only when the account first becomes
    welcomable (created verified, or email just verified). The guard is claimed
    atomically with an UPDATE *before* sending, so a failed send or a rolled-back
    request cannot cause a repeat (the failure mode that spuriously re-notified an
    existing legacy user).
    """
    # Only send once
    if instance.welcome_email_sent:
        return

    # We need a verified email to send
    if not instance.has_verified_email:
        return

    # Only welcome on the transition into a verified state — a new verified
    # account, or an existing account whose email was just confirmed. An
    # incidental save of an already-verified account must not re-notify.
    just_became_verified = created or not getattr(instance, "_was_confirmed", False)
    if not just_became_verified:
        return

    # Claim the once-only guard atomically and durably BEFORE sending. UPDATE()
    # bypasses signals (no recursion) and commits immediately under autocommit,
    # so even if the send below raises — or the surrounding request later rolls
    # back — the flag stays set and we never re-notify. We prefer a missed
    # welcome over a spurious repeat.
    claimed = sender.objects.filter(pk=instance.pk, welcome_email_sent=False).update(
        welcome_email_sent=True
    )
    if not claimed:
        return
    instance.welcome_email_sent = True

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

    except Exception:
        logger.exception(f"Failed to send welcome email to user {instance.id} | {instance.username}")


@receiver(post_delete, sender=User)
def revoke_orcid_on_delete(sender, instance, **kwargs):
    if instance.orcid_refresh_token:
        revoke_orcid_token(instance.orcid_refresh_token)
