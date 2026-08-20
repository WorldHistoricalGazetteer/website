"""Email invitations (place#155).

A signed-in user can email someone a WHG link ("look at this") or an invitation to
register ("come and join"). The recipient is, by definition, someone who has not
consented to hear from us, so the governing constraint is that **we never store
their email address**.

What that means concretely:

* No ``Invitation`` model holding an address. The address lives in memory for the
  duration of the request and in the SMTP conversation, and nowhere else.
* The Zulip mail mirror in :func:`whgmail.messaging.WHGmail` is switched off for
  this path, and recipient-bearing error logging is redacted.
* Sending is synchronous, so the address never becomes a Celery task argument.
* What *is* persisted is a salted HMAC of the address — enough to cap how often
  someone can be invited and to honour an opt-out, but not the address. The salt
  differs from ``users.models.email_lookup_hash`` so these rows can't be joined
  against the user table to reveal who has been invited.

Because the message body is entirely templated — no sender-authored free text —
the only sender-controlled content that reaches the recipient is a target URL,
which is validated against a WHG-origin allowlist below.
"""

import hashlib
import hmac
import logging
from datetime import timedelta
from urllib.parse import urlsplit, urlunsplit

from django.conf import settings
from django.core import signing
from django.core.exceptions import ValidationError
from django.urls import reverse
from django.utils import timezone

logger = logging.getLogger(__name__)

# Per-sender cap: 5 invitations per rolling 24 hours. With no free-text note and a
# URL allowlist, this is the anti-abuse control (in place of a captcha — the sender
# is already authenticated and email-verified).
DAILY_LIMIT = 5
RATE_WINDOW = timedelta(hours=24)

# Per-recipient cap, across all senders, so several users can't invite-bomb one person.
RECIPIENT_LIMIT = 3
RECIPIENT_WINDOW = timedelta(days=30)

# How long the send log is kept. Long enough to enforce the caps and investigate
# abuse; short enough that we aren't sitting on a long-term record of who was
# contacted. Enforced opportunistically on each send.
LOG_RETENTION = timedelta(days=90)

MAX_URL_LENGTH = 500

UNSUBSCRIBE_SALT = 'whg.invitation.unsubscribe'
HASH_SALT = 'whg.invitation.recipient'

# Paths an invitation may never point at: admin surfaces, the API, account
# management, and anything that could be turned into a redirect or a credential
# prompt wearing a whgazetteer.org address.
DENIED_PATH_PREFIXES = (
    '/admin', '/api', '/accounts', '/dashboard', '/dashboard_admin',
    '/media', '/static', '/celery-progress',
)


def invitation_email_hash(email):
    """Salted HMAC of a normalised address, for cap-counting and suppression only.

    Deliberately *not* ``users.models.email_lookup_hash``: a distinct salt means the
    invitation tables cannot be cross-referenced against ``auth_users.email_hash`` to
    work out which registered users have been invited by whom. Keyed with
    ``SECRET_KEY``, so a leaked dump can't be rainbow-tabled back to addresses
    without it.

    Caveat worth being honest about: email addresses are low-entropy, so a hash is
    pseudonymised personal data rather than anonymous data. It is the minimum we can
    keep and still honour "don't contact me again".
    """
    if not email:
        return None
    norm = str(email).strip().lower()
    if not norm:
        return None
    key = f"{HASH_SALT}:{settings.SECRET_KEY}".encode('utf-8')
    return hmac.new(key, norm.encode('utf-8'), hashlib.sha256).hexdigest()


WHG_DOMAIN = 'whgazetteer.org'

# Deliberately not derived from ``ALLOWED_HOSTS`` alone: deployments set that to
# ``['*']``, and ``request.get_host()`` is attacker-controlled under a wildcard, so
# either would let a sender aim an invitation anywhere. This is an explicit list of
# WHG-owned hosts, extendable per-deployment via ``settings.INVITE_ALLOWED_HOSTS``.
BASE_ALLOWED_HOSTS = {
    WHG_DOMAIN,
    f'www.{WHG_DOMAIN}',
    f'dev.{WHG_DOMAIN}',
    f'staging.{WHG_DOMAIN}',
}


def allowed_hosts():
    """Hosts an invitation link may point at."""
    hosts = set(BASE_ALLOWED_HOSTS)

    for h in getattr(settings, 'INVITE_ALLOWED_HOSTS', []) or []:
        h = (h or '').strip().lower().lstrip('.')
        if h and '*' not in h:
            hosts.add(h)

    # Anything this deployment answers to that is itself a WHG domain — so a new
    # subdomain works without a code change, while a wildcard or an unrelated host
    # in ALLOWED_HOSTS can never widen the allowlist.
    for h in getattr(settings, 'ALLOWED_HOSTS', []) or []:
        h = (h or '').strip().lower().lstrip('.')
        if h and '*' not in h and (h == WHG_DOMAIN or h.endswith(f'.{WHG_DOMAIN}')):
            hosts.add(h)

    if getattr(settings, 'DEBUG', False):
        hosts.update({'localhost', '127.0.0.1'})

    hosts.discard('')
    return hosts


def validate_target_url(raw):
    """Return a normalised WHG URL, or raise ``ValidationError``.

    Without this the feature is an open phishing redirector with WHG's sender
    reputation attached: the recipient sees mail from WHG carrying a link the
    sender chose.
    """
    raw = (raw or '').strip()
    if not raw:
        raise ValidationError('No link was supplied.')
    if len(raw) > MAX_URL_LENGTH:
        raise ValidationError('That link is too long to send.')

    parts = urlsplit(raw)
    if parts.scheme not in ('http', 'https'):
        raise ValidationError('Only http and https links can be sent.')

    host = (parts.hostname or '').lower()
    if host not in allowed_hosts():
        raise ValidationError('Only World Historical Gazetteer links can be sent.')

    path = parts.path or '/'
    lowered = path.lower().rstrip('/')
    for prefix in DENIED_PATH_PREFIXES:
        if lowered == prefix or lowered.startswith(prefix + '/'):
            raise ValidationError('That part of the site cannot be shared by email.')

    # Drop any fragment; keep scheme/host/path/query as given.
    return urlunsplit((parts.scheme, parts.netloc, path, parts.query, ''))


def is_suppressed(recipient_hash):
    from main.models import InvitationSuppression
    return InvitationSuppression.objects.filter(recipient_hash=recipient_hash).exists()


def suppress(recipient_hash):
    from main.models import InvitationSuppression
    obj, created = InvitationSuppression.objects.get_or_create(recipient_hash=recipient_hash)
    return created


def purge_expired_logs():
    """Drop send-log rows past their retention. Called on each send so retention is
    self-maintaining and doesn't depend on a scheduled job being wired up."""
    from main.models import InvitationSendLog
    cutoff = timezone.now() - LOG_RETENTION
    InvitationSendLog.objects.filter(created__lt=cutoff).delete()


def sender_remaining(user):
    """How many invitations this user may still send in the current 24-hour window."""
    from main.models import InvitationSendLog
    since = timezone.now() - RATE_WINDOW
    used = InvitationSendLog.objects.filter(sender=user, created__gte=since).count()
    return max(0, DAILY_LIMIT - used)


def recipient_over_limit(recipient_hash):
    from main.models import InvitationSendLog
    since = timezone.now() - RECIPIENT_WINDOW
    return InvitationSendLog.objects.filter(
        recipient_hash=recipient_hash, created__gte=since
    ).count() >= RECIPIENT_LIMIT


def unsubscribe_token(recipient_hash):
    """A signed token carrying only the *hash*, so the opt-out link in the email
    doesn't contain the address either. Signing stops anyone forging suppressions
    for arbitrary hashes."""
    return signing.dumps({'h': recipient_hash}, salt=UNSUBSCRIBE_SALT)


def read_unsubscribe_token(token):
    try:
        data = signing.loads(token, salt=UNSUBSCRIBE_SALT)
    except signing.BadSignature:
        return None
    h = (data or {}).get('h')
    return h if isinstance(h, str) and len(h) == 64 else None


def absolute(request, path):
    return request.build_absolute_uri(path)


class InvitationError(Exception):
    """A refusal we're happy to show the sender. ``status`` is the HTTP status an API
    caller should use (429 when the sender has run out of their daily quota)."""

    def __init__(self, message, status=400):
        super().__init__(message)
        self.status = status


def send_invitation(request, kind, to_email, target_url=None):
    """Send one invitation. Returns the number remaining in the sender's daily quota.

    Raises :class:`InvitationError` with a user-facing message if the send is refused.
    """
    from main.models import InvitationSendLog
    from whgmail.messaging import WHGmail

    user = request.user
    if not user.is_authenticated:
        raise InvitationError('Please sign in to send an invitation.')
    if not user.has_verified_email:
        raise InvitationError(
            'Please confirm your own email address before inviting others. '
            'You can do this from your profile page.'
        )

    if kind not in ('view', 'join'):
        raise InvitationError('Unknown invitation type.')

    recipient_hash = invitation_email_hash(to_email)
    if not recipient_hash:
        raise InvitationError('Please supply an email address.')

    # Not to yourself (place#203). Reached from the team invite box as well as the share modal:
    # an owner typing their own address gets "we've emailed them an invitation to create an
    # account" about an account they are signed in to. It also spends their daily quota.
    from users.models import User
    if User.objects.by_email(to_email) == user:
        raise InvitationError('That is your own email address — invitations are for other people.')

    if sender_remaining(user) <= 0:
        raise InvitationError(
            f'You have reached the limit of {DAILY_LIMIT} invitations in 24 hours. '
            'Please try again tomorrow.',
            status=429,
        )

    # Both of these are deliberately reported with the same wording as success would
    # imply nothing: telling the sender "that person opted out" would leak something
    # about the recipient to someone they chose not to hear from.
    if is_suppressed(recipient_hash) or recipient_over_limit(recipient_hash):
        logger.info('Invitation suppressed or over recipient cap (sender=%s)', user.username)
        return sender_remaining(user)

    normalised_url = None
    if kind == 'view':
        normalised_url = validate_target_url(target_url)   # ValidationError → caller renders it

    unsub = absolute(request, reverse('invite-unsubscribe', args=[unsubscribe_token(recipient_hash)]))

    context = {
        'template': 'invite_to_view' if kind == 'view' else 'invite_to_join',
        'to_email': to_email,
        'subject': (
            f'{user.name} has shared a World Historical Gazetteer link with you'
            if kind == 'view' else
            f'{user.name} has invited you to join the World Historical Gazetteer'
        ),
        'greeting_name': '',            # we don't collect the recipient's name
        'inviter_name': user.name,
        'target_url': normalised_url,
        'unsubscribe_url': unsub,
        'login_url': absolute(request, reverse('accounts:login')),
        'home_url': absolute(request, '/'),
        'atlas_url': absolute(request, reverse('atlas-page')),
        'docs_url': 'https://docs.whgazetteer.org',
        # Reply-To must not be the sender: their address is never disclosed. Deliberately
        # DEFAULT_FROM_EMAIL rather than DEFAULT_FROM_EDITORIAL — the editorial setting is a
        # named individual's inbox (als512@pitt.edu on prod), and a stranger's reply to an
        # unsolicited invitation should not land on one person. Replies go to the no-reply
        # WHG address; the email itself carries the unsubscribe link and a contact route.
        'reply_to': settings.DEFAULT_FROM_EMAIL,
        # Give mail clients a native unsubscribe affordance (and us the deliverability
        # credit for offering one). The URL carries only the signed hash.
        'extra_headers': {
            'List-Unsubscribe': f'<{unsub}>',
            'List-Unsubscribe-Post': 'List-Unsubscribe=One-Click',
        },
        # The recipient never consented to be in our systems — keep the address out
        # of the Zulip mail mirror and out of failure logs.
        'mirror_to_zulip': False,
        'redact_recipient': True,
    }

    if not WHGmail(context=context):
        raise InvitationError('Sorry — the invitation could not be sent. Please try again later.')

    InvitationSendLog.objects.create(
        sender=user,
        recipient_hash=recipient_hash,
        kind=kind,
        target_url=normalised_url or '',
    )
    purge_expired_logs()

    return sender_remaining(user)
