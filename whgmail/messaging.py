import html
import logging
import re
import requests

from django.conf import settings
from django.contrib.auth.models import AnonymousUser
from django.core.mail import EmailMultiAlternatives
from django.http import HttpResponse
from django.template.loader import render_to_string
from django.utils.html import strip_tags

logger = logging.getLogger('messaging')


def zulip_notification(notification, stream="website-notifications", topic="WHG Website Notification"):
    """
    Sends a notification to a specified Zulip stream using an incoming webhook bot.
    """
    try:
        # For incoming webhook bots, use the messages endpoint
        url = "https://chat.whgazetteer.org/api/v1/messages"

        # Message payload
        data = {
            "type": "stream",
            "to": stream,
            "topic": topic,
            "content": notification
        }

        # Use HTTP Basic Auth with bot email and API key
        response = requests.post(
            url,
            auth=("django-bot@chat.whgazetteer.org", settings.ZULIP_API_KEY),
            data=data
        )

        if response.status_code == 200:
            logger.info("Message sent to Zulip.")
            return True
        else:
            logger.error(f"Failed to send message to Zulip: {response.text}")
            return False

    except Exception as e:
        logger.exception("Error occurred while sending notification to Zulip")
        return False


def WHGmail(request=None, context=None):
    """
    Sends an email using the provided context.

    Context Parameters:
    -------------------
    - template (required): The name of the email template to be used (without the .html extension).
        Example: context['template'] = 'welcome'
        
    - user (optional): The user object: taken from the request object if not provided. Used to fetch email and display name if those are not provided in context.
        Example: context['user'] = request.user

    - to_email (optional): The recipient's email address. If not provided, it will try to fetch from the user object.
        Example: context['to_email'] = 'recipient@example.com'

    - subject (optional): The subject of the email. Defaults to "WHG Communication".
        Example: context['subject'] = 'Welcome to WHG'

    - greeting_name (optional): The name to be used in the email greeting. Defaults to the user's display name.
        Example: context['greeting_name'] = 'John Doe'

    - from_name (optional): The name to be used in the "from" field of the email. Defaults to "The WHG Project Team".
        Example: context['from_name'] = 'WHG Support Team'

    - editor_email (optional): The email address used in the reply-to field. Defaults to settings.DEFAULT_FROM_EDITORIAL.
        Example: context['editor_email'] = 'editorial@whg.com'

    - reply_to (optional): The email address used in the reply-to field. Defaults to settings.DEFAULT_FROM_EDITORIAL.
        Example: context['reply_to'] = 'support@whg.com'

    - mirror_to_zulip (optional): Whether to post a copy of the sent email to Zulip. Defaults to True.
        Set False when the recipient has not consented to be in our systems — an invitation to someone
        with no WHG account (place#155), where the promise is that we do not retain their address.
        Example: context['mirror_to_zulip'] = False

    - redact_recipient (optional): Whether to keep the recipient's address out of failure logs
        (SMTP errors often quote it). Defaults to False. Pair with mirror_to_zulip=False.
        Example: context['redact_recipient'] = True

    Returns:
    --------
    - bool: True if the email was sent successfully, False otherwise.
    
    Example usage:
    --------------
    WHGmail(request, {
        'template': 'welcome',
        'user': request.user,
        'to_email': 'recipient@example.com',
        'subject': 'Welcome to WHG',
        'greeting_name': 'John Doe',
        'from_name': 'WHG Support Team',
        'editor_email': 'editorial@whg.com',
        'reply_to': 'support@whg.com',
    })
    """
    logger.info("WHGmail function has been called.")

    # Copy rather than mutate: the setdefault() calls below would otherwise persist
    # into a shared mutable default and bleed one message's greeting/subject into the next.
    context = dict(context or {})

    try:
        user = context.get('user', getattr(request, 'user', None))
        
        to_email = context.get('to_email', getattr(user, 'email', None))
        if not to_email:
            raise ValueError("No recipient email address specified.")
        
        template = context.get('template')
        if not template:
            raise ValueError("No template name specified.")
        
        if isinstance(user, AnonymousUser) or user is None:
            context.setdefault('greeting_name', "")
        else:
            context.setdefault('greeting_name', f" {user.name}!" if user and user.name else "")
        
        context.setdefault('subject', "WHG Communication")
        context.setdefault('from_name', "The WHG Project Team")
        context.setdefault('editor_email', settings.DEFAULT_FROM_EDITORIAL)
        context.setdefault('reply_to', settings.DEFAULT_FROM_EDITORIAL)
            
        html_content = render_to_string(f'{template}.html', context)
        
        # For text content, remove css styling and footer from email
        reducing_regex = re.compile(
            r'<style.*?>.*?</style>|'
            r'<div\s+class=["\']footer-container["\'].*?>.*?</div>',
            flags=re.DOTALL
        )
        # strip_tags leaves entities behind, so a URL in the plain-text part would read
        # "…?a=1&amp;b=2". Unescape after stripping so the text alternative is usable.
        text_content = html.unescape(strip_tags(re.sub(reducing_regex, '', html_content)))
        
        email = EmailMultiAlternatives(
            subject=context.get('subject'),
            body=text_content,
            from_email=settings.DEFAULT_FROM_EMAIL,
            to=[to_email],
            cc=context.get('cc', []),
            bcc=context.get('bcc', []),
            headers={'Reply-To': context.get('reply_to'), **context.get('extra_headers', {})}
        )
    
        email.attach_alternative(html_content, "text/html")
        email.send(fail_silently=False)

        if context.get('mirror_to_zulip', True):
            notification = (
                f"*Copy of email sent to:* {context.get('greeting_name')} (email: {to_email})\n"
                f"*Subject:* {context.get('subject')}\n"
                f"*Message:* {text_content}\n"
            )

            zulip_notification(notification, topic="Email Sent")

        return True

    except Exception as e:
        if context.get('redact_recipient'):
            # The exception text can quote the recipient address (SMTP rejections
            # routinely do), and this path is used for people who never consented
            # to be in our systems.
            logger.error('WHGmail failed for a redacted recipient (template=%s): %s',
                         context.get('template'), type(e).__name__)
        else:
            logger.error(f'WHGmail failed, error: {e}')
        return False

def testWHGmail(request):    
    success = WHGmail(request, {
        'template': 'wikidata_review_complete',
        'to_email': 'stephen@docuracy.co.uk',
    })
    return HttpResponse("Test message(s) sent!" if success else "Failed to send messages(s).")
