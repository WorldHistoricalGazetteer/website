# helpers.py

from django.core.mail import send_mail
from main.choices import AUTHORITY_BASEURI


def emailer_DEPRECATED(subj, msg, from_addr, to_addr):
    """
      TODO: replaced by new_emailer()??
      email various, incl. Celery down notice
      to ['whgazetteer@gmail.com','karl@kgeographer.org'],
    """
    send_mail(
        subj, msg, from_addr, to_addr,
        fail_silently=False,
    )


# append src_id to base_uri
def hit_is_geonames(*sources):
    """True if any of the given hit dataset/authority strings says GeoNames (place#342)."""
    return any(str(x or "").lower() == "geonames" for x in sources)


def link_uri(auth, id, dataset=None):
    """Identifier for a reconciliation hit. A GeoNames hit found by a Wikidata task
    (align_wd/align_wdlocal also query GeoNames) is a ``gn:`` id, not ``wd:`` (place#342)."""
    if hit_is_geonames(dataset):
        return "gn:" + str(id)
    baseuri = AUTHORITY_BASEURI[auth]
    uri = baseuri + str(id)
    return uri
