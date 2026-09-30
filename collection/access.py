# collection/access.py
"""Write-permission gates for collections and collection groups (decision 2026-09-30).

Collections stay viewable by link, so there is no "viewable" getter here: only writes are
gated. Modelled on datasets.utils (get_*_dataset_or_404, ajax_dataset_gate):

* page views: ``get_*_or_404`` raise the same Http404 for "not permitted" as for "does not
  exist" (behind @login_required / LoginRequiredMixin, so anonymous users get the login page);
* AJAX endpoints: ``ajax_gate`` returns 401 JSON for anonymous requesters (a login redirect
  would reach jQuery as a 200 "success") and 404 JSON for not-permitted / not-found.
"""
from django.http import Http404, JsonResponse
from django.shortcuts import get_object_or_404

from .models import Collection, CollectionGroup


def _gated(model, predicate, user, lookup):
    obj = get_object_or_404(model, **lookup)
    if not getattr(obj, predicate)(user):
        raise Http404("No %s matches the given query." % model.__name__)
    return obj


def get_editable_collection_or_404(user, **lookup):
    """404 unless ``user`` may edit the collection (``Collection.user_can_edit``)."""
    return _gated(Collection, 'user_can_edit', user, lookup)


def get_manageable_collection_or_404(user, **lookup):
    """404 unless ``user`` may manage the collection (``Collection.user_can_manage``)."""
    return _gated(Collection, 'user_can_manage', user, lookup)


def get_reviewable_collection_or_404(user, **lookup):
    """404 unless ``user`` leads the collection's group, or is staff (``user_can_review``)."""
    return _gated(Collection, 'user_can_review', user, lookup)


def get_manageable_group_or_404(user, **lookup):
    """404 unless ``user`` may manage the group (``CollectionGroup.user_can_manage``)."""
    return _gated(CollectionGroup, 'user_can_manage', user, lookup)


def login_required_json(request):
    """401 JSON for an anonymous requester, else None."""
    if not getattr(request.user, 'is_authenticated', False):
        return JsonResponse({'status': 'error', 'message': 'Login required'}, status=401)
    return None


def not_found_json(what='Collection'):
    return JsonResponse({'status': 'error', 'message': '%s not found' % what}, status=404)


def ajax_gate(request, getter, what='Collection', **lookup):
    """Return ``(obj, None)`` if the requester passes ``getter``, else ``(None, JsonResponse)``:
    401 if anonymous, 404 if not permitted or not found (incl. a missing/non-numeric id)."""
    denied = login_required_json(request)
    if denied:
        return None, denied
    try:
        return getter(request.user, **lookup), None
    except (Http404, ValueError, TypeError):
        return None, not_found_json(what)
