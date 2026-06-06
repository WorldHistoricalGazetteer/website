"""Shared CRediT-contribution add/delete logic for the public widget, reused by
the dataset and collection metadata/build pages. Callers MUST check edit
permission on the target object before delegating here."""
from django.contrib.contenttypes.models import ContentType
from django.core.cache import caches
from django.http import JsonResponse

from persons.models import Contribution, CreditRole, ContributionDegree
from persons.utils import resolve_person
from utils.csl_citation_formatter import parse_names


def invalidate_citation_cache(obj):
    try:
        caches['property_cache'].delete(
            f"{obj._meta.model_name}:{obj.pk}:citation_csl")
    except Exception:
        pass


def add_contribution(request, obj):
    """Create a Contribution (and Person) for ``obj`` from POST data."""
    name_text = (request.POST.get('name') or '').strip()
    role = request.POST.get('role') or ''
    if not name_text:
        return JsonResponse({'error': 'Name is required.'}, status=400)
    if role not in CreditRole.values:
        return JsonResponse({'error': 'A valid CRediT role is required.'}, status=400)

    degree = request.POST.get('degree') or ''
    if degree and degree not in ContributionDegree.values:
        degree = ''
    is_corr = request.POST.get('is_corresponding') in ('true', 'on', '1', 'True')
    affiliation = (request.POST.get('affiliation') or '').strip() or None
    orcid_in = (request.POST.get('orcid') or '').strip() or None

    parsed = parse_names(name_text)
    if not parsed:
        return JsonResponse({'error': 'Could not parse the name.'}, status=400)
    name = parsed[0]
    if orcid_in and 'literal' not in name:
        name['ORCID'] = orcid_in
    person = resolve_person(name, affiliation=affiliation)

    ct = ContentType.objects.get_for_model(obj.__class__)
    order = Contribution.objects.filter(content_type=ct, object_id=str(obj.pk)).count()
    contrib, created = Contribution.objects.get_or_create(
        person=person, role=role, content_type=ct, object_id=str(obj.pk),
        defaults={'degree': degree, 'is_corresponding': is_corr, 'order': order},
    )
    if not created:
        return JsonResponse({'error': 'That person already has that role here.'}, status=409)
    invalidate_citation_cache(obj)
    return JsonResponse({
        'id': contrib.id,
        'person': str(person),
        'orcid': person.orcid or '',
        'affiliation': person.affiliation or '',
        'role': contrib.get_role_display(),
        'degree': contrib.get_degree_display() if contrib.degree else '',
        'is_corresponding': contrib.is_corresponding,
    })


def delete_contribution(request, obj, cid):
    ct = ContentType.objects.get_for_model(obj.__class__)
    Contribution.objects.filter(id=cid, content_type=ct, object_id=str(obj.pk)).delete()
    invalidate_citation_cache(obj)
    return JsonResponse({'ok': True})
