from django.conf import settings
from django.contrib import messages
from django.forms.models import modelform_factory
from django.http import HttpResponseRedirect
from django.shortcuts import get_object_or_404, render, redirect
from django.urls import reverse
from django.views.generic import DetailView
from django.utils.safestring import SafeString
import simplejson as json

from .models import *
from collection.models import Collection
from places.models import Place
from .forms import *
from datasets.models import Dataset
#
from django.http import JsonResponse
from django.template.loader import render_to_string
from django.views.decorators.csrf import csrf_exempt
from django.views.decorators.http import require_POST


@require_POST
def annotate(request, *args, **kwargs):
    """Save a trace annotation on a collection place. Collection editors only
    (Collection.user_can_edit): 401 JSON if anonymous, 404 JSON if not permitted. CSRF is
    enforced (no longer csrf_exempt): get_form renders {% csrf_token %} into the form, and
    the builder posts FormData(#anno_form), which carries it."""
    from collection.access import ajax_gate, get_editable_collection_or_404, not_found_json
    cid = kwargs.get('id')
    coll, denied = ajax_gate(request, get_editable_collection_or_404, id=cid)
    if denied:
        return denied
    pid = request.POST.get('place')
    anno_id = request.POST.get('anno_id')
    saved = request.POST.get('saved')
    context = {}

    # The annotation must belong to THIS collection (the form body also carries
    # ``collection`` and ``place`` fields, which must not redirect the write elsewhere).
    if str(request.POST.get('collection', coll.id)) != str(coll.id):
        return not_found_json()
    try:
        place = Place.objects.select_related('dataset').get(id=pid)
    except (Place.DoesNotExist, ValueError, TypeError):
        return not_found_json('Place')
    if place.dataset is not None and not place.dataset.user_can_view(request.user):
        return not_found_json('Place')

    if anno_id:
        # form with instance
        try:
            traceanno = TraceAnnotation.objects.get(id=anno_id, collection=coll)
        except (TraceAnnotation.DoesNotExist, ValueError, TypeError):
            return not_found_json('Annotation')
        form = TraceAnnotationModelForm(request.POST, request.FILES, instance=traceanno)
        traceanno.saved = True
        traceanno.save()
        # form = TraceAnnotationModelForm(request.POST)
    else:
        form = TraceAnnotationModelForm(request.POST, request.FILES)
    # print('form.cleaned_fields', form.cleaned_fields)

    if form.is_valid():
        form.save()
        return JsonResponse({'status': 'ok', 'msg': 'Annotation saved successfully!'}, status=200)
    else:
        error_msg = form.errors.as_json()
        return JsonResponse({'status': 'error', 'errors': error_msg}, status=400)


""" BETA: annotate collection with place """


# def annotate(request, cid, pid):
# def annotate(request, *args, **kwargs):
#   cid = kwargs.get('id')
#   anno_id = request.POST.get('anno_id')
#   returnPath = '/collections/'+str(cid)+'/update_pl'
#   print('request.POST',request.POST)
#   print('request.FILES',request.FILES)
#   return
#   if request.method == 'POST':
#     form = TraceAnnotationModelForm(request.POST, request.FILES, auto_id=False)
#     if form.is_valid():
#       instance = form.save()
#     else:
#       # you probably want to show the errors in that case to the user
#       print(form.errors)
#     # redirect to a page, for example the `page1 view
#     return redirect(returnPath)
#   else:
#     form = TraceAnnotationModelForm(auto_id=False)
#   return render(request, returnPath, {'form': form})
#
#   # print('request.POST',request.POST)
#   # print('request.FILES',request.FILES)
#   # print('traces.annotate() kwargs',kwargs)
#   # return
#   # cid = kwargs.get('id')
#   # pid = request.POST.get('place')
#   # anno_id = request.POST.get('anno_id')
#   # saved = request.POST.get('saved')
#   # coll = get_object_or_404(Collection, id=cid)
#   # # for k, v in request.POST.items():
#   # #   print('annotate POST.item', k, v)
#   # context = {}
#   #
#   # if anno_id:
#   #   # form with instance
#   #   print('has anno_id')
#   #   traceanno = TraceAnnotation.objects.get(id=anno_id)
#   #   form = TraceAnnotationModelForm(request.POST, request.FILES, instance = traceanno)
#   #   # traceanno.saved = True
#   #   # traceanno.save()
#   #   # form = TraceAnnotationModelForm(request.POST)
#   # else:
#   #   # empty form
#   #   print('no anno_id')
#   #   form = TraceAnnotationModelForm(request.POST, request.FILES)
#   #   # form.save()
#   # #
#   # # # print('form.cleaned_fields', form.cleaned_fields)
#   # #
#   # if form.is_valid():
#   #   obj = form.save(commit=False)
#   #   obj.save()
#   # else:
#   #   # new empty form
#   #   messages.error(request, "Error")
#   #   print('trace form not valid', form.errors)
#   #
#   # # return JsonResponse({'status': 'ok', 'msg': msg}, safe=False)
#   # return redirect('/collections/'+str(cid)+'/update_pl')

@csrf_exempt
def get_form(request):
    # print('get_form() request.GET', request.GET)
    # print('get_form() request.method', request.method)
    pid = request.GET['p']
    cid = request.GET['c']
    # place#310: the form renders the place's title and names, so a place outside
    # the requester's circle is "not found".
    from api.dataset_access import get_visible_place_or_404
    place = get_visible_place_or_404(request.user, id=pid)
    coll = get_object_or_404(Collection, id=cid)

    # is there a trace_annotation record already?
    existing = TraceAnnotation.objects.filter(place=pid, collection=cid, archived=False)
    if existing:
        form = TraceAnnotationModelForm(instance=existing[0], auto_id=False)
    else:
        form = TraceAnnotationModelForm(auto_id=False)
    context = {
        "form": form,
        "place": place,
        "collection": coll,
        "rel_keywords": coll.rel_keywords,
        "existing": existing[0].id if existing else None
        # "existing": existing[0].id or None
    }
    # request= so {% csrf_token %} renders: annotate() is no longer csrf_exempt.
    template = render_to_string('../templates/traceanno_form.html', context=context, request=request)
    return JsonResponse({"form": template})
