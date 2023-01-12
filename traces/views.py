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

""" BETA: annotate collection with place """
# def annotate(request, cid, pid):
def annotate(request, *args, **kwargs):
  print('request.POST',request.POST)
  print('traces.annotate() kwargs',kwargs)
  cid = kwargs.get('id')
  pid = request.POST.get('place')
  anno_id = request.POST.get('anno_id')
  saved = request.POST.get('saved')
  coll = get_object_or_404(Collection, id=cid)
  # for k, v in request.POST.items():
  #   print('annotate POST.item', k, v)
  context = {}

  if anno_id:
    # form with instance
    print('has anno_id')
    traceanno = TraceAnnotation.objects.get(id=anno_id)
    form = TraceAnnotationModelForm(request.POST, instance = traceanno)
    traceanno.saved = True
    traceanno.save()
    # form = TraceAnnotationModelForm(request.POST)
  else:
    print('no anno_id')
    form = TraceAnnotationModelForm(request.POST)
  # print('form.cleaned_fields', form.cleaned_fields)

  if form.is_valid():
    form.save()
  else:
    # new empty form
    messages.error(request, "Error")
    print('trace form not valid', form.errors)

  # return JsonResponse({'status': 'ok', 'msg': msg}, safe=False)
  return redirect('/collections/'+str(cid)+'/update_pl')

@csrf_exempt
def get_form(request):
    print('get_form() request.GET', request.GET)
    print('get_form() request.method', request.method)
    pid = request.GET['p']
    cid = request.GET['c']
    place = Place.objects.get(id=pid)
    coll = Collection.objects.get(id=cid)

    # is there a trace_annotation record already?
    existing = TraceAnnotation.objects.filter(place=pid, collection=cid)
    if existing:
      form = TraceAnnotationModelForm(instance=existing[0])
    else:
      form = TraceAnnotationModelForm()
    context = {
        "form": form,
        "place": place,
        "collection": coll,
        "existing": existing[0].id if existing else None
        # "existing": existing[0].id or None
    }
    template = render_to_string('../templates/traceanno_form.html', context=context)
    return JsonResponse({"form": template})

