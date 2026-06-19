import logging
from collections import Counter
from datetime import datetime
from itertools import groupby
import re
import requests
from urllib.parse import unquote_plus

from django.conf import settings
from django.contrib import messages
from django.contrib.auth import get_user_model
from django.contrib.gis.db.models.aggregates import Union
from django.contrib.gis.db.models.functions import Centroid, Envelope
from django.core.serializers import serialize
from django.db.models import Q
from django.http import JsonResponse, HttpResponseRedirect, Http404
from django.shortcuts import get_object_or_404, redirect
from django.urls import reverse
from django.views import View
from django.views.generic import DetailView, TemplateView
from django.db.models import Count

from elasticsearch8 import Elasticsearch
from shapely import wkt

from main.models import Comment
from collection.models import Collection
from datasets.models import Dataset
from places.models import Place, PlaceGeom
from places.utils import attribListFromSet
from elastic.es_utils import findPortalPlaces, findPortalPIDs

User = get_user_model()
logger = logging.getLogger(__name__)

def defer_review(request, pid, auth, last):
    logger.debug('defer_review() pid: %s, auth: %s, last: %s', pid, auth, last)
    p = get_object_or_404(Place, pk=pid)
    
    if auth in ['whg', 'idx']:
        p.review_whg = 2
    elif auth.startswith('wd'):
        p.review_wd = 2
    else:
        p.review_tgn = 2
    
    p.save()
    referer = request.META.get('HTTP_REFERER')
    base = re.search('^(.*?)review', referer).group(1)
    
    nextpage = int(referer[-1]) + 1 if '?page' in referer else None
    return_url = (referer[:-1] + str(nextpage) if nextpage and nextpage < int(last)
                  else base + 'reconcile')
    
    if int(last) == 1 or not '?page' in referer:
        return_url = base + 'reconcile'
    
    return HttpResponseRedirect(return_url)


class SetCurrentResultView(View):
    def post(self, request, *args, **kwargs):
        place_ids = request.POST.getlist('place_ids')
        logger.info('Setting place_ids in session: %s', place_ids)
        request.session['current_result'] = {'place_ids': place_ids}
        return JsonResponse({'status': 'ok'})


class PlaceDetailView(DetailView):
    model = Place
    template_name = 'places/place_detail.html'

    def get_object(self):
        return get_object_or_404(Place.objects.select_related('dataset'), pk=self.kwargs.get('pk'))

    def get_success_url(self):
        pid = self.kwargs.get("id")
        return f'/places/{pid}/detail'

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        place = self.object
        dataset = place.dataset

        context.update({
            'timespans': {'ts': place.timespans or None},
            'minmax': {'mm': place.minmax or None},
            'dataset': dataset,
            'dataset_minmax': dataset.minmax if dataset.minmax else None,
            'dataset_creator': dataset.creator,
            'dataset_last_modified_text': dataset.last_modified_text,
            'beta_or_better': self.request.user.groups.filter(name__in=['beta', 'whg_admins']).exists()
        })

        return context


class PlacePortalView(TemplateView):
    template_name = 'places/place_portal.html'

    def get(self, request, *args, **kwargs):
        # A place with a single match has no portal to show — redirect server-side
        # (302) to its detail page. Matches are stashed so get_context_data doesn't
        # recompute the (N+1) .matches query on the multi-match path.
        pid = kwargs.get('pid')
        if pid is not None:
            self._portal_matches = Place.objects.get(id=pid).matches
            if len(self._portal_matches) == 1:
                return redirect(f'/places/{pid}/detail')
        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        context = super().get_context_data(**kwargs)
        me = self.request.user

        filter_condition = Q(collection_class='place')
        if me.is_authenticated:
            filter_condition &= (Q(owner=me) | Q(collabs__user=me))
        context['my_collections'] = Collection.objects.filter(filter_condition)

        pid = kwargs.get('pid')
        whg_id = kwargs.get('whg_id')
        encoded_ids = kwargs.get('encoded_ids')
        place_ids = []

        if pid:
            portal_data = getattr(self, '_portal_matches', None)
            if portal_data is None:
                portal_data = Place.objects.get(id=pid).matches
            place_ids = [place.id for place in portal_data]
        elif whg_id:
            place_ids = findPortalPlaces(whg_id)
        elif encoded_ids:
            place_ids = list(map(int, encoded_ids.split(',')))
        else:
            place_ids = self.request.session.get('current_result', {}).get('place_ids', [])

        place_ids = list(map(int, place_ids))
        if not place_ids:
            messages.error(self.request, "Place IDs are required to view this page")
            raise Http404("Place IDs are required")

        context.update(self._get_portal_data(place_ids, me))
        return context

    def _get_portal_data(self, place_ids, user):
        context = {'payload': [], 'traces': [], 'allts': []}
        alltitles, allvariants = set(), []

        try:
            qs = Place.objects.filter(id__in=place_ids)
            if not qs.exists():
                raise Http404("No such place found.")
            qs = sorted(qs, key=lambda place: (place.links.count(), place.id), reverse=True)

            collections, annotations, all_geoms = [], [], []
            for place in qs:
                ds = place.dataset
                alltitles.add(place.title)

                names = attribListFromSet('names', place.names.all(), exclude_title=place.title)
                types = attribListFromSet('types', place.types.all())
                traces = list(place.traces.filter(archived=False))
                attest_collections = [
                    t.collection for t in traces
                    if t.collection.status == "published" or t.collection.owner == user
                ]

                annotations += traces
                collections = list(set(collections + attest_collections))
                geoms = [geom.jsonb for geom in place.geoms.all()]
                context['allts'] += list(t for t, _ in groupby(place.timespans)) if place.timespans else []

                record = self._build_record(place, ds, names, types, geoms, attest_collections)
                allvariants.extend([name.get('label', '') for name in names if name.get('label', '') != place.title])
                context['payload'].append(record)
                all_geoms.extend(geoms)

        except ValueError:
            messages.error(self.request, "Invalid place ID format")
            raise Http404("Invalid place ID format")

        if all_geoms:
            unioned_geometry = PlaceGeom.objects.filter(place_id__in=place_ids).aggregate(union=Union('geom'))['union']
            context['extent'] = list(wkt.loads(unioned_geometry.envelope.wkt).bounds)
            context['centroid'] = list(unioned_geometry.centroid.tuple)
        else:
            context['extent'] = [-180, -90, 180, 90]
            context['centroid'] = [0.0, 0.0]

        context.update({
            'portal_headword': "; ".join([name for name, _ in Counter(list(alltitles) + allvariants).most_common()]),
            'annotations': annotations,
            'collections': collections,
        })

        return context

    def _build_record(self, place, ds, names, types, geoms, attest_collections):
        return {
            "dataset": {
                "id": ds.id, "label": ds.label, "title": ds.title,
                "webpage": ds.webpage, "description": ds.description,
                "owner": ds.owner.name, "creator": ds.creator, "show_link": (ds.numrows or 0) <= settings.DATASETS_PLACES_LIMIT
            },
            "place_id": place.id,
            "src_id": place.src_id,
            "purl": ds.uri_base + str(place.id) if 'whgaz' in ds.uri_base else ds.uri_base + place.src_id,
            "title": place.title,
            "ccodes": place.ccodes,
            "names": names,
            "types": types,
            "geom": geoms,
            "related": [rel.jsonb for rel in place.related.all()],
            "links": [link.jsonb for link in place.links.distinct('jsonb') if not link.jsonb['identifier'].startswith('whg')],
            "descriptions": [descr.jsonb for descr in place.descriptions.all()],
            "depictions": [depict.jsonb for depict in place.depictions.all()],
            "minmax": place.minmax,
            "timespans": list(t for t, _ in groupby(place.timespans)) if place.timespans else [],
            "collections": [{
                "class": col.collection_class, "id": col.id,
                "url": reverse('collection:place-collection-browse', args=[col.id]),
                "title": col.title, "description": col.description,
                "count": col.places_all.aggregate(place_count=Count('id'))['place_count']
            } for col in attest_collections],
            "notes": [{
                'id': comment.id, 'user': comment.user.id,
                'place_id': comment.place_id.id, 'tag': comment.tag,
                'note': comment.note, 'created': comment.created.isoformat()
            } for comment in Comment.objects.filter(place_id_id=place.id)]
        }


class PlaceFullView(PlacePortalView):
    def render_to_response(self, context, **response_kwargs):
        return JsonResponse(context, **response_kwargs)
