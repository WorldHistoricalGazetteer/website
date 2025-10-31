# collection.views (collections)
import os
import requests

from dateutil.parser import isoparse, ParserError
from datetime import date
import json
import random

from django import forms
from django.contrib import messages
from django.contrib.auth.mixins import LoginRequiredMixin
from django.core.cache import cache
from django.db.models import F, Min, Max
from django.db.models.functions import Coalesce
from django.forms.models import inlineformset_factory
from django.http import JsonResponse, HttpResponseRedirect, Http404, HttpResponseServerError
from django.conf import settings
from django.shortcuts import get_object_or_404, redirect
from django.views.decorators.http import require_POST
from django.views.generic import (View, CreateView, UpdateView, DetailView, DeleteView, ListView)

from django.contrib.gis.geos import Point, GEOSGeometry, GeometryCollection
from django.contrib.gis.db.models import Extent
from django.contrib.gis.db.models.aggregates import Union
from django.contrib.gis.db.models.functions import Centroid

from elastic.es_utils import findPortalPIDs

from .forms import CollectionModelForm, CollectionGroupModelForm
from .models import *
from places.models import PlaceGeom, Place, CloseMatch
from main.models import Log, Link
from traces.forms import TraceAnnotationModelForm
from traces.models import TraceAnnotation
from whgmail.messaging import WHGmail
from array import array
from itertools import chain
import logging

logger = logging.getLogger(__name__)
""" collection group joiner"; prevent duplicate """


def collection_citation(request, id):
    try:
        collection = Collection.objects.get(id=id)
        citation_data = json.loads(collection.citation_csl)
        return JsonResponse(citation_data, safe=False)
    except Collection.DoesNotExist:
        return JsonResponse({'error': 'Collection not found'}, status=404)


def join_group(request, *args, **kwargs):
    entered_code = request.POST.get('join_code', None)
    if entered_code is None:
        return JsonResponse({'msg': 'No code provided'}, safe=False)

    try:
        cg = CollectionGroup.objects.get(join_code=entered_code)
    except CollectionGroup.DoesNotExist:
        return JsonResponse({'msg': 'Unknown code'}, safe=False)

    user = request.user
    # Check if the user is already a member of the group
    existing_membership = CollectionGroupUser.objects.filter(user=user, collectiongroup=cg).exists()
    if existing_membership:
        return JsonResponse({
            'status': 'already_member',
            'msg': 'You are already a member of group "<b>' + cg.title + '</b>"!',
            'cg_title': cg.title,
            'cg_id': cg.id,
        }, safe=False)

    cgu = CollectionGroupUser.objects.create(
        user=user, collectiongroup=cg, role='member')
    return JsonResponse({
        'status': 'success',
        'msg': 'Joined group ' + cg.title + '!',
        'cg_title': cg.title,
        'cg_id': cg.id,
    }, safe=False)


# def join_group(request, *args, **kwargs):
#   print('join_group() kwargs', kwargs)
#   print('join_group() request.POST', request.POST)
#
#   entered_code = request.POST.get('join_code', None)
#   if entered_code is None:
#     return JsonResponse({'msg': 'No code provided'}, safe=False)
#
#   try:
#     cg = CollectionGroup.objects.get(join_code=entered_code)
#   except CollectionGroup.DoesNotExist:
#     return JsonResponse({'msg': 'Unknown code'}, safe=False)
#
#   user = request.user
#   cgu = CollectionGroupUser.objects.create(
#     user=user, collectiongroup=cg, role='member')
#   return JsonResponse({
#     'msg': 'Joined group '+cg.title+'!',
#     'cg_title': cg.title,
#     'cg_id': cg.id,
#   }, safe=False)


"""collection group join code generator"""
adjectives = ['Swift', 'Wise', 'Clever', 'Eager', 'Gentle', 'Smiling', 'Lucky', 'Brave', 'Happy']
nouns = ['Wolf', 'Deer', 'Swan', 'Cat', 'Owl', 'Bear', 'Rabbit', 'Lion', 'Horse', 'Dog', 'Duck', 'Hawk', 'Eagle', 'Fox',
         'Tiger', 'Goose']


def generate_unique_join_code(request):
    while True:
        adjective = random.choice(adjectives)
        noun = random.choice(nouns)
        join_code = adjective + noun
        if not CollectionGroup.objects.filter(join_code=join_code).exists():
            return JsonResponse({'join_code': join_code})


""" collection group join code setter """


def set_joincode(request, *args, **kwargs):
    cg = CollectionGroup.objects.get(id=kwargs['cgid'])
    cg.join_code = kwargs['join_code']
    cg.save()
    return JsonResponse({'join_code': cg.join_code})


""" sets collection to inactive, removing from lists """


def inactive(request, *args, **kwargs):
    coll = Collection.objects.get(id=request.POST['id'])
    coll.active = False
    coll.save()
    result = {"msg": "collection " + coll.title + '(' + str(coll.id) + ') flagged inactive'}
    return JsonResponse(result, safe=False)


""" removes dataset from collection, refreshes page"""


def remove_link(request, *args, **kwargs):
    # print('kwargs', kwargs)
    link = Link.objects.get(id=kwargs['id'])
    # link = CollectionLink.objects.get(id=kwargs['id'])
    link.delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


"""
  set collection status by group leader: reviewed, nominated
"""


def status_update(request, *args, **kwargs):
    status = request.POST['status']
    coll = Collection.objects.get(id=request.POST['coll'])

    coll.status = status
    coll.save()

    return JsonResponse({'status': status, 'coll': coll.title}, safe=False,
                        json_dumps_params={'ensure_ascii': False, 'indent': 2})


def nominator(request, *args, **kwargs):
    nominated = True if request.POST['nominated'] == 'true' else False
    coll = Collection.objects.get(id=request.POST['coll'])
    if nominated:
        coll.nominated = True
        coll.status = 'nominated'
        coll.save()

        # Notify Nominator
        WHGmail(request, {
            'template': 'nomination_recd',
            'subject': 'WHG Student Place Collection nomination',
            'nominated_title': coll.title,
            'nominated_id': coll.id,
            'nominated_url': f'{settings.URL_FRONT}collections/{str(coll.id)}',
        })

        # Notify WHG Editorial Staff (both Email and Slack)
        WHGmail(request, {
            'to_email': f'{settings.DEFAULT_FROM_EDITORIAL}',
            'template': 'nomination_recd',
            'subject': 'WHG Student Place Collection nomination',
            'nominated_title': coll.title,
            'nominated_id': coll.id,
            'leader_name': request.user.name,
            'leader_email': request.user.email,
            'owner_name': coll.owner,
            'nominated_url': f'{settings.URL_FRONT}collections/{str(coll.id)}',
            'slack_notify': True,
        })

        # else:
    #   coll.nominated = False
    #   coll.status = 'reviewed'
    # coll.save()

    return JsonResponse({'status': coll.status, 'coll': coll.title}, safe=False,
                        json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  user adds (submits) or removes collection to/from collection group
"""


def group_connect(request, *args, **kwargs):
    action = request.POST['action']
    coll = Collection.objects.get(id=request.POST['coll'])
    cg = CollectionGroup.objects.get(id=request.POST['group'])
    if action == 'submit':
        cg.collections.add(coll)
        coll.status = 'group'
        coll.save()
        status = 'added to'
    else:
        cg.collections.remove(coll)
        coll.group = None
        coll.submit_date = None
        coll.status = 'sandbox'
        coll.save()
        status = 'removed from'

    return JsonResponse({'status': status, 'coll': coll.title, 'group': cg.title}, safe=False,
                        json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  add collaborator to collection in role
"""


def collab_add(request, cid):
    username = request.POST['username']
    response_data = {}
    try:
        user = get_object_or_404(User, username=username)
        uid = user.id
        role = request.POST['role']
    except Http404:
        response_data['status'] = 'User "' + username + '" not found'
        return JsonResponse(response_data)

    is_already_collaborator = CollectionUser.objects.filter(user_id=uid, collection_id=cid).exists()
    if is_already_collaborator:
        response_data['status'] = 'User is already a collaborator'
        return JsonResponse(response_data)

    response_data['status'] = 'ok'

    # TODO: send collaborator an email
    coll_collab = CollectionUser.objects.create(user_id=uid, collection_id=cid, role=role)
    response_data['user'] = str(coll_collab)  # name (role, email)
    response_data['uid'] = uid
    response_data['cid'] = cid

    return JsonResponse(response_data)


"""
  collab_remove(uid, cid)
  remove collaborator from collection
"""


def collab_remove(request, uid, cid):
    get_object_or_404(CollectionUser, user=uid, collection=cid).delete()
    response_data = {"status": "ok", "uid": uid}
    return JsonResponse(response_data)


def seq(coll):
    """ Utility: get the next sequence for a collection """
    cps = CollPlace.objects.filter(collection=coll).values_list("sequence", flat=True)

    # Filter out None values and convert to a list
    cps = [s for s in cps if s is not None]

    # Determine the next sequence number
    if cps:  # Check if cps is not empty
        next_sequence = max(cps) + 1
    else:
        next_sequence = 0  # Initialize to 0 if no existing sequences

    logger.debug(f"Next sequence: {next_sequence}")
    return next_sequence


"""
  add list of >=1 places to collection
  i.e. new CollPlace and TraceAnnotation rows
  ajax call from ds_places.html and place_portal.html
"""


# TODO: essentially same as add_dataset(); needs refactor
def add_places(request, *args, **kwargs):
    if request.method == 'POST':
        user = request.user
        status, msg = ['', '']
        dupes = []
        added = []
        # print('add_places request', request.POST)
        coll = Collection.objects.get(id=request.POST['collection'])
        place_list = [int(i) for i in request.POST['place_list'].split(',')]
        for p in place_list:
            place = Place.objects.get(id=p)
            gotplace = TraceAnnotation.objects.filter(collection=coll, place=place, archived=False)
            if not gotplace:
                t = TraceAnnotation.objects.create(
                    place=place,
                    src_id=place.src_id,
                    collection=coll,
                    motivation='locating',
                    owner=user,
                    anno_type='place',
                    saved=0
                )
                # coll.places.add(p)
                CollPlace.objects.create(
                    collection=coll,
                    place=place,
                    sequence=seq(coll)
                )
                added.append(p)
            else:
                dupes.append(place.title)
            msg = {"added": added, "dupes": dupes}
        return JsonResponse({'status': status, 'msg': msg}, safe=False)


"""
    create Collection if it doesn't yet exist
    add list of >=1 places to Collection
    ajax call from place_portal.html
"""


@require_POST
def add_collection_places(request):
    payload = request.POST

    # Initialize response data
    response_data = {'status': 'success', 'msg': '', 'added_places': [], 'existing_places': [],
                     'payload_received': payload}

    # Perform data processing and database operations
    try:
        collection_id = payload.get('collection')
        place_id = payload.get('primarySource')
        if not collection_id or not place_id:
            raise ValueError("Collection ID and Place ID must be provided.")

        collection_id = int(collection_id)
        place_id = int(place_id)

        title = payload.get('title')
        include_all = payload.get('includeAll')

        if collection_id == -1:
            # Create a new collection
            collobj = Collection.objects.create(
                owner=request.user,
                title=title,
                collection_class='place',
                description='new collection',
            )
            collobj.save()
            collection_id = collobj.id
            response_data['msg'] = 'New collection created successfully.'
            response_data['new_collection_id'] = collection_id

        # Add places to the collection
        collection = Collection.objects.get(id=collection_id)
        place = Place.objects.get(id=place_id)

        logger.debug(f"Adding place {place_id} to collection {collection_id}.")
        logger.debug(f"Place: {place}, Collection: {collection}")

        # Check if the place already exists in the collection
        existing_place = TraceAnnotation.objects.filter(collection=collection, place=place, archived=False)
        logger.debug(f"Existing place: {existing_place}")
        if not existing_place:
            trace_annotation = TraceAnnotation.objects.create(
                place=place,
                include_matches=True if include_all == "on" else False,
                src_id=place.src_id,
                collection=collection,
                motivation='locating',
                owner=request.user,
                anno_type='place',
                saved=0
            )
            trace_annotation.save()

            sequence = seq(collection)
            logger.debug(f"Sequence: {sequence}")
            if sequence is None:
                raise ValueError("Sequence cannot be None.")

            CollPlace.objects.create(
                collection=collection,
                place=place,
                sequence=sequence
            )

            response_data['added_places'].append(place_id)
        else:
            response_data['existing_places'].append(place_id)

        collection = Collection.objects.get(id=collection_id)  # Refresh
        response_data['collection'] = {
            'class': collection.collection_class,
            'id': collection.id,
            'url': f"/collections/{collection.id}/browse_pl",
            'title': collection.title,
            'description': collection.description,
            'count': collection.places.count()
        }

    except Exception as e:
        response_data['status'] = 'error'
        response_data['msg'] = str(e)

    return JsonResponse(response_data)


"""
  deletes CollPlace record(s) and
  archives TraceAnnotation(s) for list of pids
"""


def archive_traces(request, *args, **kwargs):
    if request.method == 'POST':
        coll = Collection.objects.get(id=request.POST['collection'])
        place_list = [int(i) for i in request.POST['place_list'].split(',')]
        # remove CollPlace, archive TraceAnnotation
        for pid in place_list:
            place = Place.objects.get(id=pid)
            if place in coll.places.all():
                # print('collection place', place)
                coll.places.remove(place)
            if place.traces:
                # can be only one but .update only works on filter
                TraceAnnotation.objects.filter(collection=coll, place=place).update(archived=True)
        # reset sequence after removals (fill in the gaps)
        coll_places = CollPlace.objects.filter(collection=coll).order_by('sequence')
        for new_sequence, coll_place in enumerate(coll_places):
            coll_place.sequence = new_sequence
            coll_place.save()
        return JsonResponse({'result': str(len(place_list)) + ' places removed, we think'}, safe=False)


""" update sequence of annotated places """


def update_sequence(request, *args, **kwargs):
    new_sequence = json.loads(request.POST['seq'])
    # print('new_sequence', new_sequence)
    cid = request.POST['coll_id']
    for cp in CollPlace.objects.filter(collection=cid):
        cp.sequence = new_sequence[str(cp.place_id)]
        cp.save()
    return JsonResponse({"msg": "updated?", "POST": new_sequence})


"""
create place collection on the fly; return id for adding place(s) to it
"""


def flash_collection_create(request, *args, **kwargs):
    if request.method == 'POST':
        collobj = Collection.objects.create(
            owner=request.user,
            title=request.POST['title'],
            collection_class='place',
            description='new collection',
            # keywords = '{replace, these, please}'
        )
        collobj.save()
        result = {"id": collobj.id, 'title': collobj.title}
    return JsonResponse(result, safe=False)


def stringer(str):
    if str:
        return isoparse(str).strftime('%Y' if len(str) <= 5 else '%b %Y' if len(str) <= 8 else '%d %b %Y')
    else:
        return None


def when_format(ts):
    return [stringer(ts[0]), stringer(ts[1])]


def year_from_string(ts):
    if ts:
        try:
            return int(isoparse(ts).strftime('%Y'))
        except (ValueError, ParserError) as e:
            if 'day is out of range for month' in str(e):
                # Attempt to handle the invalid day part, e.g., '00' day
                parts = ts.split('-')
                if len(parts) == 3 and parts[2] == '00':
                    # Replace '00' with '01' for parsing
                    ts = f"{parts[0]}-{parts[1]}-01"
                    try:
                        return int(isoparse(ts).strftime('%Y'))
                    except (ValueError, ParserError):
                        return int(parts[0])  # Fallback to just the year part
            return int(ts)
    else:
        return "null"  # String required by Maplibre filter test


""" gl map needs this """


# TODO:
def fetch_geojson_coll(request, *args, **kwargs):
    # print('fetch_geojson_coll kwargs',kwargs)
    id_ = kwargs['id']
    coll = get_object_or_404(Collection, id=id_)
    rel_keywords = coll.rel_keywords

    features_t = [
        {
            "type": "Feature",
            "geometry": t.place.geoms.all()[0].jsonb,
            "properties": {
                "pid": t.place.id,
                "title": t.place.title,
                "relation": t.relation[0],
                "when": when_format([t.start, t.end]),
                "note": t.note
            }
        }
        for t in coll.traces.filter(archived=False)
    ]

    feature_collection = {
        "type": "FeatureCollection",
        "features": features_t,
        "relations": coll.rel_keywords,
    }

    return JsonResponse(feature_collection, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


""" returns json for display """


class ListDatasetView(View):
    @staticmethod
    def get(request):
        # coll = Collection.objects.get(id=request.GET['coll_id'])
        ds = Dataset.objects.get(id=request.GET['ds_id'])
        # coll.datasets.add(ds)
        result = {
            "id": ds.id,
            "label": ds.label,
            "title": ds.title,
            "create_date": ds.create_date,
            "description": ds.description[:100] + '...',
            "numrows": ds.places.count()
        }
        return JsonResponse(result, safe=False)


"""
  adds all places in a dataset as CollPlace records
  to a place collection
  i.e. new CollPlace and TraceAnnotation rows
  url call from place_collection_build.html
  adds dataset to db:collections_datasets
"""


# TODO: essentially same as add_places(); needs refactor
def add_dataset_places(request, *args, **kwargs):
    coll = Collection.objects.get(id=kwargs['coll_id'])
    ds = Dataset.objects.get(id=kwargs['ds_id'])
    user = request.user
    status, msg = ['', '']
    dupes = []
    added = []
    coll.datasets.add(ds)
    for place in ds.places.all():
        # has non-archived trace annotation?
        gottrace = TraceAnnotation.objects.filter(collection=coll, place=place, archived=False)
        if not gottrace:
            t = TraceAnnotation.objects.create(
                place=place,
                src_id=place.src_id,
                collection=coll,
                motivation='locating',
                owner=user,
                anno_type='place',
                saved=0
            )
            # coll.places.add(p)
            CollPlace.objects.create(
                collection=coll,
                place=place,
                sequence=seq(coll)
            )
            added.append(place.id)
        else:
            dupes.append(place.title)
        msg = {"added": added, "dupes": dupes}
    # return JsonResponse({'status': status, 'msg': msg}, safe=False)
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


"""
  add_dateset() and 4 helpers below
  computes a graph of connected components, writes AggPlace summary to db
  for consumption by mapAndTable.js functions and rendering in ds_collection_browse.html
"""


def add_dataset(request, *args, **kwargs):
    coll = Collection.objects.get(id=kwargs['coll_id'])
    ds = Dataset.objects.get(id=kwargs['ds_id'])

    if not coll.datasets.filter(id=ds.id).exists():
        coll.datasets.add(ds)
        sequence = coll.datasets.count()
        dataset_details = {
            "id": ds.id,
            "label": ds.label,
            "title": ds.title,
            "create_date": ds.create_date,
            "description": ds.description[:100] + '...',
            "numrows": ds.places.count(),
            "sequence": sequence
        }

        update_collection_components(coll)

        return JsonResponse({'status': 'success',
                             'dataset': dataset_details})

    return JsonResponse({'status': 'already_added'})


def update_collection_components(coll):
    import networkx as nx
    from django.contrib.gis.geos import GeometryCollection
    from places.models import Place, CloseMatch
    place_ids = list(coll.places_all.values_list('id', flat=True))

    edges = CloseMatch.objects.filter(
        Q(place_a__in=place_ids) |
        Q(place_b__in=place_ids)
    ).values_list('place_a_id', 'place_b_id')

    G = nx.Graph()
    G.add_nodes_from(place_ids)
    G.add_edges_from(edges)

    # find connected components and inspect those w/>1 place
    components = list(nx.connected_components(G))
    multi_place_components = [component for component in components if len(component) > 1]

    # find connected components process each for additional data
    for i, component in enumerate(nx.connected_components(G)):
        if i >= 100:
            break
        # Compute headword, GeometryCollection, and aggregated place types
        headword = compute_headword_for_component(component)
        # print(type(component))
        geometry = GeometryCollection(
            [geom.geom for place in Place.objects.filter(id__in=component) for geom in place.geoms.all()])
        # print(geometry)
        place_types = aggregate_place_types(component)
        # Store or update component information in the database
        save_component_data(coll, component, headword, geometry, place_types)


def compute_headword_for_component(component):
    # Logic to compute headword
    return "Example Headword"


def aggregate_place_types(component):
    # Logic to aggregate place types
    return ["Type1", "Type2"]


def save_component_data(coll, component, headword, geometry, place_types):
    # Placeholder function to store component data
    pass


"""
  removes dataset from collection
  clean up "omitted"; refreshes page
"""


def remove_dataset(request, *args, **kwargs):
    coll = Collection.objects.get(id=kwargs['coll_id'])
    ds = Dataset.objects.get(id=kwargs['ds_id'])

    # remove CollPlace records
    CollPlace.objects.filter(place_id__in=ds.placeids).delete()
    # remove dataset from collections_dataset
    coll.datasets.remove(ds)
    # archive any non-blank trace annotations
    # someone will want to recover them, count on it
    current_traces = coll.traces.filter(collection=coll, place__in=ds.placeids)
    non_blank = [t.id for t in current_traces.all() if t.blank == False]
    blanks = current_traces.exclude(id__in=non_blank)
    if non_blank:
        current_traces.filter(id__in=non_blank).update(archived=True)
        current_traces.filter(archived=False).delete()
    blanks.delete()
    return HttpResponseRedirect(request.META.get('HTTP_REFERER'))


def create_collection_group(request, *args, **kwargs):
    # must be member of group_leaders
    result = {"status": "", "id": "", 'title': ""}
    if request.method == 'POST':
        owner = get_user_model().objects.get(id=request.POST['ownerid'])
        group_title = request.POST['title']
        description = request.POST['description']
        if group_title in CollectionGroup.objects.all().values_list('title', flat=True):
            result['status'] = "dupe"
        else:
            newgroup = CollectionGroup.objects.create(
                owner=owner,
                title=group_title,
                description=description,
            )
            # newgroup.user_set.add(request.user)
            result = {"status": "ok", "id": newgroup.id, 'title': newgroup.title}

    return JsonResponse(result, safe=False)


@require_POST
def update_vis_parameters(request, *args, **kwargs):
    try:
        coll_id = request.POST.get('coll_id')
        checked = bool(request.POST.get('checked') == 'true')

        if checked:
            vis_parameters = {
                'seq': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'min': {'tabulate': 'initial', 'temporal_control': 'filter', 'trail': False},
                'max': {'tabulate': True, 'temporal_control': 'filter', 'trail': False}
            }
        else:
            vis_parameters = {
                'seq': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'min': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'max': {'tabulate': False, 'temporal_control': 'none', 'trail': False}
            }

        # Update the vis_parameters field of the collection
        collection = get_object_or_404(Collection, id=coll_id)
        collection.vis_parameters = vis_parameters
        collection.save()

        return JsonResponse(
            {'message': 'Visualisation parameters updated successfully', 'vis_parameters': json.dumps(vis_parameters)})
    except Exception as e:
        return JsonResponse({'error': str(e)}, status=500)


CollectionLinkFormset = inlineformset_factory(
    Collection, CollectionLink, fields=('uri', 'label', 'link_type'), extra=2,
    widgets={
        'link_type': forms.Select(choices=('webpage'))}
)

"""
  PLACE COLLECTIONS
  collections from places and/or datasets; uses place_collection_build.html
"""


# TODO: refactor to fewer views
class PlaceCollectionCreateView(LoginRequiredMixin, CreateView):
    form_class = CollectionModelForm
    template_name = 'collection/place_collection_build.html'
    queryset = Collection.objects.all()

    def get_form_kwargs(self, **kwargs):
        kwargs = super(PlaceCollectionCreateView, self).get_form_kwargs()
        return kwargs

    def get_context_data(self, *args, **kwargs):
        user = self.request.user
        context = super(PlaceCollectionCreateView, self).get_context_data(**kwargs)

        datasets = []
        # add 1 or more links, images (?)
        if self.request.POST:
            context["links_form"] = CollectionLinkFormset(self.request.POST)
            # context["images_form"] = CollectionImageFormset(self.request.POST)
        else:
            context["links_form"] = CollectionLinkFormset()
            # context["images_form"] = CollectionImageFormset()

        # owners can include place from their datasets
        ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]
        if not user.is_superuser:
            ds_select.insert(len(ds_select) - 1, Dataset.objects.get(label='owt10'))

        context['action'] = 'create'
        context['ds_select'] = ds_select
        context['coll_dsset'] = datasets

        return context

    def form_valid(self, form):
        context = self.get_context_data()
        self.object = form.save()

        # TODO: write log entry
        # Log.objects.create(
        #   # category, logtype, "timestamp", subtype, dataset_id, user_id
        #   category='collection',
        #   logtype='coll_create',
        #   subtype='place',
        #   coll_id=self.object.id,
        #   user_id=self.request.user.id
        # )

        return super().form_valid(form)

    def form_invalid(self, form):
        context = self.get_context_data()
        context['errors'] = form.errors
        context = {'form': form}
        return self.render_to_response(context=context)

    def get_success_url(self):
        Log.objects.create(
            # category, logtype, "timestamp", subtype, note, dataset_id, user_id
            category='collection',
            logtype='create',
            note='created collection id: ' + str(self.object.id),
            user_id=self.request.user.id
        )
        # return to update page after create
        return reverse('collection:place-collection-update', kwargs={'id': self.object.id})


""" update place collection; uses place_collection_build.html """


class PlaceCollectionUpdateView(LoginRequiredMixin, UpdateView):
    # print('PlaceCollectionUpdateView()')
    form_class = CollectionModelForm
    template_name = 'collection/place_collection_build.html'
    queryset = Collection.objects.all()

    def get_form_kwargs(self, **kwargs):
        kwargs = super(PlaceCollectionUpdateView, self).get_form_kwargs()
        kwargs.update({'user': self.request.user})
        return kwargs

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Collection, id=id_)

    def form_invalid(self, form):
        context = {'form': form}
        return self.render_to_response(context=context)

    def form_valid(self, form):
        context = self.get_context_data()
        if not context['is_owner'] and not context['is_member'] and not context['whgteam']:
            # messages.error(self.request, 'You do not have permission to save the form.')
            return redirect('/collections/' + str(self.object.id) + '/update_pl')
        data = form.cleaned_data
        id_ = self.kwargs.get("id")

        try:
            obj = form.save(commit=False)
            if obj.group:
                obj.status = 'group'
                obj.submit_date = date.today()
            else:
                obj.status = 'sandbox'
                obj.nominated = False
                obj.submit_date = None
            obj.save()
        except Exception as e:
            return HttpResponseServerError(e)

        Log.objects.create(
            # category, logtype, "timestamp", subtype, note, dataset_id, user_id
            category='collection',
            logtype='update',
            note='collection id: ' + str(obj.id) + ' by ' + self.request.user.name,
            user_id=self.request.user.id
        )
        # return to page, or to browse
        if 'update' in self.request.POST:
            return redirect('/collections/' + str(id_) + '/update_pl')
        else:
            return redirect('/collections/' + str(id_) + '/browse_pl')

    def get_context_data(self, *args, **kwargs):
        context = super(PlaceCollectionUpdateView, self).get_context_data(*args, **kwargs)
        user = self.request.user
        _id = self.kwargs.get("id")
        coll = self.object
        datasets = self.object.datasets.all()
        in_class = coll.group.type == 'class' if coll.group else False
        form_anno = TraceAnnotationModelForm(self.request.GET or None, auto_id="anno_%s")
        # populates dropdown
        ds_select = [obj for obj in Dataset.objects.all().order_by('title') if user in obj.owners or user.is_superuser]
        if not user.is_superuser:
            ds_select.insert(len(ds_select) - 1, Dataset.objects.get(label='owt10_foo'))

        context['action'] = 'update'
        context['ds_select'] = ds_select
        context['coll_dsset'] = datasets
        context['links'] = Link.objects.filter(collection=coll.id)

        context['owner'] = True if user == coll.owner else False
        context['is_owner'] = True if user in self.object.owners else False
        context['is_member'] = True if user in coll.owners or user in coll.collaborators else False
        context['whgteam'] = True if user.groups.filter(name__in=['whg_team', 'editorial']).exists() else False
        context['whg_admins'] = True if user.groups.filter(name__in=['whg_admins', 'editorial']).exists() else False
        context['collabs'] = CollectionUser.objects.filter(collection=coll.id)
        context['mygroups'] = CollectionGroupUser.objects.filter(user_id=user)
        context['in_class'] = in_class
        # context['links'] = CollectionLink.objects.filter(collection=self.object.id)

        context['form_anno'] = form_anno
        # context['seq_places'] = [
        #     {'id': cp.id, 'p': cp.place, 'seq': cp.sequence}
        #     for cp in CollPlace.objects.filter(collection=_id).order_by('sequence')
        # ]
        context['seq_places'] = [
            {
                'id': cp.id,
                'p': {
                    'id': cp.place.id,
                    'title': cp.place.title
                },
                'seq': cp.sequence
            }
            for cp in CollPlace.objects.filter(collection=_id).order_by('sequence')
        ]
        context['created'] = self.object.create_date.strftime("%Y-%m-%d")
        # context['whgteam'] = User.objects.filter(groups__name='whg_team')

        return context


""" browse collection *all* places """


class PlaceCollectionBrowseView(DetailView):
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    model = Collection
    template_name = 'collection/place_collection_browse.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        return '/collections/' + str(id_) + '/places'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Collection, id=id_)

    def dispatch(self, request, *args, **kwargs):
        self.object = self.get_object()
        if self.object.collection_class != "place":
            raise Http404("Collection does not match expected class 'place'")
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        id_ = self.kwargs.get("id")
        coll = get_object_or_404(Collection, id=id_)

        context = super(PlaceCollectionBrowseView, self).get_context_data(*args, **kwargs)
        context['media_url'] = settings.MEDIA_URL

        context['is_admin'] = True if self.request.user.groups.filter(name__in=['whg_admins']).exists() else False
        context['ds_list'] = coll.ds_list
        context['num_places'] = coll.num_places
        context['ds_counter'] = coll.ds_counter
        context['collabs'] = coll.collaborators.all()
        context['images'] = [ta.image_file.name for ta in coll.traces.all()]
        context['links'] = coll.related_links.all()
        # context['places'] = coll.places.all().order_by('title')
        context['updates'] = {}
        context['url_front'] = settings.URL_FRONT

        # if not coll.vis_parameters:
        # Populate with default values:
        # tabulate: 'initial'|true|false - include sortable table column, 'initial' indicating the initial sort column
        # temporal_control: 'player'|'filter'|null - control to be displayed when sorting on this column
        # trail: true|false - whether to include ant-trail motion indicators on map
        # coll.vis_parameters = "{'seq': {'tabulate': false, 'temporal_control': 'player', 'trail': true},'min': {'tabulate': 'initial', 'temporal_control': 'player', 'trail': true},'max': {'tabulate': true, 'temporal_control': 'filter', 'trail': false}}"
        context['visParameters'] = coll.vis_parameters

        return context


"""
COLLECTION GROUPS
"""


class CollectionGroupCreateView(CreateView):
    form_class = CollectionGroupModelForm
    template_name = 'collection/collection_group_create.html'
    queryset = CollectionGroup.objects.all()

    #
    def get_form_kwargs(self, **kwargs):
        kwargs = super(CollectionGroupCreateView, self).get_form_kwargs()
        return kwargs

    def get_success_url(self):
        cgid = self.kwargs.get("id")
        action = self.kwargs.get("action")
        # def get_success_url(self):
        #         return reverse('doc_aide:prescription_detail', kwargs={'pk': self.object.pk})
        return reverse('collection:collection-group-update', kwargs={'id': self.object.id})
        # return redirect('collections/groups/'+str(cgid)+'/update')
        # return '/accounts/profile/'

    def form_invalid(self, form):
        context = {'form': form}
        return self.render_to_response(context=context)

    def form_valid(self, form):
        context = {}
        if form.is_valid():
            self.object = form.save()
            return HttpResponseRedirect(self.get_success_url())
        # else:
        #   print('form not valid', form.errors)
        #   context['errors'] = form.errors
        # return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(CollectionGroupCreateView, self).get_context_data(*args, **kwargs)
        # print('args',args,kwargs)
        context['action'] = 'create'
        # context['referrer'] = self.request.POST.get('referrer')
        return context


class CollectionGroupDetailView(DetailView):
    model = CollectionGroup
    template_name = 'collection/collection_group_detail.html'

    def get_success_url(self):
        pid = self.kwargs.get("id")
        # print('messages:', messages.get_messages(self.kwargs))
        return '/collection/' + str(pid) + '/detail'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(CollectionGroup, id=id_)

    def get_context_data(self, *args, **kwargs):
        context = super(CollectionGroupDetailView, self).get_context_data(*args, **kwargs)

        cg = get_object_or_404(CollectionGroup, pk=self.kwargs.get("id"))
        me = self.request.user
        # if Collection has a group, it is submitted
        context['submitted'] = Collection.objects.filter(group=cg.id).count()
        context['message'] = 'CollectionGroupDetailView() loud and clear'
        context['links'] = Link.objects.filter(collection_group_id=self.get_object())
        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'whg_admins']).exists() else False

        return context


class CollectionGroupDeleteView(DeleteView):
    template_name = 'collection/collection_group_delete.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(CollectionGroup, id=id_)

    def get_success_url(self):
        return reverse('dashboard-user')


"""
  update (edit); uses same template as create;
  context['action'] governs template display
"""


class CollectionGroupUpdateView(UpdateView):
    form_class = CollectionGroupModelForm
    template_name = 'collection/collection_group_create.html'

    def get_form_kwargs(self, **kwargs):
        kwargs = super(CollectionGroupUpdateView, self).get_form_kwargs()
        return kwargs

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(CollectionGroup, id=id_)

    def form_valid(self, form):
        id_ = self.kwargs.get("id")
        if form.is_valid():
            obj = form.save(commit=False)
            obj.save()
            return redirect('/collections/group/' + str(id_) + '/update')
        else:
            logger.debug(f'form not valid: {form.errors}')
        return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(CollectionGroupUpdateView, self).get_context_data(*args, **kwargs)
        cg = self.get_object()
        members = [m.user for m in cg.members.all()]
        context['action'] = 'update'
        context['members'] = members
        context['collections'] = Collection.objects.filter(group=cg.id)
        context['links'] = Link.objects.filter(collection_group_id=self.get_object())
        return context


class CollectionGroupGalleryView(ListView):
    redirect_field_name = 'redirect_to'

    context_object_name = 'collections'
    template_name = 'collection/collection_group_gallery.html'
    model = Collection

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(CollectionGroup, id=id_)

    def get_queryset(self):
        # original qs
        qs = super().get_queryset()
        return qs
        # return qs.filter(public = True).order_by('core','title')

    def get_context_data(self, *args, **kwargs):
        context = super(CollectionGroupGalleryView, self).get_context_data(*args, **kwargs)
        cg = CollectionGroup.objects.get(id=self.kwargs.get("id"))

        # public datasets available as dataset_list
        # public collections
        context['group'] = self.get_object()
        # context['collections'] = cg.collections.all()
        context['collections'] = Collection.objects.filter(
            group=cg.id, status__in=['reviewed', 'published']).order_by('submit_date')
        # context['viewable'] = ['uploaded','inserted','reconciling','review_hits','reviewed','review_whg','indexed']

        context['beta_or_better'] = True if self.request.user.groups.filter(
            name__in=['beta', 'whg_admins']).exists() else False
        return context


""" DATASET COLLECTIONS """
""" datasets only collection
    uses ds_collection_build.html
"""


class DatasetCollectionCreateView(LoginRequiredMixin, CreateView):
    # print('hit DatasetCollectionCreateView()')
    form_class = CollectionModelForm
    # TODO: new ds collection builder
    template_name = 'collection/ds_collection_build.html'
    queryset = Collection.objects.all()

    def get_success_url(self):
        Log.objects.create(
            # category, logtype, "timestamp", subtype, note, dataset_id, collection_id, user_id
            category='collection',
            collection_id=self.object.id,
            logtype='create',
            note='created collection id: ' + str(self.object.id),
            user_id=self.request.user.id
        )
        return reverse('collection:ds-collection-update', kwargs={'id': self.object.id})

    #
    def get_form_kwargs(self, **kwargs):
        kwargs = super(DatasetCollectionCreateView, self).get_form_kwargs()
        return kwargs

    def form_invalid(self, form):
        logger.debug(f'form invalid: {form.errors.as_data()}')
        context = {'form': form}
        return self.render_to_response(context=context)

    def form_valid(self, form):
        context = {}
        return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        user = self.request.user
        context = super(DatasetCollectionCreateView, self).get_context_data(*args, **kwargs)
        context['whgteam'] = User.objects.filter(groups__name='whg_team')

        datasets = []
        # owners create collections from their datasets
        ds_select = [obj for obj in Dataset.objects.all().order_by('title').exclude(title__startswith='(stub)')
                     if user in obj.owners or user in obj.collaborators or user.is_superuser]

        context['action'] = 'create'
        context['ds_select'] = ds_select
        context['coll_dsset'] = datasets

        return context


""" update dataset collection
    uses ds_collection_build.html
"""


class DatasetCollectionUpdateView(UpdateView):
    form_class = CollectionModelForm
    template_name = 'collection/ds_collection_build.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Collection, id=id_)

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        return '/collections/' + str(id_) + '/browse_ds'

    def form_valid(self, form):
        if form.is_valid():
            obj = form.save(commit=False)
            obj.save()
            Log.objects.create(
                # category, logtype, "timestamp", subtype, note, dataset_id, collection_id, user_id
                category='collection',
                collection_id=obj.id,
                logtype='update',
                note='collection id: ' + str(obj.id) + ' by ' + self.request.user.name,
                user_id=self.request.user.id
            )
        else:
            logger.debug(f'form not valid: {form.errors.as_data()}')
        return super().form_valid(form)

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetCollectionUpdateView, self).get_context_data(*args, **kwargs)
        user = self.request.user
        _id = self.kwargs.get("id")
        coll = self.get_object()
        datasets = coll.datasets.all()

        # populates dropdown
        assigned_datasets = coll.datasets.all()
        # eligible: indexed (need not be public), not dev stubs, not already added, and user is owner or collaborator
        ds_select = [obj for obj in Dataset.objects.filter(ds_status='indexed') \
            .exclude(id__in=assigned_datasets) \
            .exclude(title__startswith='(stub)') \
            .exclude(owner_id=119)  # TODO: remove this hard-coded user id (Ali) at deploy
                     if user in obj.owners or user in obj.collaborators or user.is_superuser]

        context['action'] = 'update'
        context['ds_select'] = ds_select
        context['coll_dsset'] = datasets
        context['created'] = self.object.create_date.strftime("%Y-%m-%d")

        context['owner'] = True if user == coll.owner else False  # actual owner
        context['is_owner'] = True if user in self.object.owners else False  # owner or co-owner
        context['is_member'] = True if user in coll.owners or user in coll.collaborators else False
        context['is_admin'] = True if user.groups.filter(name__in=['whg_admins']).exists() else False
        context['whgteam'] = True if user.groups.filter(name__in=['whg_team', 'editorial']).exists() else False
        context['collabs'] = CollectionUser.objects.filter(collection=coll.id)

        vis_parameters = coll.vis_parameters
        if not vis_parameters:
            vis_parameters = {
                'seq': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'min': {'tabulate': False, 'temporal_control': 'none', 'trail': False},
                'max': {'tabulate': False, 'temporal_control': 'none', 'trail': False}
            }
        # context['visParameters'] = json.dumps(vis_parameters)
        context['vis_parameters_dict'] = vis_parameters

        return context


""" browse collection dataset places
    same for owner(s) and public
"""


class DatasetCollectionBrowseView(DetailView):
    login_url = '/accounts/login/'
    redirect_field_name = 'redirect_to'

    model = Collection
    template_name = 'collection/ds_collection_browse.html'

    def get_success_url(self):
        id_ = self.kwargs.get("id")
        return '/collections/' + str(id_) + '/places'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Collection, id=id_)

    def dispatch(self, request, *args, **kwargs):
        self.object = self.get_object()
        if self.object.collection_class != "dataset":
            raise Http404("Collection does not match expected class 'dataset'")
        return super().dispatch(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        context = super(DatasetCollectionBrowseView, self).get_context_data(*args, **kwargs)

        context['media_url'] = settings.MEDIA_URL

        id_ = self.kwargs.get("id")
        # compute bounding boxes

        coll = get_object_or_404(Collection, id=id_)

        # sg 21-Dec-2023: These 2 lines appear to be redundant:
        placeset = coll.places.all()
        context['places'] = placeset

        # context['ds_list'] = coll.ds_list
        context['links'] = Link.objects.filter(collection=id_)
        context['updates'] = {}
        context['is_admin'] = True if self.request.user.groups.filter(name__in=['whg_admins']).exists() else False
        context[
            'visParameters'] = coll.vis_parameters or "{'seq': {'tabulate': false, 'temporal_control': 'none', 'trail': false},'min': {'tabulate': false, 'temporal_control': 'none', 'trail': false},'max': {'tabulate': false, 'temporal_control': 'none', 'trail': false}}"
        context['datasets'] = [{"id": ds["id"], "label": ds["label"], "title": ds["title"], "extent": ds["extent"]} for
                               ds in coll.ds_list]
        context['num_places'] = coll.num_places

        context['coordinate_density'] = coll.coordinate_density_value

        return context


""" browse collection collections
    w/student section?
"""


# class CollectionGalleryView(ListView):
#   redirect_field_name = 'redirect_to'
#
#   context_object_name = 'collections'
#   template_name = 'collection/collection_gallery.html'
#   model = Collection
#
#   def get_queryset(self):
#     qs = super().get_queryset()
#     return qs.filter(public = True).order_by('title')
#
#   def get_context_data(self, *args, **kwargs):
#     context = super(CollectionGalleryView, self).get_context_data(*args, **kwargs)
#     # public collections
#     # context['group'] = self.get_object()
#     context['place_collections'] = Collection.objects.filter(collection_class='place', public=True)
#     context['dataset_collections'] = Collection.objects.filter(collection_class='dataset', public=True)
#     context['student_collections'] = Collection.objects.filter(nominated=True)
#
#     context['beta_or_better'] = True if self.request.user.groups.filter(name__in=['beta', 'whg_admins']).exists() else False
#     return context

class CollectionDeleteView(DeleteView):
    template_name = 'collection/collection_delete.html'

    def get_object(self):
        id_ = self.kwargs.get("id")
        return get_object_or_404(Collection, id=id_)

    def get_success_url(self):
        return reverse('dashboard')
