# api.views

from django.conf import settings
from django.contrib.auth import get_user_model
from django.contrib.auth.decorators import login_required
from django.core.exceptions import ValidationError
from elasticsearch8 import BadRequestError
from rest_framework.request import Request

User = get_user_model()
from django.contrib.auth.models import AnonymousUser
from django.contrib.gis.geos import Polygon, Point
# from django.contrib.postgres import search
from django.contrib.gis.measure import D
from django.db.models import Case, When, Min, Max, Subquery, OuterRef, Count, \
    IntegerField
from django.http import Http404
from django.shortcuts import get_object_or_404
from django.views.generic import View
from django.utils.decorators import method_decorator

from drf_spectacular.utils import extend_schema

from rest_framework import generics
from rest_framework import permissions
from rest_framework import viewsets
from rest_framework import status
from rest_framework.authentication import SessionAuthentication
from rest_framework.decorators import api_view
from rest_framework.exceptions import APIException
from rest_framework.pagination import PageNumberPagination
from rest_framework.renderers import JSONRenderer
from rest_framework.reverse import reverse
from rest_framework.generics import ListAPIView
from rest_framework.views import APIView
from accounts.permissions import IsOwnerOrReadOnly
from api.serializers import (
    UserSerializer, DatasetSerializer, PlaceSerializer,
    PlaceTableSerializer, PlaceGeomSerializer, AreaSerializer,
    FeatureSerializer, LPFSerializer, PlaceCompareSerializer, ErrorResponseSerializer,
    GallerySerializer)
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from main.choices import FEATURE_CLASSES
from main.models import Log
from datasets.tasks import get_bounds_filter
from places.models import PlaceGeom
import json
import os, requests

import logging

logger = logging.getLogger(__name__)


class BadRequestException(APIException):
    serializer_class = ErrorResponseSerializer
    status_code = status.HTTP_400_BAD_REQUEST
    default_detail = 'Bad request.'

    def __init__(self, detail=None, code=None):
        if detail is None:
            detail = self.default_detail
        self.detail = {"error": detail}


@extend_schema(exclude=True)
class GalleryView(ListAPIView):
    pagination_class = PageNumberPagination
    pagination_class.page_size = 6
    serializer_class = GallerySerializer

    def get_gallery_type(self):
        return self.kwargs.get('type')

    def get_queryset(self):
        self.gallery_type = self.get_gallery_type()
        model = Collection if self.gallery_type == 'collections' else Dataset

        filter = Q(public=True)
        if hasattr(model, 'core'):  # Omit "core" Datasets
            filter &= Q(core=False)

        # ADD COLLECTION CLASS FILTERS
        classes = self.request.query_params.get('classes', '')
        if classes:
            class_list = classes.split(',')
            classfilters = Q(collection_class__in=class_list)
            if 'student' in class_list:
                classfilters |= Q(nominated=True)
            filter &= classfilters
        elif self.gallery_type == 'collections':
            return model.objects.none()

        # ADD TEXT FILTERS
        search_text = self.request.query_params.get('q', '')  # Default to empty string
        if search_text:
            filter &= (
                    Q(title__icontains=search_text) |  # include title field search
                    Q(description__icontains=search_text) |  # include description field search
                    Q(creator__icontains=search_text)  # include creator field search
            )
            if hasattr(model, 'contributors'):  # Check if contributors field exists in the model (Datasets only)
                filter |= Q(contributors__icontains=search_text)  # include contributors field search if it exists
        queryset = model.objects.filter(filter)

        # SPATIAL FILTERS
        country_codes = self.request.query_params.get('countries', '').split(',')
        if country_codes != ['']:
            country_codes = [code.upper() for code in country_codes]
            queryset = queryset.filter(places__ccodes__overlap=country_codes).distinct()

        # SORTING
        sort_by = self.request.query_params.get('sort', 'title')  # Default to `title` alphabetical
        if sort_by.endswith('earliest'):  # Use `endswith` to accommodate reverse sorting
            queryset = queryset.annotate(earliest=Min('places__minmax__0'))
        elif sort_by.endswith('latest'):
            queryset = queryset.annotate(latest=Max('places__minmax__1'))
        elif sort_by.endswith('numrows') and self.gallery_type == 'collections':
            queryset = queryset.annotate(numrows=Count(
                Case(
                    When(
                        places__isnull=False,
                        then=1,
                    ),
                    output_field=IntegerField(),
                ),
                distinct=True,
            ) + Count('datasets__places', distinct=True))
        elif sort_by.endswith('modified'):
            queryset = queryset.annotate(
                modified=Subquery(
                    Log.objects.filter(dataset=OuterRef('pk')).order_by('-timestamp').values('timestamp')[:1])
            )

        return queryset.order_by(sort_by)

    def list(self, request: Request, *args, **kwargs):
        self.gallery_type = self.get_gallery_type()
        queryset = self.get_queryset()

        # Paginate the data (DRF handles pagination)
        page = self.paginate_queryset(queryset)
        if page is not None:
            # Serialize the paginated data
            serializer = self.get_serializer(page, many=True,
                                             model=Collection if self.gallery_type == 'collections' else Dataset)
            serializer_data = serializer.data
            return self.get_paginated_response(self.custom_response_format(serializer_data, queryset))

        # Serialize the non-paginated data
        serializer = self.get_serializer(page, many=True,
                                         model=Collection if self.gallery_type == 'collections' else Dataset)
        return Response(self.custom_response_format(serializer.data, queryset))

    def custom_response_format(self, serializer_data, queryset):
        return {
            'items': serializer_data,
            'total_items': queryset.count(),
            'current_page': int(self.request.query_params.get('page', 1)),
            'total_pages': (queryset.count() // self.pagination_class.page_size) + (
                1 if queryset.count() % self.pagination_class.page_size > 0 else 0),
        }

    # def list(self, request, *args, **kwargs):
    #     # Retrieve the queryset
    #     queryset = self.get_queryset()
    #
    #     # Paginate the queryset (DRF handles pagination automatically)
    #     page = self.paginate_queryset(queryset)
    #     if page is not None:
    #         serializer = self.get_serializer(page, many=True)
    #         return self.get_paginated_response(serializer.data)
    #
    #     # If pagination is not applied, return all data
    #     serializer = self.get_serializer(queryset, many=True)
    #     return Response(serializer.data)

    # def list(self, request, *args, **kwargs):
    #     queryset = self.get_queryset()
    #
    #     # Paginate the data
    #     paginator = self.pagination_class()
    #     try:
    #         page = paginator.paginate_queryset(queryset, request)
    #     except PageNotAnInteger:
    #         page = 1
    #     except EmptyPage:
    #         page = paginator.page.paginator.num_pages
    #
    #     # Use the defined serializer class to serialize the data
    #     serializer = self.get_serializer(page, many=True)
    #
    #     # Get the serialized data
    #     serializer_data = serializer.data
    #
    #     # qs_list = [instance.carousel_metadata for instance in serializer_data]
    #     # qs_json = json.dumps(qs_list, default=str)
    #
    #     qs_json = json.dumps(serializer_data, default=str)
    #
    #     response_data = {
    #         'items': json.loads(qs_json),
    #         'total_items': queryset.count(),
    #         'current_page': int(request.query_params.get('page', 1)),
    #         'total_pages': paginator.page.paginator.num_pages,
    #     }
    #
    #     return JsonResponse(response_data, safe=False)


class StandardResultsSetPagination(PageNumberPagination):
    page_size = 10
    page_size_query_param = 'page_size'
    max_page_size = 20000


@extend_schema(exclude=True)
class SpatialAPIView(generics.ListAPIView):
    """
    This endpoint will return a Feature Collection (in Linked Places Format) containing places either within a given bounding
    box or within a given radial distance from a given point.

    A search `type` of either `bbox` or `nearby` is required. A `bbox` requires south-west (`sw`) and north-east (`ne`) coordinates, while
    a `nearby` search requires `lon` and `lat` of the search centre together with a `km` radius (in kilometres).
    """
    renderer_classes = [JSONRenderer]
    serializer_class = LPFSerializer

    # TODO: Add text filtering?
    # filter_backends = [filters.SearchFilter]
    # search_fields = ['@title']

    def get(self, *args, **kwargs):
        params = self.request.query_params

        qtype = params.get('type')
        lon, lat, dist = params.get('lon'), params.get('lat'), params.get('km')
        sw, ne = params.get('sw'), params.get('ne')
        fc, ds, coll = params.get('fc'), params.get('dataset'), params.get('collection')
        pagesize = int(params.get('pagesize', 20)) if params.get('pagesize', '20').isdigit() else 20

        result = {"type": "FeatureCollection", "features": [], "parameters": params, "errors": None}

        try:
            # Handle nearby and bbox query types
            if qtype == 'nearby':
                if not all([lon, lat, dist]):
                    raise BadRequestException("A 'nearby' spatial query requires 'lon', 'lat', and 'km' parameters.")
                pnt = Point(float(lon), float(lat), srid=4326)
                placeids = PlaceGeom.objects.filter(geom__distance_lte=(pnt, D(km=float(dist)))).values_list('place_id',
                                                                                                             flat=True)

            elif qtype == 'bbox':
                if not all([sw, ne]):
                    raise BadRequestException("A 'bbox' spatial query requires both 'sw' and 'ne' parameters.")
                sw_lon, sw_lat = map(float, sw.split(','))
                ne_lon, ne_lat = map(float, ne.split(','))
                if not (
                        -180 <= sw_lon <= 180 and -90 <= sw_lat <= 90 and -180 <= ne_lon <= 180 and -90 <= ne_lat <= 90):
                    raise BadRequestException("Bounding box coordinates are out of bounds.")
                bbox = Polygon.from_bbox([sw_lon, sw_lat, ne_lon, ne_lat])
                placeids = PlaceGeom.objects.filter(geom__within=bbox).values_list('place_id', flat=True)
            else:
                raise BadRequestException("Spatial query requires 'type' parameter to be 'nearby' or 'bbox'.")

            # Query places and apply additional filters
            qs = Place.objects.filter(id__in=placeids)
            if coll:
                try:
                    coll_ids = Collection.objects.get(id=coll).places.values_list('id', flat=True)
                    qs = qs.filter(id__in=coll_ids)
                except Collection.DoesNotExist:
                    raise BadRequestException(f"The requested collection with ID {coll} does not exist.")
            if ds:
                try:
                    qs = qs.filter(dataset=Dataset.objects.get(id=ds))
                except Dataset.DoesNotExist:
                    raise BadRequestException(f"The requested dataset with ID {ds} does not exist.")
            if fc:
                fclasses = list(set([x.upper() for x in fc.split(',')]))
                qs = qs.filter(fclasses__overlap=fclasses)

            filtered = qs[:pagesize]
            serializer = LPFSerializer(filtered, many=True, context={'request': self.request})
            result.update({
                "count": qs.count(),
                "features": serializer.data,
                "pagesize": len(serializer.data),
            })

        # Handle specific error types without generic fallback
        except (ValueError, TypeError, ValidationError) as e:
            logger.error("Parameter error: %s", e)
            return JsonResponse(
                {"error": str(e)}, status=status.HTTP_400_BAD_REQUEST,
                json_dumps_params={'ensure_ascii': False, 'indent': 2}
            )
        except Place.DoesNotExist:
            result["errors"] = "No matching places found."
            return JsonResponse(result, status=status.HTTP_404_NOT_FOUND,
                                json_dumps_params={'ensure_ascii': False, 'indent': 2})

        return JsonResponse(result, json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  makeGeom(); called by responseItem(), childItem
  format index locations as geojson
"""


def makeGeom(geom):
    # print('geom',geom)
    # TODO: account for non-point
    if len(geom) > 1:
        geomobj = {"type": "GeometryCollection", "geometries": []}
        for g in geom:
            geomobj['geometries'].append(g['location'])
    elif len(geom) == 1:
        geomobj = geom[0]['location']
    else:
        geomobj = None
    return geomobj


def childItem(i):
    _id = i.get('_id', None)
    score = i.get('_score', None)
    i = i.get('_source', None)

    if i is None or _id is None:
        logging.warning(f"Item is missing '_id' or '_source'")
        return None

    item = {
        "type": "Feature",
        "score": score,
        "properties": {
            "title": i.get('title', ''),
            "index_id": _id,
            "index_role": i.get('relation', {}).get('name', ''),
            "place_id": i.get('place_id', ''),
            "source_id": i.get('src_id', ''),
            "dataset": i.get('dataset', ''),
        },
        "fclasses": [fc for fc in (i.get('fclasses') or []) if fc is not None],
        "types": [t.get('sourceLabel', t.get('source_label')) for t in (i.get('types') or []) if t is not None],
        "variants": [n['toponym'] for n in (i.get('names') or []) if n.get('toponym') != i.get('title')],
        'links': i.get('links', []),
        "when": [{"start": {"in": ts.get("gte")}, "end": {"in": ts.get("lte")}} for ts in (i.get('timespans') or []) if
                 ts is not None],
        "minmax": [i['minmax'].get('gte'), i['minmax'].get('lte')] if isinstance(i.get('minmax'), dict) else [],
        "ccodes": i.get('ccodes', []),
        "geometry": makeGeom(i.get('geoms', []))
    }
    return item


"""
  responseItem(); called by collector();
  formats api search parent hits
"""


def responseItem(i):
    # print('i in responseItem',i)
    _id = i['_id']
    score = i.get('score', None)
    # serialize as geojson
    i = i['hit']
    # print("i['names']", i['names'])
    item = {
        "type": "Feature",
        "score": score,
        "properties": {
            "title": i['title'],
            "index_id": _id,
            "index_role": i['relation']['name'],
            "place_id": i['place_id'],
            "child_place_ids": [int(c) for c in i['children']],
            "dataset": i['dataset'],
            "fclasses": [c for c in i['fclasses']],
            "placetypes": [t.get('sourceLabel', t.get('source_label')) for t in i['types']],
            # "variants":[n for n in i['suggest']['input'] if n != i['title']],
            "variants": [n['toponym'] for n in i['names'] if n['toponym'] != i['title']],
            'links': i['links'],
            "timespans": i['timespans'],
            "minmax": i['minmax'] if 'minmax' in i.keys() else [],
            "ccodes": i['ccodes']
        },
        "geometry": makeGeom(i['geoms'])
    }
    return item


"""
  collector(); called by IndexAPIView(), RemoteIndexAPIView()
  execute es.search, return results post-processed by suggestionItem()
"""


def collector(q, idx):
    es = settings.ES_CONN
    items = []

    # TODO: trap errors
    res = es.search(index=idx, body=q)
    # print('res', res)
    hits = res['hits']['hits']
    # print('inner hits in collector()', json.dumps([h['inner_hits'] for h in hits], indent=2))
    # [dict_keys(['_index', '_id', '_score', '_source', 'inner_hits'])]
    count = res['hits']['total']['value']
    if len(hits) > 0:
        for h in hits:
            # print('h in collector()', h)
            items.append(
                {"_id": h['_id'],
                 "linkcount": len(h['_source']['links']),
                 "childcount": len(h['_source']['children']),
                 "score": h['_score'],
                 "hit": h['_source'],
                 "inner_hits": h['inner_hits'] if 'inner_hits' in h.keys() else "no inner hits"
                 }
            )
    # print('items', items) # 1 item
    # print('item keys', items[0].keys())
    # print('item inner_hits', items[0]['inner_hits'])
    result = {"count": count,
              "items": sorted(items, key=lambda x: x['score'], reverse=True)}
    return result


"""
  bundler();  called by IndexAPIView, case api/index?whgid=
  execute es.search, return post-processed results
"""


def bundler(q, whgid, idx):
    # idx is ['whg', 'pub']
    es = settings.ES_CONN
    res = es.search(index=idx, body=q)
    hits = res['hits']['hits']
    bundle = []
    if len(hits) > 0:
        for h in hits:
            bundle.append(
                {"_id": h['_id'],
                 "linkcount": len(h['_source']['links']),
                 "childcount": len(h['_source']['children']),
                 "score": h['_score'],
                 "hit": h['_source'],
                 }
            )
    stuff = [responseItem(i) for i in bundle]
    return stuff


"""
  /api/index?
  search whg & pub indexes
"""


@extend_schema(exclude=True)
class IndexAPIView(View):
    def get(self, request):
        params = request.GET
        # idx = ['whg', 'pub']  # search both indexes
        idx = [settings.ES_WHG, settings.ES_PUB]  # search both indexes

        name = request.GET.get('name')
        whgid = request.GET.get('whgid')  # a parent record
        pid = request.GET.get('pid')  # place.id > place_id
        fc = params.get('fclass', None)  # feature class
        fclasses = [x.upper() for x in fc.split(',')] if fc else None
        dataset = request.GET.get('dataset')  # dataset label
        cc = request.GET.get('ccode')  # country code
        ccodes = [x.upper() for x in cc.split(',')] if cc else None
        year = request.GET.get('year')
        area = request.GET.get('area')
        pagesize = params.get('pagesize', None)

        if all(v is None for v in [name, whgid, pid]):
            return JsonResponse({
                'error': 'Query requires either name, namestartswith, pid, or whgid',
                'instructions': 'API instructions can be found at https://docs.whgazetteer.org/content/400-Technical.html#api'
            }, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2}, status=400)

        else:
            if whgid and whgid != '':
                q = {
                    "query": {
                        "bool": {
                            "should": [
                                {"parent_id": {"type": "child", "id": whgid}},
                                {"match": {"_id": whgid}}
                            ]
                        }
                    }
                }
                bundle = bundler(q, whgid, idx)
                result = {"index_id": whgid,
                          "note": str(len(bundle)) + " records in WHGasserted as skos:closeMatch",
                          "type": "FeatureCollection",
                          "features": [b for b in bundle]}
            else:
                q = {
                    "size": 100,
                    "query": {
                        "bool": {
                            "must": [
                                {"exists": {"field": "whg_id"}},
                                {"multi_match": {
                                    "query": name,
                                    "fields": ["title^3", "names.toponym", "searchy"]
                                }}
                            ],
                            "should": [
                                {
                                    "has_child": {
                                        "type": "child",
                                        "query": {
                                            "match_all": {}
                                        },
                                        "inner_hits": {}
                                    }
                                }
                            ]
                        }
                    }
                }
                if fc:
                    q['query']['bool']['must'].append({"terms": {"fclasses": fclasses}})
                if dataset:
                    q['query']['bool']['must'].append({"match": {"dataset": dataset}})
                if ccodes:
                    q['query']['bool']['must'].append({"terms": {"ccodes": ccodes}})
                if year:
                    q['query']['bool']['must'].append({"term": {"timespans.value": year}})
                if area:
                    a = get_object_or_404(Area, pk=area)
                    bounds = {"id": [str(a.id)],
                              "type": [a.type]}  # necessary because some are polygons, some are multipolygons
                    # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
                    q['query']['bool']["filter"] = get_bounds_filter(bounds, settings.ES_WHG)

                try:
                    response = collector(q, settings.ES_WHG)
                except BadRequestError as e:
                    return JsonResponse({
                        'error': 'BadRequestError',
                        'message': str(e)
                    }, status=400)

                union_records = []
                for item in response['items']:
                    parent = responseItem(item)
                    # print('formatted parent', parent)
                    union_records.append(parent)  # the parent
                    # all parents have inner_hits, not all have children
                    children = item['inner_hits']['child']['hits']['hits']
                    if len(children) > 0:
                        for child in children:
                            # needs formatting as a Feature
                            # print()
                            # print('child', childItem(child))
                            union_records.append(childItem(child))  # the children
                result = {
                    'note': str(len(union_records)) + " records in WHG asserted as skos:closeMatch",
                    'type': 'FeatureCollection',
                    'pagesize': pagesize,
                    'features': union_records[:int(pagesize)] if pagesize else union_records}

        # to client
        return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


# class IndexAPIView(View):
#   def get(self, request):
#     params = request.GET
#     print('IndexAPIView request.GET', params)
#     idx = ['whg', 'pub']  # search both indexes
#
#     name = request.GET.get('name')
#     whgid = request.GET.get('whgid')  # a parent record
#     pid = request.GET.get('pid')  # place.id > place_id
#     fc = params.get('fclass', None)  # feature class
#     fclasses = [x.upper() for x in fc.split(',')] if fc else None
#     dataset = request.GET.get('dataset')  # dataset label
#     cc = request.GET.get('ccode')  # country code
#     ccodes = [x.upper() for x in cc.split(',')] if cc else None
#     year = request.GET.get('year')
#     area = request.GET.get('area')
#     pagesize = params.get('pagesize', None)
#
#     if all(v is None for v in [name, whgid, pid]):
#       return JsonResponse({
#         'error': 'Query requires either name, namestartswith, pid, or whgid',
#         'instructions': 'API instructions can be found at https://docs.whgazetteer.org/content/400-Technical.html#api'
#       }, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2}, status=400)
#
#     else:
#       if whgid and whgid != '':
#         print('fetching whg_id', whgid)
#         q = {
#           "query": {
#             "bool": {
#               "should": [
#                 {"parent_id": {"type": "child", "id": whgid}},
#                 {"match": {"_id": whgid}}
#               ]
#             }
#           }
#         }
#         bundle = bundler(q, whgid, idx)
#         print('bundler q', q)
#         result = {"index_id": whgid,
#                   "note": str(len(bundle)) + " records asserted as skos:closeMatch",
#                   "type": "FeatureCollection",
#                   "features": [b for b in bundle]}
#       else:
#         q = {
#           "size": 100,
#           "query": {
#             "bool": {
#               "must": [
#                 {"exists": {"field": "whg_id"}},
#                 {"multi_match": {
#                   "query": name,
#                   "fields": ["title^3", "names.toponym", "searchy"],
#                 }}
#               ]
#             }
#           },
#           "aggs": {
#             "children": {
#               "terms": {"field": "relation.name"},
#               "aggs": {
#                 "child_docs": {
#                   "top_hits": {
#                     "size": 100,
#                     "_source": ["title", "names", "searchy"]
#                   }
#                 }
#               }
#             }
#           }
#         }
#         if fc:
#           q['query']['bool']['must'].append({"terms": {"fclasses": fclasses}})
#         if dataset:
#           q['query']['bool']['must'].append({"match": {"dataset": dataset}})
#         if ccodes:
#           q['query']['bool']['must'].append({"terms": {"ccodes": ccodes}})
#         if year:
#           q['query']['bool']['must'].append({"term": {"timespans.value": year}})
#         if area:
#           a = get_object_or_404(Area, pk=area)
#           bounds = {"id": [str(a.id)], "type": [a.type]}  # nec. b/c some are polygons, some are multipolygons
#           q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
#
#         print('the api query was:', json.dumps(q, indent=2))
#
#         # run query
#         response = collector(q, 'whg')
#         print('response in IndexAPIView()', response)
#
#         union_records = []
#         for parent in response['hits']['hits']:
#           union_record = responseItem(parent)
#           if 'inner_hits' in parent:
#             children = parent['inner_hits']['child']['hits']['hits']
#             for child in children:
#               union_record['features'].append(responseItem(child))
#           union_records.append(union_record)
#
#         # result object
#         result = {'type': 'FeatureCollection',
#                   'count': len(union_records),
#                   'pagesize': pagesize,
#                   'features': union_records[:int(pagesize)] if pagesize else union_records}
#
#     # to client
#     return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})
#

"""

  /api/db?
  SearchAPIView()
  return lpf results from database search
"""

from django.http import JsonResponse, HttpResponse
from rest_framework.renderers import JSONRenderer
from rest_framework import generics, filters, status
from django.db.models import Q
from places.models import Place
from .serializers import LPFSerializer
import re


@extend_schema(exclude=True)
class SearchAPIView(generics.ListAPIView):
    renderer_classes = [JSONRenderer]
    filter_backends = [filters.SearchFilter]
    search_fields = ['@title']

    def get(self, request, format=None, *args, **kwargs):
        params = request.query_params

        # Validate and parse parameters
        try:
            id_ = params.get('id', None)
            if id_:
                id_list = [int(x) for x in id_.split(',')] if ',' in id_ else [int(id_)]

            name = params.get('name')
            name_contains = params.get('name_contains')

            ccode_param = params.get('ccode')
            cc = [code.strip().upper() for code in ccode_param.split(',')] if ccode_param else None
            if cc and not all(re.match(r'^[A-Z]{2}$', code) for code in cc):
                raise ValueError("Each 'ccode' must be a 2-letter uppercase ISO code.")

            ds = params.get('dataset')
            fc = params.get('fc')
            fclasses = list(set(x.strip().upper() for x in fc.split(','))) if fc else None

            year = params.get('year')
            if year:
                year = int(year)

            pagesize = int(params.get('pagesize', 20))
            if not (1 <= pagesize <= 200):
                raise ValueError("'pagesize' must be between 1 and 200.")

            context = params.get('context')
        except ValueError as e:
            return JsonResponse(
                {"error": str(e)},
                status=status.HTTP_400_BAD_REQUEST
            )

        # Ensure required parameters are present
        if all(v is None for v in [name, name_contains, id_]):
            return HttpResponse(content=b'<h3>Needs either a "name", a "name_contains", or "id" parameter at \
                minimum <br/>(e.g. ?name=myplacename or ?name_contains=astring or ?id=integer)</h3>')

        # Begin queryset
        qs = Place.objects.filter(Q(dataset__public=True) | Q(dataset__core=True))
        err_note = None

        # Filtering
        if id_:
            qs = qs.filter(id__in=id_list)
            if len(params.keys()) > 1:
                err_note = 'id given, other parameters ignored'
        else:
            if year:
                qs = qs.filter(minmax__0__lte=year, minmax__1__gte=year)
            if fclasses:
                qs = qs.filter(fclasses__overlap=fclasses)
            if name_contains:
                qs = qs.filter(title__icontains=name_contains)
            elif name:
                qs = qs.filter(names__jsonb__toponym__icontains=name)
            if ds:
                qs = qs.filter(dataset=ds)
            if cc:
                qs = qs.filter(ccodes__overlap=cc)

        filtered = qs[:pagesize]

        serializer = LPFSerializer(filtered, many=True, context={'request': self.request})
        result = {
            "count": qs.count(),
            "pagesize": len(filtered),
            "parameters": params,
            "note": err_note,
            "type": "FeatureCollection",
            "features": serializer.data
        }
        return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


@extend_schema(exclude=True)
class SearchAPIView_DEPRECATED(generics.ListAPIView):
    renderer_classes = [JSONRenderer]
    filter_backends = [filters.SearchFilter]
    search_fields = ['@title']

    def get(self, format=None, *args, **kwargs):
        params = self.request.query_params

        id_ = params.get('id', None)
        name = params.get('name', None)
        name_contains = params.get('name_contains', None)
        cc = map(str.upper, params.get('ccode').split(',')) if params.get('ccode') else None
        ds = params.get('dataset', None)
        fc = params.get('fc', None)
        fclasses = list(set([x.upper() for x in ','.join(fc)])) if fc else None
        year = params.get('year', None)
        try:
            pagesize = int(params.get('pagesize', 20))
        except (ValueError, TypeError):
            pagesize = 20
        err_note = None
        context = params.get('context', None)
        # params

        qs = Place.objects.filter(Q(dataset__public=True) | Q(dataset__core=True))

        if all(v is None for v in [name, name_contains, id_]):
            # TODO: return a template with API instructions
            return HttpResponse(content=b'<h3>Needs either a "name", a "name_contains", or "id" parameter at \
          minimum <br/>(e.g. ?name=myplacename or ?name_contains=astring or ?id=integer)</h3>')
        else:
            if id_:
                if ',' in id_:  # Check if id_ contains multiple IDs
                    id_list = id_.split(',')  # Split the comma-separated string into a list
                    qs = qs.filter(id__in=id_list)  # Use __in lookup to filter by multiple IDs
                else:
                    qs = qs.filter(id=id_)  # Single ID filter
                err_note = 'id given, other parameters ignored' if len(params.keys()) > 1 else None
            else:
                qs = qs.filter(minmax__0__lte=year, minmax__1__gte=year) if year else qs
                qs = qs.filter(fclasses__overlap=fclasses) if fc else qs

                if name_contains:
                    qs = qs.filter(title__icontains=name_contains)
                elif name and name != '':
                    # qs = qs.filter(title__istartswith=name)
                    qs = qs.filter(names__jsonb__toponym__icontains=name)

                qs = qs.filter(dataset=ds) if ds else qs
                qs = qs.filter(ccodes__overlap=cc) if cc else qs

            filtered = qs[:pagesize] if pagesize and pagesize < 200 else qs[:20]

            # serial = LPFSerializer if context else SearchDatabaseSerializer
            serial = LPFSerializer
            serializer = serial(filtered, many=True, context={'request': self.request})

            serialized_data = serializer.data
            result = {"count": qs.count(),
                      "pagesize": len(filtered),
                      "parameters": params,
                      "note": err_note,
                      "type": "FeatureCollection",
                      "features": serialized_data
                      }
            # print('place result',result)
            return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


""" *** """
""" TODO: the next two attempt the same and are WAY TOO SLOW """
"""
    api/places/<str:dslabel>/[?q={string}]
    Paged list of places in dataset.
"""


@extend_schema(exclude=True)
class PlaceAPIView(generics.ListAPIView):
    serializer_class = PlaceSerializer
    pagination_class = StandardResultsSetPagination

    def get_queryset(self, format=None, *args, **kwargs):
        dslabel = self.kwargs['dslabel']
        qs = Place.objects.all().filter(dataset=dslabel).order_by('title')
        query = self.request.GET.get('q')
        if query is not None:
            qs = qs.filter(title__icontains=query)
        return qs

    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly]


"""
    api/dataset/<str:dslabel>/lpf/
    all places in a dataset, for download
"""


@extend_schema(exclude=True)
class DownloadDatasetAPIView(generics.ListAPIView):
    """  Dataset as LPF FeatureCollection  """

    # serializer_class = PlaceSerializer
    # pagination_class = StandardResultsSetPagination

    def get(self, format=None):
        dslabel = self.request.GET.get('dataset')
        ds = get_object_or_404(Dataset, label=dslabel)
        features = []
        qs = ds.places.all()
        for p in qs:
            rec = {"type": "Feature",
                   "properties": {"id": p.id, "src_id": p.src_id, "title": p.title, "ccodes": p.ccodes},
                   "geometry": {"type": "GeometryCollection",
                                "features": [g.jsonb for g in p.geoms.all()]},
                   "names": [n.jsonb for n in p.names.all()],
                   "types": [t.jsonb for t in p.types.all()],
                   "links": [l.jsonb for l in p.links.all()],
                   "whens": [w.jsonb for w in p.whens.all()],
                   }
            # print('rec',rec)
            features.append(rec)

        result = {"type": "FeatureCollection", "features": features}
        return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})

    # permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]


"""
  /api/datasets? > query public datasets by id, label, term
"""
# class DatasetAPIView(LoginRequiredMixin, generics.ListAPIView):
#   """    List public datasets    """
#   serializer_class = DatasetSerializer
#   renderer_classes = [JSONRenderer]
#
#   def get_queryset(self, format=None, *args, **kwargs):
#     params=self.request.query_params
#     print('api/datasets params',params)
#     id_ = params.get('id', None)
#     dslabel = params.get('label', None)
#     query = params.get('q', None)
#
#     qs = Dataset.objects.filter(Q(public=True) | Q(core=True)).order_by('label')
#
#     if id_:
#       qs = qs.filter(id = id_)
#     elif dslabel:
#       qs = qs.filter(label = dslabel)
#     elif query:
#       qs = qs.filter(Q(description__icontains=query) | Q(title__icontains=query))
#
#     print('qs',qs)
#     result = {
#               "count":qs.count(),
#               "parameters": params,
#               #"features":serialized_data
#               "features":qs
#               }
#     print('ds result', result,type(result))
#     return qs
from rest_framework.response import Response


@extend_schema(exclude=True)
class DatasetAPIView(generics.ListAPIView):
    """List public datasets"""
    serializer_class = DatasetSerializer
    renderer_classes = [JSONRenderer]

    def list(self, request, *args, **kwargs):
        params = self.request.query_params
        id_ = params.get('id', None)
        dslabel = params.get('label', None)
        query = params.get('q', None)

        qs = Dataset.objects.filter(Q(public=True) | Q(core=True)).order_by('label')

        if id_:
            qs = qs.filter(id=id_)
        elif dslabel:
            qs = qs.filter(label=dslabel)
        elif query:
            qs = qs.filter(Q(description__icontains=query) | Q(title__icontains=query))

        serializer = self.get_serializer(qs, many=True)
        data = {
            "count": qs.count(),
            "parameters": params,
            "features": serializer.data
        }
        return JsonResponse(data, safe=False, json_dumps_params={'indent': 2})


"""
  /api/area_features
"""


# geojson feature for api
@extend_schema(exclude=True)
class AreaFeaturesView(generics.ListAPIView):
    # @staticmethod

    def get(self, format=None, *args, **kwargs):
        params = self.request.query_params
        user = self.request.user

        id_ = params.get('id', None)
        query = params.get('q', None)
        filter = params.get('filter', None)
        regions = params.get('regions', None)

        areas = []
        qs = Area.objects.all().filter((Q(type='predefined'))).values('id', 'title', 'type', 'description', 'geojson')

        # filter for parameters
        if id_:
            qs = qs.filter(id=id_)
        if query:
            qs = qs.filter(title__icontains=query)
        if filter and filter == 'un':
            qs = qs.filter(description="UN Statistical Division Sub-Region")
        if regions is not None and regions != '':
            qs = qs.filter(id__in=[int(region_id) for region_id in regions.split(',') if region_id.strip()])

        for a in qs:
            feat = {
                "type": "Feature",
                "properties": {"id": a['id'], "title": a['title'], "type": a['type'], "description": a['description']},
                "geometry": a['geojson']
            }
            areas.append(feat)

        # return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})
        return JsonResponse(areas, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


"""
  /api/user_area_features
"""


# geojson feature for api
@method_decorator(login_required, name='dispatch')
@extend_schema(exclude=True)
class UserAreaFeaturesView(APIView):

    def get(self, request, format=None, *args, **kwargs):
        user = self.request.user
        areas = []

        # Filter areas based on the user
        qs = Area.objects.filter(owner=user, type__in=['ccodes', 'copied', 'drawn']).values('id', 'title', 'type',
                                                                                            'description', 'geojson')

        for a in qs:
            feat = {
                "type": "Feature",
                "properties": {"id": a['id'], "title": a['title'], "type": a['type'], "description": a['description']},
                "geometry": a['geojson']
            }
            areas.append(feat)

        return Response(areas, status=status.HTTP_200_OK)


@extend_schema(exclude=True)
class UserList(generics.ListAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


@extend_schema(exclude=True)
class UserDetail(generics.RetrieveAPIView):
    queryset = User.objects.all()
    serializer_class = UserSerializer


"""
  API 'home page' (not implemented)
"""


@api_view(['GET'])
def api_root(request, format=None):
    return Response({
        # 'datasets': reverse('dataset-list', request=request, format=format)
        'datasets': reverse('api:ds-list', request=request, format=format)
    })


class PrettyJsonRenderer(JSONRenderer):
    def get_indent(self, accepted_media_type, renderer_context):
        return 2


#

# IN USE May 2020

#
"""
    place/<int:pk>/ **OR** 'place/<str:pk_list>/'
    uses: ds_browse.html; place_collection_browse.html
    "published record by place_id"
"""


## SUPERSEDED BY PlacesDetailAPIView BELOW
# class PlaceDetailAPIView(generics.RetrieveAPIView):
#   """  returns single serialized database place record by id  """
#   queryset = Place.objects.all()
#   serializer_class = PlaceSerializer
#   renderer_classes = [PrettyJsonRenderer]
#
#   permission_classes = [permissions.IsAuthenticatedOrReadOnly,IsOwnerOrReadOnly]
#   authentication_classes = [SessionAuthentication]
#
#   def get_serializer_context(self):
#     # collection id is passed from place collection browse
#     context = super().get_serializer_context()
#     context["query_params"] = self.request.query_params
#     return context

@extend_schema(exclude=True)
class PlacesDetailAPIView(View):
    """  returns serialized multiple database place records by id  """

    def get(self, request, pk_list=None, pk=None):

        cid = request.GET.get('cid')

        if pk_list is not None:
            # Split the string of IDs using the delimiter "-"
            ids = pk_list.split("-")
        elif pk is not None:
            ids = [str(pk)]
        else:
            pass

        places = Place.objects.filter(id__in=ids)

        def sort_unique(arr, key=None, sort_key=None):
            unique_items = []
            seen_items = set()

            for item in arr:
                item_unique = item.get(key) if key else json.dumps(item)
                if item_unique not in seen_items:
                    unique_items.append(item)
                    seen_items.add(item_unique)

            sort_key = sort_key or key
            if sort_key:
                unique_items.sort(key=lambda x: x.get(sort_key, ''))

            return unique_items

        with open(os.path.join(settings.STATIC_ROOT, 'aliases.json')) as json_file:
            json_data = json_file.read()
        base_urls = json.loads('{' + json_data + '}')[
            'base_urls']  # Wrap the contents in {} to create a valid JSON object

        def add_urls(data):
            return [
                {
                    **item,
                    "url": (
                        id_parts[0] + ":" + id_parts[1]
                        if id_parts[0].startswith("http")
                        else base_url + id_parts[1]
                    ),
                }
                if (identifier := item.get("identifier"))
                   and (id_parts := identifier.split(":"))  # safe because identifier is not None
                   and len(id_parts) == 2
                   and (base_url := base_urls.get(id_parts[0])) is not None
                else item
                for item in data
            ]

        # Serialize the Place records
        serialized_places = []
        attestation_years = set()
        for place in places:
            # Pass the request in the serializer context
            serializer = PlaceSerializer(place, context={'cid': cid, 'request': request})
            serialized_places.append(serializer.data)
            if place.attestation_year:
                attestation_years.add(place.attestation_year)

        # Calculate the overall extent
        aggregated_extent = None
        for place in serialized_places:
            extent = place["extent"]
            if extent:
                polygon = Polygon.from_bbox(extent)
                aggregated_extent = aggregated_extent.union(polygon) if aggregated_extent else polygon

        # Convert the overall extent to the format expected in the JSON response
        aggregated_extent = aggregated_extent.extent if aggregated_extent else None

        dataset_ids = set([place["dataset_id"] for place in serialized_places])

        # Extract the minmax values and filter out empty lists
        # minmax_values = [(place.get("minmax", [None, None])[0], place.get("minmax", [None, None])[1]) for place in serialized_places]
        minmax_values = [(place.get("minmax", [None, None])[0], place.get("minmax", [None, None])[1]) if place.get(
            "minmax") is not None else (None, None) for place in serialized_places]

        # Filter out None values
        min_values = [value[0] for value in minmax_values if value[0] is not None]
        max_values = [value[1] for value in minmax_values if value[1] is not None]
        # Calculate the min and max values, handling the case of empty sequences
        min_value = min(min_values, default=None)
        max_value = max(max_values, default=None)

        country_codes_mapping = {country['id']: country['text'] for item in
                                 json.load(open('media/data/regions_countries.json')) if item.get('text') == 'Countries'
                                 for country in item.get('children', [])}
        unique_country_codes = {ccode for place in serialized_places for ccode in place.get("ccodes", [])}
        countries_with_labels = [{'ccode': ccode, 'label': country_codes_mapping.get(ccode, '')} for ccode in
                                 unique_country_codes]

        # Aggregate places into a single object
        aggregated_place = {
            "id": "-".join(ids),  # Concatenate the IDs,
            "traces": [trace for place in serialized_places for trace in place["traces"]],
            "datasets": sort_unique(list(Dataset.objects.filter(id__in=dataset_ids).values("id", "title")), 'title'),
            "title": "|".join(set(place["title"] for place in serialized_places)),
            "names": sort_unique([name for place in serialized_places for name in place["names"]], 'toponym'),
            "types": add_urls(sort_unique([type for place in serialized_places for type in place["types"]], 'label')),
            "fclasses": [
                {"code": fclass,
                 "description": next((description for code, description in FEATURE_CLASSES if code == fclass),
                                     "Unknown")}
                for place in serialized_places
                if place.get("fclasses")  # Ensure fclasses is not None or empty
                for fclass in place["fclasses"]
            ],
            "geoms": [geom for place in serialized_places for geom in place["geoms"]],
            "extent": aggregated_extent,
            "links": add_urls(
                sort_unique([link for place in serialized_places for link in place["links"]], sort_key='identifier')),
            "related": sort_unique([related for place in serialized_places for related in place["related"]], 'label'),
            "descriptions": add_urls(
                sort_unique([description for place in serialized_places for description in place["descriptions"]],
                            'value')),
            "depictions": sort_unique([depiction for place in serialized_places for depiction in place["depictions"]]),
            "minmax": [min_value, max_value],
            "countries": sorted(countries_with_labels, key=lambda x: x['ccode']),
            "attestation_years": sorted(attestation_years),
            # "whens": {
            #     "timespans": sort_unique([timespan for place in serialized_places for when in place.get("whens", []) for timespan in when.get("timespans", [])]),
            #     "periods": sort_unique([period for place in serialized_places for when in place.get("whens", []) for period in when.get("periods", [])]),
            #     "label": "|".join(set(when.get("label", "") for place in serialized_places for when in place.get("whens", []) if when.get("label", ""))),
            #     "duration": "|".join(set(when.get("duration", "") for place in serialized_places for when in place.get("whens", []) if when.get("duration", ""))),
            # },
        }

        return JsonResponse(aggregated_place, safe=False)


"""
    place_compare/<int:pk>/
    uses: ds_update()
    "partial database record by place_id for update comparisons"
"""


@extend_schema(exclude=True)
class PlaceCompareAPIView(generics.RetrieveAPIView):
    """  returns single serialized database place record by id  """
    queryset = Place.objects.all()
    serializer_class = PlaceCompareSerializer
    renderer_classes = [PrettyJsonRenderer]

    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly]
    authentication_classes = [SessionAuthentication]


"""
    place/<str:dslabel>/<str:src_id>/
    published record by dataset label and src_id
"""


@extend_schema(exclude=True)
class PlaceDetailSourceAPIView(generics.RetrieveAPIView):
    """  single database place record by src_id  """
    queryset = Place.objects.all()
    serializer_class = PlaceSerializer
    renderer_classes = [PrettyJsonRenderer]

    lookup_field = 'src_id'
    permission_classes = [permissions.IsAuthenticatedOrReadOnly, IsOwnerOrReadOnly]
    authentication_classes = [SessionAuthentication]


"""
    /api/geoms?ds={{ ds.label }}}
    /api/geoms?coll={{ coll.id }}}
    in ds_browse and ds_places for all geoms if < 15k
    TODO: this needs refactor (make collection.geometries @property?)
"""


@extend_schema(exclude=True)
class GeomViewSet(viewsets.ModelViewSet):
    queryset = PlaceGeom.objects.all()
    serializer_class = PlaceGeomSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get_queryset(self):
        qs = PlaceGeom.objects.none()  # Initialize with an empty queryset

        if 'ds' in self.request.GET:
            dslabel = self.request.GET.get('ds')
            try:
                # Ensure the dataset exists
                ds = Dataset.objects.get(label=dslabel)
                # Directly get the place IDs for the given dataset
                dsPlaceIds = Place.objects.filter(dataset=ds).values_list('id', flat=True)
                if not dsPlaceIds:
                    return qs  # Return an empty queryset if no place IDs are found
                qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
            except Dataset.DoesNotExist:
                return qs  # Return an empty queryset if the dataset does not exist

        elif 'coll' in self.request.GET:
            cid = self.request.GET.get('coll')
            try:
                coll = Collection.objects.get(id=cid)
                collPlaceIds = [p.id for p in coll.places.all()]
                if not collPlaceIds:
                    return qs  # Return an empty queryset if no place IDs are found
                qs = PlaceGeom.objects.filter(
                    place_id__in=collPlaceIds,
                    jsonb__type__icontains='Point'
                )
            except Collection.DoesNotExist:
                return qs  # Return an empty queryset if the collection does not exist

        return qs


"""
    /api/geojson/{{ ds.id }}
"""


# class GeoJSONViewSet(viewsets.ModelViewSet):
@extend_schema(exclude=True)
class GeoJSONAPIView(generics.ListAPIView):
    # use: api/geojson
    # queryset = PlaceGeom.objects.all()
    # serializer_class = GeoJsonSerializer
    serializer_class = FeatureSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    def get_queryset(self, format=None, *args, **kwargs):
        if 'id' in self.request.GET:
            dsid = self.request.GET.get('id')
            dslabel = get_object_or_404(Dataset, pk=dsid).label
            dsPlaceIds = Place.objects.values('id').filter(dataset=dslabel)
            qs = PlaceGeom.objects.filter(place_id__in=dsPlaceIds)
        elif 'coll' in self.request.GET:
            cid = self.request.GET.get('coll')
            coll = Collection.objects.get(id=cid)
            collPlaceIds = [p.id for p in coll.places.all()]
            qs = PlaceGeom.objects.filter(place_id__in=collPlaceIds, jsonb__type='Point')

        else:
            raise ValidationError({'detail': 'Query is missing a required parameter'})
        # print('qs',qs)
        return qs


"""
    /api/featureCollection/{{ ds.id  or coll.cid}}
"""


@extend_schema(exclude=True)
class featureCollectionAPIView(generics.ListAPIView):
    permission_classes = (permissions.IsAuthenticatedOrReadOnly,)

    @extend_schema(  # Not intended as a public API
        exclude=True,
    )
    def get(self, format=None, *args, **kwargs):

        mode = self.request.GET.get('mode', 'default') or 'default'
        featureCollection = None

        if 'id' in self.request.GET:
            dsid = self.request.GET.get('id')
            datacollection = get_object_or_404(Dataset, pk=dsid)
            pass
        elif 'coll' in self.request.GET:
            cid = self.request.GET.get('coll')
            datacollection = get_object_or_404(Collection, id=cid)
            pass
        else:
            return Response({"error": "QueryString must include either an id or coll identifier"},
                            status=status.HTTP_400_BAD_REQUEST)

        if mode == 'clusterhull':
            featureCollection = datacollection.clustered_geometries
            pass
        elif mode == 'heatmap':
            featureCollection = datacollection.heatmapped_geometries
            pass
        elif mode == 'convexhull':
            featureCollection = datacollection.hull_geometries
            pass
        elif mode == 'default':
            featureCollection = datacollection.feature_collection
            pass
        else:
            return Response({"error": "Invalid QueryString"}, status=status.HTTP_400_BAD_REQUEST)

        return JsonResponse(featureCollection, content_type="application/json")


"""
    populates drf table in ds_browse.html, ds_places.html
"""


@extend_schema(exclude=True)
class PlaceTableViewSet(viewsets.ModelViewSet):
    # print('hit PlaceTableViewSet()')
    serializer_class = PlaceTableSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly)

    """
      q: query string
      ds: dataset
    """

    def get_queryset(self):
        ds = get_object_or_404(Dataset, label=self.request.GET.get('ds'))
        # qs = ds.places.all().order_by('place_id')
        qs = ds.places.all().order_by('id')
        # qs = ds.places.all()
        query = self.request.GET.get('q')
        if query is not None:
            qs = qs.filter(title__istartswith=query)
        return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action in ['list', 'retrieve']:
            permission_classes = [permissions.AllowAny]
        else:
            permission_classes = [permissions.IsAdminUser]
        return [permission() for permission in permission_classes]


"""
    populates drf table in collection.collection_places.html
"""


@extend_schema(exclude=True)
class PlaceTableCollViewSet(viewsets.ModelViewSet):
    serializer_class = PlaceTableSerializer
    permission_classes = (permissions.IsAuthenticatedOrReadOnly)

    """
      q: query string
      coll: collection
    """

    def get_queryset(self):
        # print('PlaceTableCollViewSet() path', self.request.META['PATH_INFO'])
        # /api/placetable_coll/
        # a q value if it's a search on the table
        query = self.request.GET.get('q')
        id_ = self.request.GET.get('id')
        from django.db.models import Min
        coll = get_object_or_404(Collection, id=id_)
        if coll.collection_class == 'dataset':
            qs = coll.places_all
        else:
            qs = coll.places.annotate(seq=Min('annos__sequence')).order_by('seq')
        # qs = coll.places.annotate(seq=Min('collplace__sequence')).order_by('seq')
        # print('qs from PlaceTableCollViewSet()', qs)
        if query is not None:
            qs = qs.filter(title__istartswith=query)
        return qs

    def get_permissions(self):
        """
        Instantiates and returns the list of permissions that this view requires.
        """
        if self.action in ['list', 'retrieve']:
            permission_classes = [permissions.AllowAny]
        else:
            permission_classes = [permissions.IsAdminUser]
        return [permission() for permission in permission_classes]


"""
  areas/

"""


# simple objects for dropdown
class AreaListView(View):
    @staticmethod
    def get(request):
        # print('area_list() request.user',request.user, type(request.user))
        # print('area_list() request.user',str(request.user))
        userstr = str(request.user)
        if isinstance(request.user, AnonymousUser):
            qs = Area.objects.all().filter(Q(type__in=('predefined', 'country'))).values('id', 'title', 'type')
        else:
            user = request.user
            qs = Area.objects.all().filter(Q(type__in=('predefined', 'country')) | Q(owner=user)).values('id', 'title',
                                                                                                         'type')
        area_list = []
        for a in qs:
            area = {"id": a['id'], "title": a['title'], "type": a['type']}
            area_list.append(area)

        return JsonResponse(area_list, safe=False)


# simple objects for dropdown
class AreaListAllView(View):
    @staticmethod
    def get(request):
        user = request.user
        area_list = []
        # qs = Area.objects.all().filter(Q(type='predefined')| Q(owner=request.user)).values('id','title','type')
        qs = Area.objects.all().filter(Q(type__in=('predefined', 'country')) | Q(owner=request.user)).values('id',
                                                                                                             'title',
                                                                                                             'type')
        for a in qs:
            area = {"id": a['id'], "title": a['title'], "type": a['type']}
            area_list.append(area)

        return JsonResponse(area_list, safe=False)


"""
    area/<int:pk>/
    in dataset.html#addtask
"""


@extend_schema(exclude=True)
class AreaViewSet(viewsets.ModelViewSet):
    queryset = Area.objects.all().order_by('title')
    serializer_class = AreaSerializer


"""
    regions/
    in dataset.html#addtask
"""


class RegionViewSet(View):
    queryset = Area.objects.filter(
        description='UN Statistical Division Sub-Region').order_by('title')
    serializer_class = AreaSerializer


# Country Geometries from ccode list
@extend_schema(exclude=True)
class CountryFeaturesAPIView(View):
    def get(self, request, *args, **kwargs):
        country_codes_param = self.request.GET.get('country_codes', '')
        country_codes = [code.strip().upper() for code in country_codes_param.split(',') if code.strip()]

        countries = Area.objects.filter(Q(type='country') & Q(ccodes__overlap=country_codes)).values('id', 'ccodes',
                                                                                                     'geojson')

        country_feature_collection = {
            'type': 'FeatureCollection',
            'features': []
        }

        for country in countries:
            feature = {
                'type': 'Feature',
                'properties': {'ccode': country['ccodes'][0]},
                'geometry': country['geojson']
            }
            country_feature_collection['features'].append(feature)

        return JsonResponse(country_feature_collection, safe=False)


# Fetch Watershed GeoJSON from remote URL
@extend_schema(exclude=True)
class WatershedAPIView(APIView):
    def get(self, request, *args, **kwargs):
        lat = request.query_params.get('lat', None)
        lng = request.query_params.get('lng', None)

        if lat is None or lng is None:
            raise Http404("Latitude and longitude parameters are required.")

        # Construct the URL to fetch watershed GeoJSON
        watershed_url = f"https://mghydro.com/app/watershed_api?lat={lat}&lng={lng}&precision=high"

        try:
            response = requests.get(watershed_url)
            response.raise_for_status()  # Raise an exception for non-200 status codes
            watershed_geojson = response.json()

            # Add attribution
            attribution = "Watershed: MERIT Hydro & Matthew Heberger"
            watershed_geojson['attribution'] = attribution

        except requests.RequestException as e:
            # Handle request errors
            raise Http404(f"Error fetching watershed GeoJSON: {str(e)}")

        return Response(watershed_geojson)
