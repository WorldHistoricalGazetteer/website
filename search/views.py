# various search.views

import logging
from datetime import datetime

import simplejson as json
import sys
from django.conf import settings
from django.http import JsonResponse
from django.views.generic import View
from django.views.generic.base import TemplateView

from api.reconcile import DOCS_URL
from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset
from datasets.tasks import get_bounds_filter
from places.models import Place, PlaceGeom
from sitemap.models import Toponym
from utils.regions_countries import get_regions_countries
from whg.settings import STANDARD_FIELDS

logger = logging.getLogger(__name__)


# new
class SearchPageView(TemplateView):
    template_name = 'search/search.html'

    def get(self, request, *args, **kwargs):
        if request.GET.get('filter'):
            return JsonResponse({
                "result": [],
                "message": "OpenRefine legacy search call: no results. Use /suggest/entity endpoint instead. See documentation: " + DOCS_URL
            })

        return super().get(request, *args, **kwargs)

    def get_context_data(self, *args, **kwargs):
        # return list of datasets
        dslist = Dataset.objects.filter(public=True)

        context = super(SearchPageView, self).get_context_data(*args, **kwargs)

        context['toponym'] = kwargs.get('toponym', None)

        if context['toponym']:
            toponym_qs = Toponym.objects.filter(name=context['toponym'])

            if toponym_qs.exists():
                toponym = toponym_qs.first()

                yearspans = toponym.yearspans
                if yearspans:
                    earliest_date = min(span[0] for span in yearspans)
                    latest_date = max(span[1] for span in yearspans)
                else:
                    earliest_date, latest_date = None, None  # Handle case with no dates

                context['unique_countries'] = toponym.ccodes
                context['yearspans'] = yearspans
                context['earliest_date'] = earliest_date
                context['latest_date'] = latest_date
            else:
                # Handle the case where the toponym does not exist in the database table
                context['toponym'] = None

        context['media_url'] = settings.MEDIA_URL
        context['dslist'] = dslist
        context['search_params'] = self.request.session.get('search_params')
        context['es_whg'] = settings.ES_WHG
        # context['bboxes'] = bboxes
        context['dropdown_data'] = get_regions_countries()  # Used for spatial filter

        # Human-friendly index scale for the landing blurb
        context['index_places'] = getattr(settings, 'INDEX_PLACES_LABEL', 'millions of')
        context['index_toponyms'] = getattr(settings, 'INDEX_TOPONYMS_LABEL', 'millions of')


        user_areas = []
        if self.request.user.is_authenticated:
            qs = Area.objects.filter(owner=self.request.user, type__in=['ccodes', 'copied', 'drawn']).values('id',
                                                                                                             'title',
                                                                                                             'type',
                                                                                                             'description',
                                                                                                             'geojson')
            for a in qs:
                feature = {
                    "type": "Feature",
                    "properties": {"id": a['id'], "title": a['title'], "type": a['type'],
                                   "description": a['description']},
                    "geometry": a['geojson']
                }
                user_areas.append(feature)

        context['has_areas'] = len(user_areas) > 0
        context['user_areas'] = user_areas

        return context


def fetchArea(request):
    aid = request.GET.get('pk')
    area = Area.objects.filter(id=aid)
    return JsonResponse(area)


def makeGeom(pid, geom):
    # TODO: account for non-point
    geomset = []
    if len(geom) > 0:
        for g in geom:
            geomset.append(
                {"type": g['location']['type'],
                 "coordinates": g['location']['coordinates'],
                 "properties": {"pid": pid}}
            )
    return geomset


"""
  format search result items
"""


def suggestionItem(s):
    h = s['hit']
    unique_children = list(set(h['children']))
    timespans = []
    for span in h.get('timespans', []):
        if 'gte' in span and 'lte' in span:
            timespans.append([span['gte'], span['lte']])
        else:
            timespans.append(span)

    item = {
        "whg_id": h.get('whg_id', ''),
        "pid": h['place_id'],
        "index": s['_index'],
        "children": unique_children,
        "linkcount": s['linkcount'],
        "title": h['title'],
        "variants": [n for n in h['searchy'] if n != h['title']],
        "ccodes": h['ccodes'],
        # TODO: 'label' is an AAT value; sourceLabel is probably preferred if available
        "types": [t['label'] for t in h.get('types', []) if 'label' in t],
        "geom": makeGeom(h['place_id'], h['geoms']),
        "timespans": timespans
    }
    return item


"""
  performs the ES search of 'whg' and 'pub'
"""


def suggester(q, indices):
    # print('suggester q', q)
    # print('suggester indices', indices)
    try:
        es = settings.ES_CONN
    except:
        logger.debug(f'ES query failed: {sys.exc_info()}')

    suggestions = []

    try:
        # Search across multiple indices
        res = es.search(index=','.join(indices), body=q)
        hits = res['hits']['hits']
        if len(hits) > 0:
            for h in hits:
                suggestions.append(
                    {
                        "_id": h['_id'],
                        "_index": h['_index'],
                        "_score": h.get('_score', 0) or 0,
                        "linkcount": len(set(h['_source']['children'])),
                        "hit": h['_source'],
                        "timespans": h['_source'].get('timespans', [])
                    }
                )
    except Exception as e:
        logger.debug(f"ES query failed: {e}")
        return []

    # Sort primarily by ES relevance score, then by linkcount as tiebreaker
    sortedsugs = sorted(suggestions, key=lambda x: (x['_score'], x['linkcount']), reverse=True)
    # print('sortedsugs', sortedsugs)
    # TODO: there may be parents and children
    return sortedsugs


def TypeaheadSuggestions(request):
    """
    Typeahead suggestions using SearchViewV3 query building logic
    """
    q = request.GET.get('q', '')
    mode = request.GET.get('mode', 'default')

    if not q:
        return JsonResponse([], safe=False)

    # Build minimal params for SearchViewV3
    params = {
        "qstr": q,
        "mode": mode,
    }

    # Use SearchViewV3's query builder
    query_body = SearchViewV3.build_search_query(params)
    query_body["size"] = 20  # Limit typeahead results

    es = settings.ES_CONN
    indices = [settings.ES_WHG, settings.ES_PUB]

    try:
        res = es.search(index=','.join(indices), body=query_body)
        hits = res['hits']['hits']

        # Extract unique titles for typeahead
        unique_titles = list({
            hit['_source']['title']
            for hit in hits
            if 'title' in hit['_source']
        })

        return JsonResponse(unique_titles, safe=False)

    except Exception as e:
        logger.error(f"Typeahead ES query failed: {e}")
        return JsonResponse([], safe=False)


class SearchViewV3(View):
    """
    /search/index/?
    Performs Elastic search with extended parameters.
    """

    @staticmethod
    def build_search_query(params):
        qstr = params["qstr"]
        fields = STANDARD_FIELDS
        # Lexical search component (affects score)
        search_query = {"multi_match": {"query": qstr, "fields": fields, "fuzziness": "AUTO"}}

        # Initialize the query with the lexical search in the 'must' clause
        q = {
            "size": 100,
            "query": {
                "bool": {
                    "must": [
                        search_query
                    ],
                    "filter": [
                        # Required for all records
                        {"exists": {"field": "whg_id"}}
                    ]
                }
            }
        }

        # Reference the filter list for easier manipulation
        filters = q['query']['bool']['filter']

        # --- 1. AAT TYPE CATEGORIES ---
        # Accept "types" (new) or "fclasses" (legacy) — both carry AAT identifiers
        type_param = params.get("types") or params.get("fclasses")
        if type_param:
            if isinstance(type_param, list):
                type_ids = [t.strip() for t in type_param if t.strip()]
            else:
                type_ids = [t.strip() for t in type_param.split(',') if t.strip()]
            if type_ids:
                filters.append({"terms": {"types.identifier": type_ids}})

        # --- 2. TEMPORAL FILTERS ---
        if params.get("temporal"):
            current_year = datetime.now().year
            start_year = str(params["start"])
            end_year = str(params.get("end", current_year))
            temporal_range = {"range": {"timespans": {"gte": start_year, "lte": end_year}}}

            if params.get("undated"):
                filters.append({
                    "bool": {"should": [temporal_range, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                filters.append(temporal_range)

        # --- 3. COUNTRY CODES ---
        if params.get("countries"):
            countries = params["countries"]
            filters.append({"terms": {"ccodes": countries}})

        # --- 4. GEOMETRY FILTERS (Bounds/User Areas) ---
        geometry_filters = []

        if params.get("bounds"):
            bounds = params["bounds"]["geometries"]
            for geometry in bounds:
                geometry_filters.append({
                    "geo_shape": {
                        "geoms.location": {
                            "shape": {
                                "type": geometry['type'],
                                "coordinates": geometry['coordinates']
                            },
                            "relation": "intersects"
                        }
                    }
                })

        if params.get("userareas"):
            userareas = params["userareas"]
            for userarea_id in userareas:
                # Fetch user area by ID (assuming Area model is available)
                user_area = Area.objects.filter(id=userarea_id).values('geojson').first()
                if user_area:
                    geometry_filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": user_area['geojson'],
                                "relation": "intersects"
                            }
                        }
                    })

        if len(geometry_filters) > 0:
            # Geometry constraints are combined using a strict 'should' clause (must match at least one area)
            filters.append({"bool": {"should": geometry_filters, "minimum_should_match": 1}})

        return q

    @staticmethod
    def handle_request(request):
        """
        args in request.POST:
            [string] qstr: query string
            [string] mode: search mode (starts, in, fuzzy)
            [string] idx: index to be queried
            [string] fclasses: filter on AAT type identifiers (comma-separated)
            [string] temporal: text of boolean for consideration of temporal parameters
            [string] start: filter for timespans
            [string] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
            [string] userareas: text of JSON userareas array
        """

        if request.method == 'GET':
            return JsonResponse({'error': 'GET requests are not allowed for this endpoint'}, status=400)

        qstr = request.POST.get('qstr')
        idx = request.POST.get('idx') or settings.ES_WHG
        fclasses = request.POST.get('fclasses')
        temporal = request.POST.get('temporal')
        start = request.POST.get('start')
        end = request.POST.get('end')
        undated = request.POST.get('undated')
        bounds = request.POST.get('bounds')
        spatial = request.POST.get('spatial')
        regions = request.POST.get('regions')
        countries = request.POST.get('countries')
        userareas = request.POST.get('userareas')
        mode = request.POST.get('mode')

        params = {
            "qstr": qstr,
            "idx": idx,
            "fclasses": fclasses,
            "temporal": temporal,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "spatial": spatial,  # Spatial search mode (pass-through as not used in search)
            "regions": regions,  # Array of region codes (pass-through as not used in search)
            "countries": countries,  # Array of country codes
            "userareas": userareas,  # Array of userarea ids
            "method": request.method,
            "mode": mode
        }
        request.session["search_params"] = params

        if params.get("fclasses") == "":  # Return empty result if no feature classes are given
            result = {'parameters': params, 'suggestions': []}
        else:
            q = SearchViewV3.build_search_query(params)
            suggestions = suggester(q, [idx, 'pub'])
            suggestions = [suggestionItem(s) for s in suggestions]
            result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def post(self, request):
        # Attempt to parse and decode any JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge any JSON data with request.POST
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        return self.handle_request(request)


class SearchView(View):
    @staticmethod
    def handle_request(request):
        # gets calling ip, useful for django-debug-toolbar
        """
          args in request.GET:
            [string] qstr: query string
            # [string] doc_type: place or trace
            # [string] scope: suggest or search
            [string] idx: index to be queried
            [int] year: filter for timespans including this
            [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
            [int] start: filter for timespans
            [int] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
            [string] userareas: text of JSON userareas array
        """
        qstr = request.GET.get('qstr') or request.POST.get('qstr')
        mode = request.GET.get('mode') or request.POST.get('mode')
        idx = settings.ES_WHG
        fclasses = request.GET.get('fclasses') or request.POST.get('fclasses')
        temporal = request.GET.get('temporal') or request.POST.get('temporal')
        start = request.GET.get('start') or request.POST.get('start')
        end = request.GET.get('end') or request.POST.get('end')
        undated = request.GET.get('undated') or request.POST.get('undated')
        bounds = request.GET.get('bounds') or request.POST.get('bounds')
        countries = request.GET.get('countries') or request.POST.get('countries')
        userareas = request.GET.get('userareas') or request.POST.get('userareas')

        params = {
            "qstr": qstr,
            "mode": mode,
            "idx": idx,
            "fclasses": fclasses,
            "temporal": temporal,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "countries": countries,  # Array of country codes
            "userareas": userareas  # Array of userarea codes
        }
        request.session["search_params"] = params

        q = {"size": 100,
             "query": {"bool": {
                 "must": [
                     {"exists": {"field": "whg_id"}},
                     {"multi_match": {
                         "query": qstr,
                         "fields": ["title^3", "names.toponym", "searchy"]
                     }}
                 ]
             }}}

        if fclasses:
            fclist = fclasses.split(',')
            fclist.append('X')
            q['query']['bool']['must'].append({"terms": {"fclasses": fclist}})

        if start:
            timespan_filter = {"range": {"timespans": {"gte": start, "lte": end if end else 2005}}}
            if undated:  # Include records with empty timespans
                q['query']['bool']['must'].append({
                    "bool": {"should": [timespan_filter, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                q['query']['bool']['must'].append(timespan_filter)

        if bounds:
            if request.method == 'GET':
                bounds = json.loads(bounds)
                # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
                q['query']['bool']["filter"] = get_bounds_filter(bounds, settings.ES_WHG)
            elif request.method == 'POST' and len(bounds['geometries']) > 0:
                filters = []
                for geometry in bounds['geometries']:
                    filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": {
                                    "type": geometry['type'],
                                    "coordinates": geometry['coordinates']
                                },
                                "relation": "intersects"
                            }
                        }
                    })
                q['query']['bool']["filter"] = {"bool": {"should": filters}}

        if countries:
            if request.method == 'GET':
                countries = json.loads(countries)
            q['query']['bool']['must'].append({
                "terms": {
                    "ccodes": countries
                }
            })

        suggestions = suggester(q, [idx, 'pub'])
        suggestions = [suggestionItem(s) for s in suggestions]
        # print('suggestions', suggestions)
        # return query params for ??
        result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def get(self, request):
        return self.handle_request(request)

    def post(self, request):
        # Extract JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge JSON data with request.POST for compatibility with existing code
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        # Call the handle_request method
        return self.handle_request(request)


# speculative, adding mode to search
class SearchViewNew(View):
    @staticmethod
    def build_search_query(params, search_type):
        qstr = params["qstr"]
        fields = ["title^3", "names.toponym", "searchy"]

        # Default search query
        search_query = {"multi_match": {
            "query": qstr,
            "fields": fields
        }}

        # Modify search query based on search_type
        if search_type == "starts":
            search_query = {"bool": {"should": [{"prefix": {field: qstr}} for field in fields]}}
        elif search_type == "in":
            search_query = {"bool": {"should": [{"wildcard": {field: f"*{qstr}*"}} for field in fields]}}
        elif search_type == "fuzzy":
            search_query = {"multi_match": {
                "query": qstr,
                "fields": fields,
                "fuzziness": 2
            }}

        # Construct the full query with additional filters
        q = {
            "size": 100,
            "query": {"bool": {
                "must": [
                    {"exists": {"field": "whg_id"}},
                    search_query
                ]
            }}
        }

        if params.get('fclasses'):
            fclist = params.fclasses.split(',')
            fclist.append('X')
            q['query']['bool']['must'].append({"terms": {"fclasses": fclist}})

        if params.get('start'):
            timespan_filter = {"range": {"timespans": {"gte": params.start, "lte": params.end if params.end else 2005}}}
            if params.undated:  # Include records with empty timespans
                q['query']['bool']['must'].append({
                    "bool": {"should": [timespan_filter, {"bool": {"must_not": {"exists": {"field": "timespans"}}}}]}
                })
            else:
                q['query']['bool']['must'].append(timespan_filter)

        if params.get('bounds'):
            if params.request.method == 'GET':
                bounds = json.loads(params.bounds)
                # q['query']['bool']["filter"] = get_bounds_filter(bounds, 'whg')
                q['query']['bool']["filter"] = get_bounds_filter(bounds, settings.ES_WHG)
            elif params.request.method == 'POST' and len(params.bounds['geometries']) > 0:
                filters = []
                for geometry in params.bounds['geometries']:
                    filters.append({
                        "geo_shape": {
                            "geoms.location": {
                                "shape": {
                                    "type": geometry['type'],
                                    "coordinates": geometry['coordinates']
                                },
                                "relation": "intersects"
                            }
                        }
                    })
                q['query']['bool']["filter"] = {"bool": {"should": filters}}

        if params.get('countries'):
            if params.request.method == 'GET':
                countries = json.loads(params.countries)
            q['query']['bool']['must'].append({
                "terms": {
                    "ccodes": countries
                }
            })

        return q

    @staticmethod
    def handle_request(request):
        """
          args in request.GET:
            [string] qstr: query string
            # [string] doc_type: place or trace
            # [string] scope: suggest or search
            [string] idx: index to be queried
            [int] year: filter for timespans including this
            [string[]] fclasses: filter on geonames class (A,H,L,P,S,T)
            [int] start: filter for timespans
            [int] end: filter for timespans
            [string] undated: text of boolean for inclusion of undated results
            [string] bounds: text of JSON geometry
            [string] countries: text of JSON cccodes array
        """
        qstr = request.GET.get('qstr') or request.POST.get('qstr')
        idx = settings.ES_WHG
        fclasses = request.GET.get('fclasses') or request.POST.get('fclasses')
        start = request.GET.get('start') or request.POST.get('start')
        end = request.GET.get('end') or request.POST.get('end')
        undated = request.GET.get('undated') or request.POST.get('undated')
        bounds = request.GET.get('bounds') or request.POST.get('bounds')
        countries = request.GET.get('countries') or request.POST.get('countries')
        mode = request.GET.get('mode') or request.POST.get('mode')

        params = {
            "qstr": qstr,
            "idx": idx,
            "fclasses": fclasses,
            "start": start,
            "end": end,
            "undated": undated,
            "bounds": bounds,
            "countries": countries,  # Array of country codes
            "method": request.method,
            "mode": mode
        }
        request.session["search_params"] = params

        q = SearchViewNew.build_search_query(params, params.get('mode', 'default'))
        suggestions = suggester(q, [idx, 'pub'])
        suggestions = [suggestionItem(s) for s in suggestions]
        # return query params for ??
        result = {'parameters': params, 'suggestions': suggestions}

        return JsonResponse(result, safe=False)

    def get(self, request):
        return self.handle_request(request)

    def post(self, request):
        # Extract JSON data from the request body
        try:
            json_data = json.loads(request.body.decode('utf-8'))
        except json.JSONDecodeError:
            return JsonResponse({'error': 'Invalid JSON data'}, status=400)

        # Merge JSON data with request.POST for compatibility with existing code
        request.POST = request.POST.copy()
        request.POST.update(json_data)

        # Call the handle_request method
        return self.handle_request(request)


"""
  executes search on db.places /search/db
"""


class SearchDatabaseView(View):
    @staticmethod
    def get(request):
        pagesize = 200
        """
          args in request.GET:
            [string] name: query string
            [string] fclasses: geonames class (A,H,L,P,S,T)
            [int] year: within place.minmax timespan
            [string] bounds: text of JSON geometry
            [int] dsid: dataset.id
            
        """
        name = request.GET.get('name')
        name_contains = request.GET.get('name_contains') or None
        fclasses = request.GET.get('fclasses').split(',')
        year = request.GET.get('year')
        bounds = request.GET.get('bounds')
        dsid = request.GET.get('dsid')
        ds = Dataset.objects.get(pk=int(dsid)) if dsid else None

        from django.contrib.gis.geos import GEOSGeometry
        if bounds:
            bounds = json.loads(bounds)
            area = Area.objects.get(id=bounds['id'][0])
            ga = GEOSGeometry(json.dumps(area.geojson))

        qs = Place.objects.filter(dataset__public=True)

        if bounds:
            qs = qs.filter(geoms__geom__within=ga)

        if fclasses and len(fclasses) < 7:
            qs.filter(fclasses__overlap=fclasses)

        if name_contains:
            qs = qs.filter(title__icontains=name_contains)
        elif name and name != '':
            # qs = qs.filter(title__istartswith=name)
            qs = qs.filter(names__jsonb__toponym__istartswith=name).distinct()

        qs = qs.filter(dataset=ds.label) if ds else qs
        count = len(qs)

        filtered = qs[:pagesize]

        # normalizes place.geoms objects for results display
        def dbsug_geoms(pobjs):
            suglist = []
            for p in pobjs:
                g = p.jsonb
                if 'citation' in g: del g['citation']
                g['src'] = 'db'
                g["properties"] = {"pid": p.place_id, "title": p.title}
                suglist.append(g)
            return suglist

        # mimics suggestion items from SearchView (index)
        suggestions = []
        for place in filtered:
            ds = place.dataset
            try:
                suggestions.append({
                    "pid": place.id,
                    "ds": {"id": ds.id, "label": ds.label, "title": ds.title},
                    "name": place.title,
                    "variants": [n.jsonb['toponym'] for n in place.names.all()],
                    "ccodes": place.ccodes,
                    "fclasses": place.fclasses,
                    "types": [t.jsonb['sourceLabel'] or t.jsonb['src_label'] for t in place.types.all()],
                    "geom": dbsug_geoms(place.geoms.all())
                })
            except:
                logger.debug(f"db sugbuilder error: {place.id} {sys.exc_info()}")

        result = {'get': request.GET, 'count': count, 'suggestions': suggestions}
        return JsonResponse(result, safe=False, json_dumps_params={'ensure_ascii': False})


'''
  returns 8000 index docs in current map viewport
  OR if task == 'count': count of features in area
'''


def contextSearch(idx, doctype, q, task):
    es = settings.ES_CONN
    count_hits = 0
    result_obj = {"hits": []}
    # TODO: convert calling map(s) to MapLibre.js to handle large datasets
    if task == 'count':
        res = es.count(index=idx, body=q)
        return {'count': res['count']}
    elif task == 'features':
        res = es.search(index=idx, body=q, size=8000)
    hits = res['hits']['hits']
    # TODO: refactor this bit
    if len(hits) > 0:
        # print('contextSearch() hit0 _source: ',hits[0]["_source"])
        for hit in hits:
            count_hits += 1
            if idx.startswith("whg"):
                # why normalize here?
                result_obj["hits"].append(hit['_source'])
            else:
                # this is traces
                result_obj["hits"].append(hit["_source"]['body'])
    result_obj["count"] = count_hits
    return result_obj


class FeatureContextView(View):
    @staticmethod
    def get(request):
        """
        args in request.GET:
            [string] idx: index to be queried
            [string] extent: geometry to intersect
            [string] doc_type: 'place' in this case
            [string] task: 'features' or 'count'
        """
        idx = request.GET.get('idx')
        extent = request.GET.get('extent')  # coordinates string
        doctype = request.GET.get('doc_type')
        task = request.GET.get('task')
        q_context_all = {"query": {
            "bool": {
                "must": [{"match_all": {}}],
                "filter": {"geo_shape": {
                    "geoms.location": {
                        "shape": {
                            "type": "polygon",
                            "coordinates": json.loads(extent)
                        },
                        "relation": "within"
                    }
                }}
            }
        }}
        response = contextSearch(idx, doctype, q_context_all, task)
        return JsonResponse(response, safe=False)


''' 
  Returns places in a trace body
'''


def getGeomCollection(idx, doctype, q):
    # q includes list of place_ids from a trace record
    es = settings.ES_CONN
    # try:
    # res = es.search(index='whg', body=q, size=300)
    res = es.search(index=settings.ES_WHG, body=q, size=300)
    # res = es.search(index='whg', body=q, size=300)
    # except:
    # print(sys.exc_info()[0])
    hits = res['hits']['hits']
    # geoms=[]
    collection = {"type": "FeatureCollection", "feature_count": len(hits), "features": []}
    for h in hits:
        if len(h['_source']['geoms']) > 0:
            # print('hit _source from getGeomCollection',h['_source'])
            collection['features'].append(
                {"type": "Feature",
                 "geometry": h['_source']['geoms'][0]['location'],
                 "properties": {
                     "title": h['_source']['title']
                     , "place_id": h['_source']['place_id']
                     , "whg_id": h['_id']
                 }
                 }
            )
    # print(str(len(collection['features']))+' features')
    return collection


class CollectionGeomView(View):
    @staticmethod
    def get(request):
        # print('CollectionGeomView GET:',request.GET)
        """
        args in request.GET:
            [string] coll_id: collection to be queried
        """
        coll_id = request.GET.get('coll_id')
        coll = Collection.objects.get(id=coll_id)
        pids = [p.id for p in coll.places_all]
        placegeoms = PlaceGeom.objects.filter(place_id__in=pids)
        features = [{"type": "Feature",
                     "geometry": pg.jsonb,
                     "properties": {"pid": pg.place_id, "title": pg.title}
                     } for pg in placegeoms]
        fcoll = {"type": "FeatureCollection", "features": features}

        return JsonResponse(fcoll, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})


class TraceGeomView(View):
    @staticmethod
    def get(request):
        # print('TraceGeomView GET:',request.GET)
        """
        args in request.GET:
            [string] idx: index to be queried
            [string] search: whg_id
            [string] doc_type: 'trace' in this case
        """
        idx = request.GET.get('idx')
        trace_id = request.GET.get('search')
        doctype = request.GET.get('doc_type')
        q_trace = {"query": {"bool": {"must": [{"match": {"_id": trace_id}}]}}}

        # using contextSearch() to get bodyids (i.e. place_ids)
        bodies = contextSearch(idx, doctype, q_trace, 'features')['hits'][0]

        bodyids = [b['place_id'] for b in bodies if b['place_id']]
        q_geom = {"query": {"bool": {"must": [{"terms": {"place_id": bodyids}}]}}}
        geoms = getGeomCollection(idx, doctype, q_geom)
        geoms['bodies'] = bodies

        return JsonResponse(geoms, safe=False, json_dumps_params={'ensure_ascii': False, 'indent': 2})
        # return JsonResponse(geoms, safe=False)


class BoundarySearchView(View):
    """
    Search the `boundaries` ES index for admin regions by name.

    Searches across name, name_local, and alt_names.* fields so that
    regions can be found by their name in any language.

    GET parameters:
        q           — name prefix (min 2 chars)
        admin_level — exact admin level (0-10), optional
        namespace   — 'osm', 'ohm', or comma-separated e.g. 'osm,m49'; optional
        limit       — max results (default 20, max 50)

    Returns JSON: { results: [ {id, name, name_local, admin_level,
                                 namespace, bounds, repr_point, ccodes} ] }
    """

    BOUNDARIES_INDEX = 'boundaries'

    def get(self, request):
        q = request.GET.get('q', '').strip()
        admin_level = request.GET.get('admin_level')
        namespace = request.GET.get('namespace', '').strip()
        try:
            limit = min(int(request.GET.get('limit', 20)), 50)
        except (ValueError, TypeError):
            limit = 20

        if not q or len(q) < 2:
            return JsonResponse({'results': []})

        # Search across name, name_local, and all alt_names.* sub-fields
        name_query = {
            'bool': {
                'should': [
                    {'match_phrase_prefix': {'name': {'query': q, 'max_expansions': 20, 'boost': 3}}},
                    {'match_phrase_prefix': {'name_local': {'query': q, 'max_expansions': 20, 'boost': 2}}},
                    {'multi_match': {
                        'query': q,
                        'type': 'phrase_prefix',
                        'fields': ['alt_names.*'],
                        'max_expansions': 20,
                    }},
                ],
                'minimum_should_match': 1,
            }
        }

        must = [name_query]
        if admin_level is not None and admin_level != '':
            try:
                must.append({'term': {'admin_level': int(admin_level)}})
            except (ValueError, TypeError):
                pass

        # Support comma-separated namespace values (e.g. 'osm,m49')
        if namespace:
            ns_values = [v.strip() for v in namespace.split(',') if v.strip()]
            if len(ns_values) == 1:
                must.append({'term': {'namespace': ns_values[0]}})
            elif len(ns_values) > 1:
                must.append({'terms': {'namespace': ns_values}})

        body = {
            'query': {'bool': {'must': must}},
            'size': limit,
            '_source': [
                'boundary_id', 'name', 'name_local', 'admin_level',
                'namespace', 'bounds', 'repr_point', 'ccodes',
            ],
        }

        try:
            es = settings.ES_CONN
            result = es.search(index=self.BOUNDARIES_INDEX, body=body)
        except Exception as exc:
            logger.warning('BoundarySearchView: ES query failed: %s', exc)
            return JsonResponse({'results': [], 'error': str(exc)}, status=200)

        results = []
        for hit in result.get('hits', {}).get('hits', []):
            src = hit['_source']
            results.append({
                'id': src.get('boundary_id', ''),
                'name': src.get('name', ''),
                'name_local': src.get('name_local'),
                'admin_level': src.get('admin_level'),
                'namespace': src.get('namespace', ''),
                'bounds': src.get('bounds'),
                'repr_point': src.get('repr_point'),
                'ccodes': src.get('ccodes', []),
            })

        return JsonResponse({'results': results})

