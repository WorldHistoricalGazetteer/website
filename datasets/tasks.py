# celery tasks for reconciliation and downloads
# align_tgn(), align_wdlocal(), align_idx(), align_whg, make_download
from __future__ import absolute_import, unicode_literals

import ast
import time

from django_celery_results.models import TaskResult
from django.conf import settings
from django.core import serializers
from django.db import transaction, connection
from django.db.models import Q
from django.shortcuts import get_object_or_404
from django.contrib.auth import get_user_model
from django.contrib.auth.models import Group
from django.http import JsonResponse
from django.views.decorators.csrf import csrf_exempt

from celery import shared_task
from celery.result import AsyncResult
from celery.utils.log import get_task_logger
import codecs, csv, datetime, itertools, os, re, sys, zipfile
from copy import deepcopy
from elasticsearch8.helpers import streaming_bulk, bulk, BulkIndexError
from itertools import chain
import pandas as pd
import simplejson as json

from ratelimit import limits, sleep_and_retry

from areas.models import Area
from collection.models import Collection
from datasets.models import Dataset, Hit
from datasets.static.hashes.parents import ccodes as cchash
from datasets.static.hashes.qtypes import qtypes
from elastic.es_utils import makeDoc, build_qobj, profileHit, removeDatasetFromIndex
from datasets.utils import (
    elapsed,
    getQ,
    HitRecord,
    hullify,
    parse_wkt,
    post_recon_update
)
from main.models import Log, DownloadFile
from places.models import Place, PlaceGeom, PlaceLink, PlaceName
from whg.settings import STANDARD_FIELDS
from whgmail.messaging import WHGmail

import logging
import ssl
from elasticsearch8 import Elasticsearch, exceptions, NotFoundError
from django.conf import settings

CALLS_PER_SECOND = 30

logger = get_task_logger(__name__)
es = settings.ES_CONN
User = get_user_model()


@shared_task(name="delete_reconciliation_task")
def delete_reconciliation_task(tid, scope, dsid, user_id):
    """
    Background task to delete reconciliation task results
    """
    logger = logging.getLogger('dataset')
    logger.info(f'Starting deletion of task {tid} with scope {scope}')

    try:
        tr = TaskResult.objects.get(task_id=tid)
    except TaskResult.DoesNotExist:
        logger.error(f"Task with ID {tid} does not exist.")
        return {'status': 'error', 'message': f'Task {tid} not found'}

    auth = tr.task_name[6:]  # extracts 'wdlocal' or 'idx'
    kwargs = ast.literal_eval(tr.task_kwargs.strip('"'))
    test = kwargs.get("test", "off")

    try:
        ds = Dataset.objects.get(pk=dsid)

        # Get related objects for deletion
        hits = Hit.objects.filter(task_id=tid)
        places = Place.objects.filter(id__in=[h.place_id for h in hits])

        # Reset the review status for places
        for p in places:
            if auth in ['whg', 'idx']:
                p.review_whg = None
            elif auth.startswith('wd'):
                p.review_wd = None
            else:
                p.review_tgn = None
            p.defer_comments.all().delete()
            p.save()

        # Handle deletion based on scope
        if scope == 'task':
            placelinks = PlaceLink.objects.filter(task_id=tid)
            placegeoms = PlaceGeom.objects.filter(task_id=tid)
            placenames = PlaceName.objects.filter(task_id=tid)

            hits_count = hits.count()
            placelinks.delete()
            placegeoms.delete()
            placenames.delete()
            hits.delete()
            tr.delete()

            logger.info(f'Deleted task {tid}: {hits_count} hits and related objects')

        elif scope == 'geoms':
            placegeoms = PlaceGeom.objects.filter(task_id=tid)
            geom_count = placegeoms.count()
            placegeoms.delete()
            logger.info(f'Deleted {geom_count} geometries for task {tid}')

        # Remove dataset from index if not in test mode
        if auth in ['whg', 'idx'] and test == 'off':
            removeDatasetFromIndex('whg', dsid)

        # Update the dataset status
        if ds.tasks.filter(status='SUCCESS').count() == 0:
            ds.ds_status = 'remote' if ds.file.file.name.startswith('dummy') else 'uploaded'
        ds.save()

        return {
            'status': 'success',
            'task_id': tid,
            'scope': scope,
            'dataset_id': dsid
        }

    except Exception as e:
        logger.error(f'Error deleting task {tid}: {e}', exc_info=True)
        return {'status': 'error', 'message': str(e)}


@shared_task()
def index_to_pub(dataset_id, idx=settings.ES_PUB):
    """
    Indexes places from a dataset into the 'pub' index in Elasticsearch.
    """
    es = settings.ES_CONN

    # Fetch dataset by ID. Indexing runs whenever an editor makes a dataset public (this task is only
    # triggered by that flag flip), REGARDLESS of reconciliation status — an editor may deliberately
    # publish a dataset whose review isn't finished (deferred/unreviewed records are indexed as-is).
    # The UI warns the editor before this happens (see ds_metadata publish modal).
    try:
        dataset = Dataset.objects.get(pk=dataset_id)
    except Dataset.DoesNotExist:
        logger.exception(f"Dataset with ID {dataset_id} does not exist; cannot index to pub.")
        return

    places_to_index = Place.objects.filter(dataset=dataset, indexed=False, idx_pub=False)
    place_ids_to_index = list(places_to_index.values_list('id', flat=True))

    # convert a Place into a dict that's ready for indexing
    def make_bulk_doc(place):
        doc = makeDoc(place)
        doc['whg_id'] = ''
        # Add names and title to search field
        searchy_content = set(doc.get('searchy', []))
        searchy_content.update(n['toponym'] for n in doc['names'])
        searchy_content.add(doc['title'])
        doc['searchy'] = list(searchy_content)
        return {
            "_index": idx,
            "_id": place.id,  # place ID as document ID
            "_source": doc,
        }

    actions = (make_bulk_doc(place) for place in places_to_index.iterator())

    # Perform the bulk index operation and collect the response
    successes, failed_docs = 0, []
    with transaction.atomic():  # Use a transaction to prevent race conditions
        for ok, action in streaming_bulk(es, actions, index=idx, raise_on_error=False):
            if ok:
                successes += 1
            else:
                failed_docs.append(action)

    # Update the idx_pub flag for all successful Place objects using the Place IDs
    Place.objects.filter(id__in=place_ids_to_index).update(idx_pub=True)

    logger.debug(f"Indexing complete. Total indexed places: {successes}. Failed documents: {len(failed_docs)}")
    if failed_docs:
        logger.debug(f"Failed documents: {failed_docs}")


@shared_task()
def unindex_from_pub(dataset_id=None, place_id=None, idx=settings.ES_PUB):
    """
    Removes place(s) from the 'pub' index in Elasticsearch.
    """
    es = settings.ES_CONN

    if place_id:
        try:
            # Check if the place exists in PostgreSQL and is indexed in 'pub'
            place = Place.objects.get(pk=place_id, idx_pub=True)

            # --- FIX: Perform the delete operation ---
            # The Elasticsearch client will raise NotFoundError if the document is missing.
            es.delete(index=idx, id=str(place_id), refresh=True)

            # If the delete succeeds, update the idx_pub flag
            place.idx_pub = False
            place.save()

        except NotFoundError:
            # FIX APPLIED: Catch the 404 (Not Found) from Elasticsearch/OpenSearch.
            # This means the document is already absent, so the goal is achieved.
            logger.info(f"Place ID {place_id} was not found in index '{idx}'. Unindex confirmed.")

            # Update the Django flag defensively, as the database believes it's indexed
            try:
                place = Place.objects.get(pk=place_id)
                place.idx_pub = False
                place.save()
            except Place.DoesNotExist:
                logger.warning(f"Place ID {place_id} not found in DB after unindex check.")

        except Place.DoesNotExist:
            logger.error(f"Place with ID {place_id} does not exist or is not indexed in 'pub' (DB status).")

        except Exception as e:
            # Catch other critical errors (e.g., connection errors, permission errors)
            logger.error(f"An unexpected error occurred while unindexing {place_id}: {e}")
            raise  # Re-raise to signal Celery failure

        return {'place_id': place_id}

    elif dataset_id:
        try:
            dataset = Dataset.objects.get(pk=dataset_id)
        except Dataset.DoesNotExist:
            logger.exception(f"Dataset with ID {dataset_id} does not exist.")
            return  # Exit if the dataset is not found

        # query for es.delete_by_query
        query = {
            "query": {
                "term": {
                    "dataset": dataset.label  # This field should match the field in the ES document
                }
            }
        }

        # Perform the delete_by_query operation
        response = es.delete_by_query(index=idx, body=query, refresh=True)

        # Check for failures and take necessary actions
        if response['failures']:
            logger.debug(f"Failures in unindexing: {response['failures']}")

        # Now, update the idx_pub flag for all Place objects of this dataset
        with transaction.atomic():
            Place.objects.filter(dataset=dataset).update(idx_pub=False)

        place_ids = Place.objects.filter(dataset_id=dataset_id, idx_pub=True).values_list('id', flat=True)
        return {'place_ids': list(place_ids)}

    logger.debug(f"Unindexing complete")


# test task for uptimerobot
@shared_task(name="testAdd")
def testAdd(n1, n2):
    sum = n1 + n2
    return sum


def types(hit):
    type_array = []
    for t in hit["_source"]['types']:
        if bool(t['placetype'] != None):
            type_array.append(t['placetype'] + ', ' + str(t['display']))
    return type_array


def names(hit):
    name_array = []
    for t in hit["_source"]['names']:
        if bool(t['name'] != None):
            name_array.append(t['name'] + ', ' + str(t['display']))
    return name_array


def toGeoJSON(hit):
    src = hit['_source']
    feat = {"type": "Feature", "geometry": src['location'],
            "aatid": hit['_id'], "tgnid": src['tgnid'],
            "properties": {"title": src['title'], "parents": src['parents'], "names": names(hit), "types": types(hit)}}
    return feat


def reverse(coords):
    fubar = [coords[1], coords[0]]
    return fubar


def maxID(es, idx):
    """
    Finds the maximum 'whg_id' using an Elasticsearch Max Aggregation.

    Raises:
        ValueError: If the aggregation result is None (indicating an empty index).
        Exception: For any other Elasticsearch query failure.
    """
    q = {
        "size": 0,
        "aggs": {
            "max_whg_id": {
                "max": {
                    "field": "whg_id"
                }
            }
        }
    }

    try:
        res = es.search(index=idx, body=q)

        # Extract the aggregated value
        max_value = res['aggregations']['max_whg_id']['value']

        if max_value is None:
            # An empty index returns 'None' for max aggregation value.
            # Raise a specific error for the caller to handle index initialization.
            raise ValueError(f"Index '{idx}' appears empty: max aggregation returned None.")

        # Convert to integer and return
        return int(max_value)

    except ValueError as ve:
        # Re-raise the specific ValueError for empty index
        logger.warning(f"Index check failed: {ve}")
        raise ve
    except Exception as e:
        # Catch all other query/network errors and re-raise
        logger.error(f"FATAL: Failed to retrieve max whg_id from index '{idx}'.", exc_info=True)
        raise e


def parseDateTime(string):
    year = re.search("(\d{4})-", string).group(1)
    if string[0] == '-':
        year = year + ' BCE'
    return year.lstrip('0')


def ccDecode(codes):
    countries = []
    # print('codes in ccDecode',codes)
    for c in codes:
        countries.append(cchash[0][c]['gnlabel'])
    return countries


# generate an appropriate title, language-dependent {name} ({en}) in the case of wikidata
def make_title(h, lang):
    if 'dataset' in h and h['dataset'] == 'geonames':
        title = h['variants']['names'][0]
    elif len(h['variants']) == 0:
        title = 'unnamed'
    else:
        vl_en = next((v for v in h['variants'] if v['lang'] == 'en'), None)
        vl_pref = next((v for v in h['variants'] if v['lang'] == lang), None)
        vl_first = next((v for v in h['variants']), None)

        title = vl_pref['names'][0] + (' (' + vl_en['names'][0] + ')' if vl_en else '') \
            if vl_pref and lang != 'en' else vl_en['names'][0] if vl_en else vl_first['names'][0]
    return title
    # if len(variants) == 0:
    #   return 'unnamed'
    # else:
    #   vl_en=next( (v for v in variants if v['lang'] == 'en'), None)#; print(vl_en)
    #   vl_pref=next( (v for v in variants if v['lang'] == lang), None)#; print(vl_pref)
    #   vl_first=next( (v for v in variants ), None); print(vl_first)
    #
    #   title = vl_pref['names'][0] + (' (' + vl_en['names'][0] + ')' if vl_en else '') \
    #     if vl_pref and lang != 'en' else vl_en['names'][0] if vl_en else vl_first['names'][0]
    #   return title


def wdDescriptions(descrips, lang):
    dpref = next((v for v in descrips if v['lang'] == lang), None)
    dstd = next((v for v in descrips if v['lang'] == 'en'), None)

    result = [dstd, dpref] if lang != 'en' else [dstd] \
        if dstd else []
    return result


# create cluster payload from set of hits for a place
def normalize_whg(hits):
    result = []
    src = [h['_source'] for h in hits]
    parents = [h for h in hits if 'whg_id' in h['_source']]
    children = [h for h in hits if 'whg_id' not in h['_source']]
    titles = list(set([h['_source']['title'] for h in hits]))
    [links, countries] = [[], []]
    for h in src:
        countries.append(ccDecode(h['ccodes']))
        for l in h['links']:
            links.append(l['identifier'])
    # each parent seeds cluster of >=1 hit
    for par in parents:
        kid_ids = par['_source']['children'] or None
        kids = [c['_source'] for c in children if c['_id'] in kid_ids]
        cluster = {
            "whg_id": par["_id"],
            "titles": titles,
            "countries": list(set(countries)),
            "links": list(set(links)),
            "geoms": [],
            "sources": []
        }
        result.append(cluster)
    return result.toJSON()


# normalize hit json from any authority
# language relevant only for wikidata local)
def normalize(h, auth, language=None):
    rec = None
    if auth.startswith('whg'):
        # for whg h is full hit, not only _source
        hit = deepcopy(h)
        h = hit['_source']
        # _id = hit['_id']
        # build a json object, for Hit.json field
        rec = HitRecord(
            h['place_id'],
            h['dataset'],
            h['src_id'],
            h['title']
        )
        # print('"rec" HitRecord',rec)
        rec.score = hit['_score']
        rec.passnum = hit['pass'][:5]

        # only parents have whg_id
        if 'whg_id' in h:
            rec.whg_id = h['whg_id']

        # add elements if non-empty in index record
        rec.variants = [n['toponym'] for n in h['names']]  # always >=1 names
        # TODO: fix grungy hack (index has both src_label and sourceLabel)
        key = 'src_label' if 'src_label' in h['types'][0] else 'sourceLabel'
        rec.types = [t['label'] + ' (' + t[key] + ')' if t['label'] != None else t[key] \
                     for t in h['types']] if len(h['types']) > 0 else []
        # TODO: rewrite ccDecode to handle all conditions coming from index
        # ccodes might be [] or [''] or ['ZZ', ...]
        rec.countries = ccDecode(h['ccodes']) if (
                'ccodes' in h.keys() and (len(h['ccodes']) > 0 and h['ccodes'][0] != '')) else []
        # rec.parents = ['partOf: '+r.label+' ('+parseWhen(r['when']['timespans'])+')' for r in h['relations']] \
        # TODO: what happened to parseWhen()?
        rec.parents = ['partOf: ' + r.label + ' (' + r['when']['timespans'] + ')' for r in h['relations']] \
            if 'relations' in h.keys() and len(h['relations']) > 0 else []
        rec.descriptions = h['descriptions'] if len(h['descriptions']) > 0 else []

        rec.geoms = [{
            "type": h['geoms'][0]['location']['type'],
            "coordinates": h['geoms'][0]['location']['coordinates'],
            "id": h['place_id'],
            "ds": "whg"}] \
            if len(h['geoms']) > 0 else []

        rec.minmax = dict(sorted(h['minmax'].items(), reverse=True)) if len(h['minmax']) > 0 else []

        # TODO: deal with whens
        # rec.whens = [parseWhen(t) for t in h['timespans']] \
        # if len(h['timespans']) > 0 else []
        rec.links = [l['identifier'] for l in h['links']] \
            if len(h['links']) > 0 else []
    elif auth == 'wd':
        try:
            # locations and links may be multiple, comma-delimited
            locs = [];
            links = []
            if 'locations' in h.keys():
                for l in h['locations']['value'].split(', '):
                    loc = parse_wkt(l)
                    loc["id"] = h['place']['value'][31:]
                    loc['ds'] = 'wd'
                    locs.append(loc)
            # if 'links' in h.keys():
            # for l in h['links']:
            # links.append('closeMatch: '+l)
            #  place_id, dataset, src_id, title
            rec = HitRecord(-1, 'wd', h['place']['value'][31:], h['placeLabel']['value'])
            # print('"rec" HitRecord',rec)
            rec.variants = []
            rec.types = h['types']['value'] if 'types' in h.keys() else []
            rec.ccodes = [h['countryLabel']['value']]
            rec.parents = h['parents']['value'] if 'parents' in h.keys() else []
            rec.geoms = locs if len(locs) > 0 else []
            rec.links = links if len(links) > 0 else []
            rec.minmax = []
            rec.inception = parseDateTime(h['inception']['value']) if 'inception' in h.keys() else ''
            rec.dataset = h['dataset'] if 'dataset' in h.keys() else ''
        except:
            logger.exception(f'Error in normalize(wd): {h["place"]["value"][31:]}', sys.exc_info())
    elif auth == 'wdlocal':
        # hit['_source'] keys() for dataset='wikidata': ['types', 'authids', 'claims', 'fclasses',
        # 'sitelinks', 'location', 'id', 'variants', 'type', 'descriptions', 'dataset', 'repr_point'])
        # hit['_source'] keys() for dataset='geonames': ['id', 'fclasses', 'location', 'repr_point', 'variants', 'dataset']
        try:
            # which index is the target?
            is_wdgn = 'dataset' in h.keys()
            dataset = h['dataset'] if is_wdgn else 'wd'
            variants = h['variants']
            fclasses = h['fclasses']
            title = make_title(h, language)

            # create base HitRecord(place_id, dataset, auth_id, title
            rec = HitRecord(-1, dataset, h['id'], title)

            # build variants array per dataset
            if is_wdgn and h['dataset'] == 'geonames':
                rec.variants = variants['names']
                rec.fclasses = fclasses
            else:  # wikidata
                v_array = []
                for v in variants:
                    # if not is_wdgn:
                    for n in v['names']:
                        if n != title:
                            v_array.append(n + '@' + v['lang'])
                rec.variants = v_array

            if 'location' in h.keys():
                # single MultiPoint geometry
                loc = h['location']
                loc['id'] = h['id']
                loc['ds'] = dataset
                # single MultiPoint geom if exists
                rec.geoms = [loc]
            else:
                logger.debug(f'No location in hit {h["id"]}')

            # if not is_wdgn: # it's wd
            if dataset != 'geonames':  # it's wd or wikidata
                rec.links = h['authids']

                # look up Q class labels
                htypes = set(h['claims']['P31'])
                qtypekeys = set([t[0] for t in qtypes.items()])
                rec.types = [qtypes[t] for t in list(set(htypes & qtypekeys))]

                # countries
                rec.ccodes = [
                    cchash[0][c]['gnlabel'] for c in cchash[0] \
                    if cchash[0][c]['wdid'] in h['claims']['P17']
                ]

                # include en + native lang if not en
                rec.descriptions = wdDescriptions(h['descriptions'], language) if 'descriptions' in h.keys() else []

                # not applicable
                rec.parents = []

                # no minmax in hit if no inception value(s)
                rec.minmax = [h['minmax']['gte'], h['minmax']['lte']] if 'minmax' in h else []
        except Exception as e:
            logger.exception(f'Error in normalize(wdlocal): {h["id"]}, {rec}', sys.exc_info())

    # elif auth == 'tgn':
    #   rec = HitRecord(-1, 'tgn', h['tgnid'], h['title'])
    #   rec.variants = [n['toponym'] for n in h['names']] # always >=1 names
    #   rec.types = [(t['placetype'] if 'placetype' in t and t['placetype'] != None else 'unspecified') + \
    #               (' ('+t['id']  +')' if 'id' in t and t['id'] != None else '') for t in h['types']] \
    #               if len(h['types']) > 0 else []
    #   rec.ccodes = []
    #   rec.parents = ' > '.join(h['parents']) if len(h['parents']) > 0 else []
    #   rec.descriptions = [h['note']] if h['note'] != None else []
    #   if 'location' in h.keys():
    #     rec.geoms = [{
    #       "type":"Point",
    #       "coordinates":h['location']['coordinates'],
    #       "id":h['tgnid'],
    #         "ds":"tgn"}]
    #   else:
    #     rec.geoms=[]
    #   rec.minmax = []
    #   rec.links = []
    # print(rec)
    else:
        rec = HitRecord(-1, 'unknown', 'unknown', 'unknown')

    return rec.toJSON()


# accounts for GeometryCollection case
def get_bounds_filter(bounds, idx):
    id = bounds['id'][0]
    area = Area.objects.get(id=id)

    # Check if the geometry is a GeometryCollection
    if area.geojson['type'] == 'GeometryCollection':
        # Assuming the first geometry in the collection can be used for filtering
        first_geometry = area.geojson['geometries'][0]
        geo_type = first_geometry['type']
        coordinates = first_geometry['coordinates']
    else:
        geo_type = area.geojson['type']
        coordinates = area.geojson['coordinates']

    # geofield = "geoms.location" if idx == 'whg' else "location"
    geofield = "geoms.location" if idx.startswith('whg') else "location"
    filter = {
        "geo_shape": {
            geofield: {
                "shape": {
                    "type": geo_type,
                    "coordinates": coordinates
                },
                # "relation": "intersects" if idx == 'whg' else 'within'  # within | intersects | contains
                "relation": "intersects" if idx.startswith('whg') else 'within'  # within | intersects | contains
            }
        }
    }
    return filter


def es_lookup_wdlocal(qobj, *args, logger=None, **kwargs):
    """
    Perform an Elasticsearch lookup for a given query object against the
    combined Wikidata and GeoNames index.

    Args:
        qobj (dict): Query object containing place information.
        *args: Additional positional arguments (unused).
        logger (logging.Logger, optional): Logger instance for logging messages.
        **kwargs: Additional keyword arguments, including bounds and geonames exclusion.

    Returns:
        dict: A result object containing the place ID, hits, missed count,
              and total hits.
    """

    # Define the index for the search, combining Wikidata and GeoNames.
    idx = 'wdgn'  # Wikidata + GeoNames

    # If no logger is provided, use the default logger for this module.
    if logger is None:
        logger = logging.getLogger(__name__)  # Default logger

    logger.info(f'kwargs in es_lookup_wdlocal(): {kwargs}')

    # Determine if GeoNames should be excluded based on the value in kwargs.
    exclude_geonames = kwargs.get('geonames') == 'on'
    logger.info(f'exclude_geonames: {exclude_geonames}')

    # Initialize hit count and prepare an empty result object.
    hit_count = 0
    result_obj = {
        'place_id': qobj['place_id'],
        'hits': [],
        'missed': -1,
        'total_hits': -1
    }

    # Extract distinct name variants without language specifications.
    variants = list(set(qobj['variants']))
    logger.info(f'variants: {variants}')

    # Retrieve the Wikidata Q types, stripping the 'wd:' prefix.
    # If no AAT IDs are found, default to ['Q486972'] (indicating a human settlement).
    qtypes = [t[3:] for t in getQ(qobj['placetypes'], 'types')]
    logger.info(f'qtypes: {qtypes}')

    # Extract country codes, stripping the 'wd:' prefix. If none, return an empty list.
    countries = [t[3:] for t in getQ(qobj['countries'], 'ccodes')]
    has_countries = len(countries) > 0
    logger.info(f'countries: {countries}')

    # Extract the bounds object from kwargs.
    bounds = kwargs['bounds']
    has_bounds = bounds.get('id', ['0']) != ['0']  # '0' is the default value for no bounds
    logger.info(f'bounds: {bounds if has_bounds else "None"}')

    # Check for point geometry (for proximity boosting)
    has_geom = 'geom' in qobj.keys()
    point_geom = qobj.get('geom') if has_geom else None
    logger.info(f'geom: {point_geom if has_geom else "None"}')

    # Construct the initial query (q0) to check for authid matches
    q0 = {
        "size": 10,
        "query": {
            "bool": {
                "must": [
                    {
                        "bool": {
                            "should": [
                                {"terms": {"authids": qobj['authids']}},
                                {
                                    "bool": {
                                        "must": [
                                            {"terms": {"_id": [i[3:] for i in qobj['authids'] if i.startswith("wd:")]}},
                                            {"term": {"dataset": "wikidata"}}
                                        ]
                                    }
                                },
                                {
                                    "bool": {
                                        "must": [
                                            {"terms": {"id": [i[3:] for i in qobj['authids'] if i.startswith("gn:")]}},
                                            {"term": {"dataset": "geonames"}}
                                        ]
                                    }
                                }
                            ],
                            "minimum_should_match": 1
                        }
                    }
                ]
            }
        }
    }

    # Apply hard filters for countries and bounds
    filters = []
    if has_countries:
        countries_match = {"terms": {"claims.P17": countries}}
        filters.append(countries_match)
    if has_bounds:
        area_filter = get_bounds_filter(bounds, 'wd')
        filters.append(area_filter)

    # Base query structure for subsequent queries (qbase)
    qbase = {
        "size": 10,
        "query": {
            "bool": {
                "must": [
                    {"terms": {"variants.names": variants}}
                ],
                "should": [
                    {"terms": {"authids": qobj['authids']}}
                ],
                "filter": filters
            }
        }
    }

    # Add proximity boosting if point geometry available
    if has_geom and point_geom:
        lon, lat = point_geom

        # Add proximity boosts to 'should' clause
        qbase['query']['bool']['should'].extend([
            {
                "geo_distance": {
                    "distance": "10km",
                    "repr_point": {"lon": lon, "lat": lat},
                    "boost": 4.0
                }
            },
            {
                "geo_distance": {
                    "distance": "100km",
                    "repr_point": {"lon": lon, "lat": lat},
                    "boost": 1.0
                }
            }
        ])

    # If exclude_geonames is True, add a must_not condition
    if exclude_geonames:
        exclude_condition = {"term": {"dataset": "geonames"}}
        q0["query"]["bool"]["must_not"] = [exclude_condition]
        qbase["query"]["bool"]["must_not"] = [exclude_condition]

    # Create q1 and q2 queries based on qbase
    q1 = deepcopy(qbase)
    # add types if any
    if qtypes:
        q1['query']['bool']['must'].append({"terms": {"types.id": qtypes}})

    q2 = deepcopy(qbase)
    if len(qobj['fclasses']) > 0:
        q2['query']['bool']['must'].append({"terms": {"fclasses": qobj['fclasses']}})

    # Perform query passes
    def perform_query_pass(q, pass_number):
        logger.info(f'Attempting Elasticsearch query for pass {pass_number}: {q}')
        try:
            res = es.search(index=idx, body=q)
            hits = res['hits']['hits']
            logger.info(f'Query result for pass {pass_number}: {hits}')
            return hits
        except Exception as e:
            logger.error(f'Error during pass {pass_number}: {str(e)}')
            raise e

    # Pass 0
    hits0 = perform_query_pass(q0, 0)
    if hits0:
        for hit in hits0:
            hit_count += 1
            hit['pass'] = 'pass0'
            result_obj['hits'].append(hit)
    else:
        # Pass 1
        hits1 = perform_query_pass(q1, 1)
        if hits1:
            for hit in hits1:
                hit_count += 1
                hit['pass'] = 'pass1'
                result_obj['hits'].append(hit)
        else:
            # Pass 2
            hits2 = perform_query_pass(q2, 2)
            if hits2:
                for hit in hits2:
                    hit_count += 1
                    hit['pass'] = 'pass2'
                    result_obj['hits'].append(hit)
            else:
                result_obj['missed'] = str(qobj['place_id']) + ': ' + qobj['title']
                logger.info(f'No hits found for place {qobj["place_id"]}: {qobj["title"]}')

    result_obj['hit_count'] = hit_count
    return result_obj


@shared_task(name="align_wdlocal")
def align_wdlocal(*args, **kwargs):
    """
    Manage the alignment and reconciliation of local entities to the
    Wikidata index. This function retrieves results for each place
    using the es_lookup_wdlocal function and processes the hits
    for review.
    """
    logger = logging.getLogger('reconciliation')
    logger.info(f'Starting align_wdlocal task with task_id: {align_wdlocal.request.id}')
    logger.info(f'request: {align_wdlocal.request}')
    logger.info(f'kwargs: {kwargs}')

    task_id = align_wdlocal.request.id
    task_status = AsyncResult(task_id).status
    ds = get_object_or_404(Dataset, id=kwargs['ds'])
    user = get_object_or_404(User, pk=kwargs['user'])
    bounds = kwargs['bounds']
    scope = kwargs['scope']
    scope_geom = kwargs['scope_geom']
    geonames = kwargs['geonames']  # exclude? on/off
    language = kwargs['lang']

    hit_parade = {"summary": {}, "hits": []}
    [nohits, wdlocal_es_errors, features] = [[], [], []]
    [count_hit, count_nohit, total_hits, count_p0, count_p1, count_p2] = [0, 0, 0, 0, 0, 0]
    start = datetime.datetime.now()
    # there is no test option for wikidata, but needs default
    test = 'off'

    # queryset depends on 'scope'
    qs = ds.places.all() if scope == 'all' else \
        ds.places.filter(~Q(review_wd=1))
    # TODO: scope_geom is not used presently
    if scope_geom == 'geom_free':
        qs = qs.filter(geoms__isnull=True)

    for place in qs:
        qobj = {
            "place_id": place.id,
            "src_id": place.src_id,
            "title": place.title,
            "fclasses": place.fclasses or [],

            # Country codes (ISO 2-letter, uppercase, unique)
            "countries": list(set(c.upper() for c in place.ccodes if c)) if place.ccodes else [],

            # Place types (Getty AAT integer ids if available)
            "placetypes": [
                int(t.jsonb['identifier'].replace('aat:', ''))
                for t in place.types.all()
                if t.jsonb.get('identifier', '').startswith('aat:')
            ],

            # Name variants (including title, lowercase, unique)
            "variants": list(set(
                [place.title.lower()] +
                [name.toponym.lower() for name in place.names.all()]
            )),

            # Parent relationships
            "parents": [
                rel.jsonb['label']
                for rel in place.related.all()
                if rel.jsonb.get('relationType') == 'gvp:broaderPartitive'
            ],

            # Links (authority identifiers: gn, pleiades, loc, viaf, bnf, tgn, gov, cerl, gnd)
            "authids": [l.jsonb['identifier'] for l in place.links.all()],
        }

        # Geometry (representative point) - conditional
        if place.geom_count > 0:
            qobj['geom'] = place.repr_point

        # TODO: ??? skip records that already have a Wikidata record in l_list
        # they are returned as Pass 0 hits right now
        # run pass0-pass2 ES queries
        # in progress: lookup on wdgn index instead of wd
        result_obj = es_lookup_wdlocal(qobj, bounds=bounds, geonames=geonames, logger=logger)
        logger.info(f'result_obj: {result_obj}')
        if result_obj['hit_count'] == 0:
            count_nohit += 1
            nohits.append(result_obj['missed'])
        else:
            # place/task status 0 (unreviewed hits)
            place.review_wd = 0
            place.save()

            count_hit += 1
            total_hits += len(result_obj['hits'])

            # Collect geonames IDs from wikidata hits
            geonames_ids_from_wikidata = set()

            for hit in result_obj['hits']:
                if hit['_source']['dataset'] == 'wikidata':
                    authids = hit['_source'].get('authids', [])
                    for authid in authids:
                        if authid.startswith('gn:'):
                            geonames_id = authid.split(':')[1]
                            geonames_ids_from_wikidata.add(geonames_id)

            for hit in result_obj['hits']:
                hit_id = hit['_source']['id']
                logger.info(f'Pre-write hit["_source"]: {hit["_source"]}')

                # Avoid writing geonames hit if its ID matches any geonames ID from wikidata
                if hit['_source']['dataset'] == 'geonames' and hit_id in geonames_ids_from_wikidata:
                    continue

                if hit['pass'] == 'pass0':
                    count_p0 += 1
                if hit['pass'] == 'pass1':
                    count_p1 += 1
                elif hit['pass'] == 'pass2':
                    count_p2 += 1
                hit_parade["hits"].append(hit)
                new = Hit(
                    # authority = 'wd',
                    authority='wikidata' if 'Q' in hit_id else 'geonames',
                    authrecord_id=hit['_source']['id'],
                    dataset=ds,
                    place=place,
                    task_id=task_id,
                    query_pass=hit['pass'],
                    # prepare for consistent display in review screen
                    json=normalize(hit['_source'], 'wdlocal', language),
                    src_id=qobj['src_id'],
                    score=hit['_score'],
                    reviewed=False,
                    matched=False
                )
                new.save()
                logger.info(f'Hit record saved: {new}')
    end = datetime.datetime.now()

    logger.info(f'ES errors: {wdlocal_es_errors}')
    hit_parade['summary'] = {
        'count': qs.count(),
        'got_hits': count_hit,
        'total_hits': total_hits,
        'pass0': count_p0,
        'pass1': count_p1,
        'pass2': count_p2,
        'no_hits': {'count': count_nohit},
        'elapsed': elapsed(end - start)
    }
    logger.info(f'hit_parade summary: {hit_parade["summary"]}')

    # create log entry and update ds status
    post_recon_update(ds, user, 'wdlocal', test)

    # email owner when complete
    WHGmail(context={
        'template': 'align_wdlocal',
        'to_email': user.email,
        'bcc': [settings.DEFAULT_FROM_EDITORIAL],
        'subject': 'Wikidata alignment task complete',
        'greeting_name': user.name,
        'dataset_title': ds.title if ds else 'N/A',
        'dataset_label': ds.label if ds else 'N/A',
        'dataset_id': ds.id if ds else 'N/A',
        'counthit': count_hit,
        'totalhits': total_hits,
    })

    return hit_parade['summary']


def _safe_es_search(index, body, pass_label, size=100):
    """
    Executes an ES search against a single index.
    Returns hits with pass label annotated.
    """
    start = time.perf_counter()
    hits = []

    try:
        if "size" not in body:
            body["size"] = size

        res = es.search(index=index, body=body)
        hits = res.get("hits", {}).get("hits", [])

        logger.debug(
            f"ES search ({pass_label}) on {index}: {len(hits)} hits "
            f"in {time.perf_counter() - start:.3f}s"
        )
    except Exception as e:
        logger.warning(
            f"ES error in {pass_label} for {index}: {e}", exc_info=True
        )

    return hits


def es_lookup_idx(qobj, *, bounds=None):
    """
    ElasticSearch lookup for index alignment.
    Searches the `whg` (settings.ES_WHG) index.
    Pass0: identifier matches.
    Pass1: lexical + spatial scoring (soft match).
    """
    start_total = time.perf_counter()

    idx = settings.ES_WHG

    result = {
        "place_id": qobj["place_id"],
        "title": qobj["title"],
        "hits": [],
        "hit_count": 0,
        "total_hits": 0,
        "missed": -1,
    }

    variants = list(set(qobj.get("variants", [])))
    links = list(set(qobj.get("links", [])))
    has_countries = bool(qobj.get("countries"))
    has_bounds = bounds and bounds.get("id") != ["0"]
    point_search = qobj.get('geom') if "geom" in qobj else None

    # Only strict, non-scoring filters (Country Code, Bounds)
    filters = []
    if has_countries:
        filters.append({"terms": {"ccodes": qobj["countries"]}})
    if has_bounds:
        filters.append(get_bounds_filter(bounds, "whg"))

    # ---------- pass 0 : identifier matches ----------
    hits0_raw = []
    if links:
        q0 = {
            "query": {
                "bool": {
                    "must": [
                        {"terms": {"links.identifier.keyword": links}}
                    ],
                    "filter": filters  # Use the new strict filters
                }
            }
        }
        hits0_raw = _safe_es_search(idx, q0, pass_label="pass0")

    # ---------- pass 1 : lexical + spatial ----------
    hits1_raw = []
    if variants:

        # 1. Build lexical search terms (goes in the 'must' clause for required match)
        lexical_should_clauses = [
            {"terms": {"names.toponym": variants}},
        ]
        for variant in variants:
            lexical_should_clauses.append(
                {"multi_match": {
                    "query": variant,
                    "fields": STANDARD_FIELDS,
                    "fuzziness": "AUTO",
                    "type": "best_fields"
                }}
            )

        # 2. Build overall boosting terms (goes in main 'should' clause for scoring)
        overall_should_clauses = [
            {"terms": {"types.identifier": qobj.get("placetypes", [])}},
        ]

        # --- NEW SPATIAL BOOSTING LOGIC ---
        if point_search:
            lon, lat = point_search

            # Proximity Boost 1: Tight radius, high boost (Rewards exact hits)
            overall_should_clauses.append({
                "geo_distance": {
                    "distance": "10km",
                    "geoms.location": {"lon": lon, "lat": lat},
                    "boost": 4.0
                }
            })

            # Proximity Boost 2: Wider radius, medium boost (Rewards adjacency)
            overall_should_clauses.append({
                "geo_distance": {
                    "distance": "100km",
                    "geoms.location": {"lon": lon, "lat": lat},
                    "boost": 1.0
                }
            })

        q1 = {
            "size": 10,
            "query": {
                "bool": {
                    "must": [
                        {"exists": {"field": "whg_id"}},
                        # Lexical Match: A result MUST match at least one variant/name
                        {"bool": {
                            "should": lexical_should_clauses,
                            "minimum_should_match": 1
                        }},
                    ],
                    # Types and Proximity are soft boosts (should)
                    "should": overall_should_clauses,
                    "filter": filters,
                }
            },
        }
        hits1_raw = _safe_es_search(idx, q1, pass_label="pass1")

    # ---------- consolidate and label ----------
    result["hits"] = []

    # Add all pass0 hits in the order ES returned them (these are prioritized)
    if hits0_raw:
        for h in hits0_raw:
            _mark_hit(h, "pass0")
            result["hits"].append(h)

    # Add pass1 hits (in ES order) if not already present and under limit
    if hits1_raw:
        seen_ids = {h["_id"] for h in result["hits"]}
        for h in hits1_raw:
            if h["_id"] not in seen_ids and len(result["hits"]) < 10:
                _mark_hit(h, "pass1")
                result["hits"].append(h)

    result["hit_count"] = len(result["hits"])
    result["total_hits"] = result["hit_count"]

    logger.info(
        f"es_lookup_idx(): {result['hit_count']} unique hits for "
        f"{qobj.get('title')} [{idx}] "
        f"in {time.perf_counter() - start_total:.3f}s"
    )

    return result


def _mark_hit(hit, label):
    """Annotate ES hit with minimal extra info for caller."""
    hit["pass"] = label


@sleep_and_retry
@limits(calls=CALLS_PER_SECOND, period=1)
def throttled_lookup(es, qobj, bounds):
    logger.debug(f'throttled_lookup called with qobj: {qobj}')
    return es_lookup_idx(qobj, bounds=bounds)


@shared_task(name="align_idx")
def align_idx(*args, **kwargs):
    """
    Aligns and consolidates new place records with existing indexed place records in WHG index.

    This task compares new place records in a dataset with existing records in an Elasticsearch index.
    It either matches them for review (if hits are found) or indexes them as new parent records (if no hits are found).

    - Generates 'Hit' records for manual review if there are matches.
    - Indexes unmatched records as new parent records.
    - Manages merging of parent-child relationships in the index.

    Args:
        *args: Additional positional arguments (not used).
        **kwargs: Dictionary of keyword arguments:
            - 'ds' (int): Dataset ID.
            - 'user' (int): User ID.
            - 'test' (str): Test mode ('on' or 'off'), controls whether indexing writes to the production index.
            - 'bounds' (dict): Geographic bounds for limiting the search.
            - 'scope' (str): Defines the scope of the search (e.g., 'all', 'unindexed').

    Returns:
        dict: A summary of the alignment process, including counts of records processed, hits, and new indexed records.
    """
    logger = logging.getLogger('accession')
    logger.info(f'Starting align_idx task with kwargs: {kwargs}')

    try:

        start = datetime.datetime.now()
        task_id = align_idx.request.id
        ds = get_object_or_404(Dataset, id=kwargs['ds'])
        user = get_object_or_404(User, id=kwargs['user'])
        test_mode = kwargs.get('test', 'on')  # always 'on' for dev - no writing to the production index!

        es = settings.ES_CONN
        whg_id = maxID(es, settings.ES_WHG)  # get max whg_id for new parent docs

        # Prepare tracking variables
        hit_summary, tracking_vars, places_to_review, new_seeds = initialize_tracking()

        # Get places to process
        places = get_place_queryset(ds, kwargs.get('scope', 'unindexed'))
        logger.info(f'places count: {places.count()}')

        # Process each place
        for index, place in enumerate(places):
            try:
                # logger.info(f'Processing place: {place.id} - {place.title}')

                qobj = build_qobj(place)
                logger.debug(f'Built qobj for place {place.id}: {qobj}')
                result_obj = throttled_lookup(es, qobj, bounds=kwargs['bounds'])

                if not result_obj['hits']:
                    new_seeds.append(place.id)
                else:
                    logger.info(f'Processing {len(result_obj["hits"])} hits found for place {place.id}')
                    places_to_review.append(place.id)
                    process_hits(place, result_obj, task_id, ds, tracking_vars, hit_summary, logger)

            except Exception as e:
                logger.error(f"Error processing place {place.id}: {e}", exc_info=True)
                tracking_vars['count_fail'] += 1

            # if index == 600: # break after 600 places to avoid long-running tasks TODO: comment out for production
            #     logger.info('Reached 600 places, breaking the loop for testing purposes.')
            #     break

        if new_seeds:
            batch_new_seeds.delay(new_seeds, test_mode, start_id=whg_id)

        if places_to_review:
            updated = Place.objects.filter(pk__in=places_to_review).update(review_whg=0)
            logger.info(f"Marked {updated} places for review.")

        hit_summary = finalise_summary(hit_summary, places.count(), tracking_vars, new_seeds, start)
        logger.info(f'hit_summary: {hit_summary}')

        # Finalise: Update dataset status, send email, and log results
        try:
            finalise_task(ds, user, test_mode, hit_summary, logger)
        except Exception as e:
            logger.error(f"Error finalizing task: {e}", exc_info=True)

        return hit_summary

    except Exception as e:
        logger.error(f'Error in align_idx task: {str(e)}', exc_info=True)
        raise e


def initialize_tracking():
    """Initializes the tracking variables for hits, errors, and seeds."""
    hit_summary = {"summary": {}, "hits": []}
    tracking_vars = {
        'count_hit': 0,
        'count_nohit': 0,
        'total_hits': 0,
        'count_p0': 0,
        'count_p1': 0,
        'count_errors': 0,
        'count_seeds': 0,
        'count_kids': 0,
        'count_fail': 0,
    }
    places_to_review, new_seeds = [], []
    return hit_summary, tracking_vars, places_to_review, new_seeds


def get_place_queryset(dataset, scope):
    """Fetches the queryset of places to be processed based on the scope."""
    if scope == 'all':
        return dataset.places.all()
    return dataset.places.filter(indexed=False)


def wait_until_es_ready(timeout=60, sleep_interval=2):
    logger = logging.getLogger('accession')
    start = time.time()
    while True:
        try:
            if es.ping():
                health = es.cluster.health()
                if health.get("status") in ("yellow", "green"):
                    logger.info("Elasticsearch is ready.")
                    return True
        except Exception as e:
            logger.warning(f"Waiting for Elasticsearch: {e}")

        if time.time() - start > timeout:
            logger.error("Timed out waiting for Elasticsearch to be ready.")
            return False
        time.sleep(sleep_interval)


@shared_task
def batch_new_seeds(new_seeds, test_mode, start_id):
    logger = logging.getLogger('accession')

    if not wait_until_es_ready():
        return

    if not es.indices.exists(index=settings.ES_WHG):
        logger.error(f"Index {settings.ES_WHG} does not exist. Cannot batch index new seeds.")
        return
    if not es.indices.exists(index=settings.ES_PUB):
        logger.error(f"Index {settings.ES_PUB} does not exist. Cannot batch delete places.")
        return

    BATCH_SIZE = 500  # Define the batch size for bulk indexing

    actions = []
    places_to_update = []

    for index, new_seed in enumerate(new_seeds):
        try:
            place = Place.objects.get(pk=new_seed)
            places_to_update.append(place)

            new_doc = makeDoc(place)
            new_doc['relation']['name'] = 'parent'
            new_doc['whg_id'] = start_id + index
            new_doc['searchy'] = [n.toponym for n in place.names.all()]

            actions.append({
                "_op_type": "index",
                "_index": settings.ES_WHG,
                "_id": str(start_id + index),
                "_source": new_doc
            })

            actions.append({
                "_op_type": "delete",
                "_index": settings.ES_PUB,
                "_id": str(place.id)
            })

            if len(places_to_update) >= BATCH_SIZE or index == len(new_seeds) - 1:
                success_count = 0
                failure_count = 0
                try:
                    if test_mode == 'off':

                        if not wait_until_es_ready():
                            logger.error("Elasticsearch is not ready for bulk indexing.")
                            raise Exception("Elasticsearch not ready")

                        for success, info in streaming_bulk(es, actions, raise_on_error=False):
                            if success:
                                success_count += 1
                            else:
                                failure_count += 1

                                action_type = list(info.keys())[0]
                                action_info = info[action_type]

                                logger.warning(
                                    f"Failed {action_type} action: ID={action_info.get('_id')}, "
                                    f"Error={action_info.get('error', {}).get('type')} - "
                                    f"{action_info.get('error', {}).get('reason')}"
                                )

                        Place.objects.filter(pk__in=[p.id for p in places_to_update]).update(indexed=True,
                                                                                             idx_pub=False)

                    logger.info(f"Batch bulk indexing complete: {success_count} succeeded, {failure_count} failed.")

                except Exception as e:
                    logger.error(f"Error during bulk indexing: {e}", exc_info=True)

                actions = []
                places_to_update = []

        except Place.DoesNotExist:
            logger.error(f"Place with ID {new_seed} does not exist.")
        except Exception as e:
            logger.error(f"Error preparing place {new_seed} for bulk indexing: {e}", exc_info=True)


def process_hits(place, result_obj, task_id, dataset, tracking_vars, hit_summary, logger):
    """Handles the case where hits are found, and prepares them for review."""
    try:
        tracking_vars['count_hit'] += 1

        parents, children = classify_hits(result_obj['hits'])

        for parent in parents:
            merged_hit = merge_parent_child(parent, children)
            hit_summary['hits'].append(merged_hit)
            save_hit_record(merged_hit, place, dataset, task_id, logger)
            tracking_vars['total_hits'] += 1

    except Exception as e:
        logger.error(f"Error processing hits for place {place.id}: {e}", exc_info=True)
        raise e


def classify_hits(hits):
    """
    Classifies hits into parents and children in a single pass, safely
    handling cases where relation metadata is missing or malformed.
    """
    parents, children = [], []
    for h in hits:
        relation_obj = h['_source'].get('relation', {})
        relation_name = relation_obj.get('name')
        if relation_name:
            profiled = profileHit(h)
            if relation_name == 'parent':
                parents.append(profiled)
            elif relation_name == 'child':
                children.append(profiled)

    return parents, children


def merge_parent_child(parent, children):
    """Merges parent and child records into a single hit object, safely handling missing fields."""

    def safe_list(val):
        return val if isinstance(val, list) else []

    def safe_val(obj, key, default=None):
        return obj.get(key, default)

    def safe_score(obj):
        return obj.get('score', 0) or 0

    def safe_title(obj):
        return obj.get('title', '')

    def safe_country(obj):
        return obj.get('countries', [])

    def safe_links(obj):
        return obj.get('links') or []

    def safe_geoms(obj):
        return obj.get('geoms') or []

    merged = {
        'whg_id': parent.get('_id'),
        'pid': parent.get('pid'),
        'score': safe_score(parent) + sum(safe_score(c) for c in children),
        'titles': [safe_title(parent)] + [safe_title(c) for c in children],
        'countries': safe_country(parent) + list(chain.from_iterable(
            [safe_country(c) for c in children]
        )),
        'geoms': list(uniq_geom(safe_geoms(parent))),
        'links': safe_links(parent) + list(chain.from_iterable(
            [safe_links(c) for c in children]
        )),
        'sources': build_sources(parent, children),
        'passes': list(set([s.get('pass') for s in build_sources(parent, children) if 'pass' in s])),
    }
    return merged


def build_sources(parent, children):
    """Builds the sources field for the hit object."""
    sources = [
        {'dslabel': parent['dataset'], 'pid': parent['pid'], 'variants': parent['variants'], 'types': parent['types'],
         'related': parent['related'], 'children': parent['children'], 'minmax': parent['minmax'],
         'pass': parent['pass'][:5]}
    ]
    sources.extend(
        {'dslabel': c['dataset'], 'pid': c['pid'], 'variants': c['variants'], 'types': c['types'],
         'related': parent['related'], 'minmax': c['minmax'], 'pass': c['pass'][:5]} for c in children
    )
    return sources


def save_hit_record(hit_obj, place, dataset, task_id, logger):
    """Saves a hit record to the database."""
    try:
        new_hit = Hit(
            task_id=task_id,
            authority='whg',
            dataset=dataset,
            place=place,
            src_id=place.src_id,
            authrecord_id=hit_obj['whg_id'],
            query_pass=', '.join(hit_obj['passes']),
            score=hit_obj['score'],
            geom=hit_obj['geoms'],
            reviewed=False,
            matched=False,
            json=hit_obj
        )
        new_hit.save()
    except Exception as e:
        logger.error(f"Error saving hit record for place {place.id}: {e}", exc_info=True)
        raise


def uniq_geom(geom_list):
    """Returns unique geometries from a list."""
    for _, group in itertools.groupby(geom_list, lambda g: g['coordinates']):
        yield list(group)[0]


def finalise_summary(summary, total_places, tracking_vars, new_seeds, start):
    """Finalizes the summary of the alignment process."""
    summary['summary'] = {
        'count': total_places,
        'got_hits': tracking_vars['count_hit'],
        'total_hits': tracking_vars['total_hits'],
        'seeds': len(new_seeds),
        'pass0': tracking_vars['count_p0'],
        'pass1': tracking_vars['count_p1'],
        'elapsed_min': elapsed(datetime.datetime.now() - start),
        'skipped': tracking_vars['count_fail']
    }
    return summary


def finalise_task(dataset, user, test_mode, hit_summary, logger):
    """Handles final updates after task completion, including logs and notifications."""
    try:
        post_recon_update(dataset, user, 'idx', test_mode)

        summary = hit_summary.get('summary', {})

        WHGmail(context={
            'template': 'align_idx',
            'to_email': user.email,
            'bcc': [settings.DEFAULT_FROM_EDITORIAL],
            'subject': 'WHG alignment task complete',
            'greeting_name': user.name,
            'dataset_title': dataset.title,
            'dataset_label': dataset.label,
            'dataset_id': dataset.id,
            'total_count': summary.get('count', 0),
            'counthit': summary.get('got_hits', 0),
            'totalhits': summary.get('total_hits', 0),
            'seeds': summary.get('seeds', 0),
        })
    except Exception as e:
        logger.error(f"Error in finalizing task for dataset {dataset.id}: {e}", exc_info=True)
        raise e
