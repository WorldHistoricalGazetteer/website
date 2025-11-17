# es_utils.py; created 2019-02-07;
# misc elasticsearch supporting tasks
# revs: 2021-03; 2020-03; 2019-10-01; 2019-03-05;

from django.contrib.auth import get_user_model

from datasets.geometry_utils import _simplify_geometry

# from main.views import logger

User = get_user_model()
from django.conf import settings

es = settings.ES_CONN
from django.http import JsonResponse
from places.models import Place
# from datasets.models import Dataset
from datasets.static.hashes.parents import ccodes as cchash
from copy import deepcopy
import sys, logging

logger = logging.getLogger(__name__)

GEOJSON_GEOMETRY_KEYS = {'type', 'coordinates', 'geometries', 'bbox'}
GEOJSON_GEOMETRY_BASETYPES = {
    'Point', 'MultiPoint', 'LineString', 'MultiLineString',
    'Polygon', 'MultiPolygon'
}


# given pid, gets db and index records
# called by: elastic/index_admin.html
#
def fetch(request):
    from places.models import Place
    user = User.objects.get(pk=1)
    idx = settings.ES_WHG
    if request.method == 'POST':
        pid = request.POST['pid']
        user = request.user
        place = Place.objects.get(pk=pid)
        # pid = 81228 (parent), 81229 (child)

        # database record
        order_list = ['id', 'title', 'src_id', 'dataset_id', 'ccodes']
        dbplace = {k: v for (k, v) in place.__dict__.items() if k in order_list}
        dbplace['links'] = [l.jsonb['identifier'] for l in place.links.all()]
        dbplace['names'] = [n.toponym for n in place.names.all()]
        dbplace['timespans'] = place.timespans or None
        dbplace['geom count'] = place.geoms.count()
        result = {'dbplace': dbplace}

        # index record(s)
        doc = es.search(index=idx, body=esq_pid(pid))['hits']['hits'][0]
        src = doc['_source']
        is_parent = 'whg_id' in doc['_source'].keys()
        whgid = src['whg_id'] if is_parent else 'n/a'
        idxplace = {'pid': pid,
                    'whgid': whgid,
                    'title': src['title'],
                    'role': src['relation']['name'],
                    }
        if is_parent:
            # fetch and parse children
            res = es.search(index=idx, body=esq_children(whgid))
            idxplace['children'] = res['hits']['hits']
        else:
            idx_altparents = []
            # fetch and parse children of parent (siblings)
            res_parent = es.search(index=idx, body=esq_id(src['relation']['parent']))
            parent_pid = res_parent['hits']['hits'][0]['_source']['place_id']
            # print('res_parent', res_parent)
            res = es.search(index=idx, body=esq_children(src['relation']['parent']))
            hits = res['hits']['hits']
            siblings = [{'pid': h['_source']['place_id']} for h in hits]
            # idxplace['siblings'] = siblings
            idxplace['parent'] = {'whgid': src['relation']['parent'],
                                  'pid': parent_pid,
                                  'title': src['title'],
                                  'children': siblings,
                                  'ccodes': src['ccodes']}

            # get alternate parents, omitting parent_pid
            result['altparents'] = alt_parents(place, src['relation']['parent'])
        result['idxplace'] = idxplace
        return JsonResponse(result, safe=False)


# basic search for alternate parents
def alt_parents(place, parent_pid):
    # place=Place.objects.get(id=request.POST['pid'])
    qobj = build_qobj(place)
    variants = list(set(qobj["variants"]))
    links = list(set(qobj["links"]))
    linklist = deepcopy(links)
    has_fclasses = len(qobj["fclasses"]) > 0
    has_geom = "geom" in qobj.keys()

    # empty result object
    result_obj = {
        'place_id': qobj['place_id'],
        'title': qobj['title'],
        'hits': [], 'missed': -1, 'total_hits': 0,
        'hit_count': 0
    }
    qbase = {"size": 100, "query": {
        "bool": {
            "must": [
                # must share a variant (strict match)
                {"terms": {"names.toponym": variants}},
                {"exists": {"field": "whg_id"}}
            ],
            "should": [
                # bool::should adds to score
                {"terms": {"links.identifier": links}}
                , {"terms": {"types.identifier": qobj["placetypes"]}}
            ],
            # spatial filters added according to what"s available
            "filter": []
        }
    }}
    # augment base
    if has_geom:
        # qobj["geom"] is always a polygon hull
        shape_filter = {"geo_shape": {
            "geoms.location": {
                "shape": {
                    "type": qobj["geom"]["type"],
                    "coordinates": qobj["geom"]["coordinates"]},
                "relation": "intersects"}
        }}
        qbase["query"]["bool"]["filter"].append(shape_filter)
    if has_fclasses:
        qbase["query"]["bool"]["must"].append(
            {"terms": {"fclasses": qobj["fclasses"]}})
    # grab a copy
    q1 = qbase

    try:
        # result1 = es.search(index='whg', body=q1)
        result1 = es.search(index=settings.ES_WHG, body=q1)
        hits1 = result1["hits"]["hits"]
    except:
        logger.debug(f'q1, ES error: {q1}, {sys.exc_info()}')

    if len(hits1) > 0:
        for h in hits1:
            relation = h["_source"]["relation"]
            h["pass"] = "pass1"
            hitobj = {
                "_id": h['_id'],
                "pid": h["_source"]['place_id'],
                "title": h["_source"]['title'],
                "dataset": h["_source"]['dataset'],
                "pass": "pass1",
                "links": [l["identifier"] \
                          for l in h["_source"]["links"]],
                "role": relation["name"],
                "children": h["_source"]["children"]
            }
            if "parent" in relation.keys():
                hitobj["parent"] = relation["parent"]
            # omit current parent
            if h['_id'] != parent_pid:
                result_obj["hits"].append(hitobj)
            result_obj['total_hits'] = len(result_obj["hits"])
    else:
        result_obj['total_hits'] = 0
    return result_obj

    #


# def esq_addchild(_id):
#   q = {"query":{"bool":{"should": [
#         {"parent_id": {"type": "child","id":_id}},
#         {"match":{"_id":_id}}
#       ]}}}
#   return q

# def addChild(place, parent_id):
#   childobj = makeDoc(place)
#   childobj['relation']['name'] = 'child'
#   childobj['relation']['parent'] = str(parent_id)
#
#   # modify parent:
#   parent = es.search(index='whg', body=esq_addchild(parent_id))['hits']['hits'][0]
#   # - add place.id to children;
#   # - add names.toponym to searchy if absent
#
#   print('adding place doc', childobj, 'as child of', parent_id)

"""
topParent(parents, form)
parents is set or list 
"""


def topParent(parents, form):
    # print('topParent():', parents)
    if form == 'set':
        # if eq # of kids, use lowest _id
        parents.sort(key=lambda x: (-x[1], x[0]))
        top = parents[0][0]
    else:
        # a list of external parent _ids
        # get one with most children, or just the first?
        top = parents[0]
    # print('winner_id is', top)
    return top


def ccDecode(codes):
    countries = []
    # print('codes in ccDecode',codes)
    for c in codes:
        countries.append(cchash[0][c]['gnlabel'])
    return countries


def build_qobj(place):
    qobj = {
        "place_id": place.id,
        "src_id": place.src_id,
        "title": place.title,
        "fclasses": list(set(place.fclasses)) if place.fclasses else [],

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

    # Check for missing metadata and apply minimal defaults
    # Principle: Only apply defaults if the place has NO AAT identifiers AND NO existing fclasses.
    if not qobj['placetypes'] and not qobj['fclasses']:
        qobj['fclasses'] = ['P', 'S', 'A', 'T', 'H', 'L', 'R', 'X']

    # Geometry (representative point) - conditional
    if place.geom_count > 0:
        qobj['geom'] = place.repr_point

    return qobj


def findPortalPlaces(whg_id):
    """
    Fetch place ids for a given whg_id
    HOTFIX: 2024-07-17 kg; added 'else:'; sometimes there are no hits
    """
    es = settings.ES_CONN
    # idx = 'whg'
    idx = settings.ES_WHG

    try:
        res = es.search(index=idx, query=esq_id(whg_id))
        hits = res['hits']['hits']
    except ConnectionError as e:
        logging.error(f"Elasticsearch connection error for whg_id {whg_id}: {e}")
        return []

    # Check if the document exists in the index
    if hits:
        doc = hits[0]['_source']
        place_id = int(doc.get('place_id'))
        children = [int(child) for child in doc.get('children', [])]

        # Concatenate place_id and children into a single array
        ids = [place_id] + children

    else:
        # Log a warning or error
        logging.warning(f"No hits found for whg_id: {whg_id}")
        # Return an empty list if no hits
        ids = []

    # Return the array of IDs
    return ids


def findPortalPIDs(pid):
    """
    Fetch place ids sharing a whg_id for a given place id
    """
    es = settings.ES_CONN
    # idx = 'whg'
    idx = settings.ES_WHG

    try:
        es_result = es.search(index=idx, query=esq_pid(pid))

        hits = es_result.get('hits', {}).get('hits', [])
        if hits:
            source = hits[0].get('_source', {})
            whg_id = source.get('whg_id')
            relation = source.get('relation', {})
            if not whg_id:
                whg_id = relation.get('parent')

            if whg_id:
                shared_ids = findPortalPlaces(whg_id)
                return shared_ids
            else:
                # print(f"No whg_id found for pid {pid}")
                return []
        else:
            # print(f"No document found for pid {pid}")
            return []
    except Exception as e:
        # print(f"Error finding shared pids for pid {pid}: {e}")
        return []


def profileHit(hit):
    """
    summarize a WHG hit for analysis
    """
    _id = hit['_id']
    src = hit['_source']
    pid = src['place_id']

    relation = src['relation']
    profile = {
        '_id': _id, 'pid': pid, 'title': src['title'],
        'pass': hit['pass'], 'role': relation['name'],
        'dataset': src['dataset'],
        'score': hit['_score']
    }
    types = src['types']

    profile['parent'] = relation['parent'] if \
        relation['name'] == 'child' else None
    profile['children'] = src['children'] if \
        relation['name'] == 'parent' else None
    profile['minmax'] = [src['minmax']['gte'], src['minmax']['lte']] if type(src['minmax']) == dict else None
    profile['links'] = [l['identifier'] for l in src['links']] \
        if len(src['links']) > 0 else None
    profile['countries'] = ccDecode(src['ccodes'])
    profile['variants'] = [n['toponym'] for n in src['names']]
    profile['types'] = [t['sourceLabel'] if 'sourceLabel' in t else t['source_label']
    if 'source_label' in t else '' for t in src['types']]
    # profile['types'] = [t['sourceLabel'] for t in src['types']]
    profile['related'] = [r['label'] for r in src['relations']]
    if len(src['descriptions']) > 0:
        profile['descriptions'] = [d['value'] for d in src['descriptions']]
    geom_objlist = []
    for g in src['geoms']:
        if g not in geom_objlist:
            geom_objlist.append(
                {'id': pid,
                 'ds': src['dataset'],
                 'coordinates': g['location']['coordinates'],
                 'type': g['location']['type']}
            )
    profile['geoms'] = geom_objlist
    return profile


# ***
# index dataset to builder given ds.id list
# ***
#


# from places.models import Place
# from datasets.models import Dataset
# from django.conf import settings

# DEPRECATED
# def indexToBuilder(dsid, idx='builder'):
#     es = settings.ES_CONN
#     from elasticsearch8.helpers import bulk
#     from datasets.models import Dataset
#     from places.models import Place
#     dslabel = Dataset.objects.get(id=dsid).label
#     qs = Place.objects.filter(dataset=dslabel).iterator()
#
#     def gen_data():
#         for place in qs:
#             pobj = makeDoc(place)
#             pobj['searchy'] += [n['toponym'] for n in pobj['names'] if n['toponym'] not in pobj['searchy']]
#             if place.title not in pobj['searchy']:
#                 pobj['searchy'].append(place.title)
#             # Create action for bulk API
#             yield {
#                 '_op_type': 'index',
#                 '_index': idx,
#                 '_id': place.id,
#                 '_source': pobj,
#             }
#
#     # Perform bulk indexing
#     success, failed = bulk(es, gen_data(), raise_on_error=False)
#
#     # Update indexed status in database
#     if success:
#         Place.objects.filter(id__in=[place.id for place in qs]).update(idx_builder=True)
#
#
#
#     print(f"Indexing complete. Total indexed places: {success}. Failed documents: {len(failed)}")

# ***
# index docs given place_id list
# ***
#
def indexSomeParents(es, idx, pids):
    from datasets.tasks import maxID
    from django.shortcuts import get_object_or_404
    from places.models import Place
    import sys, json
    whg_id = maxID(es, idx)
    for pid in pids:
        place = get_object_or_404(Place, id=pid)
        whg_id = whg_id + 1
        # parent_obj = makeDoc(place,'none')
        parent_obj = makeDoc(place)
        parent_obj['relation'] = {"name": "parent"}
        # parents get an incremented _id & whg_id
        parent_obj['whg_id'] = whg_id
        # add its own names to the searchy field
        # suggest['input] field no longer in use
        for n in parent_obj['names']:
            parent_obj['searchy'].append(n['toponym'])
            # parent_obj['suggest']['input'].append(n['toponym'])
        # add its title
        if place.title not in parent_obj['searchy']:
            parent_obj['searchy'].append(place.title)
        # print('parent_obj',parent_obj)
        # index it
        try:
            res = es.index(index=idx, id=str(whg_id), body=json.dumps(parent_obj))
        except:
            logger.debug(f'failed indexing (as parent) {pid}: {sys.exc_info()}')
            pass
        place.indexed = True
        place.review_whg = True
        place.save()
        # print('created parent:',idx, pid, place.title)


# ***

# replace docs in index given place_id list
# ***
def replaceInIndex(es, idx, pids):
    from django.shortcuts import get_object_or_404
    from places.models import Place
    import simplejson as json
    # from  . import makeDoc, esq_pid, esq_id, uriMaker
    # set counter
    repl_count = 0
    for pid in pids:
        # pid=6294527 (child of 13504937); also 6294533
        # a parent: 6294563
        res = es.search(index=idx, body=esq_pid(pid))
        # make sure it's in the index; in test, might not be
        if len(res['hits']['hits']) > 0:
            hits = res['hits']['hits']
            # get its key info
            # TODO: what if more than one?
            doc = hits[0]
            src = doc['_source']
            role = src['relation']['name']  # ; print(role)

            # get the db instance
            place = get_object_or_404(Place, pk=pid)

            # index doc child or parent?
            if role == 'child':
                # get parent _id
                parentid = src['relation']['parent']
                # write a new doc from db place
                # newchild = makeDoc(place, 'none')
                newchild = makeDoc(place)
                newchild['relation'] = {"name": "child", "parent": parentid}
                # get names from replacement db record
                newnames = [x.toponym for x in place.names.all()]
                # update parent sugs and searchy
                q_update = {"script": {
                    "source": "ctx._source.suggest.input.addAll(params.sugs); \
                      ctx._source.searchy.addAll(params.sugs);",
                    "lang": "painless",
                    "params": {"sugs": newnames}
                },
                    "query": {"match": {"place_id": parentid}}
                }
                try:
                    es.update_by_query(index=idx, body=q_update)
                except Exception as e:
                    logger.critical('An unrecoverable error occurred', exc_info=True)
                    sys.exit(1)

                    # delete the old
                es.delete_by_query(index=idx, body={"query": {"match": {"_id": doc['_id']}}})
                # index the new
                es.index(index=idx, id=doc['_id'],
                         routing=1, body=json.dumps(newchild))
                repl_count += 1
            elif role == 'parent':
                # get id, children, sugs from existing index doc
                kids_e = src['children']
                sugs_e = list(set(src['suggest']['input']))  # distinct only

                # new doc from db place; fill from existing
                # newparent = makeDoc(place, None)
                newparent = makeDoc(place)
                newparent['children'] = kids_e

                # merge old & new names in new doc
                previous = set([q['toponym'] for q in newparent['names']])
                names_union = list(previous.union(set(sugs_e)))
                newparent['whg_id'] = doc['_id']
                newparent['suggest']['input'] = names_union
                newparent['searchy'] = names_union
                newparent['relation'] = {"name": "parent"}

                # out with the old
                es.delete_by_query(index=idx, body=esq_id(doc['_id']))
                # in with the new
                es.index(index=idx, id=doc['_id'],
                         routing=1, body=json.dumps(newparent))

                repl_count += 1
        else:
            pass


# wrapper for removePlacesFromIndex()
# delete all docs for dataset from the whg index,
# whether record is in database or not
def removeDatasetFromIndex(request=None, *args, **kwargs):
    from datasets.models import Dataset
    ds = Dataset.objects.get(id=args[0] if args else kwargs['dsid'])
    es = settings.ES_CONN
    idx = settings.ES_WHG
    q_pids = {"match": {"dataset": ds.label}}

    # Have to use scroll for large datasets
    page_size = 1000
    resp = es.search(index=idx, query=q_pids, _source=["title", "place_id"], size=page_size, scroll='2m')
    scroll_id = resp['_scroll_id']
    hits = resp['hits']['hits']
    all_pids = [h['_source']['place_id'] for h in hits]

    while len(hits) > 0:
        resp = es.scroll(scroll_id=scroll_id, scroll='2m')
        scroll_id = resp['_scroll_id']
        hits = resp['hits']['hits']
        all_pids.extend(h['_source']['place_id'] for h in hits)

    removePlacesFromIndex(es, idx, all_pids)

    Dataset.objects.filter(id=ds.id).update(ds_status='wd-complete')
    from datasets.models import Hit
    Hit.objects.filter(authority="whg", dataset_id=ds.id).delete()

    # delete latest idx task
    tasks = ds.tasks.filter(task_name='align_idx', status="SUCCESS").order_by('-date_done')
    if tasks.exists():
        latest = tasks[0]
        latest.delete()

    # for browser console
    return JsonResponse({'msg': 'pids passed to removePlacesFromIndex(' + str(ds.id) + ')',
                         'ids': all_pids})


def chunked(iterable, size=1000):
    for i in range(0, len(iterable), size):
        yield iterable[i:i + size]


def removePlacesFromIndex(es, idx, pids):
    """
    Remove place documents from an Elasticsearch index based on a list of place IDs,
    handling parent-child relationships and updating index and database records accordingly.

    This function processes each place ID (`pid`) provided:
    - If the place is a **parent**:
      - Checks if it has children.
      - If children exist, promotes one eligible child to become the new parent.
      - Transfers parent status, including updating `whg_id`, `children` list, and `searchy` fields.
      - Marks the original parent for deletion.
      - If no eligible children remain, marks the parent directly for deletion.
    - If the place is a **child**:
      - Attempts to remove the child reference from its parent's `children` array.
      - Marks the child for deletion.
      - Skips if the parent is also slated for deletion (a "zombie").

    After processing roles:
    - Updates the corresponding Place database records by setting `indexed` to False,
      removing related `whg` authority hits, and resetting `review_whg` to None.
    - Deletes all documents marked for removal from Elasticsearch in batches.

    Args:
        es (Elasticsearch): Elasticsearch client instance.
        idx (str): Name of the Elasticsearch index to operate on.
        pids (list[int]): List of place IDs to be removed or processed.

    Returns:
        JsonResponse: A JSON response containing a message summarizing how many documents
                      were deleted and their IDs.

    Notes:
        - To optimise performance, this function batches Elasticsearch queries and deletions.
        - Painless scripting is used to safely update Elasticsearch documents atomically.
        - Places not found in the Elasticsearch index are skipped with a debug log.
        - The function assumes that the `Place` Django model and related hit set exist.
        - TODO: Improve logic for selecting which child to promote when a parent is removed.

    Raises:
        Exception: On unrecoverable Elasticsearch update errors, the exception is raised to
                   allow higher-level error handling.
    """

    delthese = []

    # Fetch all documents matching pids in batches to avoid per-pid queries
    all_docs = []
    for batch in chunked(pids, 500):
        q_batch = {"terms": {"place_id": batch}}
        res = es.search(index=idx, query=q_batch, size=len(batch))
        all_docs.extend(res['hits']['hits'])

    # Map pid to doc source for quick lookup
    pid_to_doc = {doc['_source']['place_id']: doc['_source'] for doc in all_docs}

    for pid in pids:
        src = pid_to_doc.get(pid)
        if not src:
            # Not found in index
            logger.debug(f'{pid} not in index, skipping')
            continue

        role = src['relation']['name']
        searchy = list(set(item for item in src.get('searchy', []) if not isinstance(item, list)))

        if role == 'parent':
            kids = [int(x) for x in src.get('children', [])]
            eligible = list(set(kids) - set(pids))

            if not eligible:
                delthese.append(pid)
            else:
                qeligible = {
                    "bool": {
                        "must": [{"terms": {"place_id": kids}}],
                        "should": {"exists": {"field": "links"}}
                    }
                }
                res = es.search(index=idx, query=qeligible)
                linked = [h['_source'] for h in res['hits']['hits'] if 'links' in h['_source']]
                linked_len = [{'pid': h['place_id'], 'len': len(h['links'])} for h in linked]
                winner = max(linked_len, key=lambda x: x['len'])['pid'] if linked_len else eligible[0]
                newparent = winner
                newkids = [k for k in eligible if k != winner]

                q_update = {
                    "script": {
                        "source": (
                            "ctx._source.whg_id = params._id; "
                            "ctx._source.relation.name = 'parent'; "
                            "ctx._source.children = params.newkids; "
                            "ctx._source.searchy.addAll(params.searchy);"
                        ),
                        "lang": "painless",
                        "params": {"_id": newparent, "newkids": newkids, "searchy": searchy}
                    },
                    "query": {"term": {"place_id": newparent}}
                }
                try:
                    es.update_by_query(index=idx, body=q_update)
                    delthese.append(pid)
                except Exception as e:
                    logger.critical('Error updating new parent', exc_info=True)
                    raise

        elif role == 'child':
            parent = src['relation'].get('parent')
            if not parent:
                delthese.append(pid)
                continue

            qget = {"term": {"place_id": parent}}
            res = es.search(index=idx, query=qget)
            if not res['hits']['hits']:
                delthese.append(pid)
                continue
            psrc = res['hits']['hits'][0]['_source']

            zombie = psrc['place_id'] in pids

            if not zombie and psrc.get('children'):
                q_update = {
                    "script": {
                        "lang": "painless",
                        "source": "ctx._source.children.removeIf(c -> c == params.val)",
                        "params": {"val": str(pid)}
                    },
                    "query": {"term": {"place_id": parent}}
                }
                try:
                    es.update_by_query(index=idx, body=q_update)
                    delthese.append(pid)
                except Exception as e:
                    logger.critical('Error removing child from parent', exc_info=True)
                    raise
            else:
                delthese.append(pid)

        # DB side actions, optionally batch if large
        try:
            place = Place.objects.get(id=pid)
            place.indexed = False
            place.hit_set.filter(authority='whg').delete()
            place.review_whg = None
            place.save()
        except Place.DoesNotExist:
            pass

    # Delete all collected docs in batches
    for batch in chunked(delthese, 500):
        es.delete_by_query(index=idx, body={"query": {"terms": {"place_id": batch}}})

    msg = f'deleted {len(delthese)}: {delthese}'
    return JsonResponse({'msg': msg})


# ***
# given ds label, return list of place_id 
# ***
def fetch_pids(dslabel):
    pids = []
    esq_ds = {"size": 10000, "query": {"match": {"dataset": dslabel}}}
    # res = es.search(index='whg', body=esq_ds)
    res = es.search(index=settings.ES_WHG, body=esq_ds)
    docs = res['hits']['hits']
    for d in docs:
        pids.append(d['_source']['place_id'])
    return pids


# ***
# query to get a document by place_id
# ***
def esq_pid(pid):
    q = {"bool": {"must": [{"match": {"place_id": pid}}]}}
    return q


# ***
# query to get a document by _id
# ***
def esq_id(_id):
    q = {"bool": {"must": [{"match": {"_id": _id}}]}}
    return q


# ***
# query to get children or siblings
# ***
def esq_children(_id):
    q = {"query": {"bool": {"should": [
        {"parent_id": {"type": "child", "id": _id}},
        {"match": {"_id": _id}}
    ]}}}
    return q


# ***
# count of dataset docs in index
# ***
def escount_ds(idx, label):
    es = settings.ES_CONN
    q = {"match": {"dataset": label}}
    # TODO: match new pattern query={} across platform
    res = es.search(index=idx, query=q)

    return res['hits']['total']['value']


def confirm(prompt=None, resp=False):
    """prompts for yes or no response from the user. Returns True for yes and
    False for no.
    """
    if prompt is None:
        prompt = 'Confirm'

    if resp:
        prompt = '%s [%s]|%s: ' % (prompt, 'y', 'n')
    else:
        prompt = '%s [%s]|%s: ' % (prompt, 'n', 'y')

    while True:
        ans = input(prompt)
        if not ans:
            return resp
        if ans not in ['y', 'Y', 'n', 'N']:
            continue
        if ans == 'y' or ans == 'Y':
            return True
        if ans == 'n' or ans == 'N':
            return False


# ***
# create an index
# ***
def esInit(idx):
    idx = 'wdgn_20240316'
    import os, codecs
    from django.conf import settings
    os.chdir('elastic/mappings/')
    es = settings.ES_CONN
    file = codecs.open('wdgn_202403.json', 'r', 'utf8').read()
    mappings = ''.join(line for line in file if not '/*' in line)
    # zap existing if exists, re-create
    if confirm(prompt='Zap index ' + idx + '?', resp=False):
        try:
            es.indices.delete(index=idx)
        except Exception as ex:
            logger.exception(f'Error deleting index {idx}: {ex}')
        try:
            es.indices.create(index=idx, ignore=400, body=mappings)
            es.indices.put_alias('wdgn_20240316', 'wdgn')
        except Exception as ex:
            logger.exception(f'Error creating index {idx}: {ex}')


# ***
# called from makeDoc()
# ***
def uriMaker(place):
    from django.shortcuts import get_object_or_404
    from datasets.models import Dataset
    ds = get_object_or_404(Dataset, id=place.dataset.id)
    if 'whgazetteer' in ds.uri_base:
        return ds.uri_base + str(place.id)
    else:
        return ds.uri_base + str(place.src_id)


# ***
# make an ES doc from a Place instance
# called from ALL indexing functions (initial and updates)
# ***
def makeDoc(place):
    fclasses_value = place.fclasses if place.fclasses not in [None, []] else ["X"]

    # 1. Get names from database via parsePlace
    place_names = parsePlace(place, 'names')

    # 2. Add 'title' to names if it's not already present
    title_text = place.title
    title_is_present = any(n.get('toponym') == title_text for n in place_names)

    if not title_is_present:
        title_name_doc = {
            "toponym": title_text,
        }
        place_names.append(title_name_doc)

    es_doc = {
        "relation": {},
        "children": [],
        # "suggest": {"input":[]},
        "place_id": place.id,
        "dataset": place.dataset.label,
        "src_id": place.src_id,
        "title": place.title,
        "uri": uriMaker(place),
        "ccodes": place.ccodes,
        "names": place_names,
        "types": parsePlace(place, 'types'),
        "geoms": parsePlace(place, 'geoms'),
        "links": parsePlace(place, 'links'),
        "fclasses": fclasses_value,
        "timespans": [{"gte": t[0], "lte": t[1]} for t in place.timespans] if place.timespans not in [None, []] else [],
        "minmax": {"gte": place.minmax[0], "lte": place.minmax[1]} if place.minmax not in [None, []] else [],
        "descriptions": parsePlace(place, 'descriptions'),
        "depictions": parsePlace(place, 'depictions'),
        "relations": parsePlace(place, 'related'),
        "searchy": []
    }
    return es_doc


# ***
# fill ES doc arrays from database jsonb objects
# ***
def parsePlace(place, attr):
    qs = eval('place.' + attr + '.all()')
    arr = []

    # Helper function to process a single geometry object
    def process_geometry(g):
        geom_doc = {}
        geometry_data = {}

        # 1. Iterate over all keys in the source geometry 'g'
        for key, value in g.items():
            if key in GEOJSON_GEOMETRY_KEYS:
                # 2. Whitelist: Keep these keys for the pure geometry object
                geometry_data[key] = value
            else:
                # 3. Agnostic Blacklist: Assume everything else is metadata,
                #    and assign it directly to the parent geom_doc (e.g., geom_doc["when"] = ...)
                geom_doc[key] = value

        # 4. Assign the pure GeoJSON object to 'location'
        if geometry_data.get('type'):
            geom_doc["location"] = geometry_data

        return geom_doc

    for obj in qs:
        if attr == 'geoms':
            g = obj.jsonb.copy()

            # If it's a GeometryCollection, we must flatten it
            if g.get('type') == 'GeometryCollection' and 'geometries' in g:
                # The top-level GeometryCollection object itself might contain metadata
                # (e.g., citations), so we process those fields first.
                top_level_metadata = {k: g[k] for k in g.keys() if k not in GEOJSON_GEOMETRY_KEYS}

                for sub_g in g['geometries']:
                    geom_doc = process_geometry(sub_g.copy())

                    # Merge top-level metadata into sub-geometry document
                    geom_doc.update(top_level_metadata)

                    if geom_doc.get("location"):
                        arr.append(geom_doc)

            # Handle single geometries (Point, Polygon, etc.)
            elif g.get('type') in GEOJSON_GEOMETRY_BASETYPES:
                geom_doc = process_geometry(g)
                if geom_doc.get("location"):
                    arr.append(geom_doc)

        elif attr == 'whens':
            when_ts = obj.jsonb['timespans']
            # TODO: index wants numbers, spec says strings
            # expect strings, including operators
            for t in when_ts:
                x = {"start": int(t['start'][list(t['start'])[0]]),
                     "end": int(t['end'][list(t['end'])[0]])}
                arr.append(x)
        else:
            arr.append(obj.jsonb)
    return arr


""" DEPRECATED
demoteParents(demoted, winner_id, pid)
makes each of demoted[] (and its children if any)
a child of winner_id

# """
# def demoteParents(demoted, winner_id, pid):
#   #demoted = ['14156468']
#   #newparent_id = winner_id
#   print('demoteParents()',demoted, winner_id, pid)
#   #qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""
#
#   # updates 'winner' with children & names from demoted
#   def q_updatewinner(kids, names):
#     pass
#   return {"script":{
#     "source": """ctx._source.children.addAll(params.newkids);
#       ctx._source.suggest.input.addAll(params.names);
#       ctx._source.searchy.addAll(params.names);""",
#     "lang": "painless",
#     "params":{
#       "newkids": kids,
#       "names": names }
#   }}
#
#   for d in demoted:
#     # get the demoted doc, its names and kids if any
#     #d = demoted[0]
#     #d = '14156468'
#     #winner_id = '14156467'
#     qget = """{"query": {"bool": {"must": [{"match":{"_id": "%s" }}]}}}"""
#     try:
#       qget = qget % (d)
#       doc = es.search(index='whg', body=qget)['hits']['hits'][0]
#     except:
#       print('failed getting winner; winner_id, pid',winner_id, pid)
#       sys.exit(sys.exc_info())
#     srcd = doc['_source']
#     kids = srcd['children']
#     # add this doc b/c it's now a kid
#     kids.append(doc['_id'])
#     names = list(set(srcd['suggest']['input']))
#
#     # first update the 'winner' parent
#     q=q_updatewinner(kids, names)
#     try:
#       es.update(idx,winner_id,body=q)
#     except:
#       print('q_updatewinner failed (pid, winner_id)',pid,winner_id)
#       sys.exit(sys.exc_info())
#
#     # then modify copy of demoted,
#     # delete the old, index the new
#     # --------------
#     newsrcd = deepcopy(srcd)
#     newsrcd['relation'] = {"name":"child","parent":winner_id}
#     newsrcd['children'] = []
#     if 'whg_id' in newsrcd:
#       newsrcd.pop('whg_id')
#     # zap the old demoted, index the modified
#     try:
#       es.delete('whg', d)
#       es.index(index='whg',id=d,body=newsrcd,routing=1)
#     except:
#       print('reindex failed (pid, demoted)',pid,d)
#       sys.exit(sys.exc_info())
