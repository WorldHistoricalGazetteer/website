# datasets/reconciliation_utils.py
"""
Shared utilities for reconciliation tasks.
These functions are used across different alignment tasks (wdlocal, idx, tgn).
"""
import logging
import re
from django.conf import settings

logger = logging.getLogger(__name__)


def get_bounds_filter(bounds, idx):
    """
    Create Elasticsearch geo_shape filter from Area bounds.
    Accounts for GeometryCollection case.

    Args:
        bounds: Dict with 'id' key containing area ID
        idx: Index name (determines field name)

    Returns:
        dict: Elasticsearch filter object
    """
    # Lazy import to avoid circular dependency
    from areas.models import Area

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

    geofield = "geoms.location" if idx.startswith('whg') else "location"
    filter = {
        "geo_shape": {
            geofield: {
                "shape": {
                    "type": geo_type,
                    "coordinates": coordinates
                },
                "relation": "intersects" if idx.startswith('whg') else 'within'
            }
        }
    }
    return filter


def build_exclude_geonames_filter(exclude_geonames):
    """
    Build Elasticsearch filter to exclude GeoNames results.

    Args:
        exclude_geonames: Boolean indicating whether to exclude GeoNames

    Returns:
        dict: Elasticsearch filter or None
    """
    if exclude_geonames:
        return {"term": {"dataset": "geonames"}}
    return None


def add_must_not_filter(query, filter_condition):
    """
    Add a must_not filter to an Elasticsearch query.

    Args:
        query: Elasticsearch query dict (modified in place)
        filter_condition: Filter to add to must_not clause
    """
    if filter_condition:
        if "must_not" not in query["query"]["bool"]:
            query["query"]["bool"]["must_not"] = []
        query["query"]["bool"]["must_not"].append(filter_condition)


def getQ(arr, what):
    """
    Convert array of codes/types to Wikidata Q identifiers.

    Args:
        arr: Array of country codes or type IDs
        what: 'ccodes' or 'types'

    Returns:
        list: Wikidata Q identifiers with 'wd:' prefix
    """
    # Lazy imports to avoid circular dependencies
    from datasets.static.hashes.parents import ccodes
    from datasets.static.hashes import aat_q

    qids = []
    if what == 'ccodes':
        for c in arr:
            if c.upper() in ccodes[0]:
                qids.append('wd:' + ccodes[0][c.upper()]['wdid'].upper())
    elif what == 'types':
        for t in arr:
            if t in aat_q.qnums:
                for q in aat_q.qnums[t]:
                    qids.append('wd:' + q)
    return list(set(qids))


def parseDateTime(string):
    """
    Parse datetime string and extract year.

    Args:
        string: ISO datetime string (may be negative for BCE)

    Returns:
        str: Year with BCE suffix if negative
    """
    year = re.search(r"(\d{4})-", string).group(1)
    if string[0] == '-':
        year = year + ' BCE'
    return year.lstrip('0')


def ccDecode(codes):
    """
    Decode country codes to GeoNames labels.

    Args:
        codes: List of 2-letter country codes

    Returns:
        list: GeoNames country labels
    """
    # Lazy import to avoid circular dependencies
    from datasets.static.hashes.parents import ccodes as cchash

    countries = []
    for c in codes:
        if c in cchash[0]:
            countries.append(cchash[0][c]['gnlabel'])
    return countries


def make_title(h, lang):
    """
    Generate appropriate title based on language preference.
    For wikidata: {name} ({en}) if different from preferred language.

    Args:
        h: Hit source dict with variants
        lang: Preferred language code

    Returns:
        str: Formatted title
    """
    if 'dataset' in h and h['dataset'] == 'geonames':
        return h['variants']['names'][0]

    if len(h['variants']) == 0:
        return 'unnamed'

    vl_en = next((v for v in h['variants'] if v['lang'] == 'en'), None)
    vl_pref = next((v for v in h['variants'] if v['lang'] == lang), None)
    vl_first = next((v for v in h['variants']), None)

    if vl_pref and lang != 'en':
        title = vl_pref['names'][0]
        if vl_en:
            title += ' (' + vl_en['names'][0] + ')'
    elif vl_en:
        title = vl_en['names'][0]
    else:
        title = vl_first['names'][0]

    return title


def wdDescriptions(descrips, lang):
    """
    Get descriptions in preferred and standard languages.

    Args:
        descrips: List of description dicts with 'lang' key
        lang: Preferred language code

    Returns:
        list: Description dicts in preferred order
    """
    dpref = next((v for v in descrips if v['lang'] == lang), None)
    dstd = next((v for v in descrips if v['lang'] == 'en'), None)

    if lang != 'en':
        result = [dstd, dpref] if dstd else []
    else:
        result = [dstd] if dstd else []

    return result


def post_recon_update(ds, user, task, test):
    """
    Log reconciliation action and update dataset status.

    Args:
        ds: Dataset instance
        user: User instance
        task: Task name ('idx', 'wdlocal', etc.)
        test: 'on' or 'off'
    """
    # Lazy import to avoid circular dependency
    from main.models import Log

    if test == "off":
        if task == 'idx':
            ds.ds_status = 'indexed' if ds.unindexed == 0 else 'accessioning'
        else:
            ds.ds_status = 'reconciling'
        ds.save()
    else:
        task += '_test'

    # Recon task has completed, log it
    logobj = Log.objects.create(
        category='dataset',
        logtype='ds_recon',
        subtype='align_' + task,
        dataset_id=ds.id,
        user_id=user.id
    )
    logobj.save()


def get_place_queryset(dataset, scope, review_field=None):
    """
    Build queryset of places to process for reconciliation.

    Args:
        dataset: Dataset instance
        scope: 'all' or other (unindexed)
        review_field: Optional field name to filter for unreviewed (e.g., 'review_whg')

    Returns:
        QuerySet: Places to process
    """
    if scope == 'all':
        qs = dataset.places.all()
    else:
        qs = dataset.places.filter(indexed=False)

    if review_field:
        # Filter for places that haven't been reviewed
        filter_kwargs = {f'{review_field}__isnull': True}
        qs = qs.filter(**filter_kwargs)

    return qs


def aat_lookup(aid):
    """
    Look up AAT term label from ID using Type model.

    Args:
        aid: AAT identifier

    Returns:
        str: Term label or None if not found
    """
    # Lazy import to avoid circular dependency
    from django.shortcuts import get_object_or_404
    from places.models import Type

    try:
        typeobj = get_object_or_404(Type, aat_id=aid)
        return typeobj.term
    except Exception as e:
        logger.exception(f'aat_lookup({aid})', exc_info=True)
        return None


def classy(gaz, typeArray):
    """
    Convert Black atlas place types to equivalent classes for gazetteer.

    Args:
        gaz: Gazetteer name ('gn', 'tgn', 'dbp')
        typeArray: List of Black atlas place types

    Returns:
        list: Equivalent type classes
    """
    import codecs, json

    types = []
    finhash = codecs.open('../data/feature-classes.json', 'r', 'utf8')
    classes = json.loads(finhash.read())
    finhash.close()

    if gaz == 'gn':
        t = classes['geonames']
        default = 'P'
        for k, v in t.items():
            if not set(typeArray).isdisjoint(t[k]):
                types.append(k)
            else:
                types.append(default)
    elif gaz == 'tgn':
        t = classes['tgn']
        default = 'inhabited places'
        # if 'settlement' exclude others
        typeArray = ['settlement'] if 'settlement' in typeArray else typeArray
        # if 'admin1' (US states) exclude others
        typeArray = ['admin1'] if 'admin1' in typeArray else typeArray
        for k, v in t.items():
            if not set(typeArray).isdisjoint(t[k]):
                types.append(k)
            else:
                types.append(default)
    elif gaz == "dbp":
        t = classes['dbpedia']
        default = 'Place'
        for k, v in t.items():
            if not set(typeArray).isdisjoint(t[k]):
                types.append(k)

    if len(types) == 0:
        types.append(default)

    return list(set(types))