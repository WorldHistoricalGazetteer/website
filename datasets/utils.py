# /datasets/utils.py
import datetime
import logging
import pprint
import re
import time

import simplejson as json
from django.conf import settings
from django.contrib.gis.db.models import Extent
from django.contrib.gis.geos import GEOSGeometry, Polygon
from django.http import FileResponse, JsonResponse, HttpResponse, Http404
from django.shortcuts import get_object_or_404  # , redirect
from django.views.generic import View
from shapely.geometry import mapping
from shapely.wkt import loads as wkt_loads

from areas.models import Country
from datasets.models import Dataset, Hit, DatasetFile
from datasets.static.hashes import aat_q
from datasets.static.hashes import aliases as al
# from datasets.tasks import make_download
from main.models import Log
from places.models import PlaceGeom, Type

pp = pprint.PrettyPrinter(indent=1)
from whgmail.messaging import WHGmail

logger = logging.getLogger(__name__)


def compute_dataset_bbox(label):
    dsgeoms = PlaceGeom.objects.filter(place__dataset=label)
    extent = dsgeoms.aggregate(Extent("geom"))["geom__extent"]
    if extent:
        return Polygon.from_bbox(extent)
    return None


def volunteer_offer(request, ds):
    volunteer = request.user
    owner = ds.owner

    # common parameters for both emails
    common_params = {
        'bcc': [settings.DEFAULT_FROM_EDITORIAL],
        'volunteer_username': volunteer.username,
        'volunteer_name': volunteer.name,
        'volunteer_email': volunteer.email,
        'volunteer_greeting': volunteer.name,
        'owner_username': volunteer.username,
        'owner_name': owner.name,
        'owner_email': volunteer.email,
        'owner_greeting': owner.name,
        'dataset_title': ds.title,
        'dataset_label': ds.label,
        'dataset_id': ds.id,
        'editor_email': [settings.DEFAULT_FROM_EDITORIAL]
    }

    # send email to dataset owner
    owner_params = common_params.copy()
    owner_params.update({
        'email_type': 'volunteer_offer_owner',
        'subject': 'Volunteer offer for ' + ds.title + ' dataset in WHG',
        'to_email': owner.email,
        'reply_to': volunteer.email,
        'slack_notify': True,
    })
    WHGmail(context=owner_params)

    # return success message
    user_params = common_params.copy()
    user_params.update({
        'email_type': 'volunteer_offer_user',
        'subject': 'Volunteer offer for ' + ds.title + ' dataset in WHG received',
        'to_email': volunteer.email,
    })
    WHGmail(context=user_params)

    # return success message
    return 'volunteer offer for ' + ds


def toggle_volunteers(request):
    if request.method == 'POST':
        is_checked = request.POST.get('is_checked') == 'true'
        dataset_id = request.POST.get('dataset_id')
        dataset = Dataset.objects.get(id=dataset_id)
        dataset.volunteers = is_checked
        dataset.save()
        return JsonResponse({'status': 'success'})


def download_file(request, *args, **kwargs):
    """ just gets a file and downloads it to File/Save window """
    ds = get_object_or_404(Dataset, pk=kwargs['id'])
    fileobj = ds.files.all().order_by('-rev')[0]
    fn = 'media/' + fileobj.file.name
    file_handle = fileobj.file.open()
    # set content type
    response = FileResponse(file_handle, content_type='text/csv' if fileobj.format == 'delimited' else 'text/json')
    response['Content-Disposition'] = 'attachment; filename="' + fileobj.file.name + '"'

    return response


def download_dataset(request, file_id):
    """Downloads a specific dataset file based on file_id."""
    try:
        # Get the specific file using file_id
        fileobj = get_object_or_404(DatasetFile, pk=file_id)

        # Construct the full file path
        fn = 'media/' + fileobj.file.name

        # Open the file
        file_handle = fileobj.file.open()

        logger.info('download_dataset: file_id=%s, filename=%s, format=%s', file_id, fileobj.file.name, fileobj.format)

        # Set content type based on the file format
        content_type = 'text/csv' if fileobj.format == 'delimited' else 'application/json'
        response = FileResponse(file_handle, content_type=content_type)

        # Set the Content-Disposition header
        response['Content-Disposition'] = f'attachment; filename="{fileobj.file.name.replace("/app/media/", "")}"'

        return response

    except Http404:
        logger.error('File not found: file_id=%s', file_id)
        return HttpResponse('File not found', status=404)

    except Exception as e:
        logger.exception('Error downloading file: file_id=%s', file_id)
        return HttpResponse('An error occurred while downloading the file', status=500)


#
# called by process_when()
# returns object for PlaceWhen.jsonb in db
# and minmax int years for PlacePortalView()
#
def parsedates_tsv(dates):
    s, e, attestation_year = dates
    if s and e:
        s_yr = s.year
        e_yr = e.year
        timespans = {"start": {"earliest": s.isoformat()}, "end": {"latest": e.isoformat()}}
        minmax = [s_yr, e_yr]
    elif s and not e:
        s_yr = s.year
        timespans = {"start": {"in": s.isoformat()}}
        minmax = [s_yr, s_yr]
    elif attestation_year:
        s_yr = attestation_year
        timespans = {"start": {"in": str(attestation_year)}}
        minmax = [attestation_year, attestation_year]
    else:
        return None  # Or handle this case differently if needed
    return {"timespans": [timespans], "minmax": minmax}


class HitRecord(object):
    def __init__(self, place_id, dataset, auth_id, title):
        self.place_id = place_id
        self.auth_id = auth_id
        self.title = title
        self.dataset = dataset

    def __str__(self):
        import json
        return json.dumps(str(self.__dict__))

    def toJSON(self):
        import json
        return json.loads(json.dumps(self.__dict__, indent=2))


class PlaceMapper(object):
    def __init__(self, id, src_id, title):
        self.id = id
        self.src_id = src_id
        self.title = title

    def __setitem__(self, key, value):
        setattr(self, key, value)

    def __getitem__(self, key):
        return getattr(self, key)

    def __str__(self):
        import json
        return json.dumps(str(self.__dict__))

    def toJSON(self):
        import json
        return json.loads(json.dumps(self.__dict__, indent=2))


# null fclass: 300239103, 300056006, 300155846
# refactored to use Type model in db
def aat_lookup(aid):
    try:
        typeobj = get_object_or_404(Type, aat_id=aid)
        return typeobj.term
    except:
        logger.exception(f'aat_lookup({aid})', exc_info=True)
        # return {"label": None, "fclass":None}
        return None


# use: ds_insert)json()
def aliasIt(url):
    r1 = re.compile(r"\/(?:.(?!\/))+$")
    id = re.search(r1, url)
    if id:
        id = id.group(0)[1:].replace('cb', '')
    r2 = re.compile(r"bnf|cerl|dbpedia|geonames|d-nb|loc|pleiades|tgn|viaf|wikidata|whg|wikipedia")
    tag = re.search(r2, url)
    if tag and id:
        return al.tags[tag.group(0)]['alias'] + ':' + id
    else:
        return url


# flattens nested tuple list
def flatten(l):
    for el in l:
        if isinstance(el, tuple) and any(isinstance(sub, tuple) for sub in el):
            for sub in flatten(el):
                yield sub
        else:
            yield el


def patch_geos_signatures():
    """
    Patch GEOS to function on macOS arm64 and presumably
    other odd architectures by ensuring that call signatures
    are explicit, and that Django 4 bugfixes are backported.

    Should work on Django 2.2+, minimally tested, caveat emptor.
    """
    import logging

    from ctypes import POINTER, c_uint, c_int
    from django.contrib.gis.geos import GeometryCollection, Polygon
    from django.contrib.gis.geos import prototypes as capi
    from django.contrib.gis.geos.prototypes import GEOM_PTR
    from django.contrib.gis.geos.prototypes.geom import GeomOutput
    from django.contrib.gis.geos.libgeos import geos_version, lgeos
    from django.contrib.gis.geos.linestring import LineString

    logger = logging.getLogger("geos_patch")

    _geos_version = geos_version()
    logger.debug("GEOS: %s %s", _geos_version, repr(lgeos))

    # Backport https://code.djangoproject.com/ticket/30274
    def new_linestring_iter(self):
        for i in range(len(self)):
            yield self[i]

    LineString.__iter__ = new_linestring_iter

    # macOS arm64 requires that we have explicit argtypes for cffi calls.
    # Patch in argtypes for `create_polygon` and `create_collection`,
    # and then ensure their prep functions do NOT use byref so that the
    # arrays (`(GEOM_PTR * length)(...)`) auto-convert into `Geometry**`.
    # create_empty_polygon doesn't need to be patched as it takes no args.

    # Geometry*
    # GEOSGeom_createPolygon_r(GEOSContextHandle_t extHandle,
    #   Geometry* shell, Geometry** holes, unsigned int nholes)
    capi.create_polygon = GeomOutput(
        "GEOSGeom_createPolygon", argtypes=[GEOM_PTR, POINTER(GEOM_PTR), c_uint]
    )

    # Geometry*
    # GEOSGeom_createCollection_r(GEOSContextHandle_t extHandle,
    #   int type, Geometry** geoms, unsigned int ngeoms)
    capi.create_collection = GeomOutput(
        "GEOSGeom_createCollection", argtypes=[c_int, POINTER(GEOM_PTR), c_uint]
    )

    # The below implementations are taken directly from Django 2.2.25 source;
    # the only changes are unwrapping calls to byref().

    def new_create_polygon(self, length, items):
        # Instantiate LinearRing objects if necessary, but don't clone them yet
        # _construct_ring will throw a TypeError if a parameter isn't a valid ring
        # If we cloned the pointers here, we wouldn't be able to clean up
        # in case of error.
        if not length:
            return capi.create_empty_polygon()

        rings = []
        for r in items:
            if isinstance(r, GEOM_PTR):
                rings.append(r)
            else:
                rings.append(self._construct_ring(r))

        shell = self._clone(rings.pop(0))

        n_holes = length - 1
        if n_holes:
            holes = (GEOM_PTR * n_holes)(*[self._clone(r) for r in rings])
            holes_param = holes
        else:
            holes_param = None

        return capi.create_polygon(shell, holes_param, c_uint(n_holes))

    Polygon._create_polygon = new_create_polygon

    # Need to patch to not call byref so that we can cast to a pointer
    def new_create_collection(self, length, items):
        # Creating the geometry pointer array.
        geoms = (GEOM_PTR * length)(
            *[
                # this is a little sloppy, but makes life easier
                # allow GEOSGeometry types (python wrappers) or pointer types
                capi.geom_clone(getattr(g, "ptr", g))
                for g in items
            ]
        )
        return capi.create_collection(c_int(self._typeid), geoms, c_uint(length))

    GeometryCollection._create_collection = new_create_collection


def hully(g_list):
    """
    Patch GEOS to function on macOS arm64 and presumably
    other odd architectures by ensuring that call signatures
    are explicit, and that Django 4 bugfixes are backported.

    Should work on Django 2.2+, minimally tested, caveat emptor.
    """
    import logging

    from ctypes import POINTER, c_uint, c_int
    from django.contrib.gis.geos import GeometryCollection, Polygon
    from django.contrib.gis.geos import prototypes as capi
    from django.contrib.gis.geos.prototypes import GEOM_PTR
    from django.contrib.gis.geos.prototypes.geom import GeomOutput
    from django.contrib.gis.geos.libgeos import geos_version, lgeos
    from django.contrib.gis.geos.linestring import LineString

    logger = logging.getLogger("geos_patch")

    _geos_version = geos_version()
    logger.debug("GEOS: %s %s", _geos_version, repr(lgeos))

    # Backport https://code.djangoproject.com/ticket/30274
    def new_linestring_iter(self):
        for i in range(len(self)):
            yield self[i]

    LineString.__iter__ = new_linestring_iter

    capi.create_polygon = GeomOutput(
        "GEOSGeom_createPolygon", argtypes=[GEOM_PTR, POINTER(GEOM_PTR), c_uint]
    )

    capi.create_collection = GeomOutput(
        "GEOSGeom_createCollection", argtypes=[c_int, POINTER(GEOM_PTR), c_uint]
    )

    def new_create_polygon(self, length, items):
        if not length:
            return capi.create_empty_polygon()

        rings = []
        for r in items:
            if isinstance(r, GEOM_PTR):
                rings.append(r)
            else:
                rings.append(self._construct_ring(r))

        shell = self._clone(rings.pop(0))

        n_holes = length - 1
        if n_holes:
            holes = (GEOM_PTR * n_holes)(*[self._clone(r) for r in rings])
            holes_param = holes
        else:
            holes_param = None

        return capi.create_polygon(shell, holes_param, c_uint(n_holes))

    Polygon._create_polygon = new_create_polygon

    # Need to patch to not call byref so that we can cast to a pointer
    def new_create_collection(self, length, items):
        # Creating the geometry pointer array.
        geoms = (GEOM_PTR * length)(
            *[
                # this is a little sloppy, but makes life easier
                # allow GEOSGeometry types (python wrappers) or pointer types
                capi.geom_clone(getattr(g, "ptr", g))
                for g in items
            ]
        )
        return capi.create_collection(c_int(self._typeid), geoms, c_uint(length))

    GeometryCollection._create_collection = new_create_collection

    """end hotfix """

    # 1 point -> Point; 2 points -> LineString; >2 -> Polygon
    try:
        mp = [GEOSGeometry(json.dumps(g)) for g in g_list]
        hull = GeometryCollection(mp).convex_hull
        # hull=GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).convex_hull
    except:
        logger.exception(f'hully() failed on g_list {g_list}')

    if hull.geom_type in ['Point', 'LineString', 'Polygon']:
        # buffer hull, but only a little if near meridian
        coll = GeometryCollection([GEOSGeometry(json.dumps(g)) for g in g_list]).simplify()
        # longs = list(c[0] for c in coll.coords)
        longs = list(c[0] for c in flatten(coll.coords))
        try:
            if len([i for i in longs if i >= 175]) == 0:
                hull = hull.buffer(1.4)  # ~100km radius
            else:
                hull = hull.buffer(0.1)
        except:
            logger.exception(f'hully buffer error longs: {longs}')
    # print(hull.geojson)
    return json.loads(hull.geojson) if hull.geojson != None else []


# use: insert.py process_geom()
def parse_wkt(g):
    # Load the geometry from the WKT string
    gw = wkt_loads(g)

    # Get the bounding box of the geometry
    minx, miny, maxx, maxy = gw.bounds

    # Check if the bounding box's coordinates are within the valid range
    if not (-180 <= minx <= 180 and -90 <= miny <= 90 and -180 <= maxx <= 180 and -90 <= maxy <= 90):
        raise ValueError("Invalid coordinates in WKT geometry")

    # Convert the geometry to a GeoJSON feature
    feature = json.loads(json.dumps(mapping(gw)))

    return feature


# use: tasks.create_zipfile()
def makeNow():
    ts = time.time()
    sttime = datetime.datetime.fromtimestamp(ts).strftime('%Y%m%d_%H%M%S')
    return sttime


# use: ds_update()
def makeCoords(lonstr, latstr):
    lon = float(lonstr) if lonstr not in ['', 'nan', None] else ''
    lat = float(latstr) if latstr not in ['', 'nan', None] else ''
    coords = [] if (lonstr == '' or latstr == '') else [lon, lat]
    return coords


# might be GeometryCollection or singleton
# use: insert.ds_insert_json(); insert.process_geom(); remote
#
def ccodesFromGeom(geom):
    # print(f"Input geom: {geom}")  # Print the input geom

    if geom['type'] == 'Point' and geom['coordinates'] == []:
        ccodes = []
        # print("Empty coordinates, returning empty list")  # Debug message
        return ccodes
    else:
        g = GEOSGeometry(str(geom))
        # print(f"GEOSGeometry: {g}")  # Print the GEOSGeometry object

        if g.geom_type == 'GeometryCollection':
            # just hull them all
            qs = Country.objects.filter(mpoly__intersects=g.convex_hull)
            # print(f"GeometryCollection, intersecting countries: {qs}")  # Print the queryset
        else:
            qs = Country.objects.filter(mpoly__intersects=g)
            # print(f"Intersecting countries: {qs}")  # Print the queryset

        ccodes = [c.iso for c in qs]
        # print(f"Country codes: {ccodes}")  # Print the country codes

        return ccodes


# use: tasks
def elapsed(delta):
    minutes, seconds = divmod(delta.seconds, 60)
    return '{:02}:{:02}'.format(int(minutes), int(seconds))


# wikidata Qs from ccodes
# TODO: consolidate hashes
def getQ(arr, what):
    # print('arr,what',arr, what)
    qids = []
    if what == 'ccodes':
        from datasets.static.hashes.parents import ccodes
        for c in arr:
            if c.upper() in ccodes[0]:
                qids.append('wd:' + ccodes[0][c.upper()]['wdid'].upper())
    elif what == 'types':
        # if len(arr) == 0:
        #   qids.append('wd:Q486972')
        for t in arr:
            if t in aat_q.qnums:
                for q in aat_q.qnums[t]:
                    qids.append('wd:' + q)
            # else:
            #   qids.append('wd:Q486972')
    return list(set(qids))


def roundy(x, direct="up", base=10):
    import math
    if direct == "down":
        return int(math.ceil(x / 10.0)) * 10 - base
    else:
        return int(math.ceil(x / 10.0)) * 10


def fixName(toponym):
    import re
    search_name = toponym
    r1 = re.compile(r"(.*?), Gulf of")
    r2 = re.compile(r"(.*?), Sea of")
    r3 = re.compile(r"(.*?), Cape")
    r4 = re.compile(r"^'")
    if bool(re.search(r1, toponym)):
        search_name = "Gulf of " + re.search(r1, toponym).group(1)
    if bool(re.search(r2, toponym)):
        search_name = "Sea of " + re.search(r2, toponym).group(1)
    if bool(re.search(r3, toponym)):
        search_name = "Cape " + re.search(r3, toponym).group(1)
    if bool(re.search(r4, toponym)):
        search_name = toponym[1:]
    return search_name if search_name != toponym else toponym


# in: list of Black atlas place types
# returns list of equivalent classes or types for {gaz}
def classy(gaz, typeArray):
    import codecs, json
    # print(typeArray)
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
        default = 'inhabited places'  # inhabited places
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
            # is any Black type in dbp array?
            # TODO: this is crap logic, fix it
            if not set(typeArray).isdisjoint(t[k]):
                types.append(k)
    if len(types) == 0:
        types.append(default)
    return list(set(types))


# log recon action & update status
def post_recon_update(ds, user, task, test):
    if test == "off":
        if task == 'idx':
            ds.ds_status = 'indexed' if ds.unindexed == 0 else 'accessioning'
        else:
            ds.ds_status = 'reconciling'
        ds.save()
    else:
        task += '_test'
    # recon task has completed, log it
    logobj = Log.objects.create(
        category='dataset',
        logtype='ds_recon',
        subtype='align_' + task,
        dataset_id=ds.id,
        user_id=user.id
    )
    logobj.save()
    # print('post_recon_update() logobj',logobj)


# TODO: faster?
# deprecatING Apr 2024
class UpdateCountsView(View):
    """ Returns counts of unreviewed records, per pass and total; also deferred per task
    """

    @staticmethod
    def get(request):
        # print('UpdateCountsView GET:',request.GET)
        """
        args in request.GET:
            [integer] ds_id: dataset id
        """
        ds = get_object_or_404(Dataset, id=request.GET.get('ds_id'))

        # deferred counts
        def defcountfunc(taskname, pids):
            if taskname[6:] in ['whg', 'idx']:
                return ds.places.filter(id__in=pids, review_whg=2).count()
            elif taskname[6:].startswith('wd'):
                return ds.places.filter(id__in=pids, review_wd=2).count()
            else:
                return ds.places.filter(id__in=pids, review_tgn=2).count()

        def placecounter(th):
            pcounts = {}
            # for th in taskhits.all():
            pcounts['p0'] = th.filter(query_pass='pass0').values('place_id').distinct().count()
            pcounts['p1'] = th.filter(query_pass='pass1').values('place_id').distinct().count()
            pcounts['p2'] = th.filter(query_pass='pass2').values('place_id').distinct().count()
            # pcounts['p3'] = th.filter(query_pass='pass3').values('place_id').distinct().count()
            return pcounts

        updates = {}
        # counts of distinct place ids w/unreviewed hits per task/pass
        # for t in ds.tasks.filter(status='SUCCESS'):
        #   taskhits = Hit.objects.filter(task_id=t.task_id, reviewed=False)
        for t in ds.tasks.filter(status='SUCCESS'):
            taskhits = Hit.objects.filter(task_id=t.task_id, reviewed=False)
            # taskhits = Hit.objects.filter(task_id=t.task_id, reviewed=True)
            pcounts = placecounter(taskhits)
            # ids of all unreviewed places
            pids = list(set(taskhits.all().values_list("place_id", flat=True)))
            defcount = defcountfunc(t.task_name, pids)

            updates[t.task_id] = {
                "task": t.task_name,
                "total": len(pids),
                "pass0": pcounts['p0'],
                "pass1": pcounts['p1'],
                "pass2": pcounts['p2'],
                "pass3": pcounts['p3'],
                "deferred": defcount
            }

        # print(json.dumps(updates, indent=2))
        return JsonResponse(updates, safe=False)
