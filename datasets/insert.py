# functions for inserting file data into database

import csv, json, os, re, sys
from datetime import datetime
import pandas as pd
import logging
import numpy as np
import traceback
import warnings
from dateutil.parser import parse

from django.contrib import messages
from django.contrib.gis.geos import GEOSGeometry
from django.core.exceptions import ValidationError
from django.db import transaction
from django.db.utils import IntegrityError, DataError
from django.http import HttpResponseServerError
from django.shortcuts import get_object_or_404, redirect
from django.utils import timezone
from django.utils.html import strip_tags

from .exceptions import DelimInsertError, DataAlreadyProcessedError
from .models import Dataset
from areas.models import Area
from datasets.place_types import apply_types_to_place, has_type_columns
from validation.coordinates import resolve_lonlat
from datasets.utils import aat_lookup, ccodesFromGeom, \
    makeCoords, parse_wkt, parsedates_tsv
from places.models import *
from whgmail.messaging import WHGmail

logger = logging.getLogger('validation')

# 'lugares_20.jsonld','lugares_20_broken.jsonld'
# test setup
warnings.simplefilter(action='ignore', category=FutureWarning)


def test_insert():
    from datasets.models import Dataset, DatasetFile
    Dataset.objects.get(label='delim_test').delete()
    ds = Dataset.objects.create(
        owner=User.objects.get(id=1),
        label='delim_test',
        title='Delim Test',
        description='new, empty to receive from ds_insert_delim()'
    )


def read_tsv_into_dataframe(file):
    files = ['lp7_100.tsv',
             'lp7_100_broken.tsv']
    path = os.getcwd() + '/_testdata/_upload/'
    df = pd.read_csv(path + file, sep='\t')
    # df = read_tsv_into_dataframe(files[0])
    # dfbroken = read_tsv_into_dataframe(files[1])
    # df=dfbroken


# The AAT lookup used to be built here, at import time, from a full `Type` table scan —
# which made importing this module impossible without a database. It now lives in
# `datasets.place_types.aat_index()`, built lazily and shared with the update path.


# create Place and a PlaceName for its 'title'
def create_place(row, ds):
    # build & save a Place object & PlaceName object for its title
    pl = Place.objects.create(
        src_id=row['id'],
        title=row['title'].strip(),
        dataset=ds,
        ccodes=[] if pd.isnull(row.get('ccodes')) else row.get('ccodes').split(';')
    )

    title_source = row['title_source'],
    title_uri = None if pd.isnull(row.get('title_uri')) else row.get('title_uri'),

    PlaceName.objects.create(
        place=pl,
        src_id=row['id'],
        toponym=row['title'].strip(),
        jsonb={"toponym": pl.title, "citations": [{"id": title_uri, "label": title_source}]}
    )
    return pl


# create PlaceName object for each variant
def process_variants(row, newpl):
    variants = None if pd.isnull(row.get('variants')) else \
        [x.strip() for x in str(row.get('variants', '')).split(';')] \
            if row.get('variants', '') != '' else []
    name_objects = []  # gather variants to be written
    error_msgs = []  # gather error messages
    title_uri = None if pd.isnull(row.get('title_uri')) else row.get('title_uri'),
    if variants:
        for v in variants:
            try:
                name = v.strip()
                haslang = re.search("@(.*)$", name)
                new_name = PlaceName(
                    place=newpl,
                    src_id=newpl.src_id,
                    toponym=v.strip(),
                    jsonb={"toponym": v.strip(),
                           "citations": [
                               {"id": title_uri,
                                "label": row['title_source']}
                           ]})
                if haslang:
                    new_name.jsonb['lang'] = haslang.group(1)

                name_objects.append(new_name)
            except Exception as e:
                full_trace = traceback.format_exc()
                error_msgs.append(
                    f"Error writing variant '{v}' for place <b>{row['id']}</b>: <b>{row['title']}</b>. Details: {e}")

    if error_msgs:  # if there are any error messages
        raise DelimInsertError("; ".join(error_msgs))  # raise an exception with all error messages joined

    return name_objects


# create PlaceType object for each aat_type; update Place with fclasses[]
def process_types(row, newpl):
    """
    Thin wrapper over the shared derivation in `datasets.place_types`, which the update
    path calls too — the duplicated copy here is what let the two drift (place#213).
    """
    try:
        return apply_types_to_place(newpl, row)
    except Exception as e:
        raise DelimInsertError(
            f"Error writing types and fclass codes for place <b>{newpl} ({newpl.src_id})</b>. Details: {e}")


# create PlaceWhen object; update Place with minmax, timespans
from dateutil.parser import isoparse, ParserError

from dateutil.parser import isoparse, ParserError


class CustomDate:
    def __init__(self, year, month=1, day=1):
        self.year = year
        self.month = month
        self.day = day

    def __repr__(self):
        return f"CustomDate(year={self.year}, month={self.month}, day={self.day})"

    def isoformat(self):
        year_str = f"{abs(self.year):04d}"
        if self.year < 0:
            year_str = f"-{year_str}"
        return f"{year_str}-{self.month:02d}-{self.day:02d}"


def process_when(row, newpl):
    error_msgs = []

    def pad_and_format_date(date_str):
        if date_str.startswith('-'):
            parts = date_str[1:].split('-')
            parts[0] = parts[0].zfill(4)
            formatted_date = '-' + '-'.join(parts)
        else:
            parts = date_str.split('-')
            parts[0] = parts[0].zfill(4)
            formatted_date = '-'.join(parts)

        return formatted_date

    def parse_iso_date(date_str):
        try:
            padded_date_str = pad_and_format_date(date_str)
            parts = padded_date_str.split('-')

            if padded_date_str.startswith('-'):
                year = -int(parts[1])
                remaining_parts = parts[1:]
            else:
                year = int(parts[0])
                remaining_parts = parts[1:]

            if year < 0:
                if len(remaining_parts) == 0:
                    date = CustomDate(year)
                elif len(remaining_parts) == 1:
                    date = CustomDate(year, int(remaining_parts[0]))
                elif len(remaining_parts) == 2:
                    date = CustomDate(year, int(remaining_parts[0]), int(remaining_parts[1]))
                return date
            else:
                date = isoparse(padded_date_str)
                return date
        except (ValueError, ParserError) as e:
            raise ValueError(f"Invalid ISO-8601 date: {date_str}. Error: {e}")

    try:
        start = parse_iso_date(str(row['start'])) if 'start' in row and row.get('start', '') else None
        end = parse_iso_date(str(row['end'])) if 'end' in row and row.get('end', '') else None
        attestation_year = str(row['attestation_year']) if 'attestation_year' in row and not pd.isna(
            row['attestation_year']) else None
        # attestation_year = str(row['attestation_year']) if 'attestation_year' in row and row.get('attestation_year', '') else None

        if start and end and isinstance(start, CustomDate) and isinstance(end, CustomDate) and start.year > end.year:
            raise ValueError("Start date (" + str(start) + ") is greater than end date (" + str(end) + ")")

        dates = (start, end, attestation_year)

        datesobj = parsedates_tsv(dates)

        newpl.minmax = datesobj['minmax']
        newpl.attestation_year = attestation_year
        newpl.timespans = [datesobj['minmax']]
        newpl.save()

        when_object = PlaceWhen(
            place=newpl,
            src_id=newpl.src_id,
            jsonb=datesobj
        )

    except ValueError as e:
        error_msgs.append(f"Error processing dates for place <b>{newpl} ({newpl.src_id})</b>. Details: {e}")

    if error_msgs:
        raise DelimInsertError("; ".join(error_msgs))

    return [when_object]


# create PlaceWhen object; update Place with minmax, timespans
# def process_when(row, newpl):
#   error_msgs = []
#   try:
#       # Check for existence of the columns and not empty
#       start = parse(str(row['start'])) if 'start' in row and row.get('start', '') else None
#       end = parse(str(row['end'])) if 'end' in row and row.get('end', '') else None
#       attestation_year = str(row['attestation_year']) if \
#           'attestation_year' in row and row.get('attestation_year', '') else None
#
#       # Check if start is less than end
#       if start and end and start > end:
#           raise ValueError("Start date ("+str(start)+") is greater than end date ("+str(end)+")")
#
#       # Either start or attestation_year is assured by validation
#       dates = (start, end, attestation_year)
#       datesobj = parsedates_tsv(dates)
#
#       # Update the newpl object with the computed values
#       newpl.minmax = datesobj['minmax']
#       newpl.attestation_year = attestation_year
#       newpl.timespans = [datesobj['minmax']]
#       newpl.save()
#
#       # Create the PlaceWhen object
#       when_object = PlaceWhen(
#           place=newpl,
#           src_id=newpl.src_id,
#           jsonb=datesobj
#       )
#
#   except ValueError as e:
#       error_msgs.append(f"Error processing dates for place <b>{newpl} ({newpl.src_id})</b>. Details: {e}")
#
#   if error_msgs:
#     raise DelimInsertError("; ".join(error_msgs))
#
#   return [when_object]

# create PlaceGeom object and/or generate ccodes
def process_geom(row, newpl):
    error_msgs = []
    geojson = None
    valid_ccodes = [ccode.upper() for c in Area.objects.filter(type='country') for ccode in c.ccodes]

    has_geowkt = 'geowkt' in row and row['geowkt']
    has_lonlat = any(col in row and row[col] not in ('', None) for col in ('lat', 'lon'))

    # If 'lat'/'lon' are given, generate Point geometry. The pair goes through the same
    # parser as the validation path so that a locale-mangled, transposed or unreadable cell
    # is repaired or reported rather than dropped on the floor (place#212).
    if has_lonlat and not has_geowkt:
        ccodes_raw = [c.strip() for c in str(row.get('ccodes') or '').split(';') if c.strip()]
        lon, lat, repairs, coord_errors = resolve_lonlat(row.get('lon'), row.get('lat'), ccodes=ccodes_raw)
        for repair in repairs:
            logger.info(f"Place {newpl.src_id}: {strip_tags(repair)}")
        if coord_errors:
            error_msgs.extend(
                f"{strip_tags(msg)} (place <b>{newpl} ({newpl.src_id})</b>)" for msg in coord_errors)
        elif lon is not None and lat is not None:
            coords = [lon, lat]
            geojson = {
                "type": "Point",
                "coordinates": coords,
                "geowkt": 'POINT(' + str(coords[0]) + ' ' + str(coords[1]) + ')'
            }
    # If 'geowkt' is present, parse and generate corresponding geojson. It supersedes
    # lat/lon entirely, so any problem with those is not the contributor's to fix.
    elif has_geowkt:
        # print('geowkt', row['geowkt'])
        try:
            geojson = parse_wkt(row['geowkt'])
            # print('geojson', geojson)
        except Exception as e:
            error_msgs.append(f"Error converting WKT for place <b>{newpl} ({newpl.src_id})</b>. Details: {e}")

    # Add optional 'geo_source' and 'geo_id' to geojson
    if geojson and 'geo_source' in row and row['geo_source']:
        geojson['citation'] = {
            'label': row['geo_source'],
            'id': row['geo_id'] if 'geo_id' in row else None
        }

    # Create the PlaceGeom object if geojson exists
    geom_object = None
    if geojson:
        try:
            geom_object = PlaceGeom(
                place=newpl,
                src_id=newpl.src_id,
                jsonb=geojson,
                geom=GEOSGeometry(json.dumps(geojson))
            )
        except Exception as e:
            error_msgs.append(f"Error creating GEOSGeometry for place <b>{newpl} ({newpl.src_id})</b>. Details: {e}")

    # Process ccodes
    if 'ccodes' in row and row['ccodes']:
        ccodes = [x.strip().upper() for x in row['ccodes'].split(';')]
        # print('ccodes', ccodes)
        for ccode in ccodes:
            if ccode not in valid_ccodes:
                error_msgs.append(f"At least one invalid ccode: {ccode} for place <b>{newpl} ({newpl.src_id})</b>")
    elif geojson:
        try:
            ccodes = ccodesFromGeom(geojson)  # might be if not terrestrial []
        except Exception as e:
            pass
    else:
        ccodes = []

    newpl.ccodes = ccodes
    newpl.save()

    if error_msgs:
        raise DelimInsertError("; ".join(error_msgs))

    # Return the geom_object (if it was created)
    return [geom_object] if geom_object else []


# create PlaceLink object for each matches value
def process_links(row, newpl):
    link_objects = []

    # confirmed previously to have accepted alias prefix
    for link in row['matches'].split(';'):
        link_object = PlaceLink(
            place=newpl,
            src_id=newpl.src_id,
            jsonb={"identifier": link, "type": "closeMatch"}
        )
        link_objects.append(link_object)
    return link_objects


# create PlaceRelated object from parent_name, parent_id
def process_related(row, newpl):
    # in LP-Delim, only parent relation is gvp:broaderPartitive
    related_object = PlaceRelated(
        place=newpl,
        src_id=newpl.src_id,
        jsonb={"label": row["parent_name"],
               "relationType": "gvp:broaderPartitive"},
    )
    parent_id = row.get('parent_id')
    if parent_id is not None:
        related_object.jsonb['relationTo'] = parent_id

    return [related_object]


# create PlaceDescription object
def process_descriptions(row, newpl):
    descrip_object = PlaceDescription(
        place=newpl,
        src_id=newpl.src_id,
        jsonb={"value": row['description']}
    )

    return [descrip_object]


"""
  ds_insert_delim(df, pk)
  *** replaces ds_insert_tsv() ***
  insert delimited data into database
  if insert fails anywhere, delete dataset + any related objects
"""


def ds_insert_delim(df, pk):
    """
    :param df: dataframe
    :param pk: primary key of dataset
    """
    # print('in ds_insert_delim() with df, pk', df, pk)
    # tidy up dataframe
    df.dropna(axis=1, how='all', inplace=True)
    df.replace({np.nan: None}, inplace=True)
    # get new dataset
    ds = get_object_or_404(Dataset, id=pk)
    uribase = ds.uri_base
    # print('existing places', [p.__dict__ for p in Place.objects.filter(dataset=ds.label)])
    noplaces = Place.objects.filter(dataset=ds.label).count() == 0
    skipped_rows = []
    skipped_row_ids = []

    # ensure dataset has no orphaned Place records
    # this appears to be a pre-refactor remnant
    if not noplaces:
        raise DataAlreadyProcessedError("The data appears to have already been processed.")

    # bins for bulk_insert at end
    objlists = {"PlaceName": [], "PlaceType": [], "PlaceGeom": [], "PlaceWhen": [],
                "PlaceLink": [], "PlaceRelated": [], "PlaceDescription": []}

    # Loop through rows for insert
    for index, row in df.iterrows():
        with transaction.atomic():
            """
              create new Place + a PlaceName record from its title
            """
            if not Place.objects.filter(src_id=row['id'], dataset=ds).exists():
                newpl = create_place(row, ds)
            else:
                skipped_rows += 1
                skipped_row_ids.append(row['id'])

            """
            generate new related objects for objlists[]
            """
            # PlaceName
            if 'variants' in df.columns and row['variants'] not in ['', None]:
                objlists['PlaceName'].extend(process_variants(row, newpl))

            # PlaceType
            if has_type_columns(row):
                objlists['PlaceType'].extend(process_types(row, newpl))

            # PlaceWhen (always at least a start or attestation_year)
            objlists['PlaceWhen'].extend(process_when(row, newpl))

            # PlaceGeom. NB the lat/lon test is deliberately not a truthiness test on the
            # pair: a row with only one of them, or with an unparseable value, must reach
            # process_geom to be reported rather than quietly losing its location.
            if (any(col in row and row[col] not in ('', None) for col in ('lat', 'lon'))
                    or ('geowkt' in row and row['geowkt'])):
                objlists['PlaceGeom'].extend(process_geom(row, newpl))

            # PlaceLink
            if ('matches' in row and row['matches']):
                objlists['PlaceLink'].extend(process_links(row, newpl))

            # PlaceRelated
            # TODO: does not handle parent_id
            if ('parent_name' in row and row['parent_name']):
                objlists['PlaceRelated'].extend(process_related(row, newpl))

            # PlaceDescription
            if ('description' in row and row['description']):
                objlists['PlaceDescription'].extend(process_descriptions(row, newpl))

    # print('PlaceWhen list', len(objlists['PlaceWhen']), objlists['PlaceWhen'][0].__dict__)
    # bulk_create for each related model; NB no depictions in LP-Delim
    PlaceName.objects.bulk_create(objlists['PlaceName'], batch_size=10000)
    PlaceType.objects.bulk_create(objlists['PlaceType'], batch_size=10000)
    PlaceGeom.objects.bulk_create(objlists['PlaceGeom'], batch_size=10000)
    PlaceLink.objects.bulk_create(objlists['PlaceLink'], batch_size=10000)
    PlaceRelated.objects.bulk_create(objlists['PlaceRelated'], batch_size=10000)
    PlaceWhen.objects.bulk_create(objlists['PlaceWhen'], batch_size=10000)
    PlaceDescription.objects.bulk_create(objlists['PlaceDescription'], batch_size=10000)


# """
#   ds_insert_json(data, pk)
#   *** replaces ds_insert_lpf() ***
#   insert LPF into database
# """
# def ds_insert_json_DEPRECATED(data, pk):
#     ds = get_object_or_404(Dataset, id=pk)
#     places_already_exist = Place.objects.filter(dataset=ds.label).exists()
#     if places_already_exist:
#         logger.error("Database already contains places for this dataset. Cannot add more.")
#         raise Exception("Database already contains places for this dataset. Cannot add more.")
#
#     jdata = json.loads(data) if isinstance(data, str) else data
#
#     uribase = ds.uri_base
#     # logger.debug(f"New dataset: {ds.label}, uri_base: {uribase}, data type: {type(jdata)}, feature count: {len(jdata['features'])}")
#
#     errors = []
#     try:
#         with transaction.atomic():
#             logger.debug("transaction.atomic")
#             data_mappings = {
#                 'PlaceGeoms': ('Geom', 'geometry', lambda feat: [
#                     PlaceGeom(place=newpl, src_id=newpl.src_id, jsonb=g, geom=GEOSGeometry(json.dumps(g)))
#                     for g in feat['geometry']['geometries']] if feat['geometry']['type'] == 'GeometryCollection' else
#                 [PlaceGeom(place=newpl, src_id=newpl.src_id, jsonb=feat['geometry'],
#                            geom=GEOSGeometry(json.dumps(feat['geometry'])))]),
#                 'PlaceWhens': ('When', 'when', lambda feat: [
#                     PlaceWhen(place=newpl, src_id=newpl.src_id, jsonb=feat['when'], minmax=newpl.minmax)]),
#                 'PlaceLinks': ('Link', 'links', lambda feat: [
#                     PlaceLink(place=newpl, src_id=newpl.src_id,
#                               jsonb={"type": l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))})
#                     for l in feat['links']]),
#                 'PlaceRelated': ('Related', 'relations', lambda feat: [
#                     PlaceRelated(place=newpl, src_id=newpl.src_id, jsonb=r)
#                     for r in feat['relations']]),
#                 'PlaceDescriptions': ('Description', 'descriptions', lambda feat: [
#                     PlaceDescription(place=newpl, src_id=newpl.src_id, jsonb=des)
#                     for des in feat['descriptions']]),
#                 'PlaceDepictions': ('Depiction', 'depictions', lambda feat: [
#                     PlaceDepiction(place=newpl, src_id=newpl.src_id, jsonb=dep)
#                     for dep in feat['depictions']]),
#                 'PlaceNames': ('Name', 'names', lambda feat: [
#                     PlaceName(place=newpl, src_id=newpl.src_id, toponym=n['toponym'].split(',')[0].strip(), jsonb=n)
#                     for n in feat.get('names', []) if 'toponym' in n]),
#                 'PlaceTypes': ('Type', 'types', lambda feat: [
#                     PlaceType(place=newpl, src_id=newpl.src_id, jsonb=t, fclass=fc)
#                     for t, fc in zip(feat.get('types', []), fclass_list)])
#             }
#             logger.debug(f"data_mappings: {data_mappings}")
#
#             for feat in jdata['features']:
#                 logger.debug(f"feat: {feat}")
#
#                 title = re.sub(r'\(.*?\)', '', feat.get('properties').get('title', ''))
#
#                 geojson = feat.get('geometry')
#                 ccodes = feat.get('properties').get('ccodes', [])
#                 if ccodes is None and geojson:
#                     ccodes = ccodesFromGeom(geojson)
#                 logger.debug('ccodes: {ccodes}')
#
#                 # (minmax and intervals[])
#                 datesobj = parsedates_lpf(feat)
#                 logger.debug(f"datesobj: {datesobj}")
#
#                 fclass_list = get_fclass_list(feat)
#                 logger.debug(f"fclass_list: {fclass_list}")
#
#                 newpl = Place(
#                     # strip uribase from @id
#                     src_id=feat.get('@id') if uribase in ['', None] or not feat.get('@id').startswith(
#                         uribase) else feat.get('@id')[len(uribase):],
#                     dataset=ds,
#                     title=title,
#                     fclasses=fclass_list,
#                     ccodes=ccodes,
#                     minmax=datesobj['minmax'],
#                     timespans=datesobj['intervals'],
#                     create_date=timezone.now()
#                 )
#                 newpl.save()
#                 logger.debug(f'New place: {newpl}')
#
#                 objs = {}
#                 objs.update({
#                     key: list(filter(None, [item for sublist in map(create_func, [feat]) for item in sublist]))
#                     for key, (_, feat_key, create_func) in data_mappings.items()
#                     if feat.get(feat_key)
#                 })
#
#                 for model, obj_list in [(model, objs[key]) for key, (model, _, _) in data_mappings.items() if
#                                         objs.get(key)]:
#                     try:
#                         bulk_create_method = getattr(globals()[f'Place{model}'], 'objects').bulk_create
#                         bulk_create_method(obj_list)
#                     except IntegrityError as e:
#                         errors.append({"field": model, "error": str(e)})
#                         raise IntegrityError(f"IntegrityError in bulk create for {model}: {e}")
#                     except ValidationError as e:
#                         errors.append({"field": model, "error": str(e)})
#                         raise ValidationError(f"ValidationError in bulk create for {model}: {e}")
#                     except DataError as e:
#                         errors.append({"field": model, "error": str(e)})
#                         raise DataError(f"Bulk load for {model} failed on {newpl}: {e}")
#                     except Exception as e:
#                         errors.append({"field": model, "error": str(e)})
#                         raise Exception(f"Unexpected error in bulk create for {model}: {e}")
#
#     except Exception as e:
#         logger.debug(f"Failed to insert data into dataset: {e}")
#         raise Exception(f"Failed to insert data into dataset: {e}, Errors: {errors}")
#
#     # print('new dataset:', ds.__dict__)
#
#     return  # ({"numrows": len(jdata['features'])})


def failed_insert_notification(user, fn, ds=None):
    """Send email to user when insert fails"""

    WHGmail(context={
        'template': 'failed_insert',
        'to_email': user.email,
        'subject': f"World Historical Gazetteer error followup{f' on dataset ({ds})' if ds else ''}",
        'filename': fn,
        'dataset_title': ds.title if ds else 'N/A',
        'dataset_label': ds.label if ds else 'N/A',
        'dataset_id': ds.id if ds else 'N/A',
    })


""" 
	DEPRECATED 2023-08
  ds_insert_lpf
  insert LPF into database
"""
# def ds_insert_lpf(request, pk):
#   import json
#   [countrows,countlinked,total_links]= [0,0,0]
#   ds = get_object_or_404(Dataset, id=pk)
#   user = request.user
#   # latest file
#   dsf = ds.files.all().order_by('-rev')[0]
#   uribase = ds.uri_base
#   print('new dataset, uri_base', ds.label, uribase)
#
#   # TODO: lpf can get big; support json-lines
#
#   # insert only if empty
#   dbcount = Place.objects.filter(dataset = ds.label).count()
#   print('dbcount',dbcount)
#
#   if dbcount == 0:
#     errors=[]
#     try:
#       infile = dsf.file.open(mode="r")
#       print('ds_insert_lpf() for dataset',ds)
#       print('ds_insert_lpf() request.GET, infile',request.GET,infile)
#       with infile:
#         jdata = json.loads(infile.read())
#
#         print('count of features',len(jdata['features']))
#         #print('0th feature',jdata['features'][0])
#
#         for feat in jdata['features']:
#           # create Place, save to get id, then build associated records for each
#           objs = {"PlaceNames":[], "PlaceTypes":[], "PlaceGeoms":[], "PlaceWhens":[],
#                   "PlaceLinks":[], "PlaceRelated":[], "PlaceDescriptions":[],
#                   "PlaceDepictions":[]}
#           countrows += 1
#
#           # build attributes for new Place instance
#           title=re.sub('\(.*?\)', '', feat['properties']['title'])
#
#           # geometry
#           geojson = feat['geometry'] if 'geometry' in feat.keys() else None
#
#           # ccodes
#           if 'ccodes' not in feat['properties'].keys():
#             if geojson:
#               # a GeometryCollection
#               ccodes = ccodesFromGeom(geojson)
#               print('ccodes', ccodes)
#             else:
#               ccodes = []
#           else:
#             ccodes = feat['properties']['ccodes']
#
#           # temporal
#           # send entire feat for time summary
#           # (minmax and intervals[])
#           datesobj=parsedates_lpf(feat)
#
#           # TODO: compute fclasses
#           try:
#             newpl = Place(
#               # strip uribase from @id
#               src_id=feat['@id'] if uribase in ['', None] else feat['@id'].replace(uribase,''),
#               dataset=ds,
#               title=title,
#               ccodes=ccodes,
#               minmax = datesobj['minmax'],
#               timespans = datesobj['intervals']
#             )
#             newpl.save()
#             print('new place: ',newpl.title)
#           except:
#             print('failed id' + title + 'datesobj: '+str(datesobj))
#             print(sys.exc_info())
#
#           # PlaceName: place,src_id,toponym,task_id,
#           # jsonb:{toponym, lang, citation[{label, year, @id}], when{timespans, ...}}
#           # TODO: adjust for 'ethnic', 'demonym'
#           for n in feat['names']:
#             if 'toponym' in n.keys():
#               # if comma-separated listed, get first
#               objs['PlaceNames'].append(PlaceName(
#                 place=newpl,
#                 src_id=newpl.src_id,
#                 toponym=n['toponym'].split(', ')[0],
#                 jsonb=n
#               ))
#
#           # PlaceType: place,src_id,task_id,jsonb:{identifier,label,src_label}
#           #try:
#           if 'types' in feat.keys():
#             fclass_list = []
#             for t in feat['types']:
#               if 'identifier' in t.keys() and t['identifier'][:4] == 'aat:' \
#                  and int(t['identifier'][4:]) in Type.objects.values_list('aat_id',flat=True):
#                 fc = get_object_or_404(Type, aat_id=int(t['identifier'][4:])).fclass \
#                   if t['identifier'][:4] == 'aat:' else None
#                 fclass_list.append(fc)
#               else:
#                 fc = None
#               print('from feat[types]:',t)
#               print('PlaceType record newpl,newpl.src_id,t,fc',newpl,newpl.src_id,t,fc)
#               objs['PlaceTypes'].append(PlaceType(
#                 place=newpl,
#                 src_id=newpl.src_id,
#                 jsonb=t,
#                 fclass=fc
#               ))
#             newpl.fclasses = fclass_list
#             newpl.save()
#
#           # PlaceWhen: place,src_id,task_id,minmax,jsonb:{timespans[],periods[],label,duration}
#           if 'when' in feat.keys() and feat['when'] != {}:
#             objs['PlaceWhens'].append(PlaceWhen(
#               place=newpl,
#               src_id=newpl.src_id,
#               jsonb=feat['when'],
#               minmax=newpl.minmax
#             ))
#
#           # PlaceGeom: place,src_id,task_id,jsonb:{type,coordinates[],when{},geo_wkt,src}
#           #if 'geometry' in feat.keys() and feat['geometry']['type']=='GeometryCollection':
#           if geojson and geojson['type']=='GeometryCollection':
#             #for g in feat['geometry']['geometries']:
#             for g in geojson['geometries']:
#               # print('from feat[geometry]:',g)
#               objs['PlaceGeoms'].append(PlaceGeom(
#                 place=newpl,
#                 src_id=newpl.src_id,
#                 jsonb=g
#                 ,geom=GEOSGeometry(json.dumps(g))
#               ))
#           elif geojson:
#             objs['PlaceGeoms'].append(PlaceGeom(
#               place=newpl,
#               src_id=newpl.src_id,
#               jsonb=geojson
#               ,geom=GEOSGeometry(json.dumps(geojson))
#             ))
#
#           # PlaceLink: place,src_id,task_id,jsonb:{type,identifier}
#           if 'links' in feat.keys() and len(feat['links'])>0:
#             countlinked +=1 # record has *any* links
#             #print('countlinked',countlinked)
#             for l in feat['links']:
#               total_links += 1 # record has n links
#               objs['PlaceLinks'].append(PlaceLink(
#                 place=newpl,
#                 src_id=newpl.src_id,
#                 # alias uri base for known authorities
#                 jsonb={"type":l['type'], "identifier": aliasIt(l['identifier'].rstrip('/'))}
#               ))
#
#           # PlaceRelated: place,src_id,task_id,jsonb{relationType,relationTo,label,when{}}
#           if 'relations' in feat.keys():
#             for r in feat['relations']:
#               objs['PlaceRelated'].append(PlaceRelated(
#                 place=newpl,src_id=newpl.src_id,jsonb=r))
#
#           # PlaceDescription: place,src_id,task_id,jsonb{@id,value,lang}
#           if 'descriptions' in feat.keys():
#             for des in feat['descriptions']:
#               objs['PlaceDescriptions'].append(PlaceDescription(
#                 place=newpl,src_id=newpl.src_id,jsonb=des))
#
#           # PlaceDepiction: place,src_id,task_id,jsonb{@id,title,license}
#           if 'depictions' in feat.keys():
#             for dep in feat['depictions']:
#               objs['PlaceDepictions'].append(PlaceDepiction(
#                 place=newpl,src_id=newpl.src_id,jsonb=dep))
#
#           # throw errors into user message
#           def raiser(model, e):
#             print('Bulk load for '+ model + ' failed on', newpl)
#             errors.append({"field":model, "error":e})
#             print('error', e)
#             raise DataError
#
#           # create related objects
#           try:
#             PlaceName.objects.bulk_create(objs['PlaceNames'])
#           except DataError as e:
#             raiser('Name', e)
#
#           try:
#             PlaceType.objects.bulk_create(objs['PlaceTypes'])
#           except DataError as de:
#             raiser('Type', e)
#
#           try:
#             PlaceWhen.objects.bulk_create(objs['PlaceWhens'])
#           except DataError as de:
#             raiser('When', e)
#
#           try:
#             PlaceGeom.objects.bulk_create(objs['PlaceGeoms'])
#           except DataError as de:
#             raiser('Geom', e)
#
#           try:
#             PlaceLink.objects.bulk_create(objs['PlaceLinks'])
#           except DataError as de:
#             raiser('Link', e)
#
#           try:
#             PlaceRelated.objects.bulk_create(objs['PlaceRelated'])
#           except DataError as de:
#             raiser('Related', e)
#
#           try:
#             PlaceDescription.objects.bulk_create(objs['PlaceDescriptions'])
#           except DataError as de:
#             raiser('Description', e)
#
#           try:
#             PlaceDepiction.objects.bulk_create(objs['PlaceDepictions'])
#           except DataError as de:
#             raiser('Depiction', e)
#
#           # TODO: compute newpl.ccodes (if geom), newpl.fclasses, newpl.minmax
#           # something failed in *any* Place creation; delete dataset
#
#         print('new dataset:', ds.__dict__)
#         infile.close()
#
#       return({"numrows":countrows,
#               "numlinked":countlinked,
#               "total_links":total_links})
#     except:
#       # drop the (empty) database
#       # ds.delete()
#       # email to user, admin
#       subj = 'World Historical Gazetteer error followup'
#       msg = 'Hello '+ user.username+', \n\nWe see your recent upload for the '+ds.label+\
#             ' dataset failed, very sorry about that!'+\
#             '\nThe likely cause was: '+str(errors)+'\n\n'+\
#             "If you can, fix the cause. If not, please respond to this email and we will get back to you soon.\n\nRegards,\nThe WHG Team"
#       emailer(subj,msg,'whg@kgeographer.org',[user.email, 'whgadmin@kgeographer.com'])
#
#       # return message to 500.html
#       # messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
#       # return redirect(request.GET.get('from'))
#       return HttpResponseServerError()
#
#   else:
#     print('insert_ skipped, already in')
#     messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
#     return redirect('/mydata')

"""
	DEPRECATED 2023-08
  ds_insert_tsv(pk)
  insert tsv into database
  file is validated, dataset exists
  if insert fails anywhere, delete dataset + any related objects
"""
# def ds_insert_tsv(request, pk):
#   import csv, re
#   csv.field_size_limit(300000)
#   ds = get_object_or_404(Dataset, id=pk)
#   user = request.user
#   print('ds_insert_tsv()',ds)
#   # retrieve just-added file
#   dsf = ds.files.all().order_by('-rev')[0]
#   print('ds_insert_tsv(): ds, file', ds, dsf)
#   # insert only if empty
#   dbcount = Place.objects.filter(dataset = ds.label).count()
#   # print('dbcount',dbcount)
#   insert_errors = []
#   if dbcount == 0:
#     try:
#       infile = dsf.file.open(mode="r")
#       reader = csv.reader(infile, delimiter=dsf.delimiter)
#       # reader = csv.reader(infile, delimiter='\t')
#
#       infile.seek(0)
#       header = next(reader, None)
#       header = [col.lower().strip() for col in header]
#       # print('header.lower()',[col.lower() for col in header])
#
#       # strip BOM character if exists
#       header[0] = header[0][1:] if '\ufeff' in header[0] else header[0]
#       print('header', header)
#
#       objs = {"PlaceName":[], "PlaceType":[], "PlaceGeom":[], "PlaceWhen":[],
#               "PlaceLink":[], "PlaceRelated":[], "PlaceDescription":[]}
#
#       # TODO: what if simultaneous inserts?
#       countrows=0
#       countlinked = 0
#       total_links = 0
#       for r in reader:
#         # build attributes for new Place instance
#         src_id = r[header.index('id')]
#         title = r[header.index('title')].strip() # don't try to correct incoming except strip()
#         # title = r[header.index('title')].replace("' ","'") # why?
#         # strip anything in parens for title only
#         # title = re.sub('\(.*?\)', '', title)
#         title_source = r[header.index('title_source')]
#         title_uri = r[header.index('title_uri')] if 'title_uri' in header else ''
#         ccodes = r[header.index('ccodes')] if 'ccodes' in header else []
#         variants = [x.strip() for x in r[header.index('variants')].split(';')] \
#           if 'variants' in header and r[header.index('variants')] !='' else []
#         types = [x.strip() for x in r[header.index('types')].split(';')] \
#           if 'types' in header else []
#         aat_types = [x.strip() for x in r[header.index('aat_types')].split(';')] \
#           if 'aat_types' in header else []
#         parent_name = r[header.index('parent_name')] if 'parent_name' in header else ''
#         parent_id = r[header.index('parent_id')] if 'parent_id' in header else ''
#         coords = makeCoords(r[header.index('lon')],r[header.index('lat')]) \
#           if 'lon' in header and 'lat' in header else None
#         geowkt = r[header.index('geowkt')] if 'geowkt' in header else None
#         # replaced None with '' 25 May 2023
#         geosource = r[header.index('geo_source')] if 'geo_source' in header else ''
#         geoid = r[header.index('geo_id')] if 'geo_id' in header else None
#         geojson = None # zero it out
#         # print('geosource:', geosource)
#         # print('geoid:', geoid)
#         # make Point geometry from lon/lat if there
#         if coords and len(coords) == 2:
#           geojson = {"type": "Point", "coordinates": coords,
#                       "geowkt": 'POINT('+str(coords[0])+' '+str(coords[1])+')'}
#         # else make geometry (any) w/Shapely if geowkt
#         if geowkt and geowkt not in ['']:
#           geojson = parse_wkt(geowkt)
#         if geojson and (geosource or geoid):
#           geojson['citation']={'label':geosource,'id':geoid}
#
#         # ccodes; compute if missing and there is geometry
#         if len(ccodes) == 0:
#           if geojson:
#             try:
#               ccodes = ccodesFromGeom(geojson)
#             except:
#               pass
#           else:
#             ccodes = []
#         else:
#           ccodes = [x.strip().upper() for x in r[header.index('ccodes')].split(';')]
#
#         # TODO: assign aliases if wd, tgn, pl, bnf, gn, viaf
#         matches = [aliasIt(x.strip()) for x in r[header.index('matches')].split(';')] \
#           if 'matches' in header and r[header.index('matches')] != '' else []
#
#         # TODO: patched Apr 2023; needs refactor
#         # there _should_ always be a start or attestation_year
#         # not forced by validation yet
#         # start = r[header.index('start')] if 'start' in header else None
#         start = r[header.index('start')] if 'start' in header else None
#         # source_year = r[header.index('attestation_year')] if 'attestation_year' in header else None
#         has_end = 'end' in header and r[header.index('end')] !=''
#         has_source_yr = 'attestation_year' in header and r[header.index('attestation_year')] !=''
#         end = r[header.index('end')] if has_end else None
#         source_year = r[header.index('attestation_year')] if has_source_yr else None
#         # end = r[header.index('end')] if has_end else start
#         # print('row r' , r)
#         # print('start:'+start,'; end:'+end, ';year'+source_year)
#         dates = (start,end,source_year)
#         print('dates tuple', dates)
#         # must be start and/or source_year
#         datesobj = parsedates_tsv(dates)
#         # returns, e.g. {'timespans': [{'start': {'earliest': 1015}, 'end': None}],
#         #  'minmax': [1015, None],
#         #  'source_year': 1962}
#         description = r[header.index('description')] \
#           if 'description' in header else ''
#
#         print('title, src_id (pre-newpl):', title, src_id)
#         print('datesobj', datesobj)
#         # create new Place object
#         newpl = Place(
#           src_id = src_id,
#           dataset = ds,
#           title = title,
#           ccodes = ccodes,
#           minmax = datesobj['minmax'],
#           timespans = [datesobj['minmax']], # list of lists
#           attestation_year = datesobj['source_year'] # integer or None
#         )
#         newpl.save()
#         countrows += 1
#
#         # build associated objects and add to arrays #
#         #
#         # PlaceName(); title, then variants
#         #
#         objs['PlaceName'].append(
#           PlaceName(
#             place=newpl,
#             src_id = src_id,
#             toponym = title,
#             jsonb={"toponym": title, "citations": [{"id":title_uri,"label":title_source}]}
#         ))
#         # variants if any; assume same source as title toponym
#         if len(variants) > 0:
#           for v in variants:
#             try:
#               haslang = re.search("@(.*)$", v.strip())
#               if len(v.strip()) > 200:
#                 # print(v.strip())
#                 pass
#               else:
#                 # print('variant for', newpl.id, v)
#                 new_name = PlaceName(
#                   place=newpl,
#                   src_id = src_id,
#                   toponym = v.strip(),
#                   jsonb={"toponym": v.strip(), "citations": [{"id":"","label":title_source}]}
#                 )
#                 if haslang:
#                   new_name.jsonb['lang'] = haslang.group(1)
#
#                 objs['PlaceName'].append(new_name)
#             except:
#               print('error on variant', sys.exc_info())
#               print('error on variant for newpl.id', newpl.id, v)
#
#         #
#         # PlaceType()
#         #
#         if len(types) > 0:
#           fclass_list=[]
#           for i,t in enumerate(types):
#             aatnum='aat:'+aat_types[i] if len(aat_types) >= len(types) and aat_types[i] !='' else None
#             print('aatnum in insert_tsv() PlaceTypes', aatnum)
#             if aatnum:
#               try:
#                 fclass_list.append(get_object_or_404(Type, aat_id=int(aatnum[4:])).fclass)
#               except:
#                 # report type lookup issue
#                 insert_errors.append(
#                   {'src_id':src_id,
#                   'title':newpl.title,
#                   'field':'aat_type',
#                   'msg':aatnum + ' not in WHG-supported list;'}
#                 )
#                 raise
#                 # messages.add_message(request, messages.INFO, 'choked on an invalid AAT place type id')
#                 # return redirect('/mydata')
#                 # continue
#             objs['PlaceType'].append(
#               PlaceType(
#                 place=newpl,
#                 src_id = src_id,
#                 jsonb={ "identifier":aatnum if aatnum else '',
#                         "sourceLabel":t,
#                         "label":aat_lookup(int(aatnum[4:])) if aatnum else ''
#                       }
#             ))
#           newpl.fclasses = fclass_list
#           newpl.save()
#
#         #
#         # PlaceGeom()
#         #
#         print('geojson', geojson)
#         if geojson:
#           objs['PlaceGeom'].append(
#             PlaceGeom(
#               place=newpl,
#               src_id = src_id,
#               jsonb=geojson
#               ,geom=GEOSGeometry(json.dumps(geojson))
#           ))
#
#         #
#         # PlaceWhen()
#         # via parsedates_tsv(): {"timespans":[{start{}, end{}}]}
#         # if not start in ('',None):
#         # if start != '':
#         objs['PlaceWhen'].append(
#           PlaceWhen(
#             place=newpl,
#             src_id = src_id,
#             #jsonb=datesobj['timespans']
#             jsonb=datesobj
#         ))
#
#         #
#         # PlaceLink() - all are closeMatch
#         #
#         if len(matches) > 0:
#           countlinked += 1
#           for m in matches:
#             total_links += 1
#             objs['PlaceLink'].append(
#               PlaceLink(
#                 place=newpl,
#                 src_id = src_id,
#                 jsonb={"type":"closeMatch", "identifier":m}
#             ))
#
#         #
#         # PlaceRelated()
#         #
#         if parent_name != '':
#           objs['PlaceRelated'].append(
#             PlaceRelated(
#               place=newpl,
#               src_id=src_id,
#               jsonb={
#                 "relationType": "gvp:broaderPartitive",
#                 "relationTo": parent_id,
#                 "label": parent_name}
#           ))
#
#         #
#         # PlaceDescription()
#         # @id, value, lang
#         if description != '':
#           objs['PlaceDescription'].append(
#             PlaceDescription(
#               place=newpl,
#               src_id = src_id,
#               jsonb={
#                 #"@id": "", "value":description, "lang":""
#                 "value":description
#               }
#             ))
#
#
#       # bulk_create(Class, batch_size=n) for each
#       PlaceName.objects.bulk_create(objs['PlaceName'],batch_size=10000)
#       PlaceType.objects.bulk_create(objs['PlaceType'],batch_size=10000)
#       try:
#         PlaceGeom.objects.bulk_create(objs['PlaceGeom'],batch_size=10000)
#       except:
#         print('geom insert failed', newpl, sys.exc_info())
#         pass
#       PlaceLink.objects.bulk_create(objs['PlaceLink'],batch_size=10000)
#       PlaceRelated.objects.bulk_create(objs['PlaceRelated'],batch_size=10000)
#       PlaceWhen.objects.bulk_create(objs['PlaceWhen'],batch_size=10000)
#       PlaceDescription.objects.bulk_create(objs['PlaceDescription'],batch_size=10000)
#
#       infile.close()
#       print('insert_errors', insert_errors)
#       # print('rows,linked,links:', countrows, countlinked, total_links)
#     except:
#       print('tsv insert failed', newpl, sys.exc_info())
#       # drop the (empty) dataset if insert wasn't complete
#       ds.delete()
#       # email to user, admin
#       failed_insert_notification(user, dsf.file.name, ds.label)
#
#       # return message to 500.html
#       messages.error(request, "Database insert failed, but we don't know why. The WHG team has been notified and will follow up by email to <b>"+user.username+'</b> ('+user.email+')')
#       return HttpResponseServerError()
#   else:
#     print('insert_tsv skipped, already in')
#     messages.add_message(request, messages.INFO, 'data is uploaded, but problem displaying dataset page')
#     return redirect('/mydata')
#
#   return({"numrows":countrows,
#           "numlinked":countlinked,
#           "total_links":total_links})
#
